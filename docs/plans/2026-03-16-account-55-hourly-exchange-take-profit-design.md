# Account 55 Hourly Exchange Take-Profit Design

**Goal:** Add an account-specific hourly take-profit rule for account `55`: once a live exchange short position has reached at least `20%` favorable price move at any point since its true opening time, close it at local minute `59` when the current `1h` candle is bullish.

## Context

- Account `55` is configured as `loss_cut_only`, so it does not run the normal `entry` or `manage` loops.
- Account `55` does run exchange-scoped daily loss cut and noon protection, which means its risk logic must work from live exchange positions rather than tracked strategy positions.
- The requested rule is account-specific and should not affect `full` mode accounts unless they explicitly opt in later.
- The agreed trigger semantics are:
  - profit eligibility uses price move, not ROI;
  - a position becomes eligible once it has ever reached at least `20%` favorable move since the true opening time of the current live short position;
  - actual exit checks run at local minute `59`;
  - the candle test uses the current in-progress `1h` candle at the time of the `59`-minute check.

## Recommended Approach

1. Add a new exchange-position hourly take-profit task in the runtime service instead of extending `manage` or `daily loss-cut`.
2. Add account-level runtime configuration, disabled by default:
   - `hourly_exchange_take_profit_enabled`
   - `hourly_exchange_take_profit_minute`
   - `hourly_exchange_take_profit_drop_pct`
3. Implement the rule in `PositionManager` as an exchange-position scan, similar in data source to `daily_loss_cut_scope = exchange`, but with its own method, state, and notification.
4. Persist per-account monitoring state in `locks` so the task can:
   - detect newly observed live short positions,
   - reconstruct each position's true opening time from Binance `userTrades`,
   - remember the lowest price seen since that opening time,
   - remember whether the position has ever crossed the favorable-move threshold,
   - avoid duplicate exits in the same local hour.
5. Run state maintenance on the service cadence (`manager_interval_sec`, default `60s`) and run the actual take-profit action only when local time matches the configured minute.

## Why This Approach

- Account `55` does not participate in tracked-position `manage`, so the feature must not depend on strategy position rows.
- A dedicated hourly task keeps scheduling semantics clear; this is neither a once-per-day stop-loss job nor a tracked-position maintenance pass.
- Reconstructing the true opening time once and then maintaining incremental state gives the requested accuracy without forcing a full historical rebuild every hour.
- Account-level config keeps the behavior isolated to `55` and avoids silently changing other accounts' exit behavior.

## Runtime Behavior

### State maintenance cadence

- On every service cycle for eligible accounts, scan live exchange positions.
- Only non-zero short positions are candidates.
- For each candidate:
  - if no active monitor state exists, initialize it;
  - if quantity or side structure indicates the prior monitored position ended and a new one started, rebuild state;
  - otherwise refresh the stored minimum price and eligibility flag.

### First observation of a live short position

- The first observation happens on the next service cycle after the exchange position appears.
- Initialization must:
  - read current `positionRisk` for symbol and quantity;
  - call Binance `userTrades` for that symbol;
  - reconstruct the true opening time of the currently open net short position by walking fills backward until the open quantity is explained;
  - use that opening time as the monitoring anchor;
  - fetch price data from that anchor forward to initialize the position's minimum price since opening;
  - mark the position as threshold-eligible immediately if the historical minimum already satisfies the configured drop threshold.

### Eligibility rule

- For a short position, favorable move is based on entry-to-low price drop:
  - `favorable_drop_pct = (entry_price - lowest_price_since_open) / entry_price`
- A position becomes eligible once `favorable_drop_pct >= configured_drop_pct`.
- Once eligible, it stays eligible for that live position until the position is fully closed or rebuilt as a new position.

### Exit trigger rule

- At local minute `59`, for each eligible live short position:
  - read the current in-progress hourly candle;
  - compare current-hour open against the latest price at check time;
  - if `latest_price > hour_open`, treat the current `1h` candle as bullish;
  - submit a market `BUY` close for the current short quantity.
- The task should record the local hour key it acted on so retries within the same hour do not duplicate the exit.

## Position Identity and Reset Rules

- Monitoring state is keyed by account and symbol.
- A monitored position must be reset and rebuilt when:
  - the live short position disappears;
  - the symbol reappears after being flat;
  - the side flips away from short;
  - the trade reconstruction no longer matches the stored opening anchor.
- Simple mark-price movement should not rebuild state.
- Partial reductions should preserve the existing opening anchor for the remaining live position unless trade reconstruction shows a true new short position.

## Data and Persistence

- Store this feature's state under a dedicated `locks` key, separate from noon protection and equity recovery state.
- Suggested per-symbol state:
  - `symbol`
  - `position_amt`
  - `entry_price`
  - `opened_at_utc`
  - `lowest_price_since_open`
  - `eligible_reached`
  - `eligible_reached_at_utc`
  - `last_checked_hour_key`
  - `last_seen_at_utc`
- Remove stale state for symbols no longer holding live short positions.

## Error Handling

- If trade reconstruction fails for one symbol, skip that symbol for the cycle, log it, and keep other symbols running.
- If price history fetch fails during initialization, do not assume eligibility; retry on the next cycle.
- If the exit order fails, record the failure in notification/logging and do not mark the hour as completed unless the order result is accepted.
- The task must not place orders for long positions or flat symbols.

## Testing

- Add runtime tests showing the new hourly task runs for opt-in accounts, including `loss_cut_only`, and does not run for disabled accounts.
- Add `PositionManager` tests for:
  - initializing monitoring state from first-seen exchange short positions;
  - reconstructing true opening time from `userTrades`;
  - marking a position eligible when historical low has already crossed the threshold;
  - preserving eligibility after profit retraces above the threshold;
  - triggering exit only at configured local minute and only once per hour;
  - not triggering when the current `1h` candle is not bullish;
  - clearing or rebuilding state when the live short position disappears or restarts.
