# Morning Protection Stop Design

**Goal:** Add a configurable early-morning protection-stop task that runs at local `07:55` by default and tightens the protective exit price for positions held longer than `6` hours to the current hour's highest price, supporting both tracked strategy positions and live exchange positions.

## Context

- The runtime already has a dedicated `12:00` noon protection task that tightens stop-loss orders using an intraday high/low reference window.
- The new request is another time-based defensive rule, but with different trigger time, eligibility criteria, and price window:
  - run in the morning at `07:55`;
  - only apply to positions held longer than `6` hours;
  - use the current hour's price action up to the check time, not the whole day or post-entry range.
- Unlike the existing hourly exchange take-profit rule, this feature is not account-55-specific. Any enabled account should be able to opt in.
- The rule should work for both:
  - tracked strategy positions from the local database;
  - exchange-scoped live positions for accounts that manage real exchange exposure directly.

## Recommended Approach

1. Add a new account-level runtime task named morning protection stop, disabled by default:
   - `morning_protection_enabled`
   - `morning_protection_hour`
   - `morning_protection_minute`
   - `morning_protection_min_hold_hours`
2. Add a dedicated scheduler hook in `StrategyRuntimeService`, parallel to the existing noon-protection hook.
3. Implement the rule in `PositionManager` as a new method rather than overloading noon protection:
   - tracked positions use local `positions.opened_at_utc` to determine hold age;
   - exchange-only positions use current `positionRisk` plus a live-position identity key, similar to noon protection's exchange handling.
4. Reuse the existing stop-order update path and persistent cap semantics:
   - compute a protection cap for each eligible position;
   - merge it with any existing stop so the stop never loosens;
   - persist caps in `locks` so later dynamic-stop updates cannot widen beyond the morning cap.

## Why This Approach

- This rule is conceptually close to noon protection, but the time window and eligibility logic are different enough that a dedicated task is clearer and safer than forcing both behaviors through one method.
- Runtime-level scheduling keeps the behavior available to both `full` and `loss_cut_only` accounts.
- Reusing the existing stop-order update and cap-merging behavior reduces risk: the rule remains a stop tightening, not a new exit order type.
- Account-level config preserves flexibility; accounts can opt in independently without changing existing behavior.

## Runtime Behavior

### Schedule

- Default schedule is local `07:55` in `runtime.timezone`.
- The task runs at most once per local day, matching noon protection's once-per-day semantics.
- Eligible accounts are any accounts in `full` or `loss_cut_only` mode with `morning_protection_enabled = true`.

### Position Eligibility

- A position is eligible only if its holding time is greater than or equal to `morning_protection_min_hold_hours`:
  - tracked positions: compare `positions.opened_at_utc` with the morning check timestamp;
  - exchange-only positions: reconstruct or infer the position start time from live state sufficient to determine age, reusing the same exchange-position identity conventions used elsewhere.
- Only non-zero positions are candidates.
- For short positions, the protection reference is the current hour's highest price.
- For long positions, the symmetric behavior should use the current hour's lowest price so the feature stays directionally correct for exchange-scoped accounts that might hold longs.

### Price Window

- The reference window is the current local clock hour containing the trigger time.
- With the default `07:55`, the task scans the interval from local `07:00:00` up to the exact morning protection run time.
- Price data should come from recent klines:
  - highest high for short exits;
  - lowest low for long exits.

### Stop Update Semantics

- The task does not change TP orders.
- It updates only the protective stop price.
- For shorts:
  - calculate the current-hour highest price;
  - normalize it into a valid stop trigger;
  - merge with any existing stop using `min(existing_stop, morning_cap)` so the stop only tightens.
- For longs:
  - calculate the current-hour lowest price;
  - normalize it into a valid stop trigger;
  - merge using `max(existing_stop, morning_cap)` so the stop only tightens.
- If the new merged stop is effectively unchanged at tick precision, skip the order replacement.

## Data and Persistence

- Persist morning protection caps under a dedicated `locks` key separate from noon protection caps.
- Suggested lock state:
  - `caps`
  - `updated_at_utc`
- Cap keying should match the same tracked/exchange identity model already used by noon protection:
  - tracked positions keyed by local `position_id`;
  - exchange-only positions keyed by stable exchange position identity such as `EX:<symbol>:<position_side>`.

## Interaction With Existing Rules

- This rule should be applied before later dynamic-stop updates can widen risk again, which is why persisted caps must participate in stop merging just like noon protection caps.
- Noon protection remains independent at `12:00`; whichever cap is tighter for a given position should continue to win.
- The rule should not conflict with the hourly exchange take-profit task because one tightens stops and the other may directly close positions.
- If a position has already been closed before `07:55`, it is naturally ignored.

## Error Handling

- If kline data is unavailable for one symbol, skip that symbol, log the failure, and continue processing others.
- If stop replacement fails for one position, record the failure and leave the old stop intact.
- If an exchange-only position has no reconstructable open time for hold-age comparison, skip it rather than guessing.
- Notification output should summarize updated, skipped, and errored symbols similarly to noon protection.

## Testing

- Add runtime service tests showing the morning protection task runs once per day at the configured time for enabled `full` and `loss_cut_only` accounts.
- Add `PositionManager` tests for:
  - tracked short positions older than `6` hours tightening to the current hour's high;
  - tracked positions younger than the threshold being skipped;
  - exchange-only short positions tightening correctly;
  - long-position symmetry using the current hour's low;
  - tighter existing stops not being loosened;
  - persisted morning caps being respected by later stop updates.
