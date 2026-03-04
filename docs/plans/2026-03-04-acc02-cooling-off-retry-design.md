# Acc02 Cooling-Off Retry Design

**Goal:** Let `acc02` wait `30s` and retry once when Binance returns cooling-off error `-4192` on orders that increase short exposure, while keeping all other accounts unchanged by default.

## Scope
- Add account-level runtime configuration for cooling-off retry behavior.
- Apply the retry to entry orders, failed-notional redistribution orders, and post-entry rebalance `SELL` orders.
- Keep the retry disabled by default for all accounts.
- Do not apply the retry to TP/SL exit orders or reduce-only rebalance `BUY` orders.

## Recommended Approach
1. Add `cooling_off_retry_count` and `cooling_off_retry_delay_sec` to account runtime config, both defaulting to `0`.
2. Pass those values into each `Top10ShortStrategy` instance from `create_components()`.
3. Add a strategy helper that wraps order placement for short-exposure-increasing orders:
   - detect Binance `-4192`,
   - log the retry decision,
   - `sleep(delay_sec)`,
   - retry the same order once,
   - re-raise if retries are exhausted.
4. Reuse that helper in:
   - `run_entry()` initial short entry,
   - `_redistribute_failed_notional()`,
   - `_rebalance_to_target()` for `SELL` plans only.

## Why This Approach
- The problem is account-specific behavior, not global scheduling, so the control belongs in per-account runtime config.
- The three affected paths all increase short exposure but are currently implemented at different call sites; a shared strategy helper keeps the rule consistent.
- Sleeping inside the failing order path naturally delays later symbols and later post-processing, which matches the requirement that subsequent orders are postponed.

## Error Handling
- Only Binance `-4192` should trigger the wait-and-retry path.
- Other errors such as `-2019` insufficient margin must continue through existing handling without new delay.
- If `cooling_off_retry_count <= 0` or `cooling_off_retry_delay_sec <= 0`, behavior remains unchanged.
- If the retry still fails with `-4192`, the existing exception handling should run exactly as it does today.

## Data Flow
- Runtime config:
  - `account.acc02.runtime.cooling_off_retry_count = 1`
  - `account.acc02.runtime.cooling_off_retry_delay_sec = 30`
- Component assembly reads these values and stores them in `account_runtimes`.
- `Top10ShortStrategy` stores them on the instance.
- Order-producing methods call the shared retry helper before surfacing any final failure.

## Tests
- Verify runtime config exposes the new fields and passes them into the strategy instance.
- Verify entry order retries once after a cooling-off error and sleeps before retry.
- Verify redistribution order retries once after a cooling-off error and sleeps before retry.
- Verify post-entry rebalance `SELL` retries once after a cooling-off error and sleeps before retry.
- Verify post-entry rebalance `BUY` does not use the cooling-off retry path.
- Verify default strategies with no retry config keep current behavior.
