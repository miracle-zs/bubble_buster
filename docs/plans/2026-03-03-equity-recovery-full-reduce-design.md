# Equity Recovery Full Reduce Design

**Goal:** Let `equity_recovery_reduce_ratio = 1.0` remain a true full-close configuration so account-specific "清仓100%" settings close the entire remaining position during组合止盈.

**Context**

- `acc02` and `acc03` are configured for full equity recovery exits in the production example config.
- `Top10ShortStrategy.__init__()` currently clamps `equity_recovery_reduce_ratio` to `0.95`, so any configured `1.0` becomes `0.95`.
- The runtime already has a dedicated full-reduce path in `run_equity_recovery_take_profit()` for `ratio >= 1.0`, which uses the raw position amount string and syncs the remaining tracked position state afterward.

**Recommended Approach**

- Keep the existing lower bound protection and raise the upper clamp from `0.95` to `1.0`.
- Add a regression test that instantiates the strategy with `equity_recovery_reduce_ratio=1.0` and verifies the stored value remains `1.0`.
- Leave the order placement and sync logic unchanged because it already supports the full-reduce behavior.

**Why This Approach**

- It matches the documented and configured operator intent.
- It minimizes risk by changing only initialization validation.
- It closes the exact gap that caused live accounts to retain ~5% tail positions.

**Testing**

- Add a failing unit test in `tests/test_strategy_equity_recovery.py`.
- Run the focused equity recovery test module with `python3 -m unittest tests.test_strategy_equity_recovery -v`.
