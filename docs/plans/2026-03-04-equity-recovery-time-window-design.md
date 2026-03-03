# Equity Recovery Time Window Design

**Goal:** Prevent组合止盈 from triggering between `07:30` and `12:00` inclusive, using the configured `runtime.timezone` local time.

**Context**

- `Top10ShortStrategy.run_equity_recovery_take_profit()` currently evaluates wallet snapshots and trigger thresholds without any local-time guard.
- The scheduler runtime already interprets schedules in `runtime.timezone`, but the strategy instance does not currently receive that timezone setting.
- The requested behavior is a hard block for组合止盈 during the morning session, including the exact boundaries `07:30` and `12:00`.

**Recommended Approach**

- Add a `runtime_timezone` constructor argument to `Top10ShortStrategy`.
- Parse that timezone once during strategy initialization with `ZoneInfo`, matching the runtime service fallback behavior by falling back to `UTC` on invalid values.
- In `run_equity_recovery_take_profit()`, convert the latest wallet snapshot timestamp from UTC into the configured local timezone before any trigger evaluation.
- Return `{"status": "SKIPPED", "reason": "TIME_WINDOW_BLOCKED"}` when the local timestamp falls within `07:30 <= t <= 12:00`.
- Pass `runtime.timezone` through `create_components()` so live strategy instances use the same timezone as the scheduler.

**Why This Approach**

- The guard sits at the true trigger point, so future callers cannot bypass it by calling the strategy directly.
- It reuses the existing timezone configuration rather than introducing a second scheduling setting.
- The change stays narrow: no lock semantics, order logic, or threshold math need to change.

**Behavior Details**

- The block window is inclusive on both ends: `07:30:00` is blocked and `12:00:00` is blocked.
- When blocked, the method exits early before touching wallet-window state, lock state, or order placement.
- Outside the blocked window, existing组合止盈 behavior remains unchanged.

**Testing**

- Add a failing regression test showing that a snapshot whose local time is inside the blocked window returns `TIME_WINDOW_BLOCKED` and does not place orders or write lock state.
- Add a passing-path regression test showing that a snapshot outside the blocked window still reaches the normal trigger path.
- Add a configuration propagation test in runtime component assembly to verify `runtime.timezone` is passed into the strategy instance.
