# Equity Recovery Time Window Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Prevent组合止盈 from triggering between `07:30` and `12:00` inclusive in the configured `runtime.timezone`.

**Architecture:** The block should live inside `Top10ShortStrategy.run_equity_recovery_take_profit()` so every caller respects the same time gate. `create_components()` will pass `runtime.timezone` into the strategy, and strategy initialization will normalize it into a reusable timezone object with the same UTC fallback behavior used by the runtime service.

**Tech Stack:** Python, `unittest`, `unittest.mock`, `zoneinfo`

---

### Task 1: Add regression coverage for the blocked local-time window

**Files:**
- Modify: `tests/test_strategy_equity_recovery.py`
- Test: `tests/test_strategy_equity_recovery.py`

**Step 1: Write the failing test**

```python
def test_equity_recovery_skips_inside_blocked_local_time_window(self) -> None:
    store.get_latest_wallet_snapshot.return_value = {
        "captured_at_utc": "2026-02-23T00:00:00+00:00",
        "balance_usdt": 990.0,
    }
    strategy = self._build_strategy(client, store, runtime_timezone="Asia/Shanghai")

    result = strategy.run_equity_recovery_take_profit()

    self.assertEqual(result["status"], "SKIPPED")
    self.assertEqual(result["reason"], "TIME_WINDOW_BLOCKED")
    client.create_order.assert_not_called()
    store.set_lock_state.assert_not_called()
```

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_strategy_equity_recovery.StrategyEquityRecoveryTest.test_equity_recovery_skips_inside_blocked_local_time_window -v`
Expected: FAIL because the strategy does not yet inspect local time.

**Step 3: Write minimal implementation**

```python
if self._is_equity_recovery_time_blocked(current_time_utc):
    return {"status": "SKIPPED", "reason": "TIME_WINDOW_BLOCKED"}
```

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_strategy_equity_recovery.StrategyEquityRecoveryTest.test_equity_recovery_skips_inside_blocked_local_time_window -v`
Expected: PASS

### Task 2: Add regression coverage for timezone propagation and allowed hours

**Files:**
- Modify: `tests/test_strategy_equity_recovery.py`
- Modify: `tests/test_runtime_components.py`
- Test: `tests/test_strategy_equity_recovery.py`
- Test: `tests/test_runtime_components.py`

**Step 1: Write the failing tests**

```python
def test_equity_recovery_runs_outside_blocked_local_time_window(self) -> None:
    strategy = self._build_strategy(client, store, runtime_timezone="Asia/Shanghai")
    result = strategy.run_equity_recovery_take_profit()
    self.assertEqual(result["status"], "TRIGGERED")

def test_create_components_passes_runtime_timezone_into_strategy(tmp_path) -> None:
    _, _, _, _, _, account_runtimes = create_components(cfg, base_dir=str(tmp_path))
    assert account_runtimes["good"]["strategy"].runtime_timezone_name == "UTC"
```

**Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests.test_strategy_equity_recovery.StrategyEquityRecoveryTest.test_equity_recovery_runs_outside_blocked_local_time_window tests.test_runtime_components.test_create_components_passes_runtime_timezone_into_strategy -v`
Expected: FAIL because the strategy does not yet store or receive the runtime timezone.

**Step 3: Write minimal implementation**

```python
self.runtime_timezone_name = (runtime_timezone or "").strip() or "UTC"
try:
    self.runtime_timezone = ZoneInfo(self.runtime_timezone_name)
except Exception:
    self.runtime_timezone_name = "UTC"
    self.runtime_timezone = ZoneInfo("UTC")
```

Pass `runtime_cfg.get("timezone", fallback="Asia/Shanghai")` into `Top10ShortStrategy(...)` from `create_components()`.

**Step 4: Run tests to verify they pass**

Run: `python3 -m unittest tests.test_strategy_equity_recovery.StrategyEquityRecoveryTest.test_equity_recovery_runs_outside_blocked_local_time_window tests.test_runtime_components.test_create_components_passes_runtime_timezone_into_strategy -v`
Expected: PASS

### Task 3: Verify the focused modules stay green

**Files:**
- Modify: `core/strategy_top10_short.py`
- Modify: `core/runtime_components.py`
- Modify: `tests/test_strategy_equity_recovery.py`
- Modify: `tests/test_runtime_components.py`
- Modify: `README.md`

**Step 1: Run focused verification**

Run: `python3 -m unittest tests.test_strategy_equity_recovery tests.test_runtime_components -v`
Expected: PASS with the new blocked-window and timezone propagation coverage included.

**Step 2: Update docs**

Add a short README note stating that组合止盈 is blocked from `07:30` to `12:00` inclusive in `runtime.timezone`.

**Step 3: Review for unintended changes**

Check that the diff only adds timezone propagation, the blocked-window guard, tests, and the README note.
