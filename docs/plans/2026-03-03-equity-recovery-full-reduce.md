# Equity Recovery Full Reduce Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Preserve `equity_recovery_reduce_ratio=1.0` so组合止盈 can fully close eligible positions when configured.

**Architecture:** The fix stays inside strategy initialization. `Top10ShortStrategy` should store a full-reduce ratio of `1.0` instead of clamping it to `0.95`, allowing the existing full-reduce execution branch to run unchanged. A regression test guards the config-to-runtime path.

**Tech Stack:** Python, `unittest`, `unittest.mock`

---

### Task 1: Add regression coverage for full reduce config

**Files:**
- Modify: `tests/test_strategy_equity_recovery.py`
- Test: `tests/test_strategy_equity_recovery.py`

**Step 1: Write the failing test**

```python
def test_equity_recovery_preserves_full_reduce_ratio_from_init(self) -> None:
    strategy = Top10ShortStrategy(..., equity_recovery_reduce_ratio=1.0)
    self.assertEqual(strategy.equity_recovery_reduce_ratio, 1.0)
```

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_strategy_equity_recovery.StrategyEquityRecoveryTest.test_equity_recovery_preserves_full_reduce_ratio_from_init -v`
Expected: FAIL because the stored value is `0.95`.

**Step 3: Write minimal implementation**

```python
self.equity_recovery_reduce_ratio = min(1.0, max(0.05, float(equity_recovery_reduce_ratio)))
```

**Step 4: Run test to verify it passes**

Run: `python3 -m unittest tests.test_strategy_equity_recovery.StrategyEquityRecoveryTest.test_equity_recovery_preserves_full_reduce_ratio_from_init -v`
Expected: PASS

### Task 2: Verify the focused module stays green

**Files:**
- Modify: `core/strategy_top10_short.py`
- Test: `tests/test_strategy_equity_recovery.py`

**Step 1: Run focused verification**

Run: `python3 -m unittest tests.test_strategy_equity_recovery -v`
Expected: PASS with the new regression test included.

**Step 2: Review for unintended changes**

Check that the diff only changes the clamp and the new regression coverage.
