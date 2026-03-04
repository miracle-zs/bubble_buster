# Acc02 Cooling-Off Retry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add account-level cooling-off retry controls so `acc02` can wait `30s` and retry once after Binance `-4192` on short-exposure-increasing orders, with subsequent orders naturally delayed.

**Architecture:** Extend per-account runtime config with cooling-off retry fields, pass them into `Top10ShortStrategy`, and centralize the retry decision in one helper used by entry, redistribution, and rebalance `SELL` paths. This keeps the behavior account-scoped, consistent across affected order types, and disabled by default.

**Tech Stack:** Python, unittest, unittest.mock, ConfigParser

---

### Task 1: Document and expose runtime config

**Files:**
- Modify: `core/runtime_components.py`
- Test: `tests/test_runtime_components.py`
- Docs: `README.md`

**Step 1: Write the failing test**
Add a runtime-components test asserting `cooling_off_retry_count` and `cooling_off_retry_delay_sec` can be configured per account, default to `0`, and are passed into the strategy instance.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_runtime_components.RuntimeComponentsEntryPacingTest.test_create_components_exposes_account_cooling_off_retry_override -v`
Expected: FAIL because the fields are not yet exposed.

**Step 3: Write minimal implementation**
Read the new runtime config fields in `create_components()`, store them in `account_runtimes`, and pass them into `Top10ShortStrategy`.

**Step 4: Run test to verify it passes**
Run: `python3 -m unittest tests.test_runtime_components.RuntimeComponentsEntryPacingTest.test_create_components_exposes_account_cooling_off_retry_override -v`
Expected: PASS

**Step 5: Commit**
```bash
git add core/runtime_components.py tests/test_runtime_components.py README.md docs/plans/2026-03-04-acc02-cooling-off-retry-design.md docs/plans/2026-03-04-acc02-cooling-off-retry.md
git commit -m "Document account cooling-off retry config"
```

### Task 2: Add failing tests for strategy retry helper behavior

**Files:**
- Modify: `tests/test_strategy_rebalance.py`
- Modify: `core/strategy_top10_short.py`

**Step 1: Write the failing tests**
Add focused tests that:
- entry retries once after `-4192` and sleeps before retry,
- redistribution retries once after `-4192` and sleeps before retry,
- rebalance `SELL` retries once after `-4192` and sleeps before retry,
- rebalance `BUY` does not sleep or retry on `-4192`.

**Step 2: Run tests to verify they fail**
Run: `python3 -m unittest tests.test_strategy_rebalance.StrategyRebalanceTest -v`
Expected: FAIL because no cooling-off retry logic exists.

**Step 3: Write minimal implementation**
Add strategy fields for retry count and delay, implement a shared helper for `-4192`, and route the three short-exposure-increasing call sites through it.

**Step 4: Run tests to verify they pass**
Run: `python3 -m unittest tests.test_strategy_rebalance.StrategyRebalanceTest -v`
Expected: PASS

**Step 5: Commit**
```bash
git add core/strategy_top10_short.py tests/test_strategy_rebalance.py
git commit -m "Add cooling-off retry for short exposure orders"
```

### Task 3: Keep default behavior unchanged

**Files:**
- Modify: `tests/test_strategy_rebalance.py`
- Modify: `core/strategy_top10_short.py` (only if needed)

**Step 1: Write the failing test**
Add a test verifying a default strategy with retry config left at `0` does not sleep or perform an extra create-order attempt on `-4192`.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_strategy_rebalance.StrategyRebalanceTest.test_cooling_off_retry_disabled_by_default -v`
Expected: FAIL if the retry becomes globally enabled.

**Step 3: Write minimal implementation**
Guard the retry helper on positive retry count and delay.

**Step 4: Run test to verify it passes**
Run: `python3 -m unittest tests.test_strategy_rebalance.StrategyRebalanceTest.test_cooling_off_retry_disabled_by_default -v`
Expected: PASS

**Step 5: Commit**
```bash
git add core/strategy_top10_short.py tests/test_strategy_rebalance.py
git commit -m "Keep cooling-off retry disabled by default"
```

### Task 4: Final verification

**Files:**
- Verify only

**Step 1: Run focused tests**
Run: `python3 -m unittest tests.test_runtime_components tests.test_strategy_rebalance -v`
Expected: PASS

**Step 2: Run syntax check**
Run: `python3 -m py_compile core/runtime_components.py core/strategy_top10_short.py tests/test_runtime_components.py tests/test_strategy_rebalance.py`
Expected: PASS

**Step 3: Commit**
```bash
git add core/runtime_components.py core/strategy_top10_short.py tests/test_runtime_components.py tests/test_strategy_rebalance.py README.md docs/plans/2026-03-04-acc02-cooling-off-retry-design.md docs/plans/2026-03-04-acc02-cooling-off-retry.md
git commit -m "Add account-level cooling-off retry"
```
