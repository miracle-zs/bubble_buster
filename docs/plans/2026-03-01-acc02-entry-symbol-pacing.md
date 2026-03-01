# Acc02 Entry Symbol Pacing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `acc02`-specific symbol pacing so its delayed `07:45` entry waits `30s` after each processed symbol while all other accounts keep current behavior.

**Architecture:** Extend per-account runtime configuration with a symbol pacing field, pass that field into the account-specific strategy instance, and enforce the delay inside the entry candidate loop. This keeps shared ranking and runtime scheduling unchanged while applying the pacing exactly where symbol processing occurs.

**Tech Stack:** Python, unittest, unittest.mock, ConfigParser

---

### Task 1: Document and expose the runtime config

**Files:**
- Modify: `core/runtime_components.py`
- Test: `tests/test_runtime_components.py`

**Step 1: Write the failing test**
Add a test asserting `account.acc02.runtime.entry_symbol_interval_sec = 30` appears in `account_runtimes["acc02"]` and that `acc01` defaults to `0`.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_runtime_components -v`
Expected: FAIL because `entry_symbol_interval_sec` is not exposed.

**Step 3: Write minimal implementation**
Read the merged runtime section in `_build_single_account_components()` and store `entry_symbol_interval_sec` in the returned account runtime dict.

**Step 4: Run test to verify it passes**
Run: `python3 -m unittest tests.test_runtime_components -v`
Expected: PASS

**Step 5: Commit**
```bash
git add tests/test_runtime_components.py core/runtime_components.py docs/plans/2026-03-01-acc02-entry-symbol-pacing*.md
git commit -m "Add account runtime entry symbol pacing config"
```

### Task 2: Add failing test for strategy pacing

**Files:**
- Modify: `tests/test_strategy_rebalance.py`
- Modify: `core/strategy_top10_short.py`

**Step 1: Write the failing test**
Add a test that creates a strategy with `entry_symbol_interval_sec=30`, three candidates, one skipped symbol and two processed symbols, and asserts `time.sleep(30)` is called between processed candidates but not after the final one.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_strategy_rebalance.StrategyRebalanceTest.test_entry_paces_each_processed_symbol_when_configured -v`
Expected: FAIL because no pacing exists.

**Step 3: Write minimal implementation**
Store `entry_symbol_interval_sec` on the strategy and sleep inside `run_entry()` after each processed candidate when another candidate remains.

**Step 4: Run test to verify it passes**
Run: `python3 -m unittest tests.test_strategy_rebalance.StrategyRebalanceTest.test_entry_paces_each_processed_symbol_when_configured -v`
Expected: PASS

**Step 5: Commit**
```bash
git add tests/test_strategy_rebalance.py core/strategy_top10_short.py
git commit -m "Add per-symbol entry pacing for delayed accounts"
```

### Task 3: Verify no regression for default accounts

**Files:**
- Modify: `tests/test_strategy_rebalance.py`
- Modify: `core/strategy_top10_short.py` (only if needed)

**Step 1: Write the failing test**
Add a test asserting a default strategy with no pacing config does not call `time.sleep()` during entry.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_strategy_rebalance -v`
Expected: FAIL if pacing is incorrectly applied to all accounts.

**Step 3: Write minimal implementation**
Keep default pacing at `0` and gate the sleep on `> 0`.

**Step 4: Run test to verify it passes**
Run: `python3 -m unittest tests.test_strategy_rebalance -v`
Expected: PASS

**Step 5: Commit**
```bash
git add tests/test_strategy_rebalance.py core/strategy_top10_short.py
git commit -m "Keep entry pacing disabled by default"
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
git add core/runtime_components.py core/strategy_top10_short.py tests/test_runtime_components.py tests/test_strategy_rebalance.py docs/plans/2026-03-01-acc02-entry-symbol-pacing-design.md docs/plans/2026-03-01-acc02-entry-symbol-pacing.md
git commit -m "Add acc02 entry symbol pacing"
```
