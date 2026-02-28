# Acc02 Delayed Entry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add account-specific entry scheduling so `acc02` can run at 07:45 while reusing the 07:40 shared ranking built for other accounts.

**Architecture:** Extend runtime config merging so entry time can be overridden per account, then update the runtime service to decide entry eligibility per account instead of globally. Add a small in-memory ranking cache keyed by local trade day so staggered accounts can reuse the first ranking snapshot within a short TTL.

**Tech Stack:** Python, configparser, runtime scheduler in `core/runtime_service.py`, unittest.

---

### Task 1: Cover per-account entry scheduling in tests

**Files:**
- Modify: `tests/test_runtime_service.py`
- Test: `tests/test_runtime_service.py`

**Step 1: Write the failing test**
Add a test proving that when `acc01` is scheduled for 07:40 and `acc02` for 07:45, the 07:40 cycle runs only `acc01` and skips `acc02`.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_runtime_service.RuntimeServiceTest.test_entry_runs_only_accounts_due_at_current_time`
Expected: FAIL because entry scheduling is global today.

**Step 3: Write minimal implementation**
Update runtime service to compute entry due-ness per account from merged runtime config.

**Step 4: Run test to verify it passes**
Run the same unittest command.
Expected: PASS.

**Step 5: Commit**
```bash
git add tests/test_runtime_service.py core/runtime_service.py core/runtime_components.py
git commit -m "Add per-account entry scheduling"
```

### Task 2: Cover shared ranking reuse for delayed accounts

**Files:**
- Modify: `tests/test_runtime_service.py`
- Test: `tests/test_runtime_service.py`

**Step 1: Write the failing test**
Add a test proving the ranking built at 07:40 is reused at 07:45 for `acc02` instead of rebuilding.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_runtime_service.RuntimeServiceTest.test_entry_reuses_cached_ranking_for_staggered_accounts`
Expected: FAIL because service rebuilds ranking every eligible cycle.

**Step 3: Write minimal implementation**
Add an in-memory cache with local-day key and short TTL around shared ranking construction.

**Step 4: Run test to verify it passes**
Run the same unittest command.
Expected: PASS.

**Step 5: Commit**
```bash
git add tests/test_runtime_service.py core/runtime_service.py
git commit -m "Reuse shared ranking for delayed entry accounts"
```

### Task 3: Verify config merge for account runtime overrides

**Files:**
- Modify: `tests/test_runtime_components.py`
- Test: `tests/test_runtime_components.py`

**Step 1: Write the failing test**
Add a test proving `account.acc02.runtime` can override `entry_hour` and `entry_minute` while other accounts keep the global runtime values.

**Step 2: Run test to verify it fails**
Run: `python3 -m unittest tests.test_runtime_components.RuntimeComponentsTest.test_account_runtime_overrides_entry_schedule`
Expected: FAIL if account runtime settings are not surfaced for scheduling.

**Step 3: Write minimal implementation**
Persist per-account runtime schedule values in account runtime context.

**Step 4: Run test to verify it passes**
Run the same unittest command.
Expected: PASS.

**Step 5: Commit**
```bash
git add tests/test_runtime_components.py core/runtime_components.py
git commit -m "Expose account runtime entry schedule overrides"
```

### Task 4: End-to-end verification

**Files:**
- Modify: `config.production.multi.ini.example`

**Step 1: Update example config**
Document `acc02` runtime override with `entry_hour = 7` and `entry_minute = 45`.

**Step 2: Run focused tests**
Run: `python3 -m unittest tests.test_runtime_service tests.test_runtime_components`
Expected: PASS.

**Step 3: Run regression tests around strategy entry behavior**
Run: `python3 -m unittest tests.test_strategy_equity_recovery`
Expected: PASS.

**Step 4: Review diff**
Run: `git diff -- core/runtime_service.py core/runtime_components.py tests/test_runtime_service.py tests/test_runtime_components.py config.production.multi.ini.example`

**Step 5: Commit**
```bash
git add config.production.multi.ini.example core/runtime_service.py core/runtime_components.py tests/test_runtime_service.py tests/test_runtime_components.py
git commit -m "Delay acc02 entry and reuse shared ranking"
```
