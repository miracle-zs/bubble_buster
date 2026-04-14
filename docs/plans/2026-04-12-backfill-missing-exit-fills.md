# Backfill Missing Exit Fills Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a one-off backfill tool that restores missing closing `BUY` fills for closed positions, using local DB evidence first and Binance history second.

**Architecture:** Create a standalone script under `scripts/` that scans closed positions without `BUY` fills, reconstructs candidate close fills from local `order_events`, and optionally enriches unresolved positions via Binance `get_order` and `get_user_trades`. Keep the matching logic in pure helper functions and verify them with focused `unittest` coverage.

**Tech Stack:** Python 3, SQLite, configparser, existing `BinanceFuturesClient`, standard library `unittest`

---

### Task 1: Add failing tests for recovery logic

**Files:**
- Create: `tests/test_backfill_missing_exit_fills.py`

**Step 1: Write the failing test**

Cover:
- extracting a synthetic fill from a local `BUY FILLED` order event payload
- aggregating Binance trade rows into a close fill payload

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_backfill_missing_exit_fills -v`
Expected: FAIL because the backfill module does not exist.

### Task 2: Implement the backfill tool

**Files:**
- Create: `scripts/backfill_missing_exit_fills.py`

**Step 1: Write minimal implementation**

Implement:
- scan targets: closed positions with no `BUY` fill
- local recovery from `order_events.raw_json`
- optional remote recovery via `--config` and Binance history
- dry-run summary by default, `--apply` to write `order_events` + `fills`

**Step 2: Run test to verify it passes**

Run: `python3 -m unittest tests.test_backfill_missing_exit_fills -v`
Expected: PASS

### Task 3: Smoke-check against the current DB

**Files:**
- Output: console summary only

**Step 1: Syntax verification**

Run: `python3 -m py_compile scripts/backfill_missing_exit_fills.py`
Expected: PASS

**Step 2: Local dry-run**

Run: `python3 scripts/backfill_missing_exit_fills.py --db state.db`
Expected: non-zero candidate count from local recovery.
