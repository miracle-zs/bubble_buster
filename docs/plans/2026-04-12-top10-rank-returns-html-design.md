# Top10 Rank Returns HTML Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a standalone HTML report that visualizes `reports/top10_short_rank_returns.csv` with overview metrics and linked detail analysis.

**Architecture:** Add a Python generator script under `scripts/` that reads the exported CSV, computes aggregated statistics for accounts/ranks/dates, and writes a single self-contained HTML file under `reports/`. Add a focused test that verifies the generator emits the expected report structure and embeds the computed data.

**Tech Stack:** Python 3, standard library (`csv`, `json`, `statistics`, `pathlib`), self-contained HTML/CSS/JS

---

### Task 1: Add a failing generator test

**Files:**
- Create: `tests/test_top10_rank_returns_report.py`

**Step 1: Write the failing test**

Create a temporary CSV fixture, invoke the new report generator entry point, and assert that the output HTML contains:
- the report title
- the overview section
- the detail table mount node
- embedded serialized data for at least one known account and symbol

**Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests.test_top10_rank_returns_report -v`
Expected: FAIL because the generator module does not exist yet.

### Task 2: Implement the standalone HTML generator

**Files:**
- Create: `scripts/generate_top10_rank_returns_html.py`

**Step 1: Write minimal implementation**

Implement:
- CSV loading and normalization
- aggregate metrics for overall, account-by-rank heatmap, account daily trend, return-basis breakdown, and detail rows
- self-contained HTML template with overview charts and filterable detail table
- CLI defaults:
  - input: `reports/top10_short_rank_returns.csv`
  - output: `reports/top10_short_rank_returns_report.html`

**Step 2: Run test to verify it passes**

Run: `python3 -m unittest tests.test_top10_rank_returns_report -v`
Expected: PASS

### Task 3: Generate and verify the real report

**Files:**
- Output: `reports/top10_short_rank_returns_report.html`

**Step 1: Generate report**

Run: `python3 scripts/generate_top10_rank_returns_html.py`

**Step 2: Verify syntax**

Run: `python3 -m py_compile scripts/generate_top10_rank_returns_html.py`
Expected: PASS

**Step 3: Smoke-check the output**

Confirm the generated HTML contains the intended sections and non-empty embedded data.
