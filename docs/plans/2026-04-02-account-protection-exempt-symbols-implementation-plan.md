# Account Protection Exempt Symbols Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add an account-scoped `protection_exempt_symbols` whitelist so exempt symbols skip all automated TP/SL protection logic, including initial exit-order placement.

**Architecture:** Parse a normalized exempt-symbol set from each account runtime config in `runtime_components`, then inject that set into `Top10ShortStrategy` and `PositionManager`. Centralize symbol checks in those classes so all automated exit and protection paths consistently skip exempt symbols while preserving visibility and external-close reconciliation.

**Tech Stack:** Python, configparser, Binance client wrappers, SQLite state store, pytest, unittest

---

### Task 1: Parse account-scoped exempt symbols from runtime config

**Files:**
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/core/runtime_components.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_runtime_components.py`

**Step 1: Write the failing test**

Add a test next to the existing per-account runtime override coverage that builds a config with:

```ini
[runtime]
default_account_id = acc01

[accounts]
enabled = acc01,55
mode.acc01 = full
mode.55 = loss_cut_only

[account.55.runtime]
protection_exempt_symbols = xauusdt, btcusdt , ,ethusdt
```

Assert:

```python
account_runtimes["55"]["protection_exempt_symbols"] == {"XAUUSDT", "BTCUSDT", "ETHUSDT"}
account_runtimes["acc01"]["protection_exempt_symbols"] == set()
```

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/test_runtime_components.py -k protection_exempt_symbols -v
```

Expected: FAIL because the field is not parsed or exposed yet.

**Step 3: Write minimal implementation**

In `/Users/zhangshuai/PycharmProjects/bubble_buster/core/runtime_components.py`:

- Add a helper to parse comma-separated symbols into a normalized `set[str]`
- Read `protection_exempt_symbols` from the selected runtime config
- Store it in each account runtime context
- Pass it into strategy and manager constructors

Example helper shape:

```python
def _parse_symbol_set(raw: str) -> Set[str]:
    return {
        part.strip().upper()
        for part in str(raw or "").split(",")
        if part.strip()
    }
```

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/test_runtime_components.py -k protection_exempt_symbols -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_runtime_components.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/runtime_components.py
git commit -m "feat: parse account protection exempt symbols"
```

### Task 2: Skip initial TP/SL placement for exempt symbols

**Files:**
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/core/strategy_top10_short.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_order_retry.py`

**Step 1: Write the failing test**

Add a strategy test that constructs `Top10ShortStrategy` with:

```python
protection_exempt_symbols={"XAUUSDT"}
```

Call `_place_exit_orders(position_id=1, symbol="XAUUSDT")` using a client/store double that would normally record exit orders.

Assert:

```python
store.update_position_orders.assert_not_called()
client.create_order.assert_not_called()
```

If the store double is not mock-based in this file, create a lightweight stub that records calls and assert no exit order was added.

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/test_strategy_order_retry.py -k exempt -v
```

Expected: FAIL because `_place_exit_orders()` still places TP/SL orders.

**Step 3: Write minimal implementation**

In `/Users/zhangshuai/PycharmProjects/bubble_buster/core/strategy_top10_short.py`:

- Extend `__init__` to accept `protection_exempt_symbols: Optional[Set[str]] = None`
- Normalize/store it on `self.protection_exempt_symbols`
- Add helper:

```python
def _is_protection_exempt(self, symbol: str) -> bool:
    return str(symbol or "").strip().upper() in self.protection_exempt_symbols
```

- Return early in `_place_exit_orders()` when symbol is exempt

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/test_strategy_order_retry.py -k exempt -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_order_retry.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/strategy_top10_short.py
git commit -m "feat: skip initial exits for exempt symbols"
```

### Task 3: Exclude exempt symbols from equity recovery take-profit

**Files:**
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/core/strategy_top10_short.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_equity_recovery.py`

**Step 1: Write the failing test**

Add a test with two open positions:

- `XAUUSDT` in `protection_exempt_symbols`
- `BTCUSDT` not exempt

Set up wallet snapshots so equity recovery triggers.

Assert:

```python
result["adjusted_positions"] == 1
only BTCUSDT receives a reduceOnly BUY order
```

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/test_strategy_equity_recovery.py -k exempt -v
```

Expected: FAIL because both symbols are currently eligible for reduction.

**Step 3: Write minimal implementation**

In `run_equity_recovery_take_profit()`:

- Before loading risk or creating reduction order, skip exempt symbols
- Append an informative detail row such as:

```python
{"symbol": symbol, "position_id": position_id, "status": "SKIPPED_EXEMPT"}
```

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/test_strategy_equity_recovery.py -k exempt -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_equity_recovery.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/strategy_top10_short.py
git commit -m "feat: exclude exempt symbols from equity recovery"
```

### Task 4: Skip manager-driven tracked-position automation for exempt symbols

**Files:**
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_position_manager.py`

**Step 1: Write the failing tests**

Add tests covering both cases:

1. Exempt tracked symbol with active exchange short:

```python
summary = manager.run_once()
assert summary["updated_sl"] == 0
assert summary["closed_timeout"] == 0
assert summary["closed_tp"] == 0
assert summary["closed_sl"] == 0
```

2. Exempt tracked symbol with no exchange position:

```python
summary = manager.run_once()
assert summary["closed_external"] == 1
```

The first test proves timeout and dynamic stop are skipped. The second preserves external-close reconciliation.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_position_manager.py -k "exempt and run_once" -v
```

Expected: FAIL because exempt symbols are still managed normally.

**Step 3: Write minimal implementation**

In `/Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py`:

- Extend `__init__` to accept `protection_exempt_symbols: Optional[Set[str]] = None`
- Normalize/store symbol set
- Add helper:

```python
def _is_protection_exempt(self, symbol: str) -> bool:
    return str(symbol or "").strip().upper() in self.protection_exempt_symbols
```

- In `_manage_position()`:
  - fetch risk first
  - if no risk, keep existing error behavior
  - if position gone, keep existing external-close behavior
  - if symbol is exempt, return `None` before TP/SL status checks, timeout, and dynamic stop update

This ordering matters: external close sync must survive exemption.

**Step 4: Run tests to verify they pass**

Run:

```bash
pytest tests/test_position_manager.py -k "exempt and run_once" -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_position_manager.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py
git commit -m "feat: skip tracked protection automation for exempt symbols"
```

### Task 5: Skip exchange-wide protection tasks for exempt symbols

**Files:**
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_position_manager.py`

**Step 1: Write the failing tests**

Add tests for exempt symbol behavior in:

- `run_daily_loss_cut()`
- `run_morning_protection_stop()`
- `run_noon_protection_stop()`
- `run_hourly_exchange_take_profit()`

Example assertions:

```python
summary["closed_loss_cut"] == 0
summary["updated_sl"] == 0
summary["closed_take_profit"] == 0
client.create_order.assert_not_called()
```

For hourly exchange TP, initialize monitor state so the symbol would otherwise qualify, then assert it is skipped because of exemption.

**Step 2: Run tests to verify they fail**

Run:

```bash
pytest tests/test_position_manager.py -k "exempt and (loss_cut or protection or hourly)" -v
```

Expected: FAIL because exchange-scanning tasks still act on exempt symbols.

**Step 3: Write minimal implementation**

In the relevant loops of `/Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py`, add early symbol filters:

```python
if self._is_protection_exempt(symbol):
    continue
```

Apply this to:

- `run_hourly_exchange_take_profit()`
- `_run_daily_loss_cut_tracked_positions()`
- `_run_daily_loss_cut_exchange_positions()`
- `run_noon_protection_stop()`
- `run_morning_protection_stop()`

Also skip exempt symbols during hourly monitor refresh/init so stale monitor state does not drive later actions.

**Step 4: Run tests to verify they pass**

Run:

```bash
pytest tests/test_position_manager.py -k "exempt and (loss_cut or protection or hourly)" -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_position_manager.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py
git commit -m "feat: skip exchange protections for exempt symbols"
```

### Task 6: Wire account runtime exemptions into constructed components

**Files:**
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/core/runtime_components.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_runtime_components.py`

**Step 1: Write the failing test**

Extend runtime component coverage to assert the constructed strategy and manager for account `55` receive the parsed set.

Example:

```python
assert strategy.protection_exempt_symbols == set()
assert account_runtimes["55"]["strategy"].protection_exempt_symbols == {"XAUUSDT"}
assert account_runtimes["55"]["manager"].protection_exempt_symbols == {"XAUUSDT"}
```

Adjust exact access pattern to match how the test file already inspects `account_runtimes`.

**Step 2: Run test to verify it fails**

Run:

```bash
pytest tests/test_runtime_components.py -k "protection_exempt_symbols and passes" -v
```

Expected: FAIL because constructor wiring is incomplete.

**Step 3: Write minimal implementation**

Ensure the parsed set is passed through every `Top10ShortStrategy(...)` and `PositionManager(...)` construction path in `create_components()`.

**Step 4: Run test to verify it passes**

Run:

```bash
pytest tests/test_runtime_components.py -k "protection_exempt_symbols and passes" -v
```

Expected: PASS

**Step 5: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_runtime_components.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/runtime_components.py
git commit -m "feat: wire exempt symbols into runtime components"
```

### Task 7: Run focused regression tests

**Files:**
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_runtime_components.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_position_manager.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_equity_recovery.py`
- Test: `/Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_order_retry.py`

**Step 1: Run focused suite**

Run:

```bash
pytest \
  tests/test_runtime_components.py \
  tests/test_position_manager.py \
  tests/test_strategy_equity_recovery.py \
  tests/test_strategy_order_retry.py -v
```

Expected: PASS

**Step 2: Fix any regressions minimally**

If failures appear:

- patch only the affected branch
- rerun the failing test first
- rerun the focused suite

**Step 3: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/core/runtime_components.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/position_manager.py /Users/zhangshuai/PycharmProjects/bubble_buster/core/strategy_top10_short.py /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_runtime_components.py /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_position_manager.py /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_equity_recovery.py /Users/zhangshuai/PycharmProjects/bubble_buster/tests/test_strategy_order_retry.py
git commit -m "test: verify exempt-symbol protection coverage"
```

### Task 8: Document config usage

**Files:**
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/README.md`
- Modify: `/Users/zhangshuai/PycharmProjects/bubble_buster/config.production.multi.ini.example`

**Step 1: Write the failing doc expectation**

No automated doc test exists. Define the expected documentation updates:

- README explains `protection_exempt_symbols`
- Example config shows how account `55` can exempt `XAUUSDT`
- Warning states exempt symbols skip all automated TP/SL protections, including initial exit orders

**Step 2: Implement minimal docs**

Update README runtime/account config section with:

```ini
[account.55.runtime]
protection_exempt_symbols = XAUUSDT
```

And explain:

- account-scoped only
- exact symbol match
- exempt symbols remain visible but unmanaged

Update example config with a commented or empty `protection_exempt_symbols` entry.

**Step 3: Sanity-check docs**

Run:

```bash
rg -n "protection_exempt_symbols" README.md config.production.multi.ini.example
```

Expected: both files mention the new config

**Step 4: Commit**

```bash
git add /Users/zhangshuai/PycharmProjects/bubble_buster/README.md /Users/zhangshuai/PycharmProjects/bubble_buster/config.production.multi.ini.example
git commit -m "docs: describe exempt symbol protection config"
```
