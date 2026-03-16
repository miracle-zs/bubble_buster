# Morning Protection Stop Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a configurable morning protection-stop task that runs at local `07:55` by default and tightens stop prices for positions held at least `6` hours using the current hour's high/low, supporting both tracked strategy positions and live exchange positions.

**Architecture:** Add a new account-level runtime schedule in `StrategyRuntimeService`, parallel to noon protection. Implement a dedicated `PositionManager` method for morning protection that reuses the existing stop replacement flow, persists caps in `locks`, and merges those caps into later stop updates so the tightened stop cannot be widened by dynamic-stop recalculation.

**Tech Stack:** Python, pytest/unittest-style tests, SQLite via `StateStore`, Binance Futures REST client, runtime scheduler in `core/runtime_service.py`

---

### Task 1: Add configuration plumbing for morning protection

**Files:**
- Modify: `core/runtime_components.py`
- Modify: `core/runtime_service.py`
- Modify: `config.ini.example`
- Modify: `config.production.multi.ini.example`
- Test: `tests/test_runtime_components.py`

**Step 1: Write the failing test**

```python
def test_create_components_applies_per_account_morning_protection_override(tmp_path) -> None:
    cfg_text = """
    [binance]
    api_key = k
    api_secret = s

    [strategy]

    [runtime]
    timezone = Asia/Shanghai
    morning_protection_enabled = false
    morning_protection_hour = 7
    morning_protection_minute = 55
    morning_protection_min_hold_hours = 6

    [accounts]
    enabled = acc01,55
    mode.acc01 = full
    mode.55 = loss_cut_only

    [account.55.runtime]
    morning_protection_enabled = true
    morning_protection_min_hold_hours = 8
    """
    _, _, _, _, _, account_runtimes = create_components(load_cfg(cfg_text), base_dir=str(tmp_path))
    assert account_runtimes["acc01"]["morning_protection_enabled"] is False
    assert account_runtimes["55"]["morning_protection_enabled"] is True
    assert account_runtimes["55"]["morning_protection_min_hold_hours"] == 8.0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_runtime_components.py::test_create_components_applies_per_account_morning_protection_override -v`
Expected: FAIL because the new runtime fields are not exposed yet.

**Step 3: Write minimal implementation**

```python
"morning_protection_enabled": runtime_cfg.getboolean("morning_protection_enabled", fallback=False),
"morning_protection_hour": runtime_cfg.getint("morning_protection_hour", fallback=7),
"morning_protection_minute": runtime_cfg.getint("morning_protection_minute", fallback=55),
"morning_protection_min_hold_hours": runtime_cfg.getfloat("morning_protection_min_hold_hours", fallback=6.0),
```

Also extend `ServiceRuntimeConfig` with matching global defaults and document the new keys in both example config files.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_runtime_components.py::test_create_components_applies_per_account_morning_protection_override -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_runtime_components.py core/runtime_components.py core/runtime_service.py config.ini.example config.production.multi.ini.example
git commit -m "feat: add morning protection config plumbing"
```

### Task 2: Add runtime scheduling for the morning protection task

**Files:**
- Modify: `core/runtime_service.py`
- Test: `tests/test_runtime_service.py`

**Step 1: Write the failing tests**

```python
def test_morning_protection_runs_once_for_enabled_full_and_loss_cut_only_accounts() -> None:
    full_manager = ManagerStub()
    loss_cut_manager = ManagerStub()
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=full_manager,
        cfg=build_cfg(morning_protection_enabled=True),
        account_runtimes={
            "acc01": {"mode": "full", "manager": full_manager, "morning_protection_enabled": True},
            "55": {"mode": "loss_cut_only", "manager": loss_cut_manager, "morning_protection_enabled": True},
        },
    )
    service.run_cycle(now_local=datetime(2026, 3, 17, 7, 55, tzinfo=ZoneInfo("UTC")), now_monotonic=1.0)
    assert full_manager.morning_calls == 1
    assert loss_cut_manager.morning_calls == 1


def test_morning_protection_does_not_run_twice_same_day() -> None:
    ...
    assert manager.morning_calls == 1
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_runtime_service.py -k morning_protection -v`
Expected: FAIL because the scheduler hook and manager method do not exist.

**Step 3: Write minimal implementation**

```python
def _run_morning_protection_if_due(self, now_local: datetime) -> None:
    if not self.cfg.morning_protection_enabled:
        return
    if self._last_morning_protection_local_date == now_local.date():
        return
    target = self._morning_protection_schedule_for_day(now_local.date())
    if now_local < target:
        return
    ...
    manager.run_morning_protection_stop(check_time_utc=target.astimezone(timezone.utc), min_hold_hours=...)
```

Follow the noon-protection pattern for eligible account selection, once-per-day gating, and logging.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_runtime_service.py -k morning_protection -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_runtime_service.py core/runtime_service.py
git commit -m "feat: schedule morning protection task"
```

### Task 3: Implement tracked-position morning protection stop tightening

**Files:**
- Modify: `core/position_manager.py`
- Test: `tests/test_position_manager.py`

**Step 1: Write the failing tests**

```python
def test_morning_protection_tightens_tracked_short_older_than_min_hold_to_current_hour_high() -> None:
    opened_at = datetime(2026, 3, 16, 23, 0, tzinfo=timezone.utc)
    check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
    position_id = insert_open_short(opened_at_utc=opened_at.isoformat(), sl_price=0.0025)
    client.get_klines.return_value = [[0, "0", "0.0021", "0.0019", "0", 0]]
    ...
    summary = manager.run_morning_protection_stop(check_time_utc=check_time, min_hold_hours=6.0)
    assert summary["updated_sl"] == 1
    assert get_position(position_id)["sl_price"] == 0.0021


def test_morning_protection_skips_tracked_position_younger_than_min_hold() -> None:
    ...
    assert summary["skipped"] == 1
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_position_manager.py -k morning_protection_tracked -v`
Expected: FAIL because the new method and lock handling do not exist.

**Step 3: Write minimal implementation**

```python
def run_morning_protection_stop(self, check_time_utc: datetime, min_hold_hours: float) -> Dict[str, object]:
    tracked_positions = self.store.list_open_positions()
    for pos in tracked_positions:
        opened_at = self._parse_iso_utc(str(pos.get("opened_at_utc") or ""))
        if (check_time_utc - opened_at).total_seconds() < min_hold_hours * 3600:
            continue
        hour_start_utc = check_time_utc.replace(minute=0, second=0, microsecond=0)
        highest_price, lowest_price = self._fetch_symbol_extremes_between(...)
        ...
```

Use a dedicated lock key for morning caps and reuse the existing stop replacement helper path.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_position_manager.py -k morning_protection_tracked -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_position_manager.py core/position_manager.py
git commit -m "feat: tighten tracked stops in morning protection"
```

### Task 4: Implement exchange-position and long/short symmetric morning protection

**Files:**
- Modify: `core/position_manager.py`
- Test: `tests/test_position_manager.py`

**Step 1: Write the failing tests**

```python
def test_morning_protection_applies_to_exchange_short_positions() -> None:
    client.get_position_risk.return_value = [{"symbol": "XRPUSDT", "positionAmt": "-1500", "positionSide": "BOTH", ...}]
    client.get_klines.return_value = [[0, "0", "0.62", "0.51", "0", 0]]
    ...
    summary = manager.run_morning_protection_stop(check_time_utc=check_time, min_hold_hours=6.0)
    assert summary["updated_sl"] == 1


def test_morning_protection_uses_current_hour_low_for_long_positions() -> None:
    ...
    assert created_stop_price == expected_low
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_position_manager.py -k morning_protection_exchange -v`
Expected: FAIL because exchange positions and long-side symmetry are not implemented.

**Step 3: Write minimal implementation**

```python
close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(...)
ref_price = highest_price if close_side == "BUY" else lowest_price
round_up = close_side == "BUY"
new_stop = self.client.normalize_trigger_price(symbol, ref_price, round_up=round_up)
```

For exchange positions, reuse the same cap-key convention as noon protection and only process positions whose reconstructed or inferred age meets the minimum hold threshold.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_position_manager.py -k morning_protection_exchange -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_position_manager.py core/position_manager.py
git commit -m "feat: support exchange positions in morning protection"
```

### Task 5: Merge morning caps into later stop updates and document behavior

**Files:**
- Modify: `core/position_manager.py`
- Modify: `README.md`
- Modify: `config.ini.example`
- Modify: `config.production.multi.ini.example`
- Test: `tests/test_position_manager.py`

**Step 1: Write the failing test**

```python
def test_dynamic_stop_respects_tighter_morning_protection_cap() -> None:
    store.set_lock_state(PositionManager.MORNING_PROTECTION_LOCK_NAME, {"caps": {str(position_id): 59000.0}})
    ...
    summary = manager.run_once()
    assert summary["updated_sl"] == 0
    assert get_position(position_id)["sl_price"] == 59000.0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_position_manager.py::test_dynamic_stop_respects_tighter_morning_protection_cap -v`
Expected: FAIL because dynamic-stop merging currently knows only about noon caps.

**Step 3: Write minimal implementation**

```python
morning_cap_price = self._get_morning_protection_cap(position_id)
if morning_cap_price:
    new_sl_price = min(new_sl_price, morning_cap_price)  # short path
```

Extend cap-loading helpers as needed without duplicating the entire noon-protection stack. Then document the new runtime keys and morning behavior in the README and example configs.

**Step 4: Run final focused verification**

Run: `pytest tests/test_runtime_components.py -k morning_protection -v`
Expected: PASS

Run: `pytest tests/test_runtime_service.py -k morning_protection -v`
Expected: PASS

Run: `pytest tests/test_position_manager.py -k morning_protection -v`
Expected: PASS

**Step 5: Commit**

```bash
git add README.md config.ini.example config.production.multi.ini.example tests/test_runtime_components.py tests/test_runtime_service.py tests/test_position_manager.py core/runtime_components.py core/runtime_service.py core/position_manager.py
git commit -m "docs: document morning protection stop"
```
