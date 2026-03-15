# Account 55 Hourly Exchange Take-Profit Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an account-scoped hourly exchange-position take-profit rule for account `55` that exits a live short at local minute `59` when the current `1h` candle is bullish, as long as that short has reached at least `20%` favorable price move at any point since its true opening time.

**Architecture:** Add new account-level runtime config and a dedicated service task instead of reusing `manage` or `daily_loss_cut`. Implement the exchange-position monitoring logic in `PositionManager`, persist per-symbol monitor state in `locks`, reconstruct true short opening anchors from Binance `userTrades`, and gate exits to once per local hour.

**Tech Stack:** Python, pytest, SQLite state via `StateStore`, Binance Futures REST client, runtime scheduler in `core/runtime_service.py`

---

### Task 1: Add configuration plumbing for the new hourly task

**Files:**
- Modify: `core/runtime_components.py`
- Modify: `core/runtime_service.py`
- Modify: `config.ini.example`
- Modify: `config.production.multi.ini.example`
- Test: `tests/test_runtime_components.py`

**Step 1: Write the failing test**

```python
def test_create_components_applies_per_account_hourly_exchange_take_profit_override(tmp_path) -> None:
    cfg_text = """
    [binance]
    api_key = k
    api_secret = s

    [strategy]

    [runtime]
    timezone = Asia/Shanghai
    hourly_exchange_take_profit_enabled = false
    hourly_exchange_take_profit_minute = 59
    hourly_exchange_take_profit_drop_pct = 20

    [accounts]
    enabled = acc01,55
    mode.acc01 = full
    mode.55 = loss_cut_only

    [account.55.runtime]
    hourly_exchange_take_profit_enabled = true
    hourly_exchange_take_profit_minute = 58
    hourly_exchange_take_profit_drop_pct = 18
    """
    account_runtimes, _service = create_components(load_cfg(cfg_text), base_dir=str(tmp_path))
    assert account_runtimes["acc01"]["hourly_exchange_take_profit_enabled"] is False
    assert account_runtimes["55"]["hourly_exchange_take_profit_enabled"] is True
    assert account_runtimes["55"]["hourly_exchange_take_profit_minute"] == 58
    assert account_runtimes["55"]["hourly_exchange_take_profit_drop_pct"] == 18.0
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_runtime_components.py::test_create_components_applies_per_account_hourly_exchange_take_profit_override -v`
Expected: FAIL because the new runtime fields are not exposed yet.

**Step 3: Write minimal implementation**

```python
"hourly_exchange_take_profit_enabled": runtime_cfg.getboolean(
    "hourly_exchange_take_profit_enabled", fallback=False
),
"hourly_exchange_take_profit_minute": runtime_cfg.getint(
    "hourly_exchange_take_profit_minute", fallback=59
),
"hourly_exchange_take_profit_drop_pct": runtime_cfg.getfloat(
    "hourly_exchange_take_profit_drop_pct", fallback=20.0
),
```

Also extend `ServiceRuntimeConfig` with defaults for the global fallbacks and document the new keys in both example config files, enabling them only in `[account.55.runtime]`.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_runtime_components.py::test_create_components_applies_per_account_hourly_exchange_take_profit_override -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_runtime_components.py core/runtime_components.py core/runtime_service.py config.ini.example config.production.multi.ini.example
git commit -m "feat: add hourly exchange take-profit config plumbing"
```

### Task 2: Add runtime scheduling for the hourly exchange take-profit task

**Files:**
- Modify: `core/runtime_service.py`
- Test: `tests/test_runtime_service.py`

**Step 1: Write the failing tests**

```python
def test_hourly_exchange_take_profit_runs_for_loss_cut_only_account_at_configured_minute() -> None:
    manager = ManagerStub()
    service = StrategyRuntimeService(
        strategy=None,
        manager=manager,
        cfg=build_cfg(),
        account_runtimes={
            "55": {
                "mode": "loss_cut_only",
                "manager": manager,
                "strategy": None,
                "balance_sampler": None,
                "hourly_exchange_take_profit_enabled": True,
                "hourly_exchange_take_profit_minute": 59,
                "hourly_exchange_take_profit_drop_pct": 20.0,
            }
        },
    )

    service._run_hourly_exchange_take_profit_if_due(
        datetime(2026, 3, 16, 10, 59, tzinfo=ZoneInfo("Asia/Shanghai"))
    )

    assert manager.hourly_take_profit_calls == 1


def test_hourly_exchange_take_profit_runs_only_once_per_local_hour() -> None:
    ...
    assert manager.hourly_take_profit_calls == 1
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_runtime_service.py -k hourly_exchange_take_profit -v`
Expected: FAIL because the scheduler hook and manager method do not exist.

**Step 3: Write minimal implementation**

```python
def _run_hourly_exchange_take_profit_if_due(self, now_local: datetime) -> None:
    hour_key = now_local.strftime("%Y-%m-%dT%H")
    for aid, ctx in self.account_runtimes.items():
        if not bool(ctx.get("hourly_exchange_take_profit_enabled", False)):
            continue
        if now_local.minute != int(ctx.get("hourly_exchange_take_profit_minute", 59)) % 60:
            continue
        if self._last_hourly_exchange_take_profit_hour_by_account.get(aid) == hour_key:
            continue
        manager = ctx.get("manager")
        if manager is not None and hasattr(manager, "run_hourly_exchange_take_profit"):
            manager.run_hourly_exchange_take_profit(
                now_local=now_local,
                drop_pct=float(ctx.get("hourly_exchange_take_profit_drop_pct", 20.0)),
            )
            self._last_hourly_exchange_take_profit_hour_by_account[aid] = hour_key
```

Wire the new hook into `run_pending()` after the regular service time calculations, and follow the existing loss-cut/noon-protection pattern for logging and multi-account execution.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_runtime_service.py -k hourly_exchange_take_profit -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_runtime_service.py core/runtime_service.py
git commit -m "feat: schedule hourly exchange take-profit task"
```

### Task 3: Add exchange-position monitoring state and opening-anchor reconstruction

**Files:**
- Modify: `core/position_manager.py`
- Modify: `infra/binance_futures_client.py`
- Test: `tests/test_position_manager.py`

**Step 1: Write the failing tests**

```python
def test_hourly_exchange_take_profit_initializes_state_from_true_open_time() -> None:
    client.get_position_risk.return_value = [
        {"symbol": "BTCUSDT", "positionAmt": "-1", "entryPrice": "100", "positionSide": "BOTH"}
    ]
    client.get_user_trades.return_value = [
        {"time": 1710555000000, "qty": "1", "side": "SELL"},
    ]
    client.get_klines.return_value = [
        [1710555000000, "100", "100", "79", "80"],
    ]

    summary = manager.refresh_hourly_exchange_take_profit_state(
        now_local=dt(2026, 3, 16, 10, 18, "Asia/Shanghai"),
        drop_pct=20.0,
    )

    state = store.get_lock_state(PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME)
    assert summary["initialized"] == 1
    assert state["symbols"]["BTCUSDT"]["opened_at_utc"] == "2024-03-15T..."
    assert state["symbols"]["BTCUSDT"]["eligible_reached"] is True


def test_hourly_exchange_take_profit_keeps_eligibility_after_retrace() -> None:
    ...
    assert state["symbols"]["BTCUSDT"]["eligible_reached"] is True
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_position_manager.py -k hourly_exchange_take_profit_state -v`
Expected: FAIL because no state refresh helpers or lock schema conventions exist for this feature.

**Step 3: Write minimal implementation**

```python
HOURLY_EXCHANGE_TP_LOCK_NAME = "hourly_exchange_take_profit_v1"

def refresh_hourly_exchange_take_profit_state(self, now_local: datetime, drop_pct: float) -> Dict[str, object]:
    state = self.store.get_lock_state(self.HOURLY_EXCHANGE_TP_LOCK_NAME) or {"symbols": {}}
    risks = self.client.get_position_risk()
    for risk in risks:
        if self._safe_float(risk.get("positionAmt"), default=0.0) >= 0:
            continue
        symbol = str(risk.get("symbol") or "").strip()
        monitor = self._load_or_rebuild_hourly_tp_monitor(symbol=symbol, risk=risk, drop_pct=drop_pct)
        state["symbols"][symbol] = monitor
    self.store.set_lock_state(self.HOURLY_EXCHANGE_TP_LOCK_NAME, state)
    return {"initialized": initialized_count, "updated": updated_count, "pruned": pruned_count}
```

Implement helper methods to:
- rebuild the true opening anchor from `get_user_trades`;
- fetch the minimum price since the anchor using `get_klines`;
- preserve existing monitor state when the live short is the same position;
- prune symbols that are now flat or no longer short.

Avoid changing the Binance client API shape unless tests prove a helper is necessary; reuse the existing `get_user_trades()` and `get_klines()` methods.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_position_manager.py -k hourly_exchange_take_profit_state -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_position_manager.py core/position_manager.py infra/binance_futures_client.py
git commit -m "feat: track hourly exchange take-profit state"
```

### Task 4: Implement the 59-minute bullish-candle exit path

**Files:**
- Modify: `core/position_manager.py`
- Test: `tests/test_position_manager.py`

**Step 1: Write the failing tests**

```python
def test_hourly_exchange_take_profit_closes_eligible_short_on_bullish_hour() -> None:
    store.set_lock_state(
        PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME,
        {
            "symbols": {
                "BTCUSDT": {
                    "position_amt": -1.0,
                    "entry_price": 100.0,
                    "opened_at_utc": "2026-03-16T01:00:00+00:00",
                    "lowest_price_since_open": 79.0,
                    "eligible_reached": True,
                }
            }
        },
    )
    client.get_position_risk.return_value = [
        {"symbol": "BTCUSDT", "positionAmt": "-1", "entryPrice": "100", "markPrice": "85", "positionSide": "BOTH"}
    ]
    client.get_klines.return_value = [
        [1710554400000, "84", "86", "83", "85"],
    ]

    result = manager.run_hourly_exchange_take_profit(
        now_local=dt(2026, 3, 16, 10, 59, "Asia/Shanghai"),
        drop_pct=20.0,
    )

    assert result["closed_take_profit"] == 1
    assert client.create_order.call_args.kwargs["side"] == "BUY"


def test_hourly_exchange_take_profit_skips_ineligible_or_bearish_positions() -> None:
    ...
    assert result["closed_take_profit"] == 0
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_position_manager.py -k hourly_exchange_take_profit -v`
Expected: FAIL because the action method and notification builder do not exist.

**Step 3: Write minimal implementation**

```python
def run_hourly_exchange_take_profit(self, now_local: datetime, drop_pct: float) -> Dict[str, object]:
    self.refresh_hourly_exchange_take_profit_state(now_local=now_local, drop_pct=drop_pct)
    risks = self.client.get_position_risk()
    for risk in risks:
        symbol = str(risk.get("symbol") or "").strip()
        monitor = monitors.get(symbol)
        if not monitor or not monitor.get("eligible_reached"):
            continue
        hour_open, latest_price = self._load_current_hour_open_and_latest_price(symbol=symbol, now_local=now_local)
        if latest_price <= hour_open:
            continue
        self._close_daily_loss_cut(
            symbol=symbol,
            qty=abs(self._safe_float(risk.get("positionAmt"), default=0.0)),
            side="BUY",
            position_id=None,
            cancel_pos=None,
        )
```

Add a dedicated notification formatter so these exits do not appear as daily loss-cut events, and record `last_checked_hour_key` only after a successful accepted exit or a completed skip decision for that hour.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_position_manager.py -k hourly_exchange_take_profit -v`
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_position_manager.py core/position_manager.py
git commit -m "feat: close eligible exchange shorts on bullish hourly reversal"
```

### Task 5: Update docs and run focused verification

**Files:**
- Modify: `README.md`
- Modify: `config.production.multi.ini.example`
- Modify: `config.ini.example`
- Test: `tests/test_runtime_components.py`
- Test: `tests/test_runtime_service.py`
- Test: `tests/test_position_manager.py`

**Step 1: Write the failing doc/config assertions if needed**

```python
def test_example_config_exposes_account_55_hourly_exchange_take_profit_settings() -> None:
    text = Path("config.production.multi.ini.example").read_text()
    assert "hourly_exchange_take_profit_enabled = true" in text
```

If the repo avoids config-file assertions, skip this test and just document the setting carefully in `README.md`.

**Step 2: Run verification commands before docs edits**

Run: `pytest tests/test_runtime_components.py -v`
Expected: PASS on prior changes.

Run: `pytest tests/test_runtime_service.py -k "hourly_exchange_take_profit or loss_cut_only" -v`
Expected: PASS

Run: `pytest tests/test_position_manager.py -k hourly_exchange_take_profit -v`
Expected: PASS

**Step 3: Write minimal documentation**

```markdown
- `hourly_exchange_take_profit_enabled`: account-level exchange short take-profit task.
- `hourly_exchange_take_profit_minute`: local minute within each hour to evaluate the rule.
- `hourly_exchange_take_profit_drop_pct`: minimum favorable price drop ever reached since true opening time.
```

Document that account `55` uses live exchange positions, not tracked strategy positions, and that the rule evaluates the current in-progress `1h` candle at minute `59`.

**Step 4: Run final focused verification**

Run: `pytest tests/test_runtime_components.py::test_create_components_applies_per_account_hourly_exchange_take_profit_override -v`
Expected: PASS

Run: `pytest tests/test_runtime_service.py -k hourly_exchange_take_profit -v`
Expected: PASS

Run: `pytest tests/test_position_manager.py -k hourly_exchange_take_profit -v`
Expected: PASS

**Step 5: Commit**

```bash
git add README.md config.ini.example config.production.multi.ini.example tests/test_runtime_components.py tests/test_runtime_service.py tests/test_position_manager.py core/runtime_service.py core/runtime_components.py core/position_manager.py infra/binance_futures_client.py
git commit -m "docs: document hourly exchange take-profit"
```
