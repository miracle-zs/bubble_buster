from datetime import datetime, timezone

import pytest

from scripts.backtest_wait_1h_bearish_strategy_replay import (
    Candle,
    PositionSample,
    noon_protection_reference_high,
    replay_delayed,
)


def _dt(raw: str) -> datetime:
    return datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)


def _candle(open_time: str, close_time: str, open_price: float, high: float, low: float, close: float) -> Candle:
    return Candle(
        open_time_ms=int(_dt(open_time).timestamp() * 1000),
        close_time_ms=int(_dt(close_time).timestamp() * 1000),
        open_price=open_price,
        high_price=high,
        low_price=low,
        close_price=close,
    )


def test_replay_uses_entry_candles_for_entry_and_exit_candles_for_exit() -> None:
    sample = PositionSample(
        position_id=1,
        account_id="acc01",
        symbol="TESTUSDT",
        qty=2.0,
        actual_entry_price=100.0,
        opened_at_utc=_dt("2026-06-01T00:10:00"),
        closed_at_utc=_dt("2026-06-01T04:00:00"),
        close_reason="",
        actual_exit_fill_price=None,
    )
    entry_15m = [
        _candle("2026-06-01T00:00:00", "2026-06-01T00:14:59", 100, 103, 99, 102),
        _candle("2026-06-01T00:15:00", "2026-06-01T00:29:59", 102, 103, 97, 98),
    ]
    exit_1h = [
        _candle("2026-06-01T00:00:00", "2026-06-01T00:59:59", 120, 121, 119, 120),
        _candle("2026-06-01T01:00:00", "2026-06-01T01:59:59", 120, 121, 119, 120),
    ]

    result = replay_delayed(
        sample,
        entry_15m,
        exit_1h,
        tp_drop_pct=20.0,
        max_hold_hours=1.0,
    )

    assert result.entry_time_utc == _dt("2026-06-01T00:29:59")
    assert result.entry_price == 98
    assert result.exit_time_utc == _dt("2026-06-01T01:59:59")
    assert result.exit_price == 120


def test_noon_protection_replay_uses_two_hours_before_same_day_entry() -> None:
    candles = [
        _candle("2026-06-01T06:00:00", "2026-06-01T06:59:59", 100, 105, 95, 100),
        _candle("2026-06-01T07:00:00", "2026-06-01T07:59:59", 100, 110, 95, 100),
        _candle("2026-06-01T08:00:00", "2026-06-01T08:59:59", 100, 130, 95, 100),
        _candle("2026-06-01T09:00:00", "2026-06-01T09:59:59", 100, 108, 95, 100),
    ]

    high = noon_protection_reference_high(
        candles,
        entry_time=_dt("2026-06-01T08:00:00"),
        noon_time=_dt("2026-06-01T12:00:00"),
    )

    assert high == 130


def test_replay_profit_floor_stops_at_locked_profit_on_later_candle() -> None:
    sample = PositionSample(
        position_id=2,
        account_id="acc01",
        symbol="TESTUSDT",
        qty=1.0,
        actual_entry_price=100.0,
        opened_at_utc=_dt("2026-06-01T00:00:00"),
        closed_at_utc=_dt("2026-06-01T05:00:00"),
        close_reason="",
        actual_exit_fill_price=None,
    )
    candles = [
        _candle("2026-06-01T00:00:00", "2026-06-01T00:59:59", 102, 103, 99, 100),
        _candle("2026-06-01T01:00:00", "2026-06-01T01:59:59", 100, 101, 89, 94),
        _candle("2026-06-01T02:00:00", "2026-06-01T02:59:59", 94, 98, 92, 97),
    ]

    result = replay_delayed(
        sample,
        candles,
        candles,
        tp_drop_pct=20.0,
        max_hold_hours=5.0,
        profit_floor_trigger_pct=10.0,
        profit_floor_lock_pct=3.0,
    )

    assert result.exit_reason == "PROFIT_FLOOR_STOP"
    assert result.exit_price == 97
    assert result.return_pct == 3


def test_replay_profit_floor_uses_trigger_candle_close_after_immediate_retrace() -> None:
    sample = PositionSample(
        position_id=3,
        account_id="acc01",
        symbol="TESTUSDT",
        qty=1.0,
        actual_entry_price=100.0,
        opened_at_utc=_dt("2026-06-01T00:00:00"),
        closed_at_utc=_dt("2026-06-01T05:00:00"),
        close_reason="",
        actual_exit_fill_price=None,
    )
    candles = [
        _candle("2026-06-01T00:00:00", "2026-06-01T00:59:59", 102, 103, 99, 100),
        _candle("2026-06-01T01:00:00", "2026-06-01T01:59:59", 100, 102, 89, 99),
    ]

    result = replay_delayed(
        sample,
        candles,
        candles,
        tp_drop_pct=20.0,
        max_hold_hours=5.0,
        profit_floor_trigger_pct=10.0,
        profit_floor_lock_pct=3.0,
    )

    assert result.exit_reason == "PROFIT_FLOOR_IMMEDIATE_CLOSE"
    assert result.exit_price == 99
    assert result.return_pct == 1


def test_replay_hard_stop_caps_short_loss() -> None:
    sample = PositionSample(
        position_id=4,
        account_id="acc01",
        symbol="TESTUSDT",
        qty=1.0,
        actual_entry_price=100.0,
        opened_at_utc=_dt("2026-06-01T00:00:00"),
        closed_at_utc=_dt("2026-06-01T05:00:00"),
        close_reason="",
        actual_exit_fill_price=None,
    )
    candles = [
        _candle("2026-06-01T00:00:00", "2026-06-01T00:59:59", 102, 103, 99, 100),
        _candle("2026-06-01T01:00:00", "2026-06-01T01:59:59", 100, 113, 96, 110),
    ]

    result = replay_delayed(
        sample,
        candles,
        candles,
        tp_drop_pct=20.0,
        max_hold_hours=5.0,
        hard_stop_loss_pct=10.0,
    )

    assert result.exit_reason == "HARD_STOP_LOSS"
    assert result.exit_price == pytest.approx(110)
    assert result.return_pct == pytest.approx(-10)
