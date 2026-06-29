from datetime import datetime, timezone

from scripts.backtest_wait_1h_bearish_strategy_replay import (
    Candle,
    PositionSample,
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
