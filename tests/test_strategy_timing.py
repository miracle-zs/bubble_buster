"""Unit tests for pure TimingController candlestick logic and state machine."""

from datetime import datetime, timedelta, timezone
import pytest

from core.strategy.models import RankEntry, ReadyEntry
from core.strategy.timing import (
    ENTRY_PHASE_COMPLETE,
    ENTRY_PHASE_INITIAL,
    ENTRY_PHASE_POST_INITIAL_CANDLE,
    ENTRY_PHASE_WAIT_BEARISH,
    ENTRY_PHASE_WAIT_BULLISH,
    ENTRY_STAGE_INITIAL,
    ENTRY_STAGE_SCALE_IN,
    TimingController,
)


class TestTimingController:
    def test_candlestick_direction(self):
        assert TimingController.is_bearish(100.0, 99.0) is True
        assert TimingController.is_bearish(100.0, 100.0) is False
        assert TimingController.is_bearish(100.0, 101.0) is False

        assert TimingController.is_bullish(100.0, 101.0) is True
        assert TimingController.is_bullish(100.0, 100.0) is False
        assert TimingController.is_bullish(100.0, 99.0) is False

    def test_time_math_boundaries(self):
        dt = datetime(2026, 3, 1, 14, 23, 45, 123456, tzinfo=timezone.utc)
        floored = TimingController.floor_to_utc_hour(dt)
        assert floored == datetime(2026, 3, 1, 14, 0, 0, tzinfo=timezone.utc)

        # Closed hour boundary
        # If exactly on the hour, boundary is that exact hour
        on_hour = datetime(2026, 3, 1, 14, 0, 0, tzinfo=timezone.utc)
        assert TimingController.closed_hour_boundary(on_hour) == on_hour

        # If past the hour, boundary is the next hour
        assert TimingController.closed_hour_boundary(dt) == datetime(2026, 3, 1, 15, 0, 0, tzinfo=timezone.utc)

    def test_resolve_entry_fill_time(self):
        fallback = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)

        # with updateTime in ms
        order = {"updateTime": 1772366400000}
        fill_time = TimingController.resolve_entry_fill_time(order, fallback)
        assert fill_time.timestamp() == 1772366400.0

        # fallback when no timestamp keys
        assert TimingController.resolve_entry_fill_time({}, fallback) == fallback

    def test_classify_due_states(self):
        now = datetime(2026, 3, 1, 14, 55, 30, tzinfo=timezone.utc)
        hour_open = datetime(2026, 3, 1, 14, 0, 0, tzinfo=timezone.utc)  # hour_close = 15:00
        entry = RankEntry("SOLUSDT", 10.0, 100.0, 1000.0)

        # Preclose available at 15:00 - 300s = 14:55:00. Now is 14:55:30 -> due_preclose!
        pending = {
            1: {
                "entry": entry,
                "hour_open": hour_open,
                "phase": ENTRY_PHASE_INITIAL,
                "preclose_checked": False,
            }
        }
        due_preclose, due_final, next_checks = TimingController.classify_due_states(
            pending=pending,
            now=now,
            preclose_sec=300,
            close_grace_sec=5,
        )
        assert len(due_preclose) == 1
        assert due_preclose[0][0] == 1
        assert len(due_final) == 0

        # When preclose already checked, it waits for final candle (15:00:05)
        pending[1]["preclose_checked"] = True
        due_preclose, due_final, next_checks = TimingController.classify_due_states(
            pending=pending,
            now=now,
            preclose_sec=300,
            close_grace_sec=5,
        )
        assert len(due_preclose) == 0
        assert len(due_final) == 0
        assert len(next_checks) == 1
        assert next_checks[0] == hour_open + timedelta(hours=1, seconds=5)

    def test_evaluate_preclose_candle(self):
        entry = RankEntry("SOLUSDT", 10.0, 100.0, 1000.0)
        now_utc = datetime(2026, 3, 1, 14, 55, 0, tzinfo=timezone.utc)
        sig_time = datetime(2026, 3, 1, 14, 0, 0, tzinfo=timezone.utc)

        # Bearish: open 100, close 95
        state = {
            "entry": entry,
            "hour_open": sig_time,
            "signal_time": sig_time,
            "phase": ENTRY_PHASE_INITIAL,
            "preclose_checked": False,
        }
        ready = TimingController.evaluate_preclose_candle(
            entry=entry,
            state=state,
            open_price=100.0,
            close_price=95.0,
            now_utc=now_utc,
            signal_base_time_utc=sig_time,
        )
        assert ready is not None
        assert ready.reference_price == 95.0
        assert ready.preclose_entry is True
        assert ready.entry_stage == ENTRY_STAGE_INITIAL
        assert state["phase"] == ENTRY_PHASE_POST_INITIAL_CANDLE
        assert state["preclose_checked"] is True

        # Bullish: open 100, close 105 -> None
        state2 = {
            "entry": entry,
            "hour_open": sig_time,
            "signal_time": sig_time,
            "phase": ENTRY_PHASE_INITIAL,
            "preclose_checked": False,
        }
        ready2 = TimingController.evaluate_preclose_candle(
            entry=entry,
            state=state2,
            open_price=100.0,
            close_price=105.0,
            now_utc=now_utc,
            signal_base_time_utc=sig_time,
        )
        assert ready2 is None
        assert state2["preclose_checked"] is True
        assert state2["phase"] == ENTRY_PHASE_INITIAL

    def test_evaluate_single_bearish_final_candle(self):
        entry = RankEntry("SOLUSDT", 10.0, 100.0, 1000.0)
        hour_open = datetime(2026, 3, 1, 14, 0, 0, tzinfo=timezone.utc)
        close_time = datetime(2026, 3, 1, 15, 0, 0, tzinfo=timezone.utc)

        # Bearish
        state = {"entry": entry, "hour_open": hour_open, "phase": ENTRY_PHASE_INITIAL}
        ready = TimingController.evaluate_single_bearish_final_candle(
            entry=entry,
            state=state,
            open_price=100.0,
            close_price=98.0,
            close_time_utc=close_time,
            signal_base_time_utc=hour_open,
        )
        assert ready is not None
        assert ready.bearish_close_time_utc == close_time
        assert ready.reference_price == 98.0

        # Bullish -> advance hour_open
        ready_bull = TimingController.evaluate_single_bearish_final_candle(
            entry=entry,
            state=state,
            open_price=100.0,
            close_price=102.0,
            close_time_utc=close_time,
            signal_base_time_utc=hour_open,
        )
        assert ready_bull is None
        assert state["hour_open"] == hour_open + timedelta(hours=1)

    def test_advance_bullish_bearish_final_candle_lifecycle(self):
        entry = RankEntry("SOLUSDT", 10.0, 100.0, 1000.0)
        hour_open = datetime(2026, 3, 1, 14, 0, 0, tzinfo=timezone.utc)
        close_time = datetime(2026, 3, 1, 15, 0, 0, tzinfo=timezone.utc)

        state = {"entry": entry, "hour_open": hour_open, "phase": ENTRY_PHASE_INITIAL}

        # 1. INITIAL + Bearish -> ReadyEntry (INITIAL), transitions to WAIT_BULLISH
        r1 = TimingController.advance_bullish_bearish_final_candle(
            entry, state, open_price=100.0, close_price=95.0, close_time_utc=close_time, signal_base_time_utc=hour_open
        )
        assert r1 is not None
        assert r1.entry_stage == ENTRY_STAGE_INITIAL
        assert state["phase"] == ENTRY_PHASE_WAIT_BULLISH

        # 2. WAIT_BULLISH + Another Bearish -> None, stays in WAIT_BULLISH
        close_time2 = close_time + timedelta(hours=1)
        r2 = TimingController.advance_bullish_bearish_final_candle(
            entry, state, open_price=95.0, close_price=90.0, close_time_utc=close_time2, signal_base_time_utc=hour_open
        )
        assert r2 is None
        assert state["phase"] == ENTRY_PHASE_WAIT_BULLISH

        # 3. WAIT_BULLISH + Bullish candle -> transitions to WAIT_BEARISH
        close_time3 = close_time2 + timedelta(hours=1)
        r3 = TimingController.advance_bullish_bearish_final_candle(
            entry, state, open_price=90.0, close_price=98.0, close_time_utc=close_time3, signal_base_time_utc=hour_open
        )
        assert r3 is None
        assert state["phase"] == ENTRY_PHASE_WAIT_BEARISH
        assert state["bullish_seen"] is True

        # 4. WAIT_BEARISH + Bearish candle -> ReadyEntry (SCALE_IN), transitions to COMPLETE
        close_time4 = close_time3 + timedelta(hours=1)
        r4 = TimingController.advance_bullish_bearish_final_candle(
            entry, state, open_price=98.0, close_price=94.0, close_time_utc=close_time4, signal_base_time_utc=hour_open
        )
        assert r4 is not None
        assert r4.entry_stage == ENTRY_STAGE_SCALE_IN
        assert state["phase"] == ENTRY_PHASE_COMPLETE
