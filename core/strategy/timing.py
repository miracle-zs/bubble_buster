"""Candlestick timing controller and pattern confirmation state machine."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from core.strategy.models import RankEntry, ReadyEntry

ENTRY_PHASE_INITIAL = "INITIAL"
ENTRY_PHASE_WAIT_BULLISH = "WAIT_BULLISH"
ENTRY_PHASE_WAIT_BEARISH = "WAIT_BEARISH"
ENTRY_PHASE_POST_INITIAL_CANDLE = "POST_INITIAL_CANDLE"
ENTRY_PHASE_COMPLETE = "COMPLETE"

ENTRY_STAGE_INITIAL = "INITIAL"
ENTRY_STAGE_SCALE_IN = "SCALE_IN"


class TimingController:
    """Pure logic and state machine for hourly candlestick timing and entry form confirmation."""

    ENTRY_PHASE_INITIAL = ENTRY_PHASE_INITIAL
    ENTRY_PHASE_WAIT_BULLISH = ENTRY_PHASE_WAIT_BULLISH
    ENTRY_PHASE_WAIT_BEARISH = ENTRY_PHASE_WAIT_BEARISH
    ENTRY_PHASE_POST_INITIAL_CANDLE = ENTRY_PHASE_POST_INITIAL_CANDLE
    ENTRY_PHASE_COMPLETE = ENTRY_PHASE_COMPLETE

    ENTRY_STAGE_INITIAL = ENTRY_STAGE_INITIAL
    ENTRY_STAGE_SCALE_IN = ENTRY_STAGE_SCALE_IN

    @staticmethod
    def is_bearish(open_price: float, close_price: float) -> bool:
        """Return True if the candle is strictly bearish (close < open)."""
        return float(close_price) < float(open_price)

    @staticmethod
    def is_bullish(open_price: float, close_price: float) -> bool:
        """Return True if the candle is strictly bullish (close > open)."""
        return float(close_price) > float(open_price)

    @staticmethod
    def floor_to_utc_hour(value: datetime) -> datetime:
        """Floor datetime to the top of its UTC hour."""
        return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    @staticmethod
    def closed_hour_boundary(value: datetime) -> datetime:
        """Calculate the closed hour boundary for a given timestamp."""
        close_time = value.astimezone(timezone.utc)
        boundary = close_time.replace(minute=0, second=0, microsecond=0)
        if close_time > boundary:
            boundary += timedelta(hours=1)
        return boundary

    @staticmethod
    def resolve_entry_fill_time(order: Dict[str, object], fallback: datetime) -> datetime:
        """Extract order fill execution timestamp or return fallback."""
        for key in ("updateTime", "transactTime", "time"):
            try:
                timestamp_ms = int(order.get(key) or 0)
            except (TypeError, ValueError):
                continue
            if timestamp_ms > 0:
                return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        return fallback.astimezone(timezone.utc)

    @classmethod
    def classify_due_states(
        cls,
        pending: Dict[int, Dict[str, Any]],
        now: datetime,
        preclose_sec: int,
        close_grace_sec: int,
        allow_preclose_phases: Optional[Set[str]] = None,
    ) -> Tuple[
        List[Tuple[int, RankEntry, datetime]],
        List[Tuple[int, RankEntry, datetime]],
        List[datetime],
    ]:
        """Classify pending candidate states into due preclose states, due final states, and upcoming check times."""
        if allow_preclose_phases is None:
            allow_preclose_phases = {ENTRY_PHASE_INITIAL}

        due_preclose_states: List[Tuple[int, RankEntry, datetime]] = []
        due_final_states: List[Tuple[int, RankEntry, datetime]] = []
        next_check_times: List[datetime] = []

        for idx, state in list(pending.items()):
            entry = state.get("entry")
            hour_open = state.get("hour_open")
            if not isinstance(entry, RankEntry) or not isinstance(hour_open, datetime):
                continue

            phase = str(state.get("phase") or ENTRY_PHASE_INITIAL).strip().upper()
            if phase == ENTRY_PHASE_COMPLETE:
                continue

            hour_close = hour_open + timedelta(hours=1)
            preclose_checked = bool(state.get("preclose_checked", False))

            if preclose_sec > 0 and not preclose_checked and phase in allow_preclose_phases:
                available_at = hour_close - timedelta(seconds=preclose_sec)
                if now < available_at:
                    next_check_times.append(available_at)
                    continue
                final_available_at = hour_close + timedelta(seconds=close_grace_sec)
                if now >= final_available_at:
                    due_final_states.append((idx, entry, hour_open))
                    continue
                due_preclose_states.append((idx, entry, hour_open))
                continue

            available_at = hour_close + timedelta(seconds=close_grace_sec)
            if now < available_at:
                next_check_times.append(available_at)
                continue
            due_final_states.append((idx, entry, hour_open))

        return due_preclose_states, due_final_states, next_check_times

    @classmethod
    def evaluate_preclose_candle(
        cls,
        entry: RankEntry,
        state: Dict[str, Any],
        open_price: float,
        close_price: float,
        now_utc: datetime,
        signal_base_time_utc: datetime,
    ) -> Optional[ReadyEntry]:
        """Evaluate preclose candle. If bearish, mark state and return ReadyEntry."""
        state["preclose_checked"] = True
        hour_open = state["hour_open"]
        if not cls.is_bearish(open_price, close_price):
            return None

        ready = ReadyEntry(
            entry=entry,
            reference_price=close_price,
            signal_time_utc=(
                state["signal_time"]
                if isinstance(state.get("signal_time"), datetime)
                else signal_base_time_utc
            ),
            bearish_close_time_utc=None,
            entry_stage=ENTRY_STAGE_INITIAL,
            preclose_entry=True,
            preclose_time_utc=now_utc,
            signal_hour_open_utc=hour_open,
            provisional_open_price=open_price,
            provisional_close_price=close_price,
        )
        state["phase"] = ENTRY_PHASE_POST_INITIAL_CANDLE
        state["hour_open"] = hour_open
        return ready

    @classmethod
    def evaluate_single_bearish_final_candle(
        cls,
        entry: RankEntry,
        state: Dict[str, Any],
        open_price: float,
        close_price: float,
        close_time_utc: datetime,
        signal_base_time_utc: datetime,
    ) -> Optional[ReadyEntry]:
        """Evaluate final closed candle for single bearish mode."""
        hour_open = state["hour_open"]
        if cls.is_bearish(open_price, close_price):
            return ReadyEntry(
                entry=entry,
                reference_price=close_price,
                signal_time_utc=(
                    state["signal_time"]
                    if isinstance(state.get("signal_time"), datetime)
                    else signal_base_time_utc
                ),
                bearish_close_time_utc=close_time_utc,
                signal_hour_open_utc=hour_open,
            )
        state["hour_open"] = hour_open + timedelta(hours=1)
        state["preclose_checked"] = False
        return None

    @classmethod
    def advance_bullish_bearish_final_candle(
        cls,
        entry: RankEntry,
        state: Dict[str, Any],
        open_price: float,
        close_price: float,
        close_time_utc: datetime,
        signal_base_time_utc: datetime,
    ) -> Optional[ReadyEntry]:
        """Advance multi-phase bullish-then-bearish candle confirmation state machine."""
        hour_open = state["hour_open"]
        phase = str(state.get("phase") or ENTRY_PHASE_INITIAL).strip().upper()

        if phase == ENTRY_PHASE_INITIAL:
            if cls.is_bearish(open_price, close_price):
                ready = ReadyEntry(
                    entry=entry,
                    reference_price=close_price,
                    signal_time_utc=(
                        state["signal_time"]
                        if isinstance(state.get("signal_time"), datetime)
                        else signal_base_time_utc
                    ),
                    bearish_close_time_utc=close_time_utc,
                    entry_stage=ENTRY_STAGE_INITIAL,
                    signal_hour_open_utc=hour_open,
                )
                state["phase"] = ENTRY_PHASE_WAIT_BULLISH
                state["hour_open"] = hour_open + timedelta(hours=1)
                state["preclose_checked"] = False
                return ready
            state["hour_open"] = hour_open + timedelta(hours=1)
            state["preclose_checked"] = False
            return None

        if phase == ENTRY_PHASE_POST_INITIAL_CANDLE:
            is_bull = cls.is_bullish(open_price, close_price)
            state["phase"] = ENTRY_PHASE_WAIT_BEARISH if is_bull else ENTRY_PHASE_WAIT_BULLISH
            state["bullish_seen"] = is_bull
            state["hour_open"] = hour_open + timedelta(hours=1)
            state["preclose_checked"] = False
            return None

        if phase == ENTRY_PHASE_WAIT_BULLISH:
            if cls.is_bullish(open_price, close_price):
                state["phase"] = ENTRY_PHASE_WAIT_BEARISH
                state["bullish_seen"] = True
            state["hour_open"] = hour_open + timedelta(hours=1)
            state["preclose_checked"] = False
            return None

        if phase == ENTRY_PHASE_WAIT_BEARISH:
            if cls.is_bearish(open_price, close_price):
                ready = ReadyEntry(
                    entry=entry,
                    reference_price=close_price,
                    signal_time_utc=(
                        state["signal_time"]
                        if isinstance(state.get("signal_time"), datetime)
                        else signal_base_time_utc
                    ),
                    bearish_close_time_utc=close_time_utc,
                    entry_stage=ENTRY_STAGE_SCALE_IN,
                    signal_hour_open_utc=hour_open,
                )
                state["phase"] = ENTRY_PHASE_COMPLETE
                state["preclose_checked"] = False
                return ready
            state["hour_open"] = hour_open + timedelta(hours=1)
            state["preclose_checked"] = False
            return None

        return None
