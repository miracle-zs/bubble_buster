"""Domain models and data structures for strategy candidate ranking, planning, and rebalancing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class RankEntry:
    """A ranked market candidate extracted from top gainers."""
    symbol: str
    pct_change: float
    last_price: float
    quote_volume: float


@dataclass(frozen=True)
class PlannedOrder:
    """A calculated entry order with allocated margin and notional size."""
    symbol: str
    base_margin_usdt: float
    target_notional_usdt: float
    qty: float


@dataclass(frozen=True)
class ReadyEntry:
    """A market candidate that has passed timing / candlestick form confirmation."""
    entry: RankEntry
    reference_price: float
    signal_time_utc: datetime
    bearish_close_time_utc: Optional[datetime]
    entry_stage: str = "INITIAL"
    preclose_entry: bool = False
    preclose_time_utc: Optional[datetime] = None
    signal_hour_open_utc: Optional[datetime] = None
    provisional_open_price: Optional[float] = None
    provisional_close_price: Optional[float] = None


@dataclass(frozen=True)
class EntryStructureWindow:
    """Reference window for entry structure high detection and stop-loss calculation."""
    bearish_close_time_utc: datetime
    window_start_utc: datetime
    highest_price: float


@dataclass(frozen=True)
class RebalancePlan:
    """A calculated rebalancing adjustment plan for an existing position."""
    position_id: int
    symbol: str
    side: str
    qty: float
    ref_price: float
    est_notional: float
    current_notional: float
    target_notional: float
    deviation_notional: float
    deadband_notional: float
    max_adjust_notional: float
    requested_adjust_notional: float
