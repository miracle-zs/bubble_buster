"""Domain models and data structures for risk evaluation and position exit."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class ExitActionType(str, Enum):
    """Action types emitted by pure risk evaluators."""
    NOOP = "NOOP"
    UPDATE_STOP_LOSS = "UPDATE_STOP_LOSS"
    CLOSE_POSITION = "CLOSE_POSITION"
    REDUCE_POSITION = "REDUCE_POSITION"


@dataclass(frozen=True)
class ExitIntent:
    """Immutable intent describing an intended exit or protection mutation."""
    symbol: str
    action: ExitActionType
    reason: str
    target_price: Optional[float] = None
    qty: Optional[float] = None
    close_side: str = "BUY"
    position_side: Optional[str] = None
    use_reduce_only: bool = True
    position_id: Optional[int] = None
    cap_key: Optional[str] = None
    extra_info: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class NoonProtectionEvaluationInput:
    """Input payload required to evaluate noon protection stop for a candidate."""
    symbol: str
    position_amt: float
    position_side: str
    close_side: str
    use_reduce_only: bool
    cap_key: str
    tracked_position_id: Optional[int] = None
    old_sl_price: Optional[float] = None
    noon_ref_price: Optional[float] = None
    tick_size: float = 0.0


@dataclass(frozen=True)
class NoonProtectionEvaluationResult:
    """Result of noon protection evaluation."""
    intent: Optional[ExitIntent]
    should_update: bool
    merged_sl_price: Optional[float] = None
    skip_reason: Optional[str] = None


@dataclass(frozen=True)
class MorningProtectionEvaluationInput:
    """Input payload required to evaluate morning protection stop for a candidate."""
    symbol: str
    position_amt: float
    position_side: str
    close_side: str
    use_reduce_only: bool
    cap_key: str
    check_time_utc: datetime
    opened_at_utc: datetime
    min_hold_hours: float
    morning_ref_price: Optional[float] = None
    tracked_position_id: Optional[int] = None
    old_sl_price: Optional[float] = None
    tick_size: float = 0.0


@dataclass(frozen=True)
class MorningProtectionEvaluationResult:
    """Result of morning protection evaluation."""
    intent: Optional[ExitIntent]
    should_update: bool
    merged_sl_price: Optional[float] = None
    skip_reason: Optional[str] = None


@dataclass(frozen=True)
class DailyLossCutEvaluationInput:
    """Input payload to evaluate daily floating loss cut for a position."""
    symbol: str
    position_amt: float
    unrealized_pnl: float
    is_exempt: bool
    position_side: str = "BOTH"
    close_side: str = "BUY"
    use_reduce_only: bool = True
    tracked_position_id: Optional[int] = None
    is_short_only_scope: bool = False


@dataclass(frozen=True)
class DailyLossCutEvaluationResult:
    """Result of daily floating loss cut evaluation."""
    should_close: bool
    intent: Optional[ExitIntent] = None
    skip_reason: Optional[str] = None


@dataclass(frozen=True)
class HourlyExchangeTakeProfitEvaluationInput:
    """Input payload to evaluate hourly exchange take-profit for a short position."""
    symbol: str
    position_amt: float
    position_side: str
    close_side: str
    use_reduce_only: bool
    is_exempt: bool
    eligible_reached: bool
    hour_open: Optional[float]
    hour_close: Optional[float]
    tracked_position_id: Optional[int] = None


@dataclass(frozen=True)
class HourlyExchangeTakeProfitEvaluationResult:
    """Result of hourly exchange take-profit evaluation."""
    should_close: bool
    intent: Optional[ExitIntent] = None
    skip_reason: Optional[str] = None


@dataclass(frozen=True)
class PortfolioLossCutEvaluationResult:
    """Result of portfolio daily loss-cut evaluation."""
    status: str
    should_trigger: bool
    threshold_equity: float
    baseline_equity: float
    current_equity: float
    reason: Optional[str] = None


@dataclass(frozen=True)
class PortfolioTakeProfitEvaluationResult:
    """Result of portfolio daily take-profit evaluation."""
    status: str
    should_trigger: bool
    armed: bool
    newly_armed: bool
    peak_equity: float
    peak_profit_pct: float
    actual_profit_pct: float
    arming_threshold_equity: float
    threshold_equity: float
    trailing_profit_pct: Optional[float] = None
    baseline_equity: float = 0.0
    current_equity: float = 0.0
    reason: Optional[str] = None


@dataclass(frozen=True)
class DynamicStopEvaluationInput:
    """Input payload to evaluate dynamic stop-loss update for an open position."""
    symbol: str
    position_amt: float
    normalized_liq_sl_price: float
    old_sl_price: Optional[float] = None
    noon_cap_price: Optional[float] = None
    morning_cap_price: Optional[float] = None
    entry_structure_stop_price: Optional[float] = None
    tick_size: float = 0.0
    sl_is_live: bool = False
    tracked_position_id: Optional[int] = None
    position_side: Optional[str] = None
    close_side: str = "BUY"
    use_reduce_only: bool = True


@dataclass(frozen=True)
class DynamicStopEvaluationResult:
    """Result of dynamic stop-loss evaluation."""
    should_update: bool
    target_sl_price: Optional[float] = None
    intent: Optional[ExitIntent] = None
    skip_reason: Optional[str] = None


