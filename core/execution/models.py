"""Domain models for first-principles order execution and tracking.

Represents the core domain entities defined in the refactoring architecture:
- OrderIntent: declared business desire (what should happen, target qty/price, scope)
- OrderAttempt: physical submission attempt with stable client ID and lifecycle states
- ExecutionFill: normalized trade execution fact
- PositionEpisode: logical position lifecycle tracking
- RiskCycleTargetSet: portfolio-level risk trigger targets (loss cut / take profit)
- ProtectionPolicyState: active protection constraints (noon/morning stop caps)
- EntryPlan: persistent entry waiting and scheduled wakeups
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class IntentStatus(str, enum.Enum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class AttemptStatus(str, enum.Enum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        return self in {
            AttemptStatus.FILLED,
            AttemptStatus.REJECTED,
            AttemptStatus.CANCELED,
            AttemptStatus.EXPIRED,
        }

    @property
    def is_uncertain(self) -> bool:
        return self == AttemptStatus.UNKNOWN


class EpisodeStatus(str, enum.Enum):
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"


class IntentScope(str, enum.Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    PROTECTION = "PROTECTION"
    REBALANCE = "REBALANCE"



@dataclass
class OrderIntent:
    """Declared business action specifying desired target state."""

    intent_id: str
    account_id: str
    client_intent_key: str
    symbol: str
    side: str
    order_type: str
    target_qty: Optional[float] = None
    target_price: Optional[float] = None
    intent_scope: str = "EXIT"  # ENTRY, EXIT, REBALANCE, PROTECTION
    position_id: Optional[int] = None
    episode_id: Optional[str] = None
    status: str = IntentStatus.PENDING.value
    reason: Optional[str] = None
    created_at_utc: str = field(default_factory=utc_now_iso)
    updated_at_utc: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "account_id": self.account_id,
            "client_intent_key": self.client_intent_key,
            "symbol": self.symbol,
            "side": self.side,
            "order_type": self.order_type,
            "target_qty": self.target_qty,
            "target_price": self.target_price,
            "intent_scope": self.intent_scope,
            "position_id": self.position_id,
            "episode_id": self.episode_id,
            "status": self.status,
            "reason": self.reason,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
        }


@dataclass
class OrderAttempt:
    """Concrete physical submission of an intent to the exchange."""

    attempt_id: str
    intent_id: str
    account_id: str
    symbol: str
    client_order_id: str
    exchange_order_id: Optional[str] = None
    attempt_number: int = 1
    status: str = AttemptStatus.PREPARED.value
    submitted_qty: Optional[float] = None
    executed_qty: float = 0.0
    cumulative_quote_qty: Optional[float] = None
    avg_price: Optional[float] = None
    error_message: Optional[str] = None
    exchange_response: Optional[Any] = None
    parent_attempt_id: Optional[str] = None
    created_at_utc: str = field(default_factory=utc_now_iso)
    updated_at_utc: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "attempt_id": self.attempt_id,
            "intent_id": self.intent_id,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "client_order_id": self.client_order_id,
            "exchange_order_id": self.exchange_order_id,
            "attempt_number": self.attempt_number,
            "status": self.status,
            "submitted_qty": self.submitted_qty,
            "executed_qty": self.executed_qty,
            "cumulative_quote_qty": self.cumulative_quote_qty,
            "avg_price": self.avg_price,
            "error_message": self.error_message,
            "parent_attempt_id": self.parent_attempt_id,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
        }


@dataclass
class ExecutionFill:
    """Normalized fill record representing an atomic exchange trade execution."""

    fill_id: str
    attempt_id: str
    intent_id: str
    symbol: str
    side: str
    price: float
    qty: float
    exchange_trade_id: str
    account_id: str = "default"
    exchange_order_id: Optional[str] = None
    commission: float = 0.0
    commission_asset: str = "USDT"
    trade_time_utc: str = field(default_factory=utc_now_iso)
    created_at_utc: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fill_id": self.fill_id,
            "attempt_id": self.attempt_id,
            "intent_id": self.intent_id,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "exchange_trade_id": self.exchange_trade_id,
            "exchange_order_id": self.exchange_order_id,
            "side": self.side,
            "price": self.price,
            "qty": self.qty,
            "commission": self.commission,
            "commission_asset": self.commission_asset,
            "trade_time_utc": self.trade_time_utc,
            "created_at_utc": self.created_at_utc,
        }


@dataclass
class PositionEpisode:
    """Logical lifecycle of a position exposure."""

    episode_id: str
    account_id: str
    symbol: str
    position_side: str = "SHORT"
    status: str = EpisodeStatus.OPEN.value
    opened_at_utc: str = field(default_factory=utc_now_iso)
    closed_at_utc: Optional[str] = None
    target_qty: Optional[float] = None
    current_qty: float = 0.0
    realized_pnl: float = 0.0
    created_at_utc: str = field(default_factory=utc_now_iso)
    updated_at_utc: str = field(default_factory=utc_now_iso)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "account_id": self.account_id,
            "symbol": self.symbol,
            "position_side": self.position_side,
            "status": self.status,
            "opened_at_utc": self.opened_at_utc,
            "closed_at_utc": self.closed_at_utc,
            "target_qty": self.target_qty,
            "current_qty": self.current_qty,
            "realized_pnl": self.realized_pnl,
            "created_at_utc": self.created_at_utc,
            "updated_at_utc": self.updated_at_utc,
        }


@dataclass
class RiskCycleTargetSet:
    """Represents a frozen set of targets for portfolio loss cut or take profit."""

    target_set_id: str
    account_id: str
    cycle_type: str  # "LOSS_CUT", "TAKE_PROFIT"
    cycle_key: str
    status: str  # "PENDING", "IN_PROGRESS", "COMPLETED", "EXPIRED"
    targets: Dict[str, Any]
    summary: Optional[Dict[str, Any]] = None
    created_at_utc: str = field(default_factory=utc_now_iso)
    updated_at_utc: str = field(default_factory=utc_now_iso)


@dataclass
class ProtectionPolicyState:
    """Dedicated state for protection policies (e.g. noon/morning stop caps)."""

    policy_key: str
    account_id: str
    policy_type: str
    payload: Dict[str, Any]
    updated_at_utc: str = field(default_factory=utc_now_iso)


@dataclass
class EntryPlan:
    """Persisted entry plan with schedule wakeup checkpoint."""

    plan_id: str
    account_id: str
    symbol: str
    status: str
    hour_open_utc: str
    next_wakeup_utc: str
    plan_payload: Dict[str, Any]
    created_at_utc: str = field(default_factory=utc_now_iso)
    updated_at_utc: str = field(default_factory=utc_now_iso)


class DataQuality(str, enum.Enum):
    CERTAIN = "CERTAIN"
    STALE = "STALE"
    RECONCILING = "RECONCILING"


@dataclass
class AccountView:
    """Read view of account facts with explicit quality, revision, and source timestamps."""

    account_id: str
    wallet_balance: float = 0.0
    available_balance: float = 0.0
    equity: float = 0.0
    unrealized_pnl: float = 0.0
    positions: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    episodes: Dict[str, PositionEpisode] = field(default_factory=dict)
    active_attempts: List[OrderAttempt] = field(default_factory=list)
    uncertain_symbols: set[str] = field(default_factory=set)
    revision: int = 0
    data_quality: str = DataQuality.CERTAIN.value
    as_of_utc: str = field(default_factory=utc_now_iso)


@dataclass
class MarketView:
    """Read view of market data with explicit quality and source timestamps."""

    prices: Dict[str, float] = field(default_factory=dict)
    top_gainers: List[Dict[str, Any]] = field(default_factory=list)
    candles: Dict[str, Any] = field(default_factory=dict)
    data_quality: str = DataQuality.CERTAIN.value
    as_of_utc: str = field(default_factory=utc_now_iso)


@dataclass
class TaskOccurrence:
    """Persistent task occurrence representing scheduled work units."""

    task_occurrence_id: str
    account_id: str
    task_type: str
    cycle_key: str
    due_at_utc: str
    status: str = "PENDING"
    executed_at_utc: Optional[str] = None
    completed_at_utc: Optional[str] = None
    payload: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    created_at_utc: str = field(default_factory=utc_now_iso)
    updated_at_utc: str = field(default_factory=utc_now_iso)

