"""Position rebalancing pure calculation logic."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from core.strategy.models import RebalancePlan


class RebalanceCalculator:
    """Pure mathematical and logic engine for position rebalancing calculations."""

    @staticmethod
    def parse_iso_utc(text: str) -> datetime:
        """Parse an ISO 8601 string into a UTC timezone-aware datetime."""
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @classmethod
    def position_age_hours(cls, pos: Dict[str, object], now_utc: datetime) -> float:
        """Calculate the age of an open position in hours from opened_at_utc."""
        opened_at = str(pos.get("opened_at_utc") or "").strip()
        if not opened_at:
            return 0.0
        try:
            opened_dt = cls.parse_iso_utc(opened_at)
        except Exception:  # noqa: BLE001
            return 0.0
        delta_sec = (now_utc - opened_dt).total_seconds()
        if delta_sec <= 0:
            return 0.0
        return delta_sec / 3600.0

    @staticmethod
    def age_decay_weight(age_hours: float, half_life_hours: float = 24.0) -> float:
        """Calculate exponential decay weight for position age: exp(-ln(2) * (age / half_life))."""
        if age_hours <= 0:
            return 1.0
        half_life = max(1.0, float(half_life_hours))
        decay = math.exp(-math.log(2.0) * (age_hours / half_life))
        return max(1e-4, decay)

    @classmethod
    def build_target_notional_map(
        cls,
        positions: List[Dict[str, object]],
        target_count: int,
        target_gross_notional: float,
        rebalance_mode: str = "equal_risk",
        now_utc: Optional[datetime] = None,
        half_life_hours: float = 24.0,
    ) -> Tuple[Dict[int, float], int]:
        """Compute target notional USDT for each active position based on rebalance mode."""
        if not positions or target_count <= 0 or target_gross_notional <= 0:
            return {}, 0

        target_per_position = target_gross_notional / float(target_count)
        if rebalance_mode == "equal_risk":
            return {int(pos["id"]): target_per_position for pos in positions}, max(0, target_count - len(positions))

        if now_utc is None:
            now_utc = datetime.now(timezone.utc)

        weighted_rows: List[Tuple[int, float]] = []
        for pos in positions:
            position_id = int(pos["id"])
            age_hours = cls.position_age_hours(pos=pos, now_utc=now_utc)
            weight = cls.age_decay_weight(age_hours=age_hours, half_life_hours=half_life_hours)
            weighted_rows.append((position_id, weight))

        virtual_slots = max(0, target_count - len(weighted_rows))
        total_weight = sum(weight for _, weight in weighted_rows) + float(virtual_slots)
        if total_weight <= 1e-12:
            return {int(pos["id"]): target_per_position for pos in positions}, virtual_slots

        target_map: Dict[int, float] = {}
        for position_id, weight in weighted_rows:
            target_map[position_id] = target_gross_notional * (weight / total_weight)
        return target_map, virtual_slots

    @classmethod
    def build_rebalance_plan(
        cls,
        pos: Dict[str, object],
        risk_map: Dict[str, Dict[str, Any]],
        target_notional: float,
        reduce_only: bool,
        deadband_pct: float,
        max_single_adjust_pct: float,
        min_adjust_notional_usdt: float,
        normalize_qty_fn: Callable[[str, float, float], float],
    ) -> Tuple[Optional[RebalancePlan], Dict[str, object]]:
        """Evaluate a position and construct a RebalancePlan if adjustment is needed."""
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        evaluation: Dict[str, object] = {
            "position_id": position_id,
            "symbol": symbol,
            "status": "SKIPPED",
            "reason": "UNKNOWN",
            "side": None,
            "ref_price": None,
            "current_notional": 0.0,
            "target_notional": float(target_notional),
            "deviation_notional": 0.0,
            "deadband_notional": 0.0,
            "max_adjust_notional": 0.0,
            "requested_adjust_notional": 0.0,
            "qty": 0.0,
            "est_notional": 0.0,
        }
        risk = risk_map.get(symbol)
        if not risk:
            evaluation["reason"] = "MISSING_POSITION_RISK"
            return None, evaluation

        position_amt = cls._safe_float(risk.get("positionAmt"), default=0.0)
        if position_amt >= 0:
            evaluation["reason"] = "NON_SHORT_POSITION"
            return None, evaluation

        mark_price = (
            cls._safe_positive_float(risk.get("markPrice"))
            or cls._safe_positive_float(risk.get("entryPrice"))
            or cls._safe_positive_float(pos.get("entry_price"))
        )
        if not mark_price:
            evaluation["reason"] = "MISSING_MARK_PRICE"
            return None, evaluation

        current_notional = abs(position_amt) * mark_price
        evaluation["ref_price"] = mark_price
        evaluation["current_notional"] = current_notional
        if current_notional <= 0:
            evaluation["reason"] = "NON_POSITIVE_CURRENT_NOTIONAL"
            return None, evaluation

        deviation_notional = target_notional - current_notional
        deadband = max(target_notional, 0.0) * deadband_pct
        evaluation["deviation_notional"] = deviation_notional
        evaluation["deadband_notional"] = deadband
        if abs(deviation_notional) <= deadband:
            evaluation["reason"] = "WITHIN_DEADBAND"
            return None, evaluation
        if reduce_only and deviation_notional > 0:
            evaluation["reason"] = "REDUCE_ONLY_BLOCKED_INCREASE"
            return None, evaluation

        max_adjust_notional = current_notional * max_single_adjust_pct
        adjust_notional = min(abs(deviation_notional), max_adjust_notional)
        evaluation["max_adjust_notional"] = max_adjust_notional
        evaluation["requested_adjust_notional"] = adjust_notional
        if adjust_notional < min_adjust_notional_usdt:
            evaluation["reason"] = "BELOW_MIN_ADJUST_NOTIONAL"
            return None, evaluation

        qty = normalize_qty_fn(symbol, adjust_notional, mark_price)
        evaluation["qty"] = qty
        if qty <= 0:
            evaluation["reason"] = "QTY_NORMALIZED_ZERO"
            return None, evaluation

        side = "BUY" if deviation_notional < 0 else "SELL"
        evaluation["side"] = side
        if reduce_only and side != "BUY":
            evaluation["reason"] = "REDUCE_ONLY_BLOCKED_INCREASE"
            return None, evaluation

        est_notional = qty * mark_price
        evaluation["est_notional"] = est_notional
        if est_notional < min_adjust_notional_usdt:
            evaluation["reason"] = "EST_NOTIONAL_BELOW_MIN"
            return None, evaluation

        evaluation["status"] = "PLANNED"
        evaluation["reason"] = "PLANNED"
        return (
            RebalancePlan(
                position_id=position_id,
                symbol=symbol,
                side=side,
                qty=qty,
                ref_price=mark_price,
                est_notional=est_notional,
                current_notional=current_notional,
                target_notional=target_notional,
                deviation_notional=deviation_notional,
                deadband_notional=deadband,
                max_adjust_notional=max_adjust_notional,
                requested_adjust_notional=adjust_notional,
            ),
            evaluation,
        )

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_positive_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            number = float(value)
            if number <= 0:
                return None
            return number
        except (TypeError, ValueError):
            return None
