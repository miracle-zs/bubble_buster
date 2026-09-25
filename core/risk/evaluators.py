"""Pure mathematical and rule-based risk evaluation functions.

All functions in this module must be pure:
- No network I/O
- No database persistence
- No side effects
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

from core.risk.models import (
    DailyLossCutEvaluationInput,
    DailyLossCutEvaluationResult,
    DynamicStopEvaluationInput,
    DynamicStopEvaluationResult,
    ExitActionType,
    ExitIntent,
    HourlyExchangeTakeProfitEvaluationInput,
    HourlyExchangeTakeProfitEvaluationResult,
    MorningProtectionEvaluationInput,
    MorningProtectionEvaluationResult,
    NoonProtectionEvaluationInput,
    NoonProtectionEvaluationResult,
    PortfolioLossCutEvaluationResult,
    PortfolioTakeProfitEvaluationResult,
)


def calculate_noon_protection_window_start(
    opened_at_utc: datetime,
    day_start_utc: datetime,
    noon_time_utc: datetime,
    pre_entry_hours: int = 2,
) -> datetime:
    """Return the start of the reference window for noon protection.

    Rules:
    - If opened_at >= noon_time, position was opened at or after noon, return noon_time.
    - If opened_at < day_start, position was carried from a prior day; use the
      `pre_entry_hours` full hours immediately preceding today's noon.
    - If opened_at was today before noon, floor to the hour start and subtract `pre_entry_hours`.
    """
    opened_at = opened_at_utc.astimezone(timezone.utc)
    day_start = day_start_utc.astimezone(timezone.utc)
    noon_time = noon_time_utc.astimezone(timezone.utc)

    if opened_at >= noon_time:
        return noon_time
    if opened_at < day_start:
        return noon_time - timedelta(hours=pre_entry_hours)

    entry_hour_start = opened_at.replace(minute=0, second=0, microsecond=0)
    return entry_hour_start - timedelta(hours=pre_entry_hours)


def calculate_merged_stop_loss(
    old_sl_price: Optional[float],
    new_ref_price: float,
    close_side: str = "BUY",
    tick_size: float = 0.0,
) -> tuple[float, bool]:
    """Calculate the tightened stop-loss price and whether a meaningful update is needed.

    - For short positions (close_side == "BUY"):
      Stop loss protects against price rising, so tightening means lowering stop price (min).
    - For long positions (close_side == "SELL"):
      Stop loss protects against price dropping, so tightening means raising stop price (max).
    - If old_sl_price is absent, new_ref_price is used and update is required.
    - If the delta between merged price and old price is <= tick_size (or epsilon), update is skipped.
    """
    is_buy = str(close_side or "").strip().upper() == "BUY"

    if old_sl_price is not None and old_sl_price > 0:
        merged = min(old_sl_price, new_ref_price) if is_buy else max(old_sl_price, new_ref_price)
        min_delta = max(tick_size, 1e-12)
        should_update = abs(merged - old_sl_price) > min_delta
        return merged, should_update

    return new_ref_price, True


def evaluate_noon_protection_candidate(
    candidate: NoonProtectionEvaluationInput,
) -> NoonProtectionEvaluationResult:
    """Evaluate a single position candidate for noon protection stop-loss update.

    Returns a NoonProtectionEvaluationResult containing the ExitIntent if an update
    is required, or skip information if no action is needed.
    """
    qty = abs(candidate.position_amt)
    if qty <= 1e-12:
        return NoonProtectionEvaluationResult(
            intent=None,
            should_update=False,
            merged_sl_price=None,
            skip_reason="position_qty_is_zero",
        )

    if candidate.noon_ref_price is None or candidate.noon_ref_price <= 0:
        return NoonProtectionEvaluationResult(
            intent=None,
            should_update=False,
            merged_sl_price=None,
            skip_reason="missing_or_invalid_noon_ref_price",
        )

    merged_sl_price, should_update = calculate_merged_stop_loss(
        old_sl_price=candidate.old_sl_price,
        new_ref_price=candidate.noon_ref_price,
        close_side=candidate.close_side,
        tick_size=candidate.tick_size,
    )

    if not should_update:
        return NoonProtectionEvaluationResult(
            intent=None,
            should_update=False,
            merged_sl_price=merged_sl_price,
            skip_reason="delta_below_tick_size",
        )

    intent = ExitIntent(
        symbol=candidate.symbol,
        action=ExitActionType.UPDATE_STOP_LOSS,
        reason="NOON_PROTECTION",
        target_price=merged_sl_price,
        qty=qty,
        close_side=candidate.close_side,
        position_side=candidate.position_side,
        use_reduce_only=candidate.use_reduce_only,
        position_id=candidate.tracked_position_id,
        cap_key=candidate.cap_key,
        extra_info={
            "old_sl_price": candidate.old_sl_price,
            "noon_ref_price": candidate.noon_ref_price,
        },
    )

    return NoonProtectionEvaluationResult(
        intent=intent,
        should_update=True,
        merged_sl_price=merged_sl_price,
        skip_reason=None,
    )


def is_morning_protection_hold_satisfied(
    check_time_utc: datetime,
    opened_at_utc: datetime,
    min_hold_hours: float,
) -> bool:
    """Return True if position has been held for at least min_hold_hours."""
    min_hold_seconds = max(0.0, float(min_hold_hours)) * 3600.0
    check_time = check_time_utc.astimezone(timezone.utc)
    opened_at = opened_at_utc.astimezone(timezone.utc)
    return (check_time - opened_at).total_seconds() >= min_hold_seconds


def resolve_morning_protection_old_sl(
    tracked_sl_price: Optional[float],
    cap_price: Optional[float],
    cap_updated_at: Optional[datetime],
    opened_at_utc: datetime,
    is_tracked: bool,
) -> Optional[float]:
    """Resolve the effective prior stop loss for morning protection.

    - If tracked position has a stop price, use it.
    - If tracked position has no stop price, fall back to cap_price.
    - If untracked exchange position, fall back to cap_price ONLY IF the cap was
      updated on or after this position's opened_at timestamp (to ignore stale
      caps from previous lifecycles).
    """
    if tracked_sl_price is not None and tracked_sl_price > 0:
        return tracked_sl_price
    if cap_price is not None and cap_price > 0:
        if is_tracked:
            return cap_price
        if cap_updated_at is None or opened_at_utc <= cap_updated_at:
            return cap_price
    return None


def evaluate_morning_protection_candidate(
    candidate: MorningProtectionEvaluationInput,
) -> MorningProtectionEvaluationResult:
    """Evaluate a single position candidate for morning protection stop-loss update."""
    if not is_morning_protection_hold_satisfied(
        check_time_utc=candidate.check_time_utc,
        opened_at_utc=candidate.opened_at_utc,
        min_hold_hours=candidate.min_hold_hours,
    ):
        return MorningProtectionEvaluationResult(
            intent=None,
            should_update=False,
            merged_sl_price=None,
            skip_reason="min_hold_hours_not_reached",
        )

    qty = abs(candidate.position_amt)
    if qty <= 1e-12:
        return MorningProtectionEvaluationResult(
            intent=None,
            should_update=False,
            merged_sl_price=None,
            skip_reason="position_qty_is_zero",
        )

    if candidate.morning_ref_price is None or candidate.morning_ref_price <= 0:
        return MorningProtectionEvaluationResult(
            intent=None,
            should_update=False,
            merged_sl_price=None,
            skip_reason="missing_or_invalid_morning_ref_price",
        )

    merged_sl_price, should_update = calculate_merged_stop_loss(
        old_sl_price=candidate.old_sl_price,
        new_ref_price=candidate.morning_ref_price,
        close_side=candidate.close_side,
        tick_size=candidate.tick_size,
    )

    if not should_update:
        return MorningProtectionEvaluationResult(
            intent=None,
            should_update=False,
            merged_sl_price=merged_sl_price,
            skip_reason="delta_below_tick_size",
        )

    intent = ExitIntent(
        symbol=candidate.symbol,
        action=ExitActionType.UPDATE_STOP_LOSS,
        reason="MORNING_PROTECTION",
        target_price=merged_sl_price,
        qty=qty,
        close_side=candidate.close_side,
        position_side=candidate.position_side,
        use_reduce_only=candidate.use_reduce_only,
        position_id=candidate.tracked_position_id,
        cap_key=candidate.cap_key,
        extra_info={
            "old_sl_price": candidate.old_sl_price,
            "morning_ref_price": candidate.morning_ref_price,
        },
    )

    return MorningProtectionEvaluationResult(
        intent=intent,
        should_update=True,
        merged_sl_price=merged_sl_price,
        skip_reason=None,
    )


def calculate_portfolio_cycle_window(
    now_local: datetime,
    reset_hour: int,
    reset_minute: int,
) -> tuple[date, datetime, bool]:
    """Calculate the cycle date, reset timestamp, and active status for portfolio loss/profit cycles."""
    local_dt = now_local
    if local_dt.tzinfo is None:
        local_dt = local_dt.replace(tzinfo=timezone.utc)
    reset_today = local_dt.replace(
        hour=reset_hour % 24,
        minute=reset_minute % 60,
        second=0,
        microsecond=0,
    )
    if local_dt >= reset_today:
        return local_dt.date(), reset_today, True
    previous_reset = reset_today - timedelta(days=1)
    return previous_reset.date(), previous_reset, False


def evaluate_daily_loss_cut_candidate(
    candidate: DailyLossCutEvaluationInput,
) -> DailyLossCutEvaluationResult:
    """Evaluate whether a position should be cut due to daily floating loss."""
    if candidate.is_exempt:
        return DailyLossCutEvaluationResult(should_close=False, skip_reason="exempt")

    qty = abs(candidate.position_amt)
    if qty <= 1e-12:
        return DailyLossCutEvaluationResult(should_close=False, skip_reason="zero_qty")

    if candidate.is_short_only_scope and candidate.position_amt >= 0:
        return DailyLossCutEvaluationResult(should_close=False, skip_reason="not_short")

    if candidate.unrealized_pnl >= 0:
        return DailyLossCutEvaluationResult(should_close=False, skip_reason="not_losing")

    intent = ExitIntent(
        symbol=candidate.symbol,
        action=ExitActionType.CLOSE_POSITION,
        reason="DAILY_FLOATING_LOSS_CHECK",
        qty=qty,
        close_side=candidate.close_side,
        position_side=candidate.position_side if candidate.position_side in {"LONG", "SHORT"} else None,
        use_reduce_only=candidate.use_reduce_only,
        position_id=candidate.tracked_position_id,
        extra_info={"unrealized_pnl": candidate.unrealized_pnl},
    )
    return DailyLossCutEvaluationResult(should_close=True, intent=intent)


def evaluate_hourly_take_profit_candidate(
    candidate: HourlyExchangeTakeProfitEvaluationInput,
) -> HourlyExchangeTakeProfitEvaluationResult:
    """Evaluate whether a candidate meets hourly exchange take-profit conditions."""
    if candidate.is_exempt:
        return HourlyExchangeTakeProfitEvaluationResult(should_close=False, skip_reason="exempt")

    if candidate.position_amt >= 0:
        return HourlyExchangeTakeProfitEvaluationResult(should_close=False, skip_reason="not_short")

    if not candidate.eligible_reached:
        return HourlyExchangeTakeProfitEvaluationResult(should_close=False, skip_reason="not_eligible")

    if candidate.hour_open is None or candidate.hour_close is None:
        return HourlyExchangeTakeProfitEvaluationResult(should_close=False, skip_reason="missing_kline")

    if candidate.hour_close <= candidate.hour_open:
        return HourlyExchangeTakeProfitEvaluationResult(should_close=False, skip_reason="hour_not_bullish")

    qty = abs(candidate.position_amt)
    intent = ExitIntent(
        symbol=candidate.symbol,
        action=ExitActionType.CLOSE_POSITION,
        reason="HOURLY_EXCHANGE_TAKE_PROFIT",
        qty=qty,
        close_side=candidate.close_side,
        position_side=candidate.position_side if candidate.position_side in {"LONG", "SHORT"} else None,
        use_reduce_only=candidate.use_reduce_only,
        position_id=candidate.tracked_position_id,
        extra_info={"hour_open": candidate.hour_open, "hour_close": candidate.hour_close},
    )
    return HourlyExchangeTakeProfitEvaluationResult(should_close=True, intent=intent)


def evaluate_portfolio_loss_cut_threshold(
    baseline_equity: float,
    current_equity: float,
    loss_pct: float,
    already_triggered: bool = False,
    close_complete: bool = False,
) -> PortfolioLossCutEvaluationResult:
    """Evaluate whether current portfolio equity triggers the daily loss-cut stop."""
    if current_equity <= 0:
        return PortfolioLossCutEvaluationResult(
            status="SKIPPED",
            should_trigger=False,
            threshold_equity=0.0,
            baseline_equity=baseline_equity,
            current_equity=current_equity,
            reason="INVALID_EQUITY",
        )
    if baseline_equity <= 0:
        return PortfolioLossCutEvaluationResult(
            status="SKIPPED",
            should_trigger=False,
            threshold_equity=0.0,
            baseline_equity=baseline_equity,
            current_equity=current_equity,
            reason="INVALID_BASELINE",
        )

    normalized_loss_pct = min(100.0, max(0.001, float(loss_pct)))
    threshold_equity = baseline_equity * (1.0 - normalized_loss_pct / 100.0)
    threshold_eps = max(1e-9, abs(threshold_equity) * 1e-12)

    if not already_triggered:
        if current_equity + threshold_eps > threshold_equity:
            return PortfolioLossCutEvaluationResult(
                status="MONITORING",
                should_trigger=False,
                threshold_equity=threshold_equity,
                baseline_equity=baseline_equity,
                current_equity=current_equity,
            )
        return PortfolioLossCutEvaluationResult(
            status="TRIGGERED",
            should_trigger=True,
            threshold_equity=threshold_equity,
            baseline_equity=baseline_equity,
            current_equity=current_equity,
        )

    if close_complete:
        return PortfolioLossCutEvaluationResult(
            status="ALREADY_TRIGGERED",
            should_trigger=False,
            threshold_equity=threshold_equity,
            baseline_equity=baseline_equity,
            current_equity=current_equity,
        )

    return PortfolioLossCutEvaluationResult(
        status="TRIGGERED_RETRY",
        should_trigger=True,
        threshold_equity=threshold_equity,
        baseline_equity=baseline_equity,
        current_equity=current_equity,
    )


def evaluate_portfolio_take_profit_threshold(
    baseline_equity: float,
    current_equity: float,
    persisted_peak_equity: float,
    profit_pct: float,
    giveback_pct: float,
    reduce_ratio: float,
    armed: bool = False,
    already_triggered: bool = False,
    close_complete: bool = False,
) -> PortfolioTakeProfitEvaluationResult:
    """Evaluate whether current portfolio equity triggers the daily take-profit or trailing stop."""
    if current_equity <= 0:
        return PortfolioTakeProfitEvaluationResult(
            status="SKIPPED",
            should_trigger=False,
            armed=armed,
            newly_armed=False,
            peak_equity=persisted_peak_equity,
            peak_profit_pct=0.0,
            actual_profit_pct=0.0,
            arming_threshold_equity=0.0,
            threshold_equity=0.0,
            baseline_equity=baseline_equity,
            current_equity=current_equity,
            reason="INVALID_EQUITY",
        )
    if baseline_equity <= 0:
        return PortfolioTakeProfitEvaluationResult(
            status="SKIPPED",
            should_trigger=False,
            armed=armed,
            newly_armed=False,
            peak_equity=persisted_peak_equity,
            peak_profit_pct=0.0,
            actual_profit_pct=0.0,
            arming_threshold_equity=0.0,
            threshold_equity=0.0,
            baseline_equity=baseline_equity,
            current_equity=current_equity,
            reason="INVALID_BASELINE",
        )

    normalized_profit_pct = min(100.0, max(0.001, float(profit_pct)))
    normalized_giveback_pct = min(100.0, max(0.0, float(giveback_pct)))
    actual_profit_pct = (current_equity / baseline_equity - 1.0) * 100.0

    peak_equity = max(
        baseline_equity,
        persisted_peak_equity,
        current_equity if not already_triggered else persisted_peak_equity,
    )
    peak_profit_pct = max(0.0, (peak_equity / baseline_equity - 1.0) * 100.0)
    arming_threshold_equity = baseline_equity * (1.0 + normalized_profit_pct / 100.0)

    is_armed = bool(armed) and normalized_giveback_pct > 0.0
    newly_armed = False
    arming_eps = max(1e-9, abs(arming_threshold_equity) * 1e-12)
    if not already_triggered and normalized_giveback_pct > 0.0 and not is_armed:
        if peak_equity + arming_eps >= arming_threshold_equity:
            is_armed = True
            newly_armed = True

    if normalized_giveback_pct > 0.0 and is_armed:
        trailing_profit_pct = peak_profit_pct * (1.0 - normalized_giveback_pct / 100.0)
        threshold_equity = baseline_equity * (1.0 + trailing_profit_pct / 100.0)
    else:
        trailing_profit_pct = None
        threshold_equity = arming_threshold_equity
        is_armed = False

    threshold_eps = max(1e-9, abs(threshold_equity) * 1e-12)
    should_trigger = False
    monitoring_status = "MONITORING"

    if not already_triggered:
        if normalized_giveback_pct <= 0.0:
            should_trigger = current_equity + threshold_eps >= arming_threshold_equity
        elif not is_armed:
            should_trigger = False
        else:
            should_trigger = current_equity <= threshold_equity + threshold_eps
            monitoring_status = "ARMED" if newly_armed else "TRAILING"

    if not already_triggered and not should_trigger:
        return PortfolioTakeProfitEvaluationResult(
            status=monitoring_status,
            should_trigger=False,
            armed=is_armed,
            newly_armed=newly_armed,
            peak_equity=peak_equity,
            peak_profit_pct=peak_profit_pct,
            actual_profit_pct=actual_profit_pct,
            arming_threshold_equity=arming_threshold_equity,
            threshold_equity=threshold_equity,
            trailing_profit_pct=trailing_profit_pct,
            baseline_equity=baseline_equity,
            current_equity=current_equity,
        )

    if not already_triggered:
        status = "TRIGGERED"
    elif close_complete:
        status = "ALREADY_TRIGGERED"
    else:
        status = "TRIGGERED_RETRY"

    return PortfolioTakeProfitEvaluationResult(
        status=status,
        should_trigger=not (already_triggered and close_complete),
        armed=is_armed,
        newly_armed=newly_armed,
        peak_equity=peak_equity,
        peak_profit_pct=peak_profit_pct,
        actual_profit_pct=actual_profit_pct,
        arming_threshold_equity=arming_threshold_equity,
        threshold_equity=threshold_equity,
        trailing_profit_pct=trailing_profit_pct,
        baseline_equity=baseline_equity,
        current_equity=current_equity,
    )


def calculate_dynamic_stop_price(
    base_sl_price: float,
    noon_cap_price: Optional[float] = None,
    morning_cap_price: Optional[float] = None,
    entry_structure_stop_price: Optional[float] = None,
    old_sl_price: Optional[float] = None,
) -> float:
    """Calculate the effective stop-loss price respecting all tighter protective caps.

    For short positions, a tighter stop price is strictly lower (min).
    """
    effective_sl = base_sl_price
    if noon_cap_price is not None and noon_cap_price > 0:
        effective_sl = min(effective_sl, noon_cap_price)
    if morning_cap_price is not None and morning_cap_price > 0:
        effective_sl = min(effective_sl, morning_cap_price)
    if entry_structure_stop_price is not None and entry_structure_stop_price > 0:
        effective_sl = min(effective_sl, entry_structure_stop_price)
    if old_sl_price is not None and old_sl_price > 0:
        effective_sl = min(effective_sl, old_sl_price)
    return effective_sl


def evaluate_dynamic_stop_candidate(
    candidate: DynamicStopEvaluationInput,
) -> DynamicStopEvaluationResult:
    """Evaluate whether an open position should tighten its stop-loss order."""
    qty = abs(candidate.position_amt)
    if qty <= 1e-12:
        return DynamicStopEvaluationResult(should_update=False, skip_reason="zero_qty")

    if candidate.normalized_liq_sl_price <= 0:
        return DynamicStopEvaluationResult(should_update=False, skip_reason="invalid_base_sl")

    target_sl = calculate_dynamic_stop_price(
        base_sl_price=candidate.normalized_liq_sl_price,
        noon_cap_price=candidate.noon_cap_price,
        morning_cap_price=candidate.morning_cap_price,
        entry_structure_stop_price=candidate.entry_structure_stop_price,
        old_sl_price=candidate.old_sl_price,
    )

    min_delta = max(candidate.tick_size, 1e-12)
    if candidate.old_sl_price and abs(target_sl - candidate.old_sl_price) <= min_delta and candidate.sl_is_live:
        return DynamicStopEvaluationResult(
            should_update=False,
            target_sl_price=target_sl,
            skip_reason="delta_within_tick_size",
        )

    intent = ExitIntent(
        symbol=candidate.symbol,
        action=ExitActionType.UPDATE_STOP_LOSS,
        reason="DYNAMIC_STOP_LOSS",
        target_price=target_sl,
        qty=qty,
        close_side=candidate.close_side,
        position_side=candidate.position_side,
        use_reduce_only=candidate.use_reduce_only,
        position_id=candidate.tracked_position_id,
        extra_info={
            "old_sl_price": candidate.old_sl_price,
            "target_sl_price": target_sl,
        },
    )
    return DynamicStopEvaluationResult(
        should_update=True,
        target_sl_price=target_sl,
        intent=intent,
    )


