from datetime import date, datetime, timezone
import pytest

from core.risk.evaluators import (
    calculate_merged_stop_loss,
    calculate_dynamic_stop_price,
    calculate_noon_protection_window_start,
    calculate_portfolio_cycle_window,
    evaluate_daily_loss_cut_candidate,
    evaluate_dynamic_stop_candidate,
    evaluate_hourly_take_profit_candidate,
    evaluate_morning_protection_candidate,
    evaluate_noon_protection_candidate,
    evaluate_portfolio_loss_cut_threshold,
    evaluate_portfolio_take_profit_threshold,
    is_morning_protection_hold_satisfied,
    resolve_morning_protection_old_sl,
)
from core.risk.models import (
    DailyLossCutEvaluationInput,
    DynamicStopEvaluationInput,
    ExitActionType,
    HourlyExchangeTakeProfitEvaluationInput,
    MorningProtectionEvaluationInput,
    NoonProtectionEvaluationInput,
)


class TestNoonProtectionWindowStart:
    def test_position_opened_after_noon_returns_noon(self) -> None:
        day_start = datetime(2026, 9, 25, 0, 0, tzinfo=timezone.utc)
        noon = datetime(2026, 9, 25, 4, 0, tzinfo=timezone.utc)
        opened_at = datetime(2026, 9, 25, 4, 15, tzinfo=timezone.utc)

        start = calculate_noon_protection_window_start(
            opened_at_utc=opened_at,
            day_start_utc=day_start,
            noon_time_utc=noon,
        )
        assert start == noon

    def test_position_carried_from_prior_day_uses_two_hours_before_noon(self) -> None:
        day_start = datetime(2026, 9, 25, 0, 0, tzinfo=timezone.utc)
        noon = datetime(2026, 9, 25, 4, 0, tzinfo=timezone.utc)
        opened_at = datetime(2026, 9, 24, 23, 40, tzinfo=timezone.utc)

        start = calculate_noon_protection_window_start(
            opened_at_utc=opened_at,
            day_start_utc=day_start,
            noon_time_utc=noon,
        )
        assert start == datetime(2026, 9, 25, 2, 0, tzinfo=timezone.utc)

    def test_position_opened_today_uses_two_hours_before_entry_hour(self) -> None:
        day_start = datetime(2026, 9, 25, 0, 0, tzinfo=timezone.utc)
        noon = datetime(2026, 9, 25, 4, 0, tzinfo=timezone.utc)
        opened_at = datetime(2026, 9, 25, 1, 40, tzinfo=timezone.utc)  # 01:40 -> entry hour is 01:00

        start = calculate_noon_protection_window_start(
            opened_at_utc=opened_at,
            day_start_utc=day_start,
            noon_time_utc=noon,
        )
        # 01:00 - 2 hours = 23:00 prior day
        assert start == datetime(2026, 9, 24, 23, 0, tzinfo=timezone.utc)


class TestMergedStopLoss:
    def test_short_position_tightens_when_ref_is_lower(self) -> None:
        merged, should_update = calculate_merged_stop_loss(
            old_sl_price=10.0,
            new_ref_price=9.5,
            close_side="BUY",
            tick_size=0.01,
        )
        assert merged == 9.5
        assert should_update is True

    def test_short_position_does_not_widen_when_ref_is_higher(self) -> None:
        merged, should_update = calculate_merged_stop_loss(
            old_sl_price=10.0,
            new_ref_price=10.5,
            close_side="BUY",
            tick_size=0.01,
        )
        assert merged == 10.0
        assert should_update is False

    def test_short_position_skips_when_delta_within_tick_size(self) -> None:
        merged, should_update = calculate_merged_stop_loss(
            old_sl_price=10.00,
            new_ref_price=9.995,
            close_side="BUY",
            tick_size=0.01,
        )
        assert merged == 9.995
        assert should_update is False

    def test_long_position_tightens_when_ref_is_higher(self) -> None:
        merged, should_update = calculate_merged_stop_loss(
            old_sl_price=10.0,
            new_ref_price=10.5,
            close_side="SELL",
            tick_size=0.01,
        )
        assert merged == 10.5
        assert should_update is True

    def test_initial_stop_without_old_sl_always_updates(self) -> None:
        merged, should_update = calculate_merged_stop_loss(
            old_sl_price=None,
            new_ref_price=9.5,
            close_side="BUY",
            tick_size=0.01,
        )
        assert merged == 9.5
        assert should_update is True


class TestEvaluateNoonProtectionCandidate:
    def test_candidate_emits_update_stop_loss_intent(self) -> None:
        cand = NoonProtectionEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-0.5,
            position_side="BOTH",
            close_side="BUY",
            use_reduce_only=True,
            cap_key="cap_btc",
            tracked_position_id=123,
            old_sl_price=50000.0,
            noon_ref_price=48000.0,
            tick_size=0.1,
        )
        res = evaluate_noon_protection_candidate(cand)
        assert res.should_update is True
        assert res.merged_sl_price == 48000.0
        assert res.intent is not None
        assert res.intent.action == ExitActionType.UPDATE_STOP_LOSS
        assert res.intent.symbol == "BTCUSDT"
        assert res.intent.target_price == 48000.0
        assert res.intent.qty == 0.5
        assert res.intent.close_side == "BUY"
        assert res.intent.position_id == 123
        assert res.intent.reason == "NOON_PROTECTION"

    def test_zero_qty_skipped(self) -> None:
        cand = NoonProtectionEvaluationInput(
            symbol="BTCUSDT",
            position_amt=0.0,
            position_side="BOTH",
            close_side="BUY",
            use_reduce_only=True,
            cap_key="cap_btc",
            noon_ref_price=48000.0,
        )
        res = evaluate_noon_protection_candidate(cand)
        assert res.should_update is False
        assert res.skip_reason == "position_qty_is_zero"
        assert res.intent is None

    def test_delta_within_tick_size_skipped(self) -> None:
        cand = NoonProtectionEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-0.5,
            position_side="BOTH",
            close_side="BUY",
            use_reduce_only=True,
            cap_key="cap_btc",
            old_sl_price=50000.0,
            noon_ref_price=49999.95,
            tick_size=0.1,
        )
        res = evaluate_noon_protection_candidate(cand)
        assert res.should_update is False
        assert res.skip_reason == "delta_below_tick_size"
        assert res.intent is None


class TestMorningProtectionHold:
    def test_hold_time_less_than_min_hold_returns_false(self) -> None:
        opened_at = datetime(2026, 3, 17, 3, 0, tzinfo=timezone.utc)
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        # 4 hours 55 minutes < 6.0 hours
        assert is_morning_protection_hold_satisfied(check_time, opened_at, min_hold_hours=6.0) is False

    def test_hold_time_greater_than_min_hold_returns_true(self) -> None:
        opened_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        # 6 hours 55 minutes >= 6.0 hours
        assert is_morning_protection_hold_satisfied(check_time, opened_at, min_hold_hours=6.0) is True


class TestResolveMorningProtectionOldSl:
    def test_tracked_position_prefers_tracked_sl(self) -> None:
        opened_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)
        sl = resolve_morning_protection_old_sl(
            tracked_sl_price=10.5,
            cap_price=9.5,
            cap_updated_at=None,
            opened_at_utc=opened_at,
            is_tracked=True,
        )
        assert sl == 10.5

    def test_tracked_position_falls_back_to_cap(self) -> None:
        opened_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)
        sl = resolve_morning_protection_old_sl(
            tracked_sl_price=None,
            cap_price=9.5,
            cap_updated_at=None,
            opened_at_utc=opened_at,
            is_tracked=True,
        )
        assert sl == 9.5

    def test_untracked_position_ignores_stale_cap(self) -> None:
        opened_at = datetime(2026, 3, 17, 2, 0, tzinfo=timezone.utc)
        cap_updated_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)  # stale cap!
        sl = resolve_morning_protection_old_sl(
            tracked_sl_price=None,
            cap_price=9.5,
            cap_updated_at=cap_updated_at,
            opened_at_utc=opened_at,
            is_tracked=False,
        )
        assert sl is None

    def test_untracked_position_accepts_valid_cap(self) -> None:
        opened_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)
        cap_updated_at = datetime(2026, 3, 17, 2, 0, tzinfo=timezone.utc)  # updated after opened_at
        sl = resolve_morning_protection_old_sl(
            tracked_sl_price=None,
            cap_price=9.5,
            cap_updated_at=cap_updated_at,
            opened_at_utc=opened_at,
            is_tracked=False,
        )
        assert sl == 9.5


class TestEvaluateMorningProtectionCandidate:
    def test_candidate_skipped_if_hold_time_not_reached(self) -> None:
        opened_at = datetime(2026, 3, 17, 3, 0, tzinfo=timezone.utc)
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        cand = MorningProtectionEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-1.0,
            position_side="BOTH",
            close_side="BUY",
            use_reduce_only=True,
            cap_key="cap_btc",
            check_time_utc=check_time,
            opened_at_utc=opened_at,
            min_hold_hours=6.0,
            morning_ref_price=50000.0,
        )
        res = evaluate_morning_protection_candidate(cand)
        assert res.should_update is False
        assert res.skip_reason == "min_hold_hours_not_reached"

    def test_candidate_emits_intent_when_eligible(self) -> None:
        opened_at = datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc)
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        cand = MorningProtectionEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-1.0,
            position_side="BOTH",
            close_side="BUY",
            use_reduce_only=True,
            cap_key="cap_btc",
            check_time_utc=check_time,
            opened_at_utc=opened_at,
            min_hold_hours=6.0,
            old_sl_price=52000.0,
            morning_ref_price=51000.0,
            tick_size=0.1,
            tracked_position_id=99,
        )
        res = evaluate_morning_protection_candidate(cand)
        assert res.should_update is True
        assert res.merged_sl_price == 51000.0
        assert res.intent is not None
        assert res.intent.reason == "MORNING_PROTECTION"
        assert res.intent.target_price == 51000.0
        assert res.intent.position_id == 99


class TestCalculatePortfolioCycleWindow:
    def test_after_reset_hour_returns_today(self) -> None:
        now = datetime(2026, 3, 20, 9, 30, tzinfo=timezone.utc)
        cycle_date, reset_at, active = calculate_portfolio_cycle_window(now, reset_hour=8, reset_minute=0)
        assert cycle_date == date(2026, 3, 20)
        assert reset_at == datetime(2026, 3, 20, 8, 0, tzinfo=timezone.utc)
        assert active is True

    def test_before_reset_hour_returns_yesterday(self) -> None:
        now = datetime(2026, 3, 20, 7, 30, tzinfo=timezone.utc)
        cycle_date, reset_at, active = calculate_portfolio_cycle_window(now, reset_hour=8, reset_minute=0)
        assert cycle_date == date(2026, 3, 19)
        assert reset_at == datetime(2026, 3, 19, 8, 0, tzinfo=timezone.utc)
        assert active is False


class TestEvaluateDailyLossCutCandidate:
    def test_exempt_symbol_skipped(self) -> None:
        cand = DailyLossCutEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-1.0,
            unrealized_pnl=-100.0,
            is_exempt=True,
        )
        res = evaluate_daily_loss_cut_candidate(cand)
        assert res.should_close is False
        assert res.skip_reason == "exempt"

    def test_profitable_position_skipped(self) -> None:
        cand = DailyLossCutEvaluationInput(
            symbol="ETHUSDT",
            position_amt=-1.0,
            unrealized_pnl=50.0,
            is_exempt=False,
        )
        res = evaluate_daily_loss_cut_candidate(cand)
        assert res.should_close is False
        assert res.skip_reason == "not_losing"

    def test_losing_short_emits_close_intent(self) -> None:
        cand = DailyLossCutEvaluationInput(
            symbol="SOLUSDT",
            position_amt=-10.0,
            unrealized_pnl=-50.0,
            is_exempt=False,
            close_side="BUY",
            position_side="BOTH",
            tracked_position_id=42,
        )
        res = evaluate_daily_loss_cut_candidate(cand)
        assert res.should_close is True
        assert res.intent is not None
        assert res.intent.action == ExitActionType.CLOSE_POSITION
        assert res.intent.qty == 10.0
        assert res.intent.close_side == "BUY"
        assert res.intent.position_id == 42


class TestEvaluateHourlyTakeProfitCandidate:
    def test_ineligible_symbol_skipped(self) -> None:
        cand = HourlyExchangeTakeProfitEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-1.0,
            position_side="BOTH",
            close_side="BUY",
            use_reduce_only=True,
            is_exempt=False,
            eligible_reached=False,
            hour_open=100.0,
            hour_close=105.0,
        )
        res = evaluate_hourly_take_profit_candidate(cand)
        assert res.should_close is False
        assert res.skip_reason == "not_eligible"

    def test_bearish_hour_skipped(self) -> None:
        cand = HourlyExchangeTakeProfitEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-1.0,
            position_side="BOTH",
            close_side="BUY",
            use_reduce_only=True,
            is_exempt=False,
            eligible_reached=True,
            hour_open=105.0,
            hour_close=100.0,
        )
        res = evaluate_hourly_take_profit_candidate(cand)
        assert res.should_close is False
        assert res.skip_reason == "hour_not_bullish"

    def test_eligible_bullish_hour_emits_close_intent(self) -> None:
        cand = HourlyExchangeTakeProfitEvaluationInput(
            symbol="DOGEUSDT",
            position_amt=-500.0,
            position_side="SHORT",
            close_side="BUY",
            use_reduce_only=True,
            is_exempt=False,
            eligible_reached=True,
            hour_open=0.10,
            hour_close=0.12,
            tracked_position_id=10,
        )
        res = evaluate_hourly_take_profit_candidate(cand)
        assert res.should_close is True
        assert res.intent is not None
        assert res.intent.action == ExitActionType.CLOSE_POSITION
        assert res.intent.qty == 500.0
        assert res.intent.position_side == "SHORT"
        assert res.intent.position_id == 10


class TestEvaluatePortfolioLossCutThreshold:
    def test_above_threshold_monitors(self) -> None:
        res = evaluate_portfolio_loss_cut_threshold(
            baseline_equity=10000.0,
            current_equity=9800.0,
            loss_pct=3.5,
        )
        assert res.status == "MONITORING"
        assert res.should_trigger is False
        assert res.threshold_equity == 9650.0

    def test_at_or_below_threshold_triggers(self) -> None:
        res = evaluate_portfolio_loss_cut_threshold(
            baseline_equity=10000.0,
            current_equity=9640.0,
            loss_pct=3.5,
        )
        assert res.status == "TRIGGERED"
        assert res.should_trigger is True

    def test_already_triggered_and_complete(self) -> None:
        res = evaluate_portfolio_loss_cut_threshold(
            baseline_equity=10000.0,
            current_equity=9500.0,
            loss_pct=3.5,
            already_triggered=True,
            close_complete=True,
        )
        assert res.status == "ALREADY_TRIGGERED"
        assert res.should_trigger is False


class TestEvaluatePortfolioTakeProfitThreshold:
    def test_fixed_tp_triggers_when_equity_reaches_threshold(self) -> None:
        res = evaluate_portfolio_take_profit_threshold(
            baseline_equity=10000.0,
            current_equity=10950.0,
            persisted_peak_equity=10950.0,
            profit_pct=9.0,
            giveback_pct=0.0,
            reduce_ratio=1.0,
        )
        assert res.status == "TRIGGERED"
        assert res.should_trigger is True

    def test_trailing_arms_when_peak_reaches_threshold(self) -> None:
        res = evaluate_portfolio_take_profit_threshold(
            baseline_equity=10000.0,
            current_equity=10950.0,
            persisted_peak_equity=10950.0,
            profit_pct=9.0,
            giveback_pct=20.0,
            reduce_ratio=1.0,
            armed=False,
        )
        assert res.armed is True
        assert res.newly_armed is True
        assert res.status == "ARMED"
        assert res.should_trigger is False

    def test_trailing_triggers_on_giveback(self) -> None:
        # Peak was +10% (11000), giveback 20% means retains 80% of profit -> 8% gain = 10800
        # If current drops to 10750, it triggers!
        res = evaluate_portfolio_take_profit_threshold(
            baseline_equity=10000.0,
            current_equity=10750.0,
            persisted_peak_equity=11000.0,
            profit_pct=9.0,
            giveback_pct=20.0,
            reduce_ratio=1.0,
            armed=True,
        )
        assert res.status == "TRIGGERED"
        assert res.should_trigger is True


class TestCalculateDynamicStopPrice:
    def test_respects_all_tighter_caps(self) -> None:
        price = calculate_dynamic_stop_price(
            base_sl_price=10.0,
            noon_cap_price=9.5,
            morning_cap_price=9.2,
            entry_structure_stop_price=9.0,
            old_sl_price=9.1,
        )
        assert price == 9.0

    def test_ignores_loose_caps(self) -> None:
        price = calculate_dynamic_stop_price(
            base_sl_price=10.0,
            noon_cap_price=12.0,
            morning_cap_price=11.0,
            entry_structure_stop_price=None,
            old_sl_price=10.5,
        )
        assert price == 10.0


class TestEvaluateDynamicStopCandidate:
    def test_skips_when_delta_within_tick_size_and_live(self) -> None:
        cand = DynamicStopEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-1.0,
            normalized_liq_sl_price=100.0,
            old_sl_price=100.01,
            tick_size=0.1,
            sl_is_live=True,
        )
        res = evaluate_dynamic_stop_candidate(cand)
        assert res.should_update is False
        assert res.skip_reason == "delta_within_tick_size"

    def test_updates_when_not_live_even_if_delta_small(self) -> None:
        cand = DynamicStopEvaluationInput(
            symbol="BTCUSDT",
            position_amt=-1.0,
            normalized_liq_sl_price=100.0,
            old_sl_price=100.01,
            tick_size=0.1,
            sl_is_live=False,  # e.g. order lost or cancelled
        )
        res = evaluate_dynamic_stop_candidate(cand)
        assert res.should_update is True
        assert res.intent is not None
        assert res.intent.target_price == 100.0

    def test_tightens_when_cap_is_lower(self) -> None:
        cand = DynamicStopEvaluationInput(
            symbol="ETHUSDT",
            position_amt=-2.0,
            normalized_liq_sl_price=3000.0,
            old_sl_price=3000.0,
            noon_cap_price=2950.0,
            tick_size=0.1,
            sl_is_live=True,
            tracked_position_id=88,
        )
        res = evaluate_dynamic_stop_candidate(cand)
        assert res.should_update is True
        assert res.target_sl_price == 2950.0
        assert res.intent is not None
        assert res.intent.target_price == 2950.0
        assert res.intent.qty == 2.0
        assert res.intent.position_id == 88


