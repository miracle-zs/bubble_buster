"""Unit tests for pure RebalanceCalculator logic."""

from datetime import datetime, timezone
import pytest

from core.strategy.rebalance import RebalanceCalculator


class TestRebalanceCalculator:
    def test_parse_iso_utc(self):
        dt_naive = RebalanceCalculator.parse_iso_utc("2026-03-01T12:00:00")
        assert dt_naive.tzinfo == timezone.utc
        assert dt_naive.hour == 12

        dt_aware = RebalanceCalculator.parse_iso_utc("2026-03-01T14:00:00+02:00")
        assert dt_aware.tzinfo == timezone.utc
        assert dt_aware.hour == 12

    def test_position_age_hours(self):
        now_utc = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        pos = {"opened_at_utc": "2026-03-01T10:00:00+00:00"}
        age = RebalanceCalculator.position_age_hours(pos, now_utc)
        assert pytest.approx(age) == 2.0

        # Empty or invalid opened_at_utc
        assert RebalanceCalculator.position_age_hours({}, now_utc) == 0.0
        assert RebalanceCalculator.position_age_hours({"opened_at_utc": "invalid"}, now_utc) == 0.0

        # Future date (negative delta)
        future_pos = {"opened_at_utc": "2026-03-01T14:00:00+00:00"}
        assert RebalanceCalculator.position_age_hours(future_pos, now_utc) == 0.0

    def test_age_decay_weight(self):
        # age <= 0 -> 1.0
        assert RebalanceCalculator.age_decay_weight(0.0, 24.0) == 1.0
        assert RebalanceCalculator.age_decay_weight(-5.0, 24.0) == 1.0

        # age = half_life -> 0.5
        assert pytest.approx(RebalanceCalculator.age_decay_weight(24.0, 24.0)) == 0.5

        # age = 2 * half_life -> 0.25
        assert pytest.approx(RebalanceCalculator.age_decay_weight(48.0, 24.0)) == 0.25

        # large age -> bound at 1e-4
        assert RebalanceCalculator.age_decay_weight(1000.0, 24.0) == 1e-4

    def test_build_target_notional_map_equal_risk(self):
        positions = [
            {"id": 1, "symbol": "BTCUSDT"},
            {"id": 2, "symbol": "ETHUSDT"},
        ]
        target_map, virtual_slots = RebalanceCalculator.build_target_notional_map(
            positions=positions,
            target_count=4,
            target_gross_notional=1000.0,
            rebalance_mode="equal_risk",
        )
        assert virtual_slots == 2
        assert target_map[1] == 250.0
        assert target_map[2] == 250.0

    def test_build_target_notional_map_age_decay(self):
        now_utc = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        positions = [
            {"id": 1, "symbol": "BTCUSDT", "opened_at_utc": "2026-03-01T12:00:00+00:00"},  # age 0 -> weight 1.0
            {"id": 2, "symbol": "ETHUSDT", "opened_at_utc": "2026-02-28T12:00:00+00:00"},  # age 24 -> weight 0.5
        ]
        # target_count = 3 (1 virtual slot with weight 1.0)
        # total_weight = 1.0 + 0.5 + 1.0 = 2.5
        target_map, virtual_slots = RebalanceCalculator.build_target_notional_map(
            positions=positions,
            target_count=3,
            target_gross_notional=2500.0,
            rebalance_mode="age_decay",
            now_utc=now_utc,
            half_life_hours=24.0,
        )
        assert virtual_slots == 1
        assert pytest.approx(target_map[1]) == 2500.0 * (1.0 / 2.5)  # 1000.0
        assert pytest.approx(target_map[2]) == 2500.0 * (0.5 / 2.5)  # 500.0

    def test_build_target_notional_map_empty_or_zero(self):
        assert RebalanceCalculator.build_target_notional_map([], 5, 1000.0) == ({}, 0)
        assert RebalanceCalculator.build_target_notional_map([{"id": 1}], 0, 1000.0) == ({}, 0)
        assert RebalanceCalculator.build_target_notional_map([{"id": 1}], 5, 0.0) == ({}, 0)

    def test_build_rebalance_plan_skipped_conditions(self):
        pos = {"id": 10, "symbol": "SOLUSDT", "entry_price": 100.0}
        dummy_norm = lambda symbol, notional, price: notional / price

        # Missing risk
        plan, eval_data = RebalanceCalculator.build_rebalance_plan(
            pos=pos,
            risk_map={},
            target_notional=100.0,
            reduce_only=False,
            deadband_pct=0.1,
            max_single_adjust_pct=0.5,
            min_adjust_notional_usdt=10.0,
            normalize_qty_fn=dummy_norm,
        )
        assert plan is None
        assert eval_data["reason"] == "MISSING_POSITION_RISK"

        # Non short position (amt >= 0)
        plan, eval_data = RebalanceCalculator.build_rebalance_plan(
            pos=pos,
            risk_map={"SOLUSDT": {"positionAmt": "1.0", "markPrice": "100.0"}},
            target_notional=100.0,
            reduce_only=False,
            deadband_pct=0.1,
            max_single_adjust_pct=0.5,
            min_adjust_notional_usdt=10.0,
            normalize_qty_fn=dummy_norm,
        )
        assert plan is None
        assert eval_data["reason"] == "NON_SHORT_POSITION"

        # Within deadband (deviation <= deadband)
        # current notional = 100.0, target = 105.0, deviation = 5.0, deadband = 105 * 0.1 = 10.5
        plan, eval_data = RebalanceCalculator.build_rebalance_plan(
            pos=pos,
            risk_map={"SOLUSDT": {"positionAmt": "-1.0", "markPrice": "100.0"}},
            target_notional=105.0,
            reduce_only=False,
            deadband_pct=0.1,
            max_single_adjust_pct=0.5,
            min_adjust_notional_usdt=10.0,
            normalize_qty_fn=dummy_norm,
        )
        assert plan is None
        assert eval_data["reason"] == "WITHIN_DEADBAND"

        # Reduce only blocked increase (deviation > 0 means current < target, need to SELL more short)
        plan, eval_data = RebalanceCalculator.build_rebalance_plan(
            pos=pos,
            risk_map={"SOLUSDT": {"positionAmt": "-1.0", "markPrice": "100.0"}},
            target_notional=200.0,
            reduce_only=True,
            deadband_pct=0.1,
            max_single_adjust_pct=0.5,
            min_adjust_notional_usdt=10.0,
            normalize_qty_fn=dummy_norm,
        )
        assert plan is None
        assert eval_data["reason"] == "REDUCE_ONLY_BLOCKED_INCREASE"

    def test_build_rebalance_plan_success(self):
        # Current notional = 200 (amt = -2.0, markPrice = 100), target = 100 -> deviation = -100
        # Reduce position -> side = BUY
        pos = {"id": 10, "symbol": "SOLUSDT"}
        risk_map = {"SOLUSDT": {"positionAmt": "-2.0", "markPrice": "100.0"}}
        norm_fn = lambda sym, notional, price: round(notional / price, 4)

        plan, eval_data = RebalanceCalculator.build_rebalance_plan(
            pos=pos,
            risk_map=risk_map,
            target_notional=100.0,
            reduce_only=False,
            deadband_pct=0.05,
            max_single_adjust_pct=0.6,
            min_adjust_notional_usdt=10.0,
            normalize_qty_fn=norm_fn,
        )
        assert plan is not None
        assert plan.position_id == 10
        assert plan.symbol == "SOLUSDT"
        assert plan.side == "BUY"
        # max adjust = 200 * 0.6 = 120. min(100, 120) = 100.
        assert plan.requested_adjust_notional == 100.0
        assert plan.qty == 1.0
        assert plan.est_notional == 100.0
        assert eval_data["status"] == "PLANNED"
