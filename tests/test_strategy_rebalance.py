import importlib.util
import unittest
from unittest.mock import MagicMock

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from core.strategy_top10_short import Top10ShortStrategy


class StrategyRebalanceTest(unittest.TestCase):
    def _build_strategy(self, client: MagicMock, store: MagicMock, **overrides) -> Top10ShortStrategy:
        return Top10ShortStrategy(
            client=client,
            store=store,
            notifier=MagicMock(),
            leverage=2,
            top_n=10,
            volume_threshold=0.0,
            tp_price_drop_pct=20.0,
            sl_liq_buffer_pct=1.0,
            max_hold_hours=47.5,
            trigger_price_type="CONTRACT_PRICE",
            allocation_splits=10,
            entry_fee_buffer_pct=1.0,
            entry_shrink_retry_count=3,
            entry_shrink_step_pct=10.0,
            entry_rank_fetch_multiplier=3,
            ranker_max_workers=4,
            ranker_weight_limit_per_minute=1000,
            ranker_min_request_interval_ms=20,
            rebalance_enabled=True,
            rebalance_pre_entry_reduce=True,
            rebalance_after_entry=True,
            rebalance_utilization=overrides.get("rebalance_utilization", 0.9),
            rebalance_deadband_pct=overrides.get("rebalance_deadband_pct", 0.10),
            rebalance_min_adjust_notional_usdt=overrides.get("rebalance_min_adjust_notional_usdt", 5.0),
            rebalance_max_single_adjust_pct=overrides.get("rebalance_max_single_adjust_pct", 0.95),
            rebalance_max_adjust_orders=30,
            rebalance_mode=overrides.get("rebalance_mode", "equal_risk"),
            rebalance_age_decay_half_life_hours=overrides.get("rebalance_age_decay_half_life_hours", 36.0),
        )

    @staticmethod
    def _mock_order_factory():
        state = {"seq": 0}

        def _create_order(**kwargs):
            state["seq"] += 1
            return {
                "orderId": 1000 + state["seq"],
                "symbol": kwargs["symbol"],
                "side": kwargs["side"],
                "type": kwargs["type"],
                "origQty": kwargs["quantity"],
                "status": "FILLED",
            }

        return _create_order

    def test_rebalance_reduce_only_sells_nothing_and_reduces_old_positions(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
            {"symbol": "BUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
            {"id": 2, "symbol": "BUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-6", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-6", "entryPrice": "10"},
            ]
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=4, reduce_only=True, reason_tag="pre")

        self.assertEqual(int(summary["planned"]), 2)
        self.assertEqual(int(summary["adjusted"]), 2)
        self.assertEqual(int(summary["errors"]), 0)
        self.assertAlmostEqual(float(summary["reduced_notional"]), 80.0, places=6)
        self.assertAlmostEqual(float(summary["added_notional"]), 0.0, places=6)
        self.assertEqual(summary["mode"], "equal_risk")
        self.assertEqual(client.create_order.call_count, 2)
        for call in client.create_order.call_args_list:
            kwargs = call.kwargs
            self.assertEqual(kwargs["side"], "BUY")
            self.assertTrue(kwargs["reduceOnly"])
        client.ensure_isolated_and_leverage.assert_not_called()

    def test_rebalance_full_runs_reduce_then_increase(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
            {"symbol": "BUSDT", "positionAmt": "-2", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
            {"id": 2, "symbol": "BUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-9", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-3.9", "entryPrice": "10"},
            ]
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=2, reduce_only=False, reason_tag="post")

        self.assertEqual(int(summary["planned"]), 2)
        self.assertEqual(int(summary["adjusted"]), 2)
        self.assertEqual(client.create_order.call_count, 2)
        self.assertEqual(client.create_order.call_args_list[0].kwargs["side"], "BUY")
        self.assertEqual(client.create_order.call_args_list[1].kwargs["side"], "SELL")
        self.assertTrue(client.create_order.call_args_list[0].kwargs["reduceOnly"])
        self.assertNotIn("reduceOnly", client.create_order.call_args_list[1].kwargs)
        client.ensure_isolated_and_leverage.assert_called_once_with("BUSDT", 2)

    def test_rebalance_skips_when_within_deadband(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "55"}]
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store, rebalance_deadband_pct=0.10)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=1, reduce_only=False, reason_tag="post")

        self.assertEqual(int(summary["planned"]), 0)
        self.assertEqual(int(summary["adjusted"]), 0)
        client.create_order.assert_not_called()

    def test_rebalance_age_decay_biases_to_newer_positions(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {"symbol": "OLDUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
            {"symbol": "NEWUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.list_open_positions.return_value = [
            {
                "id": 1,
                "symbol": "OLDUSDT",
                "entry_price": 10.0,
                "opened_at_utc": "2026-02-13T00:00:00+00:00",
                "tp_order_id": None,
                "sl_order_id": None,
            },
            {
                "id": 2,
                "symbol": "NEWUSDT",
                "entry_price": 10.0,
                "opened_at_utc": "2026-02-15T00:00:00+00:00",
                "tp_order_id": None,
                "sl_order_id": None,
            },
        ]

        strategy = self._build_strategy(
            client,
            store,
            rebalance_mode="age_decay",
            rebalance_age_decay_half_life_hours=24.0,
            rebalance_deadband_pct=0.0,
        )
        strategy._utc_now_datetime = MagicMock(return_value=strategy._parse_iso_utc("2026-02-15T00:00:00+00:00"))
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "OLDUSDT", "positionAmt": "-6.4", "entryPrice": "10"},
                {"symbol": "NEWUSDT", "positionAmt": "-13.6", "entryPrice": "10"},
            ]
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=2, reduce_only=False, reason_tag="post")

        self.assertEqual(summary["mode"], "age_decay")
        self.assertEqual(int(summary["planned"]), 2)
        self.assertEqual(int(summary["adjusted"]), 2)
        self.assertEqual(client.create_order.call_args_list[0].kwargs["symbol"], "OLDUSDT")
        self.assertEqual(client.create_order.call_args_list[0].kwargs["side"], "BUY")
        self.assertEqual(client.create_order.call_args_list[1].kwargs["symbol"], "NEWUSDT")
        self.assertEqual(client.create_order.call_args_list[1].kwargs["side"], "SELL")

    def test_rebalance_age_decay_falls_back_when_opened_time_missing(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store, rebalance_mode="age_decay", rebalance_deadband_pct=0.10)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=1, reduce_only=False, reason_tag="post")

        self.assertEqual(summary["mode"], "age_decay")
        self.assertEqual(int(summary["planned"]), 0)
        self.assertEqual(int(summary["adjusted"]), 0)
        client.create_order.assert_not_called()


if __name__ == "__main__":
    unittest.main()
