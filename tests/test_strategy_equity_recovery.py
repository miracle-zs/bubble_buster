import importlib.util
import unittest
from unittest.mock import MagicMock

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from core.strategy_top10_short import Top10ShortStrategy


class StrategyEquityRecoveryTest(unittest.TestCase):
    def _build_strategy(self, client: MagicMock, store: MagicMock) -> Top10ShortStrategy:
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
            equity_recovery_take_profit_enabled=True,
            equity_recovery_lookback_hours=24.0,
            equity_recovery_trigger_pct=0.10,
            equity_recovery_reduce_ratio=0.5,
        )

    @staticmethod
    def _mock_order_factory():
        state = {"seq": 0}

        def _create_order(**kwargs):
            state["seq"] += 1
            return {
                "orderId": 3000 + state["seq"],
                "symbol": kwargs["symbol"],
                "side": kwargs["side"],
                "type": kwargs["type"],
                "origQty": kwargs["quantity"],
                "status": "FILLED",
            }

        return _create_order

    def test_equity_recovery_triggers_once_per_cycle(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.get_latest_wallet_snapshot.return_value = {
            "captured_at_utc": "2026-02-23T07:40:00+00:00",
            "balance_usdt": 990.0,
        }
        store.get_wallet_snapshot_min_since.return_value = {
            "captured_at_utc": "2026-02-23T01:00:00+00:00",
            "balance_usdt": 900.0,
        }
        store.get_lock_state.side_effect = [
            None,
            {"cycle_key": "2026-02-23T01:00:00+00:00", "triggered": True},
        ]
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0},
            {"id": 2, "symbol": "BUSDT", "entry_price": 20.0},
        ]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-5", "markPrice": "20", "entryPrice": "20"},
            ]
        )
        strategy._sync_position_after_adjustment = MagicMock(return_value=True)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        first = strategy.run_equity_recovery_take_profit()
        second = strategy.run_equity_recovery_take_profit()

        self.assertEqual(first["status"], "TRIGGERED")
        self.assertEqual(first["adjusted"], 2)
        self.assertEqual(client.create_order.call_count, 2)
        self.assertEqual(second["status"], "SKIPPED")
        self.assertEqual(second["reason"], "ALREADY_TRIGGERED_IN_CYCLE")

    def test_equity_recovery_resets_window_start_at_trigger_time(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.get_latest_wallet_snapshot.side_effect = [
            {"captured_at_utc": "2026-02-23T07:40:00+00:00", "balance_usdt": 990.0},
            {"captured_at_utc": "2026-02-23T07:41:00+00:00", "balance_usdt": 992.0},
        ]
        store.get_wallet_snapshot_min_since.side_effect = [
            {"captured_at_utc": "2026-02-23T01:00:00+00:00", "balance_usdt": 900.0},
            {"captured_at_utc": "2026-02-23T07:40:00+00:00", "balance_usdt": 990.0},
        ]
        store.get_lock_state.side_effect = [
            None,
            {
                "cycle_key": "2026-02-23T07:40:00+00:00",
                "triggered": True,
                "window_start_utc": "2026-02-23T07:40:00+00:00",
            },
        ]
        store.list_open_positions.return_value = [{"id": 1, "symbol": "AUSDT", "entry_price": 10.0}]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            return_value={"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"}
        )
        strategy._sync_position_after_adjustment = MagicMock(return_value=True)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        first = strategy.run_equity_recovery_take_profit()
        second = strategy.run_equity_recovery_take_profit()

        self.assertEqual(first["status"], "TRIGGERED")
        self.assertEqual(second["status"], "SKIPPED")
        first_call = store.get_wallet_snapshot_min_since.call_args_list[0].kwargs
        second_call = store.get_wallet_snapshot_min_since.call_args_list[1].kwargs
        self.assertNotEqual(first_call["start_captured_at_utc"], "2026-02-23T07:40:00+00:00")
        self.assertEqual(second_call["start_captured_at_utc"], "2026-02-23T07:40:00+00:00")


if __name__ == "__main__":
    unittest.main()
