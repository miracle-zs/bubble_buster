import importlib.util
import sys
import types
import unittest
from datetime import timedelta
from unittest.mock import MagicMock, patch

if importlib.util.find_spec("requests") is None:
    requests_stub = types.ModuleType("requests")

    class _DummySession:
        def __init__(self):
            self.headers = {}
            self.proxies = {}

        def mount(self, *_args, **_kwargs):
            return None

    class _DummyRequestException(Exception):
        pass

    requests_stub.Session = _DummySession
    requests_stub.RequestException = _DummyRequestException

    adapters_stub = types.ModuleType("requests.adapters")

    class _DummyHTTPAdapter:
        def __init__(self, *args, **kwargs):
            pass

    adapters_stub.HTTPAdapter = _DummyHTTPAdapter
    requests_stub.adapters = adapters_stub
    sys.modules["requests"] = requests_stub
    sys.modules["requests.adapters"] = adapters_stub

from infra.binance_futures_client import BinanceAPIError, BinanceRateLimitError, OrderStateUnknownError
from core.strategy_top10_short import Top10ShortStrategy


class StrategyOrderRetryTest(unittest.TestCase):
    def _build_strategy(self, client: MagicMock, **overrides) -> Top10ShortStrategy:
        return Top10ShortStrategy(
            client=client,
            store=MagicMock(),
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
            fixed_take_profit_enabled=overrides.get("fixed_take_profit_enabled", True),
        )

    def test_shrink_retry_on_insufficient_margin(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = [10.0, 9.0]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = [
            BinanceAPIError(-2019, "Margin is insufficient."),
            {"orderId": 123, "status": "FILLED", "type": "MARKET", "side": "SELL", "origQty": "9"},
        ]

        strategy = self._build_strategy(client)
        order, retry_count = strategy._place_market_short_with_shrink_retry(
            symbol="ABCUSDT",
            target_notional=100.0,
            reference_price=10.0,
            client_id_tag="ent",
        )

        self.assertEqual(order["orderId"], 123)
        self.assertEqual(retry_count, 1)
        self.assertEqual(client.normalize_order_qty.call_args_list[0].args[1], 100.0)
        self.assertAlmostEqual(client.normalize_order_qty.call_args_list[1].args[1], 90.0)
        self.assertEqual(client.create_order.call_count, 2)

    def test_non_margin_error_does_not_shrink_retry(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.return_value = 10.0
        client.format_order_qty.return_value = "10"
        client.create_order.side_effect = BinanceAPIError(-1111, "Precision is over the maximum defined.")

        strategy = self._build_strategy(client)

        with self.assertRaises(BinanceAPIError):
            strategy._place_market_short_with_shrink_retry(
                symbol="ABCUSDT",
                target_notional=100.0,
                reference_price=10.0,
                client_id_tag="ent",
            )

    def test_risk_off_market_close_happens_before_exit_cancellation(self) -> None:
        events = []
        client = MagicMock()
        client.format_order_qty.return_value = "10"
        client.get_user_trades.return_value = []

        def create_order(**_kwargs):
            events.append("MARKET")
            return {
                "orderId": 9001,
                "clientOrderId": "risk-off",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "10",
                "executedQty": "10",
                "status": "FILLED",
            }

        def cancel_order(**_kwargs):
            events.append("CANCEL")
            return {"status": "CANCELED"}

        client.create_order.side_effect = create_order
        client.cancel_order.side_effect = cancel_order
        strategy = self._build_strategy(client)
        strategy._load_short_position = MagicMock(
            return_value={"symbol": "ABCUSDT", "positionAmt": "-10", "entryPrice": "10"}
        )
        strategy.store.get_position.return_value = {
            "id": 7,
            "symbol": "ABCUSDT",
            "tp_order_id": 101,
            "tp_client_order_id": "tp-old",
            "sl_order_id": 102,
            "sl_client_order_id": "sl-old",
        }
        strategy.store.get_lock_state.return_value = {}

        result = strategy._force_close_position(
            position_id=7,
            symbol="ABCUSDT",
            reason="EXIT_SETUP_FAILED",
        )

        self.assertEqual(result["status"], "CLOSED_RISK_OFF")
        self.assertEqual(events, ["MARKET", "CANCEL", "CANCEL"])
        strategy.store.mark_position_closed.assert_called_once()
        self.assertEqual(client.create_order.call_count, 1)

    @patch("core.strategy_top10_short.time.sleep")
    def test_unknown_entry_order_recovers_from_exchange_short_without_resubmit(
        self,
        _sleep_mock: MagicMock,
    ) -> None:
        client = MagicMock()
        client.normalize_order_qty.return_value = 10.0
        client.format_order_qty.return_value = "10"
        client.create_order.side_effect = OrderStateUnknownError(
            symbol="ABCUSDT",
            client_order_id="ent-abc-1",
            cause=BinanceAPIError("NETWORK", "reset"),
        )
        strategy = self._build_strategy(client)
        strategy._load_short_position = MagicMock(
            side_effect=[
                None,
                {"symbol": "ABCUSDT", "positionAmt": "-10", "entryPrice": "10"},
            ]
        )

        order, retry_count = strategy._place_market_short_with_shrink_retry(
            symbol="ABCUSDT",
            target_notional=100.0,
            reference_price=10.0,
            client_id_tag="ent",
        )

        self.assertEqual(retry_count, 0)
        self.assertEqual(order["status"], "POSITION_RECONCILED")
        self.assertEqual(order["clientOrderId"], "ent-abc-1")
        self.assertEqual(client.create_order.call_count, 1)

    def test_pending_exit_recovery_defers_fresh_entry_setup(self) -> None:
        client = MagicMock()
        strategy = self._build_strategy(client)
        now = strategy._utc_now_datetime()
        strategy.store.list_pending_exit_setup_positions.return_value = [
            {
                "id": 7,
                "symbol": "ABCUSDT",
                "created_at_utc": now.replace(microsecond=0).isoformat(),
            }
        ]

        summary = strategy.recover_pending_exit_setups()

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["deferred"], 1)
        strategy.store.mark_position_open.assert_not_called()
        client.get_position_risk.assert_not_called()

    def test_pending_exit_recovery_defers_rate_limit_without_risk_off(self) -> None:
        client = MagicMock()
        strategy = self._build_strategy(client)
        now = strategy._utc_now_datetime()
        strategy.store.list_pending_exit_setup_positions.return_value = [
            {
                "id": 7,
                "symbol": "ABCUSDT",
                "created_at_utc": (now - timedelta(seconds=60)).isoformat(),
            }
        ]
        strategy._load_short_position = MagicMock(
            return_value={"symbol": "ABCUSDT", "positionAmt": "-10", "entryPrice": "10"}
        )
        strategy._place_exit_orders = MagicMock(
            side_effect=BinanceRateLimitError(
                code=-1003,
                message="Too many requests",
                http_status=429,
                retry_after_sec=60.0,
            )
        )
        strategy._force_close_position = MagicMock()

        summary = strategy.recover_pending_exit_setups()

        self.assertEqual(summary["deferred"], 1)
        self.assertEqual(summary["risk_off"], 0)
        strategy._force_close_position.assert_not_called()

    def test_exit_refresh_places_replacement_before_canceling_old_orders(self) -> None:
        client = MagicMock()
        client.cancel_order.side_effect = BinanceAPIError(-1001, "disconnected")
        strategy = self._build_strategy(client)
        strategy.store.list_open_positions.return_value = [
            {
                "id": 7,
                "symbol": "ABCUSDT",
                "tp_order_id": 101,
                "tp_client_order_id": "tp-old",
                "sl_order_id": 102,
                "sl_client_order_id": "sl-old",
            }
        ]
        strategy._place_exit_orders = MagicMock()

        strategy._refresh_exit_orders_for_positions({7})

        strategy._place_exit_orders.assert_called_once_with(position_id=7, symbol="ABCUSDT")
        strategy.store.set_position_error.assert_called_once()
        self.assertIn("cancel_order failed", strategy.store.set_position_error.call_args.args[1])

    def test_exit_refresh_keeps_old_orders_when_replacement_creation_fails(self) -> None:
        client = MagicMock()
        strategy = self._build_strategy(client)
        strategy.store.list_open_positions.return_value = [
            {
                "id": 7,
                "symbol": "ABCUSDT",
                "tp_order_id": 101,
                "tp_client_order_id": "tp-old",
                "sl_order_id": 102,
                "sl_client_order_id": "sl-old",
            }
        ]
        strategy._place_exit_orders = MagicMock(side_effect=RuntimeError("replacement failed"))

        strategy._refresh_exit_orders_for_positions({7})

        client.cancel_order.assert_not_called()
        strategy.store.set_position_error.assert_called_once()

    def test_margin_error_detected_by_message_when_code_missing(self) -> None:
        err = BinanceAPIError("NETWORK", "margin is insufficient")
        self.assertTrue(Top10ShortStrategy._is_insufficient_margin_error(err))

    def test_logs_margin_shortfall_when_entry_retries_exhausted(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = [10.0, 9.0, 8.1, 7.29]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = [
            BinanceAPIError(-2019, "Margin is insufficient."),
            BinanceAPIError(-2019, "Margin is insufficient."),
            BinanceAPIError(-2019, "Margin is insufficient."),
            BinanceAPIError(-2019, "Margin is insufficient."),
        ]
        client.get_available_balance.return_value = 5.0

        strategy = self._build_strategy(client)
        with patch("core.strategy_top10_short.LOGGER") as logger:
            with self.assertRaises(BinanceAPIError):
                strategy._place_market_short_with_shrink_retry(
                    symbol="ABCUSDT",
                    target_notional=100.0,
                    reference_price=10.0,
                    client_id_tag="ent",
                )

            margin_calls = [
                call
                for call in logger.warning.call_args_list
                if call.args and "Margin shortfall detail:" in str(call.args[0])
            ]
            self.assertEqual(len(margin_calls), 1)
            args = margin_calls[0].args
            self.assertEqual(args[1], "entry")
            self.assertEqual(args[2], "ABCUSDT")
            self.assertEqual(args[3], "SELL")

    def test_place_exit_orders_skips_tp_when_fixed_take_profit_disabled(self) -> None:
        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "ABCUSDT",
                "entryPrice": "100",
                "liquidationPrice": "120",
                "positionAmt": "-2",
            }
        ]
        client.normalize_trigger_price.side_effect = [118.8]
        client.format_trigger_price.return_value = "118.8"
        client.format_order_qty.return_value = "2"
        client.create_order.return_value = {
            "orderId": 222,
            "clientOrderId": "sl-only",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "2",
            "status": "NEW",
        }

        store = MagicMock()
        strategy = Top10ShortStrategy(
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
            fixed_take_profit_enabled=False,
        )

        strategy._place_exit_orders(position_id=123, symbol="ABCUSDT")

        client.create_order.assert_called_once()
        create_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(create_kwargs["type"], "STOP_MARKET")
        self.assertEqual(create_kwargs["stopPrice"], "118.8")
        update_kwargs = store.update_position_orders.call_args.kwargs
        self.assertIsNone(update_kwargs["tp_order_id"])
        self.assertIsNone(update_kwargs["tp_client_order_id"])
        self.assertIsNone(update_kwargs["tp_price"])
        self.assertEqual(update_kwargs["sl_order_id"], 222)

    def test_place_exit_orders_skips_all_initial_exit_orders_for_exempt_symbol(self) -> None:
        client = MagicMock()
        store = MagicMock()
        strategy = Top10ShortStrategy(
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
            protection_exempt_symbols={"XAUUSDT"},
        )

        strategy._place_exit_orders(position_id=123, symbol="XAUUSDT")

        client.get_position_risk.assert_not_called()
        client.create_order.assert_not_called()
        store.update_position_orders.assert_not_called()

    def test_place_exit_orders_uses_tighter_entry_structure_stop(self) -> None:
        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "ABCUSDT",
                "entryPrice": "100",
                "liquidationPrice": "120",
                "positionAmt": "-2",
            }
        ]
        client.normalize_trigger_price.side_effect = [118.8, 105.0]
        client.format_trigger_price.return_value = "105"
        client.create_order.return_value = {
            "orderId": 222,
            "clientOrderId": "sl-structure",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "2",
            "status": "NEW",
        }
        store = MagicMock()
        strategy = self._build_strategy(client)
        strategy.store = store
        strategy._entry_structure_protection_state.store = store
        strategy.fixed_take_profit_enabled = False

        strategy._place_exit_orders(
            position_id=123,
            symbol="ABCUSDT",
            entry_structure_stop_price=105.0,
        )

        self.assertEqual(client.create_order.call_args.kwargs["stopPrice"], "105")
        self.assertEqual(store.update_position_orders.call_args.kwargs["sl_price"], 105.0)

    def test_exit_order_falls_back_to_reduce_only_when_close_position_already_exists(self) -> None:
        client = MagicMock()
        client.format_order_qty.return_value = "2"
        client.create_order.side_effect = [
            BinanceAPIError(
                -4130,
                "An open stop or take profit order with GTE and closePosition in the direction is existing.",
            ),
            {
                "orderId": 333,
                "clientOrderId": "sl-structure",
                "type": "STOP_MARKET",
                "side": "BUY",
                "origQty": "2",
                "status": "NEW",
            },
        ]
        strategy = self._build_strategy(client)

        order = strategy._create_exit_order_with_fallback(
            symbol="ABCUSDT",
            order_type="STOP_MARKET",
            stop_price="105",
            qty=2.0,
            client_order_id="sl-structure",
        )

        self.assertEqual(order["orderId"], 333)
        self.assertEqual(client.create_order.call_count, 2)
        first_call = client.create_order.call_args_list[0].kwargs
        fallback_call = client.create_order.call_args_list[1].kwargs
        self.assertTrue(first_call["closePosition"])
        self.assertNotIn("closePosition", fallback_call)
        self.assertTrue(fallback_call["reduceOnly"])
        self.assertEqual(fallback_call["quantity"], "2")


if __name__ == "__main__":
    unittest.main()
