import importlib.util
import sys
import types
import unittest
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

from infra.binance_futures_client import BinanceAPIError
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
        self.assertEqual(client.create_order.call_count, 1)

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


if __name__ == "__main__":
    unittest.main()
