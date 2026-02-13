import importlib.util
import unittest
from unittest.mock import MagicMock

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from binance_futures_client import BinanceAPIError
from strategy_top10_short import Top10ShortStrategy


class StrategyOrderRetryTest(unittest.TestCase):
    def _build_strategy(self, client: MagicMock) -> Top10ShortStrategy:
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


if __name__ == "__main__":
    unittest.main()
