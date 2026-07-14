import unittest

from scripts.backfill_missing_exit_fills import (
    aggregate_trade_rows,
    build_fill_from_order_event_row,
    should_skip_recovered_fill,
)


class BackfillMissingExitFillsTest(unittest.TestCase):
    def test_build_fill_from_order_event_row_uses_filled_buy_payload(self) -> None:
        row = {
            "id": 114,
            "position_id": 2,
            "symbol": "VVVUSDT",
            "order_id": 829587626,
            "client_order_id": "t10s-close-VVVUSDT",
            "type": "MARKET",
            "side": "BUY",
            "price": 0.0,
            "qty": 70.11,
            "status": "FILLED",
            "event_time_utc": "2026-02-15T03:55:00+00:00",
            "raw_json": '{"avgPrice":"3.26500000","executedQty":"70.11","cumQuote":"228.90915000","commission":"0.09156366","commissionAsset":"USDT","realizedPnl":"-4.22","reduceOnly":true}',
        }

        fill = build_fill_from_order_event_row(row)

        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertEqual(fill["side"], "BUY")
        self.assertEqual(fill["position_id"], 2)
        self.assertEqual(fill["order_id"], 829587626)
        self.assertAlmostEqual(fill["executed_qty"], 70.11)
        self.assertAlmostEqual(fill["avg_price"], 3.265)
        self.assertAlmostEqual(fill["quote_qty"], 228.90915)

    def test_aggregate_trade_rows_sums_same_order(self) -> None:
        trades = [
            {
                "symbol": "BTRUSDT",
                "orderId": 615013993,
                "id": 1,
                "side": "BUY",
                "qty": "100",
                "quoteQty": "21.5",
                "price": "0.215",
                "commission": "0.01",
                "commissionAsset": "USDT",
                "realizedPnl": "-1.1",
                "time": 1739591702000,
                "buyer": True,
                "maker": False,
            },
            {
                "symbol": "BTRUSDT",
                "orderId": 615013993,
                "id": 2,
                "side": "BUY",
                "qty": "310",
                "quoteQty": "67.8185",
                "price": "0.2187693548",
                "commission": "0.03",
                "commissionAsset": "USDT",
                "realizedPnl": "-2.4",
                "time": 1739591702500,
                "buyer": True,
                "maker": False,
            },
        ]

        agg = aggregate_trade_rows(position_id=12, symbol="BTRUSDT", trades=trades)

        self.assertEqual(agg["side"], "BUY")
        self.assertEqual(agg["position_id"], 12)
        self.assertEqual(agg["order_id"], 615013993)
        self.assertAlmostEqual(agg["executed_qty"], 410.0)
        self.assertAlmostEqual(agg["quote_qty"], 89.3185)
        self.assertAlmostEqual(agg["avg_price"], 89.3185 / 410.0)
        self.assertAlmostEqual(agg["commission"], 0.04)
        self.assertAlmostEqual(agg["realized_pnl"], -3.5)

    def test_should_not_skip_when_filled_order_event_exists_but_fill_missing(self) -> None:
        self.assertFalse(
            should_skip_recovered_fill(
                has_existing_buy_fill=False,
                has_matching_buy_fill=False,
            )
        )
        self.assertFalse(
            should_skip_recovered_fill(
                has_existing_buy_fill=True,
                has_matching_buy_fill=False,
            )
        )
        self.assertTrue(
            should_skip_recovered_fill(
                has_existing_buy_fill=True,
                has_matching_buy_fill=True,
                has_complete_matching_buy_fill=True,
            )
        )


if __name__ == "__main__":
    unittest.main()
