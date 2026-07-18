import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

from core.market_fill_reconciler import MarketFillReconciler
from core.state_store import StateStore


class MarketFillReconcilerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(
            db_path=self.db_path,
            schema_path=schema_path,
            account_id="acc01",
        )
        self.store.init_schema()
        self.run_id, _ = self.store.create_run("2026-07-18", account_id="acc01")
        now = datetime(2026, 7, 18, 1, 0, tzinfo=timezone.utc)
        self.position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="BTCUSDT",
            side="SHORT",
            qty=0.02,
            entry_price=50000.0,
            liq_price_open=60000.0,
            tp_price=None,
            sl_price=59000.0,
            tp_order_id=None,
            sl_order_id=12,
            tp_client_order_id=None,
            sl_client_order_id="sl-old",
            opened_at_utc=now.isoformat(),
            expire_at_utc=(now + timedelta(days=1)).isoformat(),
            status="OPEN",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_records_user_trade_price_and_realized_pnl_immediately(self) -> None:
        client = MagicMock()
        client.get_user_trades.return_value = [
            {
                "orderId": 9001,
                "side": "BUY",
                "qty": "0.01",
                "quoteQty": "490",
                "price": "49000",
                "realizedPnl": "10",
                "commission": "0.2",
                "commissionAsset": "USDT",
                "time": 1784337000000,
            },
            {
                "orderId": 9001,
                "side": "BUY",
                "qty": "0.01",
                "quoteQty": "488",
                "price": "48800",
                "realizedPnl": "12",
                "commission": "0.2",
                "commissionAsset": "USDT",
                "time": 1784337001000,
            },
        ]
        reconciler = MarketFillReconciler(client, self.store)

        recorded = reconciler.record_market_order(
            symbol="BTCUSDT",
            position_id=self.position_id,
            order={
                "orderId": 9001,
                "clientOrderId": "close-btc",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "0.02",
                "executedQty": "0.02",
                "status": "FILLED",
                "reduceOnly": True,
            },
        )

        self.assertTrue(recorded)
        fill = self._get_priced_fill()
        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertAlmostEqual(float(fill["avg_price"]), 48900.0)
        self.assertAlmostEqual(float(fill["realized_pnl"]), 22.0)
        state = self.store.get_lock_state(MarketFillReconciler.LOCK_NAME) or {}
        self.assertFalse(state.get("items"))

    def test_persists_missing_fill_and_retries_later(self) -> None:
        client = MagicMock()
        client.get_user_trades.side_effect = [
            [],
            [
                {
                    "orderId": 9002,
                    "side": "BUY",
                    "qty": "0.02",
                    "quoteQty": "980",
                    "price": "49000",
                    "realizedPnl": "20",
                    "commission": "0.4",
                    "commissionAsset": "USDT",
                    "time": 1784337000000,
                }
            ],
        ]
        reconciler = MarketFillReconciler(client, self.store)
        order = {
            "orderId": 9002,
            "clientOrderId": "close-btc-late",
            "type": "MARKET",
            "side": "BUY",
            "origQty": "0.02",
            "executedQty": "0.02",
            "status": "FILLED",
            "reduceOnly": True,
        }

        recorded = reconciler.record_market_order("BTCUSDT", self.position_id, order)

        self.assertFalse(recorded)
        state = self.store.get_lock_state(MarketFillReconciler.LOCK_NAME) or {}
        items = state.get("items")
        self.assertIsInstance(items, dict)
        assert isinstance(items, dict)
        self.assertEqual(len(items), 1)
        pending = next(iter(items.values()))
        retry_at = datetime.fromisoformat(str(pending["next_retry_at_utc"]))

        summary = reconciler.reconcile_pending(now_utc=retry_at + timedelta(seconds=1))

        self.assertEqual(summary["reconciled"], 1)
        self.assertEqual(summary["pending"], 0)
        fill = self._get_priced_fill()
        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertAlmostEqual(float(fill["avg_price"]), 49000.0)
        self.assertAlmostEqual(float(fill["realized_pnl"]), 20.0)

    def test_partial_user_trades_are_not_persisted_as_complete_fill(self) -> None:
        client = MagicMock()
        client.get_user_trades.return_value = [
            {
                "orderId": 9010,
                "side": "BUY",
                "qty": "0.01",
                "quoteQty": "490",
                "price": "49000",
                "realizedPnl": "10",
                "time": 1784337000000,
            }
        ]
        reconciler = MarketFillReconciler(client, self.store)

        recorded = reconciler.record_market_order(
            "BTCUSDT",
            self.position_id,
            {
                "orderId": 9010,
                "type": "MARKET",
                "side": "BUY",
                "executedQty": "0.02",
                "status": "FILLED",
            },
        )

        self.assertFalse(recorded)
        self.assertIsNone(self._get_priced_fill())
        state = self.store.get_lock_state(MarketFillReconciler.LOCK_NAME) or {}
        self.assertEqual(len(state.get("items") or {}), 1)

    def test_discovers_and_backfills_existing_unpriced_market_close(self) -> None:
        self.store.add_order_event(
            symbol="BTCUSDT",
            position_id=self.position_id,
            event_time_utc="2026-07-18T02:00:00+00:00",
            order_payload={
                "orderId": 9003,
                "clientOrderId": "t10s-to-BTC-1",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "0.02",
                "executedQty": "0.02",
                "status": "FILLED",
                "reduceOnly": True,
            },
        )
        self.store.mark_position_closed(
            position_id=self.position_id,
            status="CLOSED_TIMEOUT",
            close_reason="MAX_HOLD_EXCEEDED",
            close_order_id=9003,
        )
        client = MagicMock()
        client.get_user_trades.return_value = [
            {
                "orderId": 9003,
                "side": "BUY",
                "qty": "0.02",
                "quoteQty": "970",
                "price": "48500",
                "realizedPnl": "30",
                "commission": "0.4",
                "commissionAsset": "USDT",
                "time": 1784337000000,
            }
        ]
        reconciler = MarketFillReconciler(client, self.store)

        summary = reconciler.reconcile_persisted_missing()

        self.assertEqual(summary["found"], 1)
        self.assertEqual(summary["reconciled"], 1)
        fill = self._get_priced_fill()
        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertAlmostEqual(float(fill["avg_price"]), 48500.0)
        self.assertAlmostEqual(float(fill["realized_pnl"]), 30.0)

    def test_retry_state_failure_does_not_turn_successful_exchange_close_into_error(self) -> None:
        client = MagicMock()
        client.get_user_trades.return_value = []
        store = MagicMock()
        store.account_id = "acc01"
        store.add_order_event.return_value = 88
        store.get_lock_state.return_value = {}
        store.set_lock_state.side_effect = RuntimeError("database busy")
        reconciler = MarketFillReconciler(client, store)

        recorded = reconciler.record_market_order(
            symbol="BTCUSDT",
            position_id=7,
            order={
                "orderId": 9004,
                "type": "MARKET",
                "side": "BUY",
                "origQty": "1",
                "executedQty": "1",
                "status": "FILLED",
            },
        )

        self.assertFalse(recorded)
        store.add_order_event.assert_called_once()

    def _get_priced_fill(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute(
                """
                SELECT *
                FROM fills
                WHERE position_id = ? AND avg_price IS NOT NULL AND realized_pnl IS NOT NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.position_id,),
            ).fetchone()


if __name__ == "__main__":
    unittest.main()
