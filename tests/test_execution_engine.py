"""Tests for ExecutionEngine, OrderIntent, OrderAttempt, and unknown order recovery."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from infra.binance_futures_client import BinanceAPIError, OrderStateUnknownError
from core.execution.engine import ExecutionEngine, sanitize_client_order_id
from core.execution.models import AttemptStatus, EpisodeStatus, IntentStatus, OrderIntent
from core.state_store import StateStore


class ExecutionEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(
            db_path=self.db_path,
            schema_path=schema_path,
            account_id="acc_exec",
        )
        self.store.init_schema()
        self.client = MagicMock()
        self.client.format_order_qty.side_effect = lambda sym, qty: str(qty)
        self.reconciler = MagicMock()
        self.engine = ExecutionEngine(
            client=self.client,
            store=self.store,
            reconciler=self.reconciler,
            now_iso_fn=lambda: "2026-10-01T12:00:00Z",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_submit_intent_market_fill(self) -> None:
        self.client.create_order.return_value = {
            "orderId": 5001,
            "status": "FILLED",
            "executedQty": "0.5",
            "avgPrice": "50000.0",
            "cumQuote": "25000.0",
        }

        intent = OrderIntent(
            intent_id="intent-1",
            account_id="acc_exec",
            client_intent_key="close-btc-pos-1",
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            target_qty=0.5,
            intent_scope="EXIT",
            position_id=10,
        )

        attempt = self.engine.submit_intent(intent, reduce_only=True)

        self.assertEqual(attempt.status, AttemptStatus.FILLED.value)
        self.assertEqual(attempt.executed_qty, 0.5)
        self.assertEqual(attempt.avg_price, 50000.0)

        # Check intent in database
        saved_intent = self.store.get_order_intent("intent-1")
        self.assertIsNotNone(saved_intent)
        self.assertEqual(saved_intent["status"], IntentStatus.COMPLETED.value)

        # Check attempt in database
        saved_attempt = self.store.get_order_attempt(attempt.attempt_id)
        self.assertIsNotNone(saved_attempt)
        self.assertEqual(saved_attempt["status"], AttemptStatus.FILLED.value)

        # Reconciler was invoked
        self.reconciler.record_market_order.assert_called_once()

    def test_submit_intent_rejected_on_api_error(self) -> None:
        self.client.create_order.side_effect = BinanceAPIError(-2019, "Margin insufficient")

        intent = OrderIntent(
            intent_id="intent-2",
            account_id="acc_exec",
            client_intent_key="entry-eth-1",
            symbol="ETHUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=2.0,
            intent_scope="ENTRY",
        )

        attempt = self.engine.submit_intent(intent)

        self.assertEqual(attempt.status, AttemptStatus.REJECTED.value)
        self.assertIn("-2019", attempt.error_message or "")

        saved_intent = self.store.get_order_intent("intent-2")
        self.assertEqual(saved_intent["status"], IntentStatus.FAILED.value)

    def test_submit_intent_unknown_order_recovery_success(self) -> None:
        # First call times out / unknown state
        self.client.create_order.side_effect = OrderStateUnknownError(
            "SOLUSDT", "cid-unknown", BinanceAPIError(-1000, "Socket timeout")
        )

        # Recovery call finds the order was filled
        self.client.get_order.return_value = {
            "orderId": "7777",
            "clientOrderId": "cid-unknown",
            "status": "FILLED",
            "executedQty": "1.5",
            "avgPrice": "3000.0",
        }

        intent = OrderIntent(
            intent_id="intent-3",
            account_id="acc_exec",
            client_intent_key="close-sol-1",
            symbol="SOLUSDT",
            side="BUY",
            order_type="MARKET",
            target_qty=1.5,
            intent_scope="EXIT",
        )

        attempt = self.engine.submit_intent(intent, client_order_id="cid-unknown")

        # Should recover to FILLED
        self.assertEqual(attempt.status, AttemptStatus.FILLED.value)
        self.assertEqual(attempt.executed_qty, 1.5)
        self.assertEqual(attempt.exchange_order_id, "7777")

        saved_intent = self.store.get_order_intent("intent-3")
        self.assertEqual(saved_intent["status"], IntentStatus.COMPLETED.value)

    def test_submit_intent_unknown_order_not_found(self) -> None:
        self.client.create_order.side_effect = OrderStateUnknownError(
            "DOGEUSDT", "cid-not-found", BinanceAPIError(-1000, "Connection reset")
        )
        self.client.get_order.side_effect = BinanceAPIError(-2013, "Order does not exist")

        intent = OrderIntent(
            intent_id="intent-4",
            account_id="acc_exec",
            client_intent_key="rebalance-doge-1",
            symbol="DOGEUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=100.0,
            intent_scope="REBALANCE",
        )

        attempt = self.engine.submit_intent(intent, client_order_id="cid-not-found")

        # Should transition to REJECTED because exchange confirmed order doesn't exist
        self.assertEqual(attempt.status, AttemptStatus.REJECTED.value)
        saved_intent = self.store.get_order_intent("intent-4")
        self.assertEqual(saved_intent["status"], IntentStatus.FAILED.value)

    def test_position_episode_sync_on_exit_fill(self) -> None:
        self.store.save_position_episode(
            episode_id="ep-001",
            symbol="BTCUSDT",
            position_side="SHORT",
            status=EpisodeStatus.OPEN.value,
            target_qty=1.0,
            current_qty=1.0,
        )

        self.client.create_order.return_value = {
            "orderId": 8001,
            "status": "FILLED",
            "executedQty": "1.0",
            "avgPrice": "60000.0",
        }

        intent = OrderIntent(
            intent_id="intent-5",
            account_id="acc_exec",
            client_intent_key="exit-btc-ep-1",
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            target_qty=1.0,
            intent_scope="EXIT",
            episode_id="ep-001",
        )

        attempt = self.engine.submit_intent(intent, reduce_only=True)
        self.assertEqual(attempt.status, AttemptStatus.FILLED.value)

        episode = self.store.get_position_episode("ep-001")
        self.assertIsNotNone(episode)
        self.assertEqual(episode["status"], EpisodeStatus.CLOSED.value)
        self.assertEqual(episode["current_qty"], 0.0)
        self.assertEqual(episode["closed_at_utc"], "2026-10-01T12:00:00Z")

    def test_idempotent_duplicate_submit_intent_returns_existing_attempt(self) -> None:
        self.client.create_order.return_value = {
            "orderId": 9001,
            "status": "FILLED",
            "executedQty": "0.1",
            "avgPrice": "65000.0",
        }

        intent = OrderIntent(
            intent_id="intent-idemp-1",
            account_id="acc_exec",
            client_intent_key="idemp-btc-1",
            symbol="BTCUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=0.1,
            intent_scope="ENTRY",
        )

        attempt1 = self.engine.submit_intent(intent)
        self.assertEqual(attempt1.status, AttemptStatus.FILLED.value)
        self.assertEqual(self.client.create_order.call_count, 1)

        # Duplicate submit with identical client_intent_key
        intent_dup = OrderIntent(
            intent_id="intent-idemp-dup",
            account_id="acc_exec",
            client_intent_key="idemp-btc-1",
            symbol="BTCUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=0.1,
            intent_scope="ENTRY",
        )

        attempt2 = self.engine.submit_intent(intent_dup)
        # Must return the existing attempt and NOT place another order
        self.assertEqual(attempt2.attempt_id, attempt1.attempt_id)
        self.assertEqual(attempt2.status, AttemptStatus.FILLED.value)
        self.assertEqual(self.client.create_order.call_count, 1)

    def test_replacement_attempt_tracking_and_parent_link(self) -> None:
        # First call rejected
        self.client.create_order.side_effect = BinanceAPIError(-2010, "Order would immediately trigger")

        intent = OrderIntent(
            intent_id="intent-replace-1",
            account_id="acc_exec",
            client_intent_key="replace-key-1",
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            target_qty=0.2,
            intent_scope="EXIT",
        )

        att1 = self.engine.submit_intent(intent, client_order_id="cid_first")
        self.assertEqual(att1.status, AttemptStatus.REJECTED.value)
        self.assertEqual(att1.attempt_number, 1)
        self.assertIsNone(att1.parent_attempt_id)

        # Second call succeeds
        self.client.create_order.side_effect = None
        self.client.create_order.return_value = {
            "orderId": 9002,
            "status": "FILLED",
            "executedQty": "0.2",
            "avgPrice": "64000.0",
        }

        att2 = self.engine.submit_intent(intent, client_order_id="cid_first")
        self.assertEqual(att2.status, AttemptStatus.FILLED.value)
        self.assertEqual(att2.attempt_number, 2)
        self.assertEqual(att2.parent_attempt_id, att1.attempt_id)
        self.assertEqual(att2.client_order_id, "cid_first_2")

        attempts = self.store.list_order_attempts_for_intent(intent.intent_id)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0]["attempt_id"], att1.attempt_id)
        self.assertEqual(attempts[1]["attempt_id"], att2.attempt_id)
        self.assertEqual(attempts[1]["parent_attempt_id"], att1.attempt_id)

    def test_execution_fills_recording(self) -> None:
        self.client.create_order.return_value = {
            "orderId": 9003,
            "status": "FILLED",
            "executedQty": "0.4",
            "avgPrice": "62000.0",
            "fills": [
                {"id": 101, "price": "62000.0", "qty": "0.2", "commission": "0.01", "commissionAsset": "USDT"},
                {"id": 102, "price": "62000.0", "qty": "0.2", "commission": "0.01", "commissionAsset": "USDT"},
            ],
        }

        intent = OrderIntent(
            intent_id="intent-fill-test",
            account_id="acc_exec",
            client_intent_key="fills-test-1",
            symbol="BTCUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=0.4,
            intent_scope="ENTRY",
        )

        attempt = self.engine.submit_intent(intent)
        fills = self.store.list_execution_fills_for_attempt(attempt.attempt_id)
        self.assertEqual(len(fills), 2)
        self.assertEqual(fills[0]["exchange_trade_id"], "101")
        self.assertEqual(fills[0]["qty"], 0.2)
        self.assertEqual(fills[1]["exchange_trade_id"], "102")
        self.assertEqual(fills[1]["qty"], 0.2)

    def test_position_episode_sync_on_entry_and_scale_in(self) -> None:
        # 1. First entry
        self.client.create_order.return_value = {
            "orderId": 9004,
            "status": "FILLED",
            "executedQty": "2.0",
            "avgPrice": "3000.0",
        }

        intent1 = OrderIntent(
            intent_id="intent-entry-1",
            account_id="acc_exec",
            client_intent_key="entry-eth-ep",
            symbol="ETHUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=2.0,
            intent_scope="ENTRY",
            episode_id="ep-eth-001",
        )

        attempt1 = self.engine.submit_intent(intent1)
        self.assertEqual(attempt1.status, AttemptStatus.FILLED.value)

        ep = self.store.get_position_episode("ep-eth-001")
        self.assertIsNotNone(ep)
        self.assertEqual(ep["status"], EpisodeStatus.OPEN.value)
        self.assertEqual(ep["current_qty"], 2.0)
        self.assertEqual(ep["target_qty"], 2.0)

        # 2. Scale-in entry on same episode
        self.client.create_order.return_value = {
            "orderId": 9005,
            "status": "FILLED",
            "executedQty": "1.5",
            "avgPrice": "3100.0",
        }

        intent2 = OrderIntent(
            intent_id="intent-entry-2",
            account_id="acc_exec",
            client_intent_key="scalein-eth-ep",
            symbol="ETHUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=1.5,
            intent_scope="ENTRY",
            episode_id="ep-eth-001",
        )

        attempt2 = self.engine.submit_intent(intent2)
        self.assertEqual(attempt2.status, AttemptStatus.FILLED.value)

        ep_updated = self.store.get_position_episode("ep-eth-001")
        self.assertIsNotNone(ep_updated)
        self.assertEqual(ep_updated["status"], EpisodeStatus.OPEN.value)
        self.assertEqual(ep_updated["current_qty"], 3.5)
        self.assertEqual(ep_updated["target_qty"], 3.5)

    def test_cancel_order_via_execution_engine(self) -> None:
        self.store.save_order_intent(
            intent_id="intent-to-cancel",
            client_intent_key="intent-key-cancel",
            symbol="BTCUSDT",
            side="SELL",
            order_type="LIMIT",
        )
        self.store.save_order_attempt(
            attempt_id="att-cancel-me",
            intent_id="intent-to-cancel",
            symbol="BTCUSDT",
            client_order_id="cid-cancel-me",
            status=AttemptStatus.ACKNOWLEDGED.value,
        )

        self.client.cancel_order.return_value = {
            "symbol": "BTCUSDT",
            "orderId": 12345,
            "clientOrderId": "cid-cancel-me",
            "status": "CANCELED",
        }

        resp = self.engine.cancel_order("BTCUSDT", client_order_id="cid-cancel-me")
        self.assertEqual(resp["status"], "CANCELED")

        # Check attempt updated to CANCELED
        att = self.store.get_order_attempt("att-cancel-me")
        self.assertIsNotNone(att)
        self.assertEqual(att["status"], AttemptStatus.CANCELED.value)

        # Check cancel intent logged
        intent = self.store.get_order_intent_by_key("cancel_BTCUSDT_cid-cancel-me")
        self.assertIsNotNone(intent)
        self.assertEqual(intent["status"], IntentStatus.COMPLETED.value)

    def test_sanitize_client_order_id(self) -> None:
        import re
        legal_regex = re.compile(r"^[.A-Z:/a-z0-9_-]{1,36}$")

        # Chinese characters sanitized
        res = sanitize_client_order_id("nsl_龙虾USDT_1791354391176_4")
        self.assertTrue(legal_regex.match(res), f"Regex mismatch: {res}")
        self.assertNotIn("龙虾", res)
        self.assertLessEqual(len(res), 36)

        # Normal ASCII string preserved
        res2 = sanitize_client_order_id("nsl_BTCUSDT_1791354391176")
        self.assertTrue(legal_regex.match(res2))
        self.assertEqual(res2, "nsl_BTCUSDT_1791354391176")

        # Very long string truncated with hash
        long_cid = "a" * 50
        res3 = sanitize_client_order_id(long_cid)
        self.assertTrue(legal_regex.match(res3))
        self.assertLessEqual(len(res3), 36)

    def test_submit_intent_stop_market_omits_price_and_sets_stopprice(self) -> None:
        self.client.create_order.return_value = {
            "orderId": 6001,
            "status": "NEW",
            "clientOrderId": "cid-stop-1",
        }

        intent = OrderIntent(
            intent_id="intent-stop-1",
            account_id="acc_exec",
            client_intent_key="stop-loss-pos-1",
            symbol="NIGHTUSDT",
            side="BUY",
            order_type="STOP_MARKET",
            target_price=0.08,
            intent_scope="PROTECTION",
            position_id=8023,
        )

        attempt = self.engine.submit_intent(intent, close_position=True, stop_price="0.08")
        self.assertEqual(attempt.status, AttemptStatus.ACKNOWLEDGED.value)

        # Verify create_order was called WITHOUT 'price' and WITH 'stopPrice' and 'closePosition'
        self.client.create_order.assert_called_once()
        call_kwargs = self.client.create_order.call_args.kwargs
        self.assertNotIn("price", call_kwargs)
        self.assertIn("stopPrice", call_kwargs)
        self.assertEqual(call_kwargs["stopPrice"], "0.08")
        self.assertTrue(call_kwargs.get("closePosition"))
        self.assertNotIn("quantity", call_kwargs)
        self.assertNotIn("reduceOnly", call_kwargs)

    def test_submit_intent_sanitizes_chinese_symbol_cid(self) -> None:
        self.client.create_order.return_value = {
            "orderId": 6002,
            "status": "NEW",
        }

        intent = OrderIntent(
            intent_id="intent-cn-1",
            account_id="acc_exec",
            client_intent_key="stop-loss-pos-cn",
            symbol="龙虾USDT",
            side="BUY",
            order_type="STOP_MARKET",
            target_price=0.05,
            intent_scope="PROTECTION",
        )

        attempt = self.engine.submit_intent(intent, client_order_id="nsl_龙虾USDT_1234567890", close_position=True)
        self.assertNotIn("龙虾", attempt.client_order_id)
        self.assertLessEqual(len(attempt.client_order_id), 36)
        call_kwargs = self.client.create_order.call_args.kwargs
        self.assertNotIn("龙虾", call_kwargs["newClientOrderId"])
        self.assertNotIn("price", call_kwargs)


