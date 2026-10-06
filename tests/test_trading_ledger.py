"""Unit tests for TradingLedger transactional guarantees and episode lifecycle."""

import os
import tempfile
import unittest

from pathlib import Path
from core.execution.ledger import TradingLedger
from core.execution.models import (
    AttemptStatus,
    EpisodeStatus,
    ExecutionFill,
    IntentScope,
    IntentStatus,
    OrderAttempt,
    OrderIntent,
    PositionEpisode,
)
from core.state_store import StateStore


class TestTradingLedger(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_trading_ledger.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(
            db_path=self.db_path,
            schema_path=schema_path,
            account_id="acc01",
        )
        self.store.init_schema()
        self.ledger = TradingLedger(store=self.store)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_episode_open_and_close(self):
        """Episode creation and closure tracking."""
        ep = self.ledger.open_episode(
            episode_id="ep_001",
            symbol="BTCUSDT",
            position_side="SHORT",
            target_qty=1.5,
            current_qty=0.0,
        )
        self.assertEqual(ep.episode_id, "ep_001")
        self.assertEqual(ep.status, EpisodeStatus.OPEN.value)

        loaded = self.ledger.get_episode("ep_001")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.symbol, "BTCUSDT")

        open_list = self.ledger.list_open_episodes()
        self.assertEqual(len(open_list), 1)

        self.ledger.close_episode("ep_001", reason="TEST_CLOSE")
        loaded_closed = self.ledger.get_episode("ep_001")
        self.assertEqual(loaded_closed.status, EpisodeStatus.CLOSED.value)
        self.assertEqual(len(self.ledger.list_open_episodes()), 0)

    def test_record_intent_and_attempt(self):
        """Intent and attempt persistence and retrieval."""
        intent = OrderIntent(
            intent_id="intent_100",
            account_id="acc01",
            client_intent_key="key_100",
            symbol="ETHUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=5.0,
            intent_scope="ENTRY",
            episode_id="ep_eth_1",
        )
        self.ledger.record_intent(intent)

        loaded_intent = self.ledger.get_intent("intent_100")
        self.assertIsNotNone(loaded_intent)
        self.assertEqual(loaded_intent.symbol, "ETHUSDT")

        attempt = OrderAttempt(
            attempt_id="att_100",
            intent_id="intent_100",
            account_id="acc01",
            symbol="ETHUSDT",
            client_order_id="cid_100",
            status=AttemptStatus.ACKNOWLEDGED.value,
            submitted_qty=5.0,
        )
        self.ledger.record_attempt(attempt)

        loaded_att = self.ledger.get_attempt("att_100")
        self.assertIsNotNone(loaded_att)
        self.assertEqual(loaded_att.client_order_id, "cid_100")

    def test_fill_attribution_updates_episode_and_closes_on_exit(self):
        """Fills atomically update episode current_qty and close episode on full exit."""
        # 1. Open episode with entry
        self.ledger.open_episode(
            episode_id="ep_sol_1",
            symbol="SOLUSDT",
            position_side="SHORT",
            target_qty=10.0,
            current_qty=0.0,
        )

        entry_intent = OrderIntent(
            intent_id="in_entry",
            account_id="acc01",
            client_intent_key="k_entry",
            symbol="SOLUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=10.0,
            intent_scope="ENTRY",
            episode_id="ep_sol_1",
        )
        self.ledger.record_intent(entry_intent)
        self.ledger.record_attempt(
            OrderAttempt(
                attempt_id="att_e",
                intent_id="in_entry",
                account_id="acc01",
                symbol="SOLUSDT",
                client_order_id="cid_e",
                status=AttemptStatus.ACKNOWLEDGED.value,
                submitted_qty=10.0,
            )
        )

        # Entry fill: +10.0
        fill_entry = ExecutionFill(
            fill_id="f_1",
            attempt_id="att_e",
            intent_id="in_entry",
            account_id="acc01",
            symbol="SOLUSDT",
            exchange_trade_id="t_1",
            side="SELL",
            price=150.0,
            qty=10.0,
            commission=0.1,
            commission_asset="USDT",
            trade_time_utc="2026-10-05T00:00:00Z",
        )
        self.ledger.record_fill(fill_entry, episode_id="ep_sol_1", intent_scope="ENTRY")

        ep_after_entry = self.ledger.get_episode("ep_sol_1")
        self.assertEqual(ep_after_entry.current_qty, 10.0)
        self.assertEqual(ep_after_entry.status, EpisodeStatus.OPEN.value)

        # 2. Exit fill: -10.0
        exit_intent = OrderIntent(
            intent_id="in_exit",
            account_id="acc01",
            client_intent_key="k_exit",
            symbol="SOLUSDT",
            side="BUY",
            order_type="MARKET",
            target_qty=10.0,
            intent_scope="EXIT",
            episode_id="ep_sol_1",
        )
        self.ledger.record_intent(exit_intent)
        self.ledger.record_attempt(
            OrderAttempt(
                attempt_id="att_x",
                intent_id="in_exit",
                account_id="acc01",
                symbol="SOLUSDT",
                client_order_id="cid_x",
                status=AttemptStatus.ACKNOWLEDGED.value,
                submitted_qty=10.0,
            )
        )

        fill_exit = ExecutionFill(
            fill_id="f_2",
            attempt_id="att_x",
            intent_id="in_exit",
            account_id="acc01",
            symbol="SOLUSDT",
            exchange_trade_id="t_2",
            side="BUY",
            price=145.0,
            qty=10.0,
            commission=0.1,
            commission_asset="USDT",
            trade_time_utc="2026-10-05T01:00:00Z",
        )
        self.ledger.record_fill(fill_exit, episode_id="ep_sol_1", intent_scope="EXIT")

        ep_after_exit = self.ledger.get_episode("ep_sol_1")
        self.assertEqual(ep_after_exit.current_qty, 0.0)
        self.assertEqual(ep_after_exit.status, EpisodeStatus.CLOSED.value)
        self.assertIsNotNone(ep_after_exit.closed_at_utc)


if __name__ == "__main__":
    unittest.main()
