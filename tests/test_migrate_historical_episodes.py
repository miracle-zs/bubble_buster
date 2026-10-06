"""Unit tests for historical data migration script."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.migrate_historical_episodes_and_fills import HistoricalDataMigrator, MIGRATION_VERSION
from core.state_store import StateStore


class TestMigrateHistoricalEpisodes(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_hist_mig.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(
            db_path=self.db_path,
            schema_path=schema_path,
            account_id="acc01",
        )
        self.store.init_schema()

        # Seed some historical positions and order events without episode_id
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO positions (
                    id, run_id, symbol, side, qty, entry_price, status, opened_at_utc, expire_at_utc, created_at_utc, updated_at_utc
                ) VALUES (1, 'run-1', 'BTCUSDT', 'SHORT', 1.0, 50000.0, 'OPEN', '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z', '2026-09-01T00:00:00Z', '2026-09-01T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO positions (
                    id, run_id, symbol, side, qty, entry_price, status, opened_at_utc, closed_at_utc, expire_at_utc, created_at_utc, updated_at_utc
                ) VALUES (2, 'run-1', 'ETHUSDT', 'SHORT', 5.0, 3000.0, 'CLOSED', '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z', '2026-09-02T00:00:00Z', '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z')
                """
            )
            conn.execute(
                """
                INSERT INTO order_events (
                    id, account_id, position_id, symbol, client_order_id, order_id,
                    side, type, price, qty, status, event_time_utc
                ) VALUES (10, 'acc01', 1, 'BTCUSDT', 'cid_btc_10', 9001,
                          'SELL', 'MARKET', 50000.0, 1.0, 'FILLED',
                          '2026-09-01T00:00:00Z')
                """
            )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_dry_run_leaves_database_unmodified(self):
        """Dry-run must report migration stats without committing changes."""
        migrator = HistoricalDataMigrator(db_path=self.db_path, dry_run=True)
        stats = migrator.run_migration()

        self.assertEqual(stats["positions_scanned"], 2)
        self.assertEqual(stats["episodes_created"], 2)
        self.assertEqual(stats["intents_created"], 1)

        # Verify nothing written to DB
        with sqlite3.connect(self.db_path) as conn:
            ep_cnt = conn.execute("SELECT COUNT(*) FROM position_episodes").fetchone()[0]
            self.assertEqual(ep_cnt, 0)
            pos_1_ep = conn.execute("SELECT episode_id FROM positions WHERE id = 1").fetchone()[0]
            self.assertIsNone(pos_1_ep)

    def test_migration_and_verification_and_rollback(self):
        """Full migration, verification pass, and rollback cycle."""
        migrator = HistoricalDataMigrator(db_path=self.db_path, dry_run=False)

        # 1. Run migration
        stats = migrator.run_migration()
        self.assertEqual(stats["positions_scanned"], 2)
        self.assertEqual(stats["episodes_created"], 2)
        self.assertEqual(stats["positions_updated"], 2)
        self.assertEqual(stats["intents_created"], 1)
        self.assertEqual(stats["attempts_created"], 1)
        self.assertEqual(stats["fills_created"], 1)

        # 2. Verify state
        success, issues = migrator.verify()
        self.assertTrue(success, f"Issues: {issues}")
        self.assertEqual(len(issues), 0)

        with sqlite3.connect(self.db_path) as conn:
            pos_1 = conn.execute("SELECT episode_id FROM positions WHERE id = 1").fetchone()[0]
            self.assertEqual(pos_1, "ep_pos_1_BTCUSDT")

            ep_1 = conn.execute("SELECT * FROM position_episodes WHERE episode_id = 'ep_pos_1_BTCUSDT'").fetchone()
            self.assertIsNotNone(ep_1)
            self.assertEqual(ep_1[4], "OPEN")  # status is OPEN for pos 1

            ep_2 = conn.execute("SELECT * FROM position_episodes WHERE episode_id = 'ep_pos_2_ETHUSDT'").fetchone()
            self.assertIsNotNone(ep_2)
            self.assertEqual(ep_2[4], "CLOSED")  # status is CLOSED for pos 2

            fill_row = conn.execute("SELECT * FROM execution_fills WHERE fill_id = 'fill_hist_oe_10'").fetchone()
            self.assertIsNotNone(fill_row)

        # 3. Rollback
        rollback_stats = migrator.rollback()
        self.assertEqual(rollback_stats["episodes_deleted"], 2)
        self.assertEqual(rollback_stats["positions_reverted"], 2)
        self.assertEqual(rollback_stats["fills_deleted"], 1)

        # Post-rollback verification
        with sqlite3.connect(self.db_path) as conn:
            ep_cnt = conn.execute("SELECT COUNT(*) FROM position_episodes").fetchone()[0]
            self.assertEqual(ep_cnt, 0)
            pos_1_ep = conn.execute("SELECT episode_id FROM positions WHERE id = 1").fetchone()[0]
            self.assertIsNone(pos_1_ep)


if __name__ == "__main__":
    unittest.main()
