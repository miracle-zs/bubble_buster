"""Test proving production database migration and verification."""

import os
import sqlite3
import unittest
from pathlib import Path

from scripts.migrate_historical_episodes_and_fills import HistoricalDataMigrator, MIGRATION_VERSION


class TestProductionDatabaseMigration(unittest.TestCase):
    def setUp(self):
        self.repo_root = Path(__file__).resolve().parents[1]
        self.prod_db_path = self.repo_root / "state.db"

    def test_production_database_migration_verified(self):
        """If state.db is present in the repository, it must pass migration verification."""
        if not self.prod_db_path.exists():
            self.skipTest("state.db not present in repository root")

        migrator = HistoricalDataMigrator(str(self.prod_db_path), dry_run=False)
        success, issues = migrator.verify()
        self.assertTrue(success, f"Production state.db migration issues: {issues}")
        self.assertEqual(len(issues), 0)

        with sqlite3.connect(str(self.prod_db_path)) as conn:
            # 1. Verify schema_migrations has recorded the backfill
            row = conn.execute(
                "SELECT version, applied_at_utc FROM schema_migrations WHERE version = ?",
                (MIGRATION_VERSION,),
            ).fetchone()
            self.assertIsNotNone(row, f"Migration version {MIGRATION_VERSION} not found in schema_migrations")

            # 2. Verify all positions have an episode_id
            missing_episode_count = conn.execute(
                "SELECT COUNT(*) FROM positions WHERE episode_id IS NULL OR episode_id = ''"
            ).fetchone()[0]
            self.assertEqual(missing_episode_count, 0, "Found positions with null or empty episode_id")

            # 3. Verify position_episodes table is populated
            episode_count = conn.execute("SELECT COUNT(*) FROM position_episodes").fetchone()[0]
            self.assertGreater(episode_count, 0, "position_episodes table should not be empty")

            # 4. Verify order_intents, order_attempts, execution_fills are populated
            intent_count = conn.execute("SELECT COUNT(*) FROM order_intents").fetchone()[0]
            attempt_count = conn.execute("SELECT COUNT(*) FROM order_attempts").fetchone()[0]
            fill_count = conn.execute("SELECT COUNT(*) FROM execution_fills").fetchone()[0]
            self.assertGreater(intent_count, 0, "order_intents should not be empty")
            self.assertGreater(attempt_count, 0, "order_attempts should not be empty")
            self.assertGreater(fill_count, 0, "execution_fills should not be empty")
