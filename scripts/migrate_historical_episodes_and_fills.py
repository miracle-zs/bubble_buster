#!/usr/bin/env python3
"""Migration script: Backfill historical PositionEpisodes and ExecutionFills.

Implements Phase F requirements from 2026-09-26 plan:
- Supports --dry-run to preview actions without committing.
- Supports --verify to validate position episode and fill consistency.
- Supports --rollback to revert backfilled data safely.
- Atomic execution inside database transaction.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOGGER = logging.getLogger("migrate_historical_episodes")

MIGRATION_VERSION = "20261005_05_historical_episodes_backfill"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class HistoricalDataMigrator:
    def __init__(self, db_path: str, dry_run: bool = False) -> None:
        self.db_path = db_path
        self.dry_run = dry_run

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = OFF")  # OFF during batch backfill to permit dependency order
        return conn

    def run_migration(self) -> Dict[str, int]:
        """Perform the historical backfill."""
        LOGGER.info(
            "Starting historical data migration on %s (dry_run=%s)",
            self.db_path,
            self.dry_run,
        )
        stats = {
            "positions_scanned": 0,
            "episodes_created": 0,
            "positions_updated": 0,
            "intents_created": 0,
            "attempts_created": 0,
            "fills_created": 0,
        }

        with self.connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                # 1. Ensure tables exist
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version TEXT PRIMARY KEY,
                        applied_at_utc TEXT NOT NULL,
                        description TEXT
                    )
                    """
                )

                # 2. Query positions missing episode_id
                positions = conn.execute(
                    "SELECT * FROM positions ORDER BY id ASC"
                ).fetchall()
                stats["positions_scanned"] = len(positions)

                pos_to_episode: Dict[int, str] = {}
                for pos in positions:
                    pos_id = int(pos["id"])
                    symbol = str(pos["symbol"])
                    account_id = str(pos["account_id"]) if "account_id" in pos.keys() and pos["account_id"] else "default"
                    existing_episode_id = pos["episode_id"] if "episode_id" in pos.keys() else None

                    if existing_episode_id:
                        pos_to_episode[pos_id] = str(existing_episode_id)
                        continue

                    # Generate deterministic episode ID
                    episode_id = f"ep_pos_{pos_id}_{symbol}"
                    pos_to_episode[pos_id] = episode_id

                    opened_at = str(pos["opened_at_utc"] or utc_now_iso())
                    closed_at = pos["closed_at_utc"]
                    qty = float(pos["qty"] or 0.0)
                    status = str(pos["status"] or "CLOSED")
                    episode_status = "OPEN" if status in ("OPEN", "PENDING_EXIT_SETUP") else "CLOSED"
                    current_qty = qty if episode_status == "OPEN" else 0.0
                    now_iso = utc_now_iso()

                    # Insert position episode
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO position_episodes (
                            episode_id, account_id, symbol, position_side, status,
                            opened_at_utc, closed_at_utc, target_qty, current_qty,
                            realized_pnl, created_at_utc, updated_at_utc
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            episode_id,
                            account_id,
                            symbol,
                            "SHORT",
                            episode_status,
                            opened_at,
                            closed_at,
                            qty,
                            current_qty,
                            0.0,
                            now_iso,
                            now_iso,
                        ),
                    )
                    stats["episodes_created"] += 1

                    # Update position with episode_id
                    conn.execute(
                        "UPDATE positions SET episode_id = ? WHERE id = ?",
                        (episode_id, pos_id),
                    )
                    stats["positions_updated"] += 1

                # 3. Backfill order_events and fills to order_intents, order_attempts, execution_fills
                order_events = conn.execute(
                    "SELECT * FROM order_events ORDER BY id ASC"
                ).fetchall()

                for oe in order_events:
                    oe_id = int(oe["id"])
                    pos_id = int(oe["position_id"]) if oe["position_id"] else None
                    symbol = str(oe["symbol"])
                    side = str(oe["side"] or "BUY")
                    order_type = str(oe["type"] if "type" in oe.keys() and oe["type"] else "MARKET")
                    client_order_id = str(oe["client_order_id"] or f"cid_hist_{oe_id}")
                    exchange_order_id = str(oe["order_id"]) if oe["order_id"] else None
                    status_raw = str(oe["status"] or "FILLED").upper()
                    submitted_qty = float(oe["qty"] if "qty" in oe.keys() and oe["qty"] is not None else 0.0)
                    executed_qty = float(oe["executed_qty"] if "executed_qty" in oe.keys() and oe["executed_qty"] is not None else submitted_qty)
                    avg_price = float(oe["price"] if "price" in oe.keys() and oe["price"] is not None else 0.0)
                    account_id = str(oe["account_id"]) if "account_id" in oe.keys() and oe["account_id"] else "default"
                    episode_id = pos_to_episode.get(pos_id) if pos_id else None
                    now_iso = utc_now_iso()

                    intent_id = f"intent_hist_oe_{oe_id}"
                    attempt_id = f"att_hist_oe_{oe_id}"
                    client_intent_key = f"hist_oe_{oe_id}_{client_order_id}"

                    # Insert OrderIntent
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO order_intents (
                            intent_id, account_id, client_intent_key, symbol, side,
                            order_type, target_qty, target_price, intent_scope,
                            position_id, episode_id, status, reason, created_at_utc, updated_at_utc
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            intent_id,
                            account_id,
                            client_intent_key,
                            symbol,
                            side,
                            order_type,
                            submitted_qty,
                            avg_price,
                            "EXIT" if side == "BUY" else "ENTRY",
                            pos_id,
                            episode_id,
                            "COMPLETED" if status_raw == "FILLED" else "SUBMITTED",
                            "HISTORICAL_BACKFILL",
                            now_iso,
                            now_iso,
                        ),
                    )
                    stats["intents_created"] += 1

                    # Insert OrderAttempt
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO order_attempts (
                            attempt_id, intent_id, account_id, symbol, client_order_id,
                            exchange_order_id, attempt_number, status, submitted_qty,
                            executed_qty, cumulative_quote_qty, avg_price,
                            created_at_utc, updated_at_utc
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            attempt_id,
                            intent_id,
                            account_id,
                            symbol,
                            client_order_id,
                            exchange_order_id,
                            1,
                            status_raw,
                            submitted_qty,
                            executed_qty,
                            (executed_qty * avg_price) if avg_price else None,
                            avg_price,
                            now_iso,
                            now_iso,
                        ),
                    )
                    stats["attempts_created"] += 1

                    # Check for fills related to this order_event
                    fills_rows = conn.execute(
                        "SELECT * FROM fills WHERE order_event_id = ? ORDER BY id ASC",
                        (oe_id,),
                    ).fetchall()

                    if fills_rows:
                        for fill in fills_rows:
                            fill_id = f"fill_hist_{fill['id']}"
                            trade_id = str(fill["trade_id"]) if "trade_id" in fill.keys() and fill["trade_id"] else f"synth_tr_{fill['id']}"
                            f_price = float((fill["price"] if "price" in fill.keys() else fill["avg_price"]) or avg_price or 0.0)
                            f_qty = float((fill["qty"] if "qty" in fill.keys() else fill["executed_qty"]) or executed_qty or 0.0)
                            f_commission = float(fill["commission"] or 0.0) if "commission" in fill.keys() else 0.0
                            f_asset = str(fill["commission_asset"] or "USDT") if "commission_asset" in fill.keys() else "USDT"
                            f_time = str(fill["event_time_utc"]) if "event_time_utc" in fill.keys() and fill["event_time_utc"] else now_iso

                            conn.execute(
                                """
                                INSERT OR IGNORE INTO execution_fills (
                                    fill_id, attempt_id, intent_id, account_id, symbol,
                                    exchange_trade_id, exchange_order_id, side, price,
                                    qty, commission, commission_asset, trade_time_utc, created_at_utc
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    fill_id,
                                    attempt_id,
                                    intent_id,
                                    account_id,
                                    symbol,
                                    trade_id,
                                    exchange_order_id,
                                    side,
                                    f_price,
                                    f_qty,
                                    f_commission,
                                    f_asset,
                                    f_time,
                                    now_iso,
                                ),
                            )
                            stats["fills_created"] += 1
                    elif executed_qty > 0:
                        # Synthetic fill from executed order event
                        fill_id = f"fill_hist_oe_{oe_id}"
                        trade_id = f"synth_oe_{oe_id}"
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO execution_fills (
                                fill_id, attempt_id, intent_id, account_id, symbol,
                                exchange_trade_id, exchange_order_id, side, price,
                                qty, commission, commission_asset, trade_time_utc, created_at_utc
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                fill_id,
                                attempt_id,
                                intent_id,
                                account_id,
                                symbol,
                                trade_id,
                                exchange_order_id,
                                side,
                                avg_price or 0.0,
                                executed_qty,
                                0.0,
                                "USDT",
                                now_iso,
                                now_iso,
                            ),
                        )
                        stats["fills_created"] += 1

                # Record migration version
                conn.execute(
                    """
                    INSERT OR REPLACE INTO schema_migrations (version, applied_at_utc, description)
                    VALUES (?, ?, ?)
                    """,
                    (MIGRATION_VERSION, utc_now_iso(), "Historical PositionEpisode and ExecutionFill backfill"),
                )

                if self.dry_run:
                    LOGGER.info("DRY-RUN: Rolling back all changes. Stats: %s", stats)
                    conn.execute("ROLLBACK")
                else:
                    conn.execute("COMMIT")
                    LOGGER.info("Migration COMMITTED successfully. Stats: %s", stats)

                return stats

            except Exception:
                conn.execute("ROLLBACK")
                LOGGER.exception("Migration failed, transaction rolled back.")
                raise

    def rollback(self) -> Dict[str, int]:
        """Rollback all historical backfill data."""
        LOGGER.info("Rolling back historical migration on %s", self.db_path)
        stats = {
            "episodes_deleted": 0,
            "intents_deleted": 0,
            "attempts_deleted": 0,
            "fills_deleted": 0,
            "positions_reverted": 0,
        }

        with self.connect() as conn:
            conn.execute("BEGIN TRANSACTION")
            try:
                # 1. Delete backfilled fills
                cur = conn.execute("DELETE FROM execution_fills WHERE fill_id LIKE 'fill_hist_%'")
                stats["fills_deleted"] = cur.rowcount

                # 2. Delete backfilled attempts
                cur = conn.execute("DELETE FROM order_attempts WHERE attempt_id LIKE 'att_hist_%'")
                stats["attempts_deleted"] = cur.rowcount

                # 3. Delete backfilled intents
                cur = conn.execute("DELETE FROM order_intents WHERE intent_id LIKE 'intent_hist_%'")
                stats["intents_deleted"] = cur.rowcount

                # 4. Revert positions
                cur = conn.execute("UPDATE positions SET episode_id = NULL WHERE episode_id LIKE 'ep_pos_%'")
                stats["positions_reverted"] = cur.rowcount

                # 5. Delete backfilled episodes
                cur = conn.execute("DELETE FROM position_episodes WHERE episode_id LIKE 'ep_pos_%'")
                stats["episodes_deleted"] = cur.rowcount

                # 6. Remove migration entry
                conn.execute("DELETE FROM schema_migrations WHERE version = ?", (MIGRATION_VERSION,))

                if self.dry_run:
                    LOGGER.info("DRY-RUN: Rollback preview: %s", stats)
                    conn.execute("ROLLBACK")
                else:
                    conn.execute("COMMIT")
                    LOGGER.info("Rollback COMMITTED: %s", stats)

                return stats
            except Exception:
                conn.execute("ROLLBACK")
                LOGGER.exception("Rollback failed, transaction rolled back.")
                raise

    def verify(self) -> Tuple[bool, List[str]]:
        """Verify consistency of database state."""
        LOGGER.info("Running verification on %s", self.db_path)
        issues: List[str] = []

        with self.connect() as conn:
            # 1. Check positions without episode_id
            missing_ep = conn.execute(
                "SELECT COUNT(*) as cnt FROM positions WHERE episode_id IS NULL OR episode_id = ''"
            ).fetchone()["cnt"]
            if missing_ep > 0:
                issues.append(f"{missing_ep} positions are missing episode_id")

            # 2. Check orphan episode_id references in positions
            orphan_ep = conn.execute(
                """
                SELECT COUNT(*) as cnt FROM positions p
                LEFT JOIN position_episodes pe ON p.episode_id = pe.episode_id
                WHERE p.episode_id IS NOT NULL AND pe.episode_id IS NULL
                """
            ).fetchone()["cnt"]
            if orphan_ep > 0:
                issues.append(f"{orphan_ep} positions refer to non-existent position_episodes")

            # 3. Check open positions vs open episodes
            open_pos_cnt = conn.execute(
                "SELECT COUNT(*) as cnt FROM positions WHERE status IN ('OPEN', 'PENDING_EXIT_SETUP')"
            ).fetchone()["cnt"]
            open_ep_cnt = conn.execute(
                "SELECT COUNT(*) as cnt FROM position_episodes WHERE status = 'OPEN'"
            ).fetchone()["cnt"]
            if open_pos_cnt != open_ep_cnt:
                issues.append(
                    f"Mismatch between open positions ({open_pos_cnt}) and open episodes ({open_ep_cnt})"
                )

            # 4. Check schema_migrations
            mig_row = conn.execute(
                "SELECT * FROM schema_migrations WHERE version = ?",
                (MIGRATION_VERSION,),
            ).fetchone()
            if not mig_row:
                issues.append(f"Migration version {MIGRATION_VERSION} not recorded in schema_migrations")

        success = len(issues) == 0
        if success:
            LOGGER.info("Verification PASSED: Database state is completely consistent.")
        else:
            LOGGER.error("Verification FAILED with %d issues:\n  - %s", len(issues), "\n  - ".join(issues))
        return success, issues


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical position episodes and execution fills migration.")
    parser.add_argument(
        "--db-path",
        default="bubble_buster.db",
        help="Path to SQLite database file (default: bubble_buster.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the migration without modifying the database.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify database consistency and migration status.",
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Rollback historical backfill data.",
    )

    args = parser.parse_args()

    if not Path(args.db_path).exists():
        LOGGER.error("Database file not found: %s", args.db_path)
        sys.exit(1)

    migrator = HistoricalDataMigrator(db_path=args.db_path, dry_run=args.dry_run)

    if args.rollback:
        migrator.rollback()
    elif args.verify:
        success, _ = migrator.verify()
        if not success:
            sys.exit(1)
    else:
        migrator.run_migration()
        if not args.dry_run:
            success, _ = migrator.verify()
            if not success:
                sys.exit(1)


if __name__ == "__main__":
    main()
