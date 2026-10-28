#!/usr/bin/env python3
"""Build the derived hourly curve table used by equity comparison."""

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.wallet_snapshot_rollup import (  # noqa: E402
    HOUR_MICROSECONDS,
    _epoch_microseconds,
    insert_hourly_rollup_aggregates,
    merge_hourly_sample,
)

DEFAULT_DB = ROOT / "state.db"


def backfill(db_path: Path) -> Tuple[int, int, int, float]:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 30000")
    started = time.perf_counter()
    try:
        required = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        if not {
            "wallet_snapshots",
            "wallet_snapshot_hourly_rollups",
            "wallet_snapshot_hourly_rollup_state",
        }.issubset(required):
            raise RuntimeError(
                "hourly rollup tables are missing; start the updated app once to apply schema.sql first"
            )

        conn.execute("BEGIN IMMEDIATE")
        account_ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT account_id FROM wallet_snapshots ORDER BY account_id"
            )
        ]
        conn.execute("DELETE FROM wallet_snapshot_hourly_rollups")
        conn.execute("DELETE FROM wallet_snapshot_hourly_rollup_state")

        aggregates: Dict[Tuple[str, int], tuple] = {}
        snapshot_rows = 0
        for snapshot_id, account_id, captured_at_utc, balance_usdt in conn.execute(
            """
            SELECT id, account_id, captured_at_utc, balance_usdt
            FROM wallet_snapshots
            WHERE error IS NULL
            ORDER BY account_id, captured_at_utc, id
            """
        ):
            account_id = str(account_id)
            snapshot_id = int(snapshot_id)
            captured_at_utc = str(captured_at_utc)
            balance_usdt = float(balance_usdt)
            epoch_us = _epoch_microseconds(captured_at_utc)
            hour_start_epoch = (epoch_us // HOUR_MICROSECONDS) * 3600
            key = (account_id, hour_start_epoch)
            candidate = (snapshot_id, captured_at_utc, balance_usdt, epoch_us)
            aggregates[key] = merge_hourly_sample(aggregates.get(key), candidate)
            snapshot_rows += 1

        rollup_rows = insert_hourly_rollup_aggregates(
            conn,
            (
                (account_id, hour_start_epoch, samples)
                for (account_id, hour_start_epoch), samples in aggregates.items()
            ),
        )
        completed_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        conn.executemany(
            """
            INSERT INTO wallet_snapshot_hourly_rollup_state (
                account_id, completed_at_utc
            ) VALUES (?, ?)
            """,
            ((account_id, completed_at_utc) for account_id in account_ids),
        )
        conn.commit()
        return snapshot_rows, rollup_rows, len(account_ids), time.perf_counter() - started
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help=f"SQLite database path (default: {DEFAULT_DB})",
    )
    args = parser.parse_args()
    snapshots, rollups, accounts, elapsed = backfill(args.db)
    print(
        f"Backfilled {snapshots:,} valid snapshots into {rollups:,} hourly rows "
        f"for {accounts:,} accounts in {elapsed:.2f}s."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
