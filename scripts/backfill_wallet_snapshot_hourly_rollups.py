#!/usr/bin/env python3
"""Build derived hourly and four-hour wallet curve tables."""

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
    FOUR_HOUR_SECONDS,
    HOUR_SECONDS,
    _epoch_microseconds,
    insert_rollup_aggregates,
    merge_rollup_sample,
)

DEFAULT_DB = ROOT / "state.db"


def _bucket_start_epoch(epoch_us: int, bucket_seconds: int) -> int:
    bucket_width_us = bucket_seconds * 1_000_000
    return (epoch_us // bucket_width_us) * bucket_seconds


def backfill(db_path: Path) -> Tuple[int, int, int, int, float]:
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
        required_tables = {
            "wallet_snapshots",
            "wallet_snapshot_hourly_rollups",
            "wallet_snapshot_hourly_rollup_state",
            "wallet_snapshot_four_hour_rollups",
            "wallet_snapshot_four_hour_rollup_state",
        }
        if not required_tables.issubset(required):
            raise RuntimeError(
                "wallet rollup tables are missing; start the updated app once to apply schema.sql first"
            )

        conn.execute("BEGIN IMMEDIATE")
        account_ids = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT account_id FROM wallet_snapshots ORDER BY account_id"
            )
        ]
        for table in (
            "wallet_snapshot_hourly_rollups",
            "wallet_snapshot_hourly_rollup_state",
            "wallet_snapshot_four_hour_rollups",
            "wallet_snapshot_four_hour_rollup_state",
        ):
            conn.execute(f"DELETE FROM {table}")

        hourly: Dict[Tuple[str, int], tuple] = {}
        four_hour: Dict[Tuple[str, int], tuple] = {}
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
            candidate = (snapshot_id, captured_at_utc, balance_usdt, epoch_us)
            for bucket_seconds, aggregates in (
                (HOUR_SECONDS, hourly),
                (FOUR_HOUR_SECONDS, four_hour),
            ):
                bucket_start_epoch = _bucket_start_epoch(epoch_us, bucket_seconds)
                key = (account_id, bucket_start_epoch)
                aggregates[key] = merge_rollup_sample(
                    aggregates.get(key), candidate
                )
            snapshot_rows += 1

        hourly_rows = insert_rollup_aggregates(
            conn,
            (
                (account_id, bucket_start_epoch, samples)
                for (account_id, bucket_start_epoch), samples in hourly.items()
            ),
            HOUR_SECONDS,
        )
        four_hour_rows = insert_rollup_aggregates(
            conn,
            (
                (account_id, bucket_start_epoch, samples)
                for (account_id, bucket_start_epoch), samples in four_hour.items()
            ),
            FOUR_HOUR_SECONDS,
        )
        completed_at_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        for state_table in (
            "wallet_snapshot_hourly_rollup_state",
            "wallet_snapshot_four_hour_rollup_state",
        ):
            conn.executemany(
                f"INSERT INTO {state_table} (account_id, completed_at_utc) VALUES (?, ?)",
                ((account_id, completed_at_utc) for account_id in account_ids),
            )
        conn.commit()
        return (
            snapshot_rows,
            hourly_rows,
            four_hour_rows,
            len(account_ids),
            time.perf_counter() - started,
        )
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
    snapshots, hourly_rows, four_hour_rows, accounts, elapsed = backfill(args.db)
    print(
        f"Backfilled {snapshots:,} valid snapshots for {accounts:,} accounts into "
        f"{hourly_rows:,} hourly rows and {four_hour_rows:,} four-hour rows "
        f"in {elapsed:.2f}s."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
