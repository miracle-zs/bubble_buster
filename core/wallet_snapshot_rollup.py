"""First/min/max/last summaries for dashboard wallet curves."""

import logging
import sqlite3
from datetime import datetime, timezone
from functools import lru_cache
from typing import Iterable, List, Optional, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

HOUR_SECONDS = 3600
FOUR_HOUR_SECONDS = 4 * HOUR_SECONDS
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SAMPLE_KINDS = ("first", "min", "max", "last")
_SAMPLE_FIELDS = ("snapshot_id", "captured_at_utc", "balance_usdt", "epoch_us")
_ROLLUP_SAMPLE_SELECT = ", ".join(
    f"{kind}_{field}" for kind in _SAMPLE_KINDS for field in _SAMPLE_FIELDS
)
_ROLLUP_CONFIGS = {
    HOUR_SECONDS: (
        "wallet_snapshot_hourly_rollups",
        "hour_start_epoch",
        "wallet_snapshot_hourly_rollup_state",
    ),
    FOUR_HOUR_SECONDS: (
        "wallet_snapshot_four_hour_rollups",
        "bucket_start_epoch",
        "wallet_snapshot_four_hour_rollup_state",
    ),
}


def _epoch_microseconds(captured_at_utc: str) -> int:
    captured_at = datetime.fromisoformat(str(captured_at_utc))
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    delta = captured_at.astimezone(timezone.utc) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _rollup_config(bucket_seconds: int) -> Tuple[str, str, str]:
    try:
        return _ROLLUP_CONFIGS[bucket_seconds]
    except KeyError as exc:
        raise ValueError(f"unsupported wallet snapshot rollup interval: {bucket_seconds}") from exc


def _rollup_columns(bucket_column: str) -> Tuple[str, ...]:
    return (
        "account_id",
        bucket_column,
        *(f"{kind}_{field}" for kind in _SAMPLE_KINDS for field in _SAMPLE_FIELDS),
    )


@lru_cache(maxsize=2)
def _rollup_sql(bucket_seconds: int) -> Tuple[str, str, str, str]:
    table_name, bucket_column, state_table = _rollup_config(bucket_seconds)
    columns = _rollup_columns(bucket_column)
    update_columns = ", ".join(
        f"{column} = excluded.{column}" for column in columns[2:]
    )
    upsert_sql = (
        f"INSERT INTO {table_name} ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)}) "
        f"ON CONFLICT(account_id, {bucket_column}) DO UPDATE SET "
        f"{update_columns}"
    )
    return table_name, bucket_column, state_table, upsert_sql


def _sample_set(
    snapshot_id: int,
    captured_at_utc: str,
    balance_usdt: float,
    bucket_seconds: int,
) -> Tuple[int, Tuple[Tuple[int, str, float, int], ...]]:
    epoch_us = _epoch_microseconds(captured_at_utc)
    bucket_width_us = bucket_seconds * 1_000_000
    bucket_start_epoch = (epoch_us // bucket_width_us) * bucket_seconds
    sample = (int(snapshot_id), str(captured_at_utc), float(balance_usdt), epoch_us)
    return bucket_start_epoch, (sample, sample, sample, sample)


def _rollup_params(
    account_id: str,
    bucket_start_epoch: int,
    samples: Sequence[Tuple[int, str, float, int]],
) -> Tuple[object, ...]:
    return account_id, bucket_start_epoch, *(value for sample in samples for value in sample)


def merge_rollup_sample(
    current: Optional[Sequence[Tuple[int, str, float, int]]],
    candidate: Tuple[int, str, float, int],
) -> Tuple[Tuple[int, str, float, int], ...]:
    """Merge a row into a bucket's first/min/max/last sample set."""
    if current is None:
        return (candidate, candidate, candidate, candidate)

    first, minimum, maximum, last = current
    candidate_key = (candidate[3], candidate[0])
    if candidate_key < (first[3], first[0]):
        first = candidate
    if candidate_key > (last[3], last[0]):
        last = candidate
    if candidate[2] < minimum[2] or (
        candidate[2] == minimum[2] and candidate_key < (minimum[3], minimum[0])
    ):
        minimum = candidate
    if candidate[2] > maximum[2] or (
        candidate[2] == maximum[2] and candidate_key < (maximum[3], maximum[0])
    ):
        maximum = candidate
    return first, minimum, maximum, last


def _upsert_one_rollup(
    conn: sqlite3.Connection,
    account_id: str,
    snapshot_id: int,
    captured_at_utc: str,
    balance_usdt: float,
    bucket_seconds: int,
) -> None:
    table_name, bucket_column, state_table, upsert_sql = _rollup_sql(bucket_seconds)
    bucket_start_epoch, incoming = _sample_set(
        snapshot_id, captured_at_utc, balance_usdt, bucket_seconds
    )
    try:
        current_row = conn.execute(
            f"SELECT {_ROLLUP_SAMPLE_SELECT} FROM {table_name} "
            f"WHERE account_id = ? AND {bucket_column} = ?",
            (account_id, bucket_start_epoch),
        ).fetchone()
        current = None
        if current_row is not None:
            current = tuple(
                tuple(current_row[offset + j] for j in range(4))
                for offset in (0, 4, 8, 12)
            )
        samples = merge_rollup_sample(current, incoming[0])
        conn.execute(
            upsert_sql, _rollup_params(account_id, bucket_start_epoch, samples)
        )
    except sqlite3.Error:
        # A derived table is optional; invalidate only its marker so queries
        # continue with a different complete rollup or the source snapshots.
        try:
            conn.execute(
                f"DELETE FROM {state_table} WHERE account_id = ?", (account_id,)
            )
        except sqlite3.Error:
            pass
        raise


def upsert_wallet_snapshot_rollups(
    conn: sqlite3.Connection,
    account_id: str,
    snapshot_id: int,
    captured_at_utc: str,
    balance_usdt: float,
) -> None:
    """Update hourly and four-hour summaries beside their source snapshot."""
    account_id = (account_id or "").strip()
    if not account_id:
        return

    for bucket_seconds in (HOUR_SECONDS, FOUR_HOUR_SECONDS):
        try:
            _upsert_one_rollup(
                conn,
                account_id,
                snapshot_id,
                captured_at_utc,
                balance_usdt,
                bucket_seconds,
            )
        except sqlite3.Error as exc:
            LOGGER.warning(
                "%s-second wallet rollup update failed for %s; source snapshots remain usable: %s",
                bucket_seconds,
                account_id,
                exc,
            )


def insert_rollup_aggregates(
    conn: sqlite3.Connection,
    aggregates: Iterable[
        Tuple[str, int, Sequence[Tuple[int, str, float, int]]]
    ],
    bucket_seconds: int,
) -> int:
    """Persist pre-aggregated buckets during a backfill transaction."""
    table_name, _bucket_column, _state_table, upsert_sql = _rollup_sql(bucket_seconds)
    params = (
        _rollup_params(account_id, bucket_start_epoch, samples)
        for account_id, bucket_start_epoch, samples in aggregates
    )
    conn.executemany(upsert_sql, params)
    return conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def load_rollup_wallet_snapshot_candidates(
    conn: sqlite3.Connection,
    account_id: str,
    window_start_utc: str,
    bucket_seconds: int,
) -> Optional[List[Tuple[int, str, float]]]:
    """Load interval samples if backfill is complete; otherwise return None.

    Read source rows only from a partial leading bucket. The newest bucket may
    be incomplete in clock time, but its rollup contains every row through the
    query's latest snapshot.
    """
    table_name, bucket_column, state_table, _upsert_sql = _rollup_sql(bucket_seconds)
    complete = conn.execute(
        f"SELECT 1 FROM {state_table} WHERE account_id = ?", (account_id,)
    ).fetchone()
    if complete is None:
        return None

    bucket_width_us = bucket_seconds * 1_000_000
    start_epoch_us = _epoch_microseconds(window_start_utc)
    first_bucket_start_epoch = (
        (start_epoch_us + bucket_width_us - 1) // bucket_width_us
    ) * bucket_seconds
    candidates: List[Tuple[int, str, float, int]] = []

    if start_epoch_us % bucket_width_us:
        edge_end_utc = datetime.fromtimestamp(
            first_bucket_start_epoch, timezone.utc
        ).isoformat()
        edge_rows = conn.execute(
            """
            SELECT id, captured_at_utc, balance_usdt
            FROM wallet_snapshots
            WHERE account_id = ? AND error IS NULL
              AND captured_at_utc >= ? AND captured_at_utc < ?
            ORDER BY captured_at_utc ASC, id ASC
            """,
            (account_id, window_start_utc, edge_end_utc),
        ).fetchall()
        candidates.extend(
            (
                int(row["id"]),
                str(row["captured_at_utc"]),
                float(row["balance_usdt"]),
                _epoch_microseconds(str(row["captured_at_utc"])),
            )
            for row in edge_rows
        )

    rollup_rows = conn.execute(
        f"""
        SELECT {_ROLLUP_SAMPLE_SELECT}
        FROM {table_name}
        WHERE account_id = ? AND {bucket_column} >= ?
        ORDER BY {bucket_column} ASC
        """,
        (account_id, first_bucket_start_epoch),
    ).fetchall()
    for row in rollup_rows:
        for offset in (0, 4, 8, 12):
            candidates.append(
                (
                    int(row[offset]),
                    str(row[offset + 1]),
                    float(row[offset + 2]),
                    int(row[offset + 3]),
                )
            )

    unique = {row[0]: row for row in candidates}
    ordered = sorted(unique.values(), key=lambda row: (row[3], row[0]))
    return [
        (snapshot_id, captured_at, balance)
        for snapshot_id, captured_at, balance, _epoch_us in ordered
    ]


def load_hourly_wallet_snapshot_candidates(
    conn: sqlite3.Connection,
    account_id: str,
    window_start_utc: str,
) -> Optional[List[Tuple[int, str, float]]]:
    """Load hourly rollup samples for a comparison window."""
    return load_rollup_wallet_snapshot_candidates(
        conn, account_id, window_start_utc, HOUR_SECONDS
    )


def load_four_hour_wallet_snapshot_candidates(
    conn: sqlite3.Connection,
    account_id: str,
    window_start_utc: str,
) -> Optional[List[Tuple[int, str, float]]]:
    """Load four-hour rollup samples for a long comparison window."""
    return load_rollup_wallet_snapshot_candidates(
        conn, account_id, window_start_utc, FOUR_HOUR_SECONDS
    )
