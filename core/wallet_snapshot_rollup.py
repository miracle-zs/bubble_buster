"""Hourly first/min/max/last summaries for dashboard wallet curves."""

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

LOGGER = logging.getLogger(__name__)

HOUR_SECONDS = 3600
HOUR_MICROSECONDS = HOUR_SECONDS * 1_000_000
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SAMPLE_KINDS = ("first", "min", "max", "last")
_SAMPLE_FIELDS = ("snapshot_id", "captured_at_utc", "balance_usdt", "epoch_us")
_ROLLUP_COLUMNS = (
    "account_id",
    "hour_start_epoch",
    *(f"{kind}_{field}" for kind in _SAMPLE_KINDS for field in _SAMPLE_FIELDS),
)
_UPDATE_COLUMNS = ", ".join(
    f"{column} = excluded.{column}" for column in _ROLLUP_COLUMNS[2:]
)
_UPSERT_SQL = (
    f"INSERT INTO wallet_snapshot_hourly_rollups ({', '.join(_ROLLUP_COLUMNS)}) "
    f"VALUES ({', '.join('?' for _ in _ROLLUP_COLUMNS)}) "
    "ON CONFLICT(account_id, hour_start_epoch) DO UPDATE SET "
    + _UPDATE_COLUMNS
)
_ROLLUP_SAMPLE_SELECT = ", ".join(
    f"{kind}_{field}" for kind in _SAMPLE_KINDS for field in _SAMPLE_FIELDS
)


def _epoch_microseconds(captured_at_utc: str) -> int:
    captured_at = datetime.fromisoformat(str(captured_at_utc))
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)
    delta = captured_at.astimezone(timezone.utc) - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _sample_set(
    snapshot_id: int, captured_at_utc: str, balance_usdt: float
) -> Tuple[int, Tuple[Tuple[int, str, float, int], ...]]:
    epoch_us = _epoch_microseconds(captured_at_utc)
    hour_start_epoch = (epoch_us // HOUR_MICROSECONDS) * HOUR_SECONDS
    sample = (int(snapshot_id), str(captured_at_utc), float(balance_usdt), epoch_us)
    return hour_start_epoch, (sample, sample, sample, sample)


def _rollup_params(
    account_id: str,
    hour_start_epoch: int,
    samples: Sequence[Tuple[int, str, float, int]],
) -> Tuple[object, ...]:
    return account_id, hour_start_epoch, *(value for sample in samples for value in sample)


def merge_hourly_sample(
    current: Optional[Sequence[Tuple[int, str, float, int]]],
    candidate: Tuple[int, str, float, int],
) -> Tuple[Tuple[int, str, float, int], ...]:
    """Merge a row into an hourly first/min/max/last sample set."""
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


def upsert_wallet_snapshot_hourly_rollup(
    conn: sqlite3.Connection,
    account_id: str,
    snapshot_id: int,
    captured_at_utc: str,
    balance_usdt: float,
) -> None:
    """Update the summary in the same transaction as its source snapshot."""
    account_id = (account_id or "").strip()
    if not account_id:
        return

    try:
        hour_start_epoch, incoming = _sample_set(
            snapshot_id, captured_at_utc, balance_usdt
        )
        current_row = conn.execute(
            f"SELECT {_ROLLUP_SAMPLE_SELECT} FROM wallet_snapshot_hourly_rollups "
            "WHERE account_id = ? AND hour_start_epoch = ?",
            (account_id, hour_start_epoch),
        ).fetchone()
        current = None
        if current_row is not None:
            current = tuple(
                tuple(current_row[offset + j] for j in range(4))
                for offset in (0, 4, 8, 12)
            )
        samples = merge_hourly_sample(current, incoming[0])
        conn.execute(
            _UPSERT_SQL,
            _rollup_params(account_id, hour_start_epoch, samples),
        )
    except sqlite3.Error as exc:
        # Keep the source snapshot usable and fall back to raw reads if the
        # optional derived table cannot be updated.
        try:
            conn.execute(
                "DELETE FROM wallet_snapshot_hourly_rollup_state WHERE account_id = ?",
                (account_id,),
            )
        except sqlite3.Error:
            pass
        LOGGER.warning(
            "Hourly wallet rollup update failed for %s; raw comparison reads remain enabled: %s",
            account_id,
            exc,
        )


def insert_hourly_rollup_aggregates(
    conn: sqlite3.Connection,
    aggregates: Iterable[
        Tuple[str, int, Sequence[Tuple[int, str, float, int]]]
    ],
) -> int:
    """Persist pre-aggregated buckets during an offline backfill transaction."""
    params = (
        _rollup_params(account_id, hour_start_epoch, samples)
        for account_id, hour_start_epoch, samples in aggregates
    )
    conn.executemany(_UPSERT_SQL, params)
    return conn.execute(
        "SELECT COUNT(*) FROM wallet_snapshot_hourly_rollups"
    ).fetchone()[0]


def load_hourly_wallet_snapshot_candidates(
    conn: sqlite3.Connection,
    account_id: str,
    window_start_utc: str,
) -> Optional[List[Tuple[int, str, float]]]:
    """Load hourly samples if backfill is complete; otherwise return None.

    Read exact source rows only from a partial leading hour. The newest hour
    may be incomplete in clock time, but its rollup contains every row through
    the query's latest snapshot.
    """
    complete = conn.execute(
        "SELECT 1 FROM wallet_snapshot_hourly_rollup_state WHERE account_id = ?",
        (account_id,),
    ).fetchone()
    if complete is None:
        return None

    start_epoch_us = _epoch_microseconds(window_start_utc)
    first_hour_start_epoch = (
        (start_epoch_us + HOUR_MICROSECONDS - 1) // HOUR_MICROSECONDS
    ) * HOUR_SECONDS
    candidates: List[Tuple[int, str, float, int]] = []

    if start_epoch_us % HOUR_MICROSECONDS:
        edge_end_utc = datetime.fromtimestamp(
            first_hour_start_epoch, timezone.utc
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
        FROM wallet_snapshot_hourly_rollups
        WHERE account_id = ? AND hour_start_epoch >= ?
        ORDER BY hour_start_epoch ASC
        """,
        (account_id, first_hour_start_epoch),
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
    return [(snapshot_id, captured_at, balance) for snapshot_id, captured_at, balance, _ in ordered]
