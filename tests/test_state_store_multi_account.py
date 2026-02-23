import sqlite3
from pathlib import Path

from core.state_store import StateStore


def test_migration_adds_account_id_and_scopes_run_uniqueness(tmp_path: Path):
    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                trade_day_utc TEXT NOT NULL UNIQUE,
                started_at_utc TEXT NOT NULL,
                completed_at_utc TEXT,
                status TEXT NOT NULL,
                message TEXT
            );
            INSERT INTO runs (run_id, trade_day_utc, started_at_utc, status, message)
            VALUES ('r1', '2026-02-13', '2026-02-13T00:00:00+00:00', 'DONE', NULL);
            """
        )

    store = StateStore(db_path=str(db_path))
    store.migrate_to_multi_account(default_account_id="acc01")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cols = conn.execute("PRAGMA table_info(runs)").fetchall()
        col_names = {str(row["name"]) for row in cols}
        assert "account_id" in col_names

        # Same trade_day_utc should be allowed for different accounts.
        conn.execute(
            """
            INSERT INTO runs (run_id, account_id, trade_day_utc, started_at_utc, status, message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("r2", "acc02", "2026-02-13", "2026-02-13T00:01:00+00:00", "DONE", None),
        )
        conn.execute(
            """
            INSERT INTO runs (run_id, account_id, trade_day_utc, started_at_utc, status, message)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("r3", "acc01", "2026-02-14", "2026-02-14T00:01:00+00:00", "DONE", None),
        )
        conn.commit()
