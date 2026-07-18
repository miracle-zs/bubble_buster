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


def test_legacy_single_account_database_can_init_before_multi_account_migration(tmp_path: Path):
    db_path = tmp_path / "legacy-init-order.db"
    schema_path = Path(__file__).resolve().parents[1] / "schema.sql"
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
            INSERT INTO runs VALUES ('r1', '2026-02-13', '2026-02-13T00:00:00+00:00', NULL, 'DONE', NULL);
            CREATE TABLE order_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER,
                symbol TEXT NOT NULL,
                order_id INTEGER,
                client_order_id TEXT,
                type TEXT,
                side TEXT,
                price REAL,
                qty REAL,
                status TEXT,
                event_time_utc TEXT NOT NULL,
                raw_json TEXT
            );
            """
        )

    store = StateStore(
        db_path=str(db_path),
        schema_path=str(schema_path),
        account_id="acc01",
    )
    store.init_schema()
    store.migrate_to_multi_account(default_account_id="acc01")

    with sqlite3.connect(db_path) as conn:
        run_account = conn.execute("SELECT account_id FROM runs WHERE run_id='r1'").fetchone()[0]
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    assert run_account == "acc01"
    assert violations == []


def test_init_schema_adds_order_event_account_before_creating_its_index(tmp_path: Path):
    db_path = tmp_path / "legacy-order-events.db"
    schema_path = Path(__file__).resolve().parents[1] / "schema.sql"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE order_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id INTEGER,
                symbol TEXT NOT NULL,
                order_id INTEGER,
                client_order_id TEXT,
                type TEXT,
                side TEXT,
                price REAL,
                qty REAL,
                status TEXT,
                event_time_utc TEXT NOT NULL,
                raw_json TEXT
            );
            """
        )

    store = StateStore(
        db_path=str(db_path),
        schema_path=str(schema_path),
        account_id="acc01",
    )
    store.init_schema()

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(order_events)")}
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(order_events)")}
    assert "account_id" in columns
    assert "idx_order_events_account_id_id" in indexes


def test_init_schema_repairs_foreign_keys_left_by_legacy_runs_rename(tmp_path: Path):
    db_path = tmp_path / "legacy-runs-fk.db"
    schema_path = Path(__file__).resolve().parents[1] / "schema.sql"
    store = StateStore(db_path=str(db_path), schema_path=str(schema_path), account_id="acc01")
    store.init_schema()
    run_id, _ = store.create_run("2026-02-13", account_id="acc01")
    store.insert_position(
        run_id=run_id,
        symbol="BTCUSDT",
        side="SHORT",
        qty=1.0,
        entry_price=10.0,
        liq_price_open=12.0,
        tp_price=None,
        sl_price=11.0,
        tp_order_id=None,
        sl_order_id=1,
        tp_client_order_id=None,
        sl_client_order_id="sl-1",
        opened_at_utc="2026-02-13T00:00:00+00:00",
        expire_at_utc="2026-02-14T00:00:00+00:00",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("ALTER TABLE runs RENAME TO runs_legacy")
        conn.execute(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                trade_day_utc TEXT NOT NULL,
                started_at_utc TEXT NOT NULL,
                completed_at_utc TEXT,
                status TEXT NOT NULL,
                message TEXT,
                UNIQUE(account_id, trade_day_utc)
            )
            """
        )
        conn.execute("INSERT INTO runs SELECT * FROM runs_legacy")
        conn.commit()

    store.init_schema()

    with sqlite3.connect(db_path) as conn:
        fk_targets = {row[2] for row in conn.execute("PRAGMA foreign_key_list(positions)")}
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        legacy_refs = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND sql LIKE '%runs_legacy%'"
        ).fetchall()
    assert fk_targets == {"runs"}
    assert violations == []
    assert legacy_refs == []
