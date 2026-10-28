import sqlite3
from pathlib import Path

from core.state_store import StateStore


SCHEMA = Path(__file__).resolve().parents[1] / "schema.sql"


def test_legacy_fill_identity_migration_preserves_data_and_is_idempotent(tmp_path):
    legacy_schema = tmp_path / "legacy.sql"
    schema = SCHEMA.read_text()
    legacy_schema.write_text(
        schema.replace("exchange_trade_id TEXT NOT NULL,", "exchange_trade_id TEXT NOT NULL UNIQUE,")
        .replace("    UNIQUE(account_id, symbol, exchange_trade_id),\n", "")
    )
    db = tmp_path / "state.db"
    store = StateStore(str(db), str(legacy_schema), account_id="acc01")
    store.init_schema()
    with sqlite3.connect(db) as conn:
        conn.execute("DROP TABLE execution_fills")
        conn.execute('''CREATE TABLE execution_fills (
            fill_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, intent_id TEXT NOT NULL,
            account_id TEXT NOT NULL DEFAULT 'default', symbol TEXT NOT NULL,
            exchange_trade_id TEXT NOT NULL UNIQUE, exchange_order_id TEXT,
            side TEXT NOT NULL, price REAL NOT NULL, qty REAL NOT NULL,
            commission REAL NOT NULL DEFAULT 0, commission_asset TEXT NOT NULL DEFAULT 'USDT',
            trade_time_utc TEXT NOT NULL, created_at_utc TEXT NOT NULL,
            FOREIGN KEY(attempt_id) REFERENCES order_attempts(attempt_id),
            FOREIGN KEY(intent_id) REFERENCES order_intents(intent_id))''')
        conn.execute("INSERT INTO execution_fills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     ("fill1", "attempt1", "intent1", "acc01", "BTCUSDT", "123", "1",
                      "BUY", 10, 1, 0, "USDT", "2026-01-01", "2026-01-01"))
    upgraded = StateStore(str(db), str(SCHEMA), account_id="acc01")
    upgraded.init_schema()
    upgraded.init_schema()
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT fill_id, exchange_trade_id FROM execution_fills").fetchall() == [("fill1", "123")]
        conn.execute("INSERT INTO execution_fills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     ("fill2", "attempt2", "intent2", "acc01", "ETHUSDT", "123", "2",
                      "BUY", 20, 1, 0, "USDT", "2026-01-01", "2026-01-01"))
        conn.execute("INSERT INTO execution_fills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     ("fill3", "attempt3", "intent3", "acc02", "BTCUSDT", "123", "3",
                      "BUY", 20, 1, 0, "USDT", "2026-01-01", "2026-01-01"))
        assert conn.execute("SELECT count(*) FROM execution_fills").fetchone()[0] == 3
