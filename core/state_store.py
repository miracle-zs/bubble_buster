import json
import os
import re
import sqlite3
import uuid
import hashlib
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class RunState:
    run_id: str
    account_id: str
    trade_day_utc: str
    started_at_utc: str
    completed_at_utc: Optional[str]
    status: str
    reason: Optional[str]


@dataclass(frozen=True)
class PositionState:
    id: int
    run_id: str
    symbol: str
    side: str
    qty: float
    entry_price: float
    liq_price_open: Optional[float]
    liq_price_latest: Optional[float]
    tp_price: Optional[float]
    sl_price: Optional[float]
    tp_order_id: Optional[int]
    sl_order_id: Optional[int]
    opened_at_utc: str
    expire_at_utc: str
    closed_at_utc: Optional[str]
    status: str
    close_reason: Optional[str]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


ACCOUNT_ID_MIGRATION_TABLES = {
    "runs",
    "order_events",
    "wallet_snapshots",
    "cashflow_events",
    "equity_recovery_events",
}
SQLITE_BUSY_TIMEOUT_MS = 30000
ACTIVE_POSITION_STATUSES = ("PENDING_ENTRY", "PENDING_EXIT_SETUP", "OPEN")


class StateStore:
    def __init__(self, db_path: str, schema_path: Optional[str] = None, account_id: str = "default"):
        self.db_path = db_path
        self.schema_path = schema_path
        self.account_id = (account_id or "").strip() or "default"
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    def scoped(self, account_id: str) -> "StateStore":
        return StateStore(
            db_path=self.db_path,
            schema_path=self.schema_path,
            account_id=account_id,
        )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _connect_ctx(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        # Migrate legacy account columns before schema.sql creates account-scoped indexes.
        # This also makes direct callers (including the dashboard) safe on old databases.
        self.migrate_to_multi_account(default_account_id=self.account_id)
        if self.schema_path:
            with open(self.schema_path, "r", encoding="utf-8") as f:
                schema_sql = f.read()
        else:
            schema_sql = ""
        with self._connect_ctx() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("PRAGMA journal_mode = WAL")
            if schema_sql:
                # The migration above normally adds these columns. Keep this guard
                # for databases created concurrently between the two connections.
                for table_name in ACCOUNT_ID_MIGRATION_TABLES:
                    self._ensure_account_id_column(conn, table_name, self.account_id)
                conn.executescript(schema_sql)
            else:
                raise ValueError("schema_path is required for StateStore.init_schema")
            self._backfill_order_event_account_ids(conn)
            self._repair_legacy_run_foreign_keys(conn)

    def create_run(self, trade_day_utc: str, account_id: str = "default") -> Tuple[str, bool]:
        """Creates a run for a UTC trade day.

        Returns (run_id, created). If created is False, run_id is the existing run.
        """
        run_id = str(uuid.uuid4())
        started_at = utc_now_iso()
        with self._connect_ctx() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO runs (run_id, account_id, trade_day_utc, started_at_utc, status, message)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, account_id, trade_day_utc, started_at, "RUNNING", None),
                )
                return run_id, True
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT run_id FROM runs WHERE account_id = ? AND trade_day_utc = ?",
                    (account_id, trade_day_utc),
                ).fetchone()
                return str(row["run_id"]), False

    def finalize_run(self, run_id: str, status: str, message: Optional[str] = None) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE runs
                SET status = ?, message = ?, completed_at_utc = ?
                WHERE run_id = ?
                """,
                (status, message, utc_now_iso(), run_id),
            )

    def get_run(self, run_id: str) -> Optional[RunState]:
        with self._connect_ctx() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            return RunState(
                run_id=row["run_id"],
                account_id=row["account_id"] if "account_id" in row.keys() else "default",
                trade_day_utc=row["trade_day_utc"],
                started_at_utc=row["started_at_utc"],
                completed_at_utc=row["completed_at_utc"],
                status=row["status"],
                reason=row["message"],
            )

    def count_run_opened_positions(self, run_id: str) -> int:
        """Count positions that reached an exchange-backed entry state for a run.

        A resumed entry run can create positions across several invocations.  A
        pending entry or an entry explicitly marked as failed is not counted,
        while open, pending-exit-setup, and closed positions represent an entry
        that did complete.
        """
        with self._connect_ctx() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS opened_count
                FROM positions
                WHERE run_id = ?
                  AND status NOT IN ('PENDING_ENTRY', 'ENTRY_FAILED')
                """,
                (run_id,),
            ).fetchone()
            return int(row["opened_count"] or 0) if row is not None else 0

    def migrate_to_multi_account(self, default_account_id: str) -> None:
        default_account_id = (default_account_id or "").strip()
        if not default_account_id:
            raise ValueError("default_account_id is required")

        with self._connect_ctx() as conn:
            conn.execute("PRAGMA foreign_keys = OFF")
            row = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='runs'"
            ).fetchone()
            if row is None:
                return

            columns = conn.execute("PRAGMA table_info(runs)").fetchall()
            has_account_id = any(str(col["name"]) == "account_id" for col in columns)
            if not has_account_id:
                conn.execute(
                    """
                    CREATE TABLE runs_new (
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
                conn.execute(
                    """
                    INSERT INTO runs_new (run_id, account_id, trade_day_utc, started_at_utc, completed_at_utc, status, message)
                    SELECT run_id, ?, trade_day_utc, started_at_utc, completed_at_utc, status, message
                    FROM runs
                    """,
                    (default_account_id,),
                )
                conn.execute("DROP TABLE runs")
                conn.execute("ALTER TABLE runs_new RENAME TO runs")

            self._ensure_account_id_column(
                conn=conn,
                table_name="wallet_snapshots",
                default_account_id=default_account_id,
            )
            self._ensure_account_id_column(
                conn=conn,
                table_name="cashflow_events",
                default_account_id=default_account_id,
            )
            self._ensure_account_id_column(
                conn=conn,
                table_name="equity_recovery_events",
                default_account_id=default_account_id,
            )
            self._ensure_account_id_column(
                conn=conn,
                table_name="order_events",
                default_account_id=default_account_id,
            )
            self._backfill_order_event_account_ids(conn)
            self._repair_legacy_run_foreign_keys(conn)

    def _ensure_account_id_column(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        default_account_id: str,
    ) -> None:
        if table_name not in ACCOUNT_ID_MIGRATION_TABLES:
            raise ValueError(f"Unexpected account_id migration table: {table_name}")
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,),
        ).fetchone()
        if row is None:
            return
        cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        if any(str(col["name"]) == "account_id" for col in cols):
            return
        escaped = default_account_id.replace("'", "''")
        conn.execute(
            f"ALTER TABLE {table_name} ADD COLUMN account_id TEXT NOT NULL DEFAULT '{escaped}'"
        )

    @staticmethod
    def _backfill_order_event_account_ids(conn: sqlite3.Connection) -> None:
        columns = conn.execute("PRAGMA table_info(order_events)").fetchall()
        if not any(str(column["name"]) == "account_id" for column in columns):
            return
        positions_row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='positions'"
        ).fetchone()
        if positions_row is None:
            return
        run_columns = conn.execute("PRAGMA table_info(runs)").fetchall()
        if not any(str(column["name"]) == "account_id" for column in run_columns):
            # migrate_to_multi_account() will call this again after rebuilding runs.
            return
        conn.execute(
            """
            UPDATE order_events
            SET account_id = COALESCE(
                (
                    SELECT r.account_id
                    FROM positions p
                    JOIN runs r ON r.run_id = p.run_id
                    WHERE p.id = order_events.position_id
                ),
                account_id
            )
            WHERE position_id IS NOT NULL
            """
        )

    @staticmethod
    def _repair_legacy_run_foreign_keys(conn: sqlite3.Connection) -> None:
        for table_name in ("positions", "rebalance_cycles", "rebalance_actions"):
            foreign_keys = conn.execute(f"PRAGMA foreign_key_list({table_name})").fetchall()
            if not any(str(row["table"]) == "runs_legacy" for row in foreign_keys):
                continue

            table_row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table_name,),
            ).fetchone()
            if table_row is None or not table_row["sql"]:
                continue
            index_rows = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
                (table_name,),
            ).fetchall()
            columns = [str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
            temp_name = f"{table_name}__fk_repair"
            create_sql = str(table_row["sql"])
            create_sql = re.sub(
                rf"^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\"{re.escape(table_name)}\"|{re.escape(table_name)})",
                f'CREATE TABLE "{temp_name}"',
                create_sql,
                count=1,
                flags=re.IGNORECASE,
            )
            create_sql = create_sql.replace('"runs_legacy"', '"runs"').replace("runs_legacy", "runs")
            quoted_columns = ", ".join(f'"{column}"' for column in columns)
            conn.execute(f'DROP TABLE IF EXISTS "{temp_name}"')
            conn.execute(create_sql)
            conn.execute(
                f'INSERT INTO "{temp_name}" ({quoted_columns}) SELECT {quoted_columns} FROM "{table_name}"'
            )
            conn.execute(f'DROP TABLE "{table_name}"')
            conn.execute(f'ALTER TABLE "{temp_name}" RENAME TO "{table_name}"')
            for index_row in index_rows:
                conn.execute(str(index_row["sql"]))

        legacy_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runs_legacy'"
        ).fetchone()
        if legacy_table is None:
            return
        referenced = False
        table_rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        for row in table_rows:
            table_name = str(row["name"])
            if table_name == "runs_legacy":
                continue
            foreign_keys = conn.execute(f'PRAGMA foreign_key_list("{table_name}")').fetchall()
            if any(str(foreign_key["table"]) == "runs_legacy" for foreign_key in foreign_keys):
                referenced = True
                break
        if not referenced:
            conn.execute('DROP TABLE "runs_legacy"')

    def insert_position(
        self,
        run_id: str,
        symbol: str,
        side: str,
        qty: float,
        entry_price: float,
        liq_price_open: Optional[float],
        tp_price: Optional[float],
        sl_price: Optional[float],
        tp_order_id: Optional[int],
        sl_order_id: Optional[int],
        tp_client_order_id: Optional[str],
        sl_client_order_id: Optional[str],
        opened_at_utc: str,
        expire_at_utc: str,
        status: str = "OPEN",
        last_error: Optional[str] = None,
    ) -> int:
        now_iso = utc_now_iso()
        with self._connect_ctx() as conn:
            if status in ACTIVE_POSITION_STATUSES:
                existing = conn.execute(
                    """
                    SELECT p.id
                    FROM positions p
                    JOIN runs r ON r.run_id = p.run_id
                    WHERE r.account_id = ?
                      AND UPPER(p.symbol) = UPPER(?)
                      AND p.status IN ('PENDING_ENTRY', 'PENDING_EXIT_SETUP', 'OPEN')
                    LIMIT 1
                    """,
                    (self.account_id, symbol),
                ).fetchone()
                if existing is not None:
                    raise sqlite3.IntegrityError(
                        f"Active position already exists for account={self.account_id} symbol={symbol} "
                        f"position_id={existing['id']}"
                    )
            cursor = conn.execute(
                """
                INSERT INTO positions (
                    run_id, symbol, side, qty, entry_price,
                    liq_price_open, liq_price_latest,
                    tp_price, sl_price,
                    tp_order_id, sl_order_id,
                    tp_client_order_id, sl_client_order_id,
                    opened_at_utc, expire_at_utc,
                    status, last_error,
                    created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    symbol,
                    side,
                    qty,
                    entry_price,
                    liq_price_open,
                    liq_price_open,
                    tp_price,
                    sl_price,
                    tp_order_id,
                    sl_order_id,
                    tp_client_order_id,
                    sl_client_order_id,
                    opened_at_utc,
                    expire_at_utc,
                    status,
                    last_error,
                    now_iso,
                    now_iso,
                ),
            )
            return int(cursor.lastrowid)

    def list_open_positions(self) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT p.*
                FROM positions p
                INNER JOIN runs r ON r.run_id = p.run_id
                WHERE p.status = 'OPEN'
                  AND r.account_id = ?
                """,
                (self.account_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_pending_exit_setup_positions(self) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT p.*
                FROM positions p
                INNER JOIN runs r ON r.run_id = p.run_id
                WHERE p.status = 'PENDING_EXIT_SETUP'
                  AND r.account_id = ?
                ORDER BY p.id ASC
                """,
                (self.account_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_pending_entry_positions(self) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT p.*
                FROM positions p
                INNER JOIN runs r ON r.run_id = p.run_id
                WHERE p.status = 'PENDING_ENTRY'
                  AND r.account_id = ?
                ORDER BY p.id ASC
                """,
                (self.account_id,),
            ).fetchall()
            return [dict(row) for row in rows]

    def get_position(self, position_id: int) -> Optional[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            row = conn.execute(
                """
                SELECT p.*
                FROM positions p
                INNER JOIN runs r ON r.run_id = p.run_id
                WHERE p.id = ? AND r.account_id = ?
                LIMIT 1
                """,
                (int(position_id), self.account_id),
            ).fetchone()
            return dict(row) if row is not None else None

    def list_open_symbols(self) -> Set[str]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT p.symbol
                FROM positions p
                INNER JOIN runs r ON r.run_id = p.run_id
                WHERE p.status = 'OPEN'
                  AND r.account_id = ?
                """,
                (self.account_id,),
            ).fetchall()
            return {str(row["symbol"]) for row in rows}

    def list_active_symbols(self) -> Set[str]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT p.symbol
                FROM positions p
                INNER JOIN runs r ON r.run_id = p.run_id
                WHERE p.status IN ('PENDING_ENTRY', 'PENDING_EXIT_SETUP', 'OPEN')
                  AND r.account_id = ?
                """,
                (self.account_id,),
            ).fetchall()
            return {str(row["symbol"]) for row in rows}

    def update_position_orders(
        self,
        position_id: int,
        tp_order_id: Optional[int],
        sl_order_id: Optional[int],
        tp_client_order_id: Optional[str],
        sl_client_order_id: Optional[str],
        tp_price: Optional[float],
        sl_price: Optional[float],
        liq_price_latest: Optional[float] = None,
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET tp_order_id = ?,
                    sl_order_id = ?,
                    tp_client_order_id = ?,
                    sl_client_order_id = ?,
                    tp_price = ?,
                    sl_price = ?,
                    liq_price_latest = COALESCE(?, liq_price_latest),
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (
                    tp_order_id,
                    sl_order_id,
                    tp_client_order_id,
                    sl_client_order_id,
                    tp_price,
                    sl_price,
                    liq_price_latest,
                    utc_now_iso(),
                    position_id,
                ),
            )

    def update_stop_loss(
        self,
        position_id: int,
        sl_order_id: Optional[int],
        sl_client_order_id: Optional[str],
        sl_price: float,
        liq_price_latest: Optional[float],
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET sl_order_id = ?,
                    sl_client_order_id = ?,
                    sl_price = ?,
                    liq_price_latest = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (sl_order_id, sl_client_order_id, sl_price, liq_price_latest, utc_now_iso(), position_id),
            )

    def update_take_profit(
        self,
        position_id: int,
        tp_order_id: Optional[int],
        tp_client_order_id: Optional[str],
        tp_price: Optional[float],
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET tp_order_id = ?,
                    tp_client_order_id = ?,
                    tp_price = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (tp_order_id, tp_client_order_id, tp_price, utc_now_iso(), int(position_id)),
            )

    def set_position_qty(self, position_id: int, qty: float, entry_price: float) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET qty = ?, entry_price = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (qty, entry_price, utc_now_iso(), position_id),
            )

    def set_position_entry_fill(
        self,
        position_id: int,
        qty: float,
        entry_price: float,
        liq_price_open: Optional[float],
        opened_at_utc: str,
        expire_at_utc: str,
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET qty = ?,
                    entry_price = ?,
                    liq_price_open = COALESCE(?, liq_price_open),
                    liq_price_latest = COALESCE(?, liq_price_latest),
                    opened_at_utc = ?,
                    expire_at_utc = ?,
                    status = 'PENDING_EXIT_SETUP',
                    updated_at_utc = ?
                WHERE id = ? AND status = 'PENDING_ENTRY'
                """,
                (
                    qty,
                    entry_price,
                    liq_price_open,
                    liq_price_open,
                    opened_at_utc,
                    expire_at_utc,
                    utc_now_iso(),
                    int(position_id),
                ),
            )

    def mark_position_open(self, position_id: int) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET status = 'OPEN',
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (utc_now_iso(), position_id),
            )

    def mark_position_closed(
        self,
        position_id: int,
        status: str,
        close_reason: Optional[str],
        close_order_id: Optional[int] = None,
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET status = ?,
                    close_reason = ?,
                    close_order_id = COALESCE(?, close_order_id),
                    closed_at_utc = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (status, close_reason, close_order_id, utc_now_iso(), utc_now_iso(), position_id),
            )

    def set_position_error(self, position_id: int, error_message: str) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET last_error = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (error_message[:1000], utc_now_iso(), position_id),
            )

    def clear_position_error(self, position_id: int) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE positions
                SET last_error = NULL, updated_at_utc = ?
                WHERE id = ?
                """,
                (utc_now_iso(), position_id),
            )

    def add_order_event(
        self,
        symbol: str,
        event_time_utc: str,
        order_payload: Dict[str, Any],
        position_id: Optional[int] = None,
    ) -> int:
        with self._connect_ctx() as conn:
            cursor = conn.execute(
                """
                INSERT INTO order_events (
                    account_id, position_id, symbol, order_id, client_order_id,
                    type, side, price, qty, status,
                    event_time_utc, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.account_id,
                    position_id,
                    symbol,
                    order_payload.get("orderId"),
                    order_payload.get("clientOrderId"),
                    order_payload.get("type"),
                    order_payload.get("side"),
                    _safe_float(order_payload.get("price")),
                    _safe_float(order_payload.get("origQty") or order_payload.get("executedQty")),
                    order_payload.get("status"),
                    event_time_utc,
                    json.dumps(order_payload, ensure_ascii=False),
                ),
            )
            event_id = int(cursor.lastrowid)
            self._insert_fill_from_order_event(
                conn=conn,
                order_event_id=event_id,
                position_id=position_id,
                symbol=symbol,
                event_time_utc=event_time_utc,
                order_payload=order_payload,
            )
        self.upsert_exchange_order_state(
            order_payload,
            source="LOCAL_ORDER_EVENT",
            event_time_utc=event_time_utc,
        )
        return event_id

    def update_order_event(
        self,
        order_event_id: int,
        symbol: str,
        event_time_utc: str,
        order_payload: Dict[str, Any],
        position_id: Optional[int] = None,
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE order_events
                SET account_id = ?,
                    position_id = COALESCE(?, position_id),
                    symbol = ?,
                    order_id = COALESCE(?, order_id),
                    client_order_id = COALESCE(?, client_order_id),
                    type = COALESCE(?, type),
                    side = COALESCE(?, side),
                    price = COALESCE(?, price),
                    qty = COALESCE(?, qty),
                    status = COALESCE(?, status),
                    event_time_utc = ?,
                    raw_json = ?
                WHERE id = ?
                """,
                (
                    self.account_id,
                    position_id,
                    symbol,
                    order_payload.get("orderId"),
                    order_payload.get("clientOrderId"),
                    order_payload.get("type"),
                    order_payload.get("side"),
                    _safe_float(order_payload.get("price") or order_payload.get("avgPrice")),
                    _safe_float(order_payload.get("origQty") or order_payload.get("executedQty")),
                    order_payload.get("status"),
                    event_time_utc,
                    json.dumps(order_payload, ensure_ascii=False),
                    int(order_event_id),
                ),
            )
            self._insert_fill_from_order_event(
                conn=conn,
                order_event_id=int(order_event_id),
                position_id=position_id,
                symbol=symbol,
                event_time_utc=event_time_utc,
                order_payload=order_payload,
            )

    @staticmethod
    def _deferred_structure_boundary_reached(
        entry_audit: Dict[str, Any],
        runtime_timezone: str,
    ) -> bool:
        logical_close_raw = entry_audit.get("final_candle_logical_close_time_utc")
        close_raw = entry_audit.get("final_candle_close_time_utc")
        hour_open_raw = entry_audit.get("signal_hour_open_utc")

        def parse_utc(raw: Any) -> Optional[datetime]:
            if not isinstance(raw, str) or not raw.strip():
                return None
            try:
                parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
            except ValueError:
                return None
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)

        logical_close = parse_utc(logical_close_raw)
        if logical_close is None:
            raw_close = parse_utc(close_raw)
            if raw_close is not None:
                hour_boundary = raw_close.replace(minute=0, second=0, microsecond=0)
                logical_close = (
                    hour_boundary + timedelta(hours=1)
                    if raw_close > hour_boundary
                    else hour_boundary
                )
        if logical_close is None:
            hour_open = parse_utc(hour_open_raw)
            if hour_open is not None:
                logical_close = hour_open + timedelta(hours=1)
        if logical_close is None:
            return False

        try:
            local_timezone = ZoneInfo(runtime_timezone or "Asia/Shanghai")
        except Exception:  # noqa: BLE001
            local_timezone = ZoneInfo("Asia/Shanghai")
        local_close = logical_close.astimezone(local_timezone)
        local_noon = local_close.replace(hour=12, minute=0, second=0, microsecond=0)
        return local_close >= local_noon

    def list_open_preclose_entry_audits_needing_structure(
        self,
        runtime_timezone: str = "Asia/Shanghai",
    ) -> List[Dict[str, Any]]:
        completed_statuses = {
            "REPLACED",
            "REPLACED_OLD_STOP_CANCEL_FAILED",
            "KEPT_TIGHTER_EXISTING_STOP",
            "CLOSED_IMMEDIATE_TRIGGER",
            "SKIPPED_EXEMPT",
            "DEFERRED_BEFORE_NOON",
        }
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT
                    oe.id AS order_event_id,
                    oe.position_id,
                    oe.symbol,
                    oe.raw_json
                FROM order_events oe
                INNER JOIN positions p ON p.id = oe.position_id
                INNER JOIN runs r ON r.run_id = p.run_id
                WHERE oe.account_id = ?
                  AND r.account_id = ?
                  AND p.status = 'OPEN'
                  AND UPPER(COALESCE(oe.type, '')) = 'MARKET'
                  AND UPPER(COALESCE(oe.side, '')) = 'SELL'
                ORDER BY oe.id ASC
                """,
                (self.account_id, self.account_id),
            ).fetchall()

        pending: List[Dict[str, Any]] = []
        seen_position_ids: Set[int] = set()
        for row in rows:
            try:
                payload = json.loads(str(row["raw_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            entry_audit = payload.get("entry_audit")
            if not isinstance(entry_audit, dict):
                continue
            if str(entry_audit.get("entry_mode") or "").strip().upper() != "PRECLOSE":
                continue
            if not str(entry_audit.get("signal_hour_open_utc") or "").strip():
                continue
            structure_status = str(entry_audit.get("structure_stop_status") or "").strip().upper()
            if structure_status in completed_statuses:
                if structure_status != "DEFERRED_BEFORE_NOON":
                    continue
                if not self._deferred_structure_boundary_reached(entry_audit, runtime_timezone):
                    continue
            position_id = int(row["position_id"])
            if position_id in seen_position_ids:
                continue
            seen_position_ids.add(position_id)
            pending.append(
                {
                    "order_event_id": int(row["order_event_id"]),
                    "position_id": position_id,
                    "symbol": str(row["symbol"] or "").strip().upper(),
                    "hour_open_utc": str(entry_audit["signal_hour_open_utc"]),
                    "order_payload": payload,
                }
            )
        return pending

    def find_order_event_id(
        self,
        symbol: str,
        position_id: Optional[int],
        order_id: Optional[int],
        client_order_id: Optional[str],
    ) -> Optional[int]:
        if order_id is None and not client_order_id:
            return None
        clauses = ["account_id = ?", "UPPER(symbol) = UPPER(?)"]
        params: List[Any] = [self.account_id, symbol]
        if position_id is None:
            clauses.append("position_id IS NULL")
        else:
            clauses.append("position_id = ?")
            params.append(int(position_id))
        if order_id is not None and client_order_id:
            clauses.append("(order_id = ? OR client_order_id = ?)")
            params.extend((int(order_id), str(client_order_id)))
        elif order_id is not None:
            clauses.append("order_id = ?")
            params.append(int(order_id))
        else:
            clauses.append("client_order_id = ?")
            params.append(str(client_order_id))
        with self._connect_ctx() as conn:
            row = conn.execute(
                f"SELECT id FROM order_events WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT 1",
                tuple(params),
            ).fetchone()
            return int(row["id"]) if row is not None else None

    def list_market_order_events_missing_realized_fill(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT
                    oe.id AS order_event_id,
                    oe.position_id,
                    oe.symbol,
                    oe.order_id,
                    oe.client_order_id,
                    oe.type,
                    oe.side,
                    oe.status,
                    oe.event_time_utc,
                    oe.raw_json
                FROM order_events oe
                LEFT JOIN positions p ON p.id = oe.position_id
                WHERE oe.account_id = ?
                  AND UPPER(COALESCE(oe.side, '')) = 'BUY'
                  AND UPPER(COALESCE(oe.status, '')) IN ('FILLED', 'POSITION_RECONCILED')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM fills f
                      WHERE (
                            f.order_event_id = oe.id
                            OR (
                            oe.position_id IS NOT NULL
                            AND f.position_id = oe.position_id
                            AND oe.order_id IS NOT NULL
                            AND f.order_id = oe.order_id
                            )
                         )
                        AND f.realized_pnl IS NOT NULL
                  )
                ORDER BY oe.id DESC
                LIMIT ?
                """,
                (self.account_id, max(1, int(limit))),
            ).fetchall()
            return [dict(row) for row in rows]

    def create_rebalance_cycle(
        self,
        run_id: Optional[str],
        reason_tag: str,
        mode: str,
        reduce_only: bool,
        target_count: int,
    ) -> int:
        now_iso = utc_now_iso()
        with self._connect_ctx() as conn:
            cursor = conn.execute(
                """
                INSERT INTO rebalance_cycles (
                    run_id, reason_tag, mode, reduce_only, target_count,
                    started_at_utc, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (run_id or "").strip() or None,
                    (reason_tag or "").strip()[:24] or "unknown",
                    (mode or "").strip()[:24] or "equal_risk",
                    _safe_bool_int(reduce_only),
                    int(target_count),
                    now_iso,
                    now_iso,
                ),
            )
            return int(cursor.lastrowid)

    def finalize_rebalance_cycle(
        self,
        cycle_id: int,
        summary: Dict[str, Any],
        skip_reason: Optional[str] = None,
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE rebalance_cycles
                SET open_positions = ?,
                    virtual_slots = ?,
                    equity_usdt = ?,
                    target_gross_notional_usdt = ?,
                    target_notional_per_position_usdt = ?,
                    planned_count = ?,
                    adjusted_count = ?,
                    error_count = ?,
                    reduced_notional_usdt = ?,
                    added_notional_usdt = ?,
                    skip_reason = ?,
                    completed_at_utc = ?
                WHERE id = ?
                """,
                (
                    int(summary.get("open_positions") or 0),
                    int(summary.get("virtual_slots") or 0),
                    float(summary.get("equity_usdt") or 0.0),
                    float(summary.get("target_gross_notional") or 0.0),
                    float(summary.get("target_notional_per_position") or 0.0),
                    int(summary.get("planned") or 0),
                    int(summary.get("adjusted") or 0),
                    int(summary.get("errors") or 0),
                    float(summary.get("reduced_notional") or 0.0),
                    float(summary.get("added_notional") or 0.0),
                    ((skip_reason or "").strip()[:128] or None),
                    utc_now_iso(),
                    int(cycle_id),
                ),
            )

    def add_rebalance_action(
        self,
        cycle_id: int,
        run_id: Optional[str],
        position_id: Optional[int],
        symbol: str,
        action_side: Optional[str],
        reduce_only: bool,
        ref_price: Optional[float],
        current_notional_usdt: Optional[float],
        target_notional_usdt: Optional[float],
        deviation_notional_usdt: Optional[float],
        deadband_notional_usdt: Optional[float],
        max_adjust_notional_usdt: Optional[float],
        requested_adjust_notional_usdt: Optional[float],
        qty: Optional[float],
        est_notional_usdt: Optional[float],
        status: str,
        skip_reason: Optional[str] = None,
        error: Optional[str] = None,
    ) -> int:
        now_iso = utc_now_iso()
        with self._connect_ctx() as conn:
            cursor = conn.execute(
                """
                INSERT INTO rebalance_actions (
                    cycle_id, run_id, position_id, symbol, action_side, reduce_only,
                    ref_price, current_notional_usdt, target_notional_usdt,
                    deviation_notional_usdt, deadband_notional_usdt,
                    max_adjust_notional_usdt, requested_adjust_notional_usdt,
                    qty, est_notional_usdt, status, skip_reason, error,
                    created_at_utc, updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(cycle_id),
                    (run_id or "").strip() or None,
                    int(position_id) if position_id is not None else None,
                    (symbol or "").strip().upper(),
                    (action_side or "").strip().upper() or None,
                    _safe_bool_int(reduce_only),
                    float(ref_price) if ref_price is not None else None,
                    float(current_notional_usdt) if current_notional_usdt is not None else None,
                    float(target_notional_usdt) if target_notional_usdt is not None else None,
                    float(deviation_notional_usdt) if deviation_notional_usdt is not None else None,
                    float(deadband_notional_usdt) if deadband_notional_usdt is not None else None,
                    float(max_adjust_notional_usdt) if max_adjust_notional_usdt is not None else None,
                    (
                        float(requested_adjust_notional_usdt)
                        if requested_adjust_notional_usdt is not None
                        else None
                    ),
                    float(qty) if qty is not None else None,
                    float(est_notional_usdt) if est_notional_usdt is not None else None,
                    (status or "").strip().upper()[:24] or "PLANNED",
                    ((skip_reason or "").strip()[:128] or None),
                    ((error or "").strip()[:1000] or None),
                    now_iso,
                    now_iso,
                ),
            )
            return int(cursor.lastrowid)

    def update_rebalance_action_result(
        self,
        action_id: int,
        status: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                UPDATE rebalance_actions
                SET status = ?,
                    order_id = COALESCE(?, order_id),
                    client_order_id = COALESCE(?, client_order_id),
                    error = ?,
                    updated_at_utc = ?
                WHERE id = ?
                """,
                (
                    (status or "").strip().upper()[:24] or "UNKNOWN",
                    int(order_id) if order_id is not None else None,
                    (client_order_id or "").strip()[:128] or None,
                    ((error or "").strip()[:1000] or None),
                    utc_now_iso(),
                    int(action_id),
                ),
            )

    def add_wallet_snapshot(
        self,
        captured_at_utc: str,
        balance_usdt: float,
        source: str = "API",
        error: Optional[str] = None,
    ) -> int:
        with self._connect_ctx() as conn:
            cursor = conn.execute(
                """
                INSERT INTO wallet_snapshots (
                    account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    self.account_id,
                    captured_at_utc,
                    float(balance_usdt),
                    source[:24],
                    (error or "")[:1000] or None,
                    utc_now_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def get_latest_wallet_snapshot(self) -> Optional[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            row = conn.execute(
                """
                SELECT id, account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                FROM wallet_snapshots
                WHERE account_id = ? AND error IS NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (self.account_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_wallet_snapshot_first_since(
        self,
        start_captured_at_utc: str,
        end_captured_at_utc: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return the first valid equity snapshot captured in a time window."""
        start = (start_captured_at_utc or "").strip()
        if not start:
            return None
        with self._connect_ctx() as conn:
            params: List[Any] = [self.account_id, start]
            end_clause = ""
            if end_captured_at_utc:
                end_clause = " AND captured_at_utc <= ?"
                params.append(end_captured_at_utc.strip())
            row = conn.execute(
                f"""
                SELECT id, account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                FROM wallet_snapshots
                WHERE account_id = ?
                  AND error IS NULL
                  AND captured_at_utc >= ?
                  {end_clause}
                ORDER BY captured_at_utc ASC, id ASC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_wallet_snapshot_min_since(
        self,
        start_captured_at_utc: str,
        end_captured_at_utc: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        start = (start_captured_at_utc or "").strip()
        if not start:
            return None
        with self._connect_ctx() as conn:
            if end_captured_at_utc:
                row = conn.execute(
                    """
                    SELECT id, account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                    FROM wallet_snapshots
                    WHERE account_id = ?
                      AND error IS NULL
                      AND captured_at_utc >= ?
                      AND captured_at_utc <= ?
                    ORDER BY balance_usdt ASC, captured_at_utc ASC, id ASC
                    LIMIT 1
                    """,
                    (self.account_id, start, end_captured_at_utc.strip()),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT id, account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                    FROM wallet_snapshots
                    WHERE account_id = ?
                      AND error IS NULL
                      AND captured_at_utc >= ?
                    ORDER BY balance_usdt ASC, captured_at_utc ASC, id ASC
                    LIMIT 1
                    """,
                    (self.account_id, start),
                ).fetchone()
            return dict(row) if row is not None else None

    def get_wallet_snapshot_max_since(
        self,
        start_captured_at_utc: str,
        end_captured_at_utc: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Return the highest valid equity snapshot captured in a time window."""
        start = (start_captured_at_utc or "").strip()
        if not start:
            return None
        with self._connect_ctx() as conn:
            params: List[Any] = [self.account_id, start]
            end_clause = ""
            if end_captured_at_utc:
                end_clause = " AND captured_at_utc <= ?"
                params.append(end_captured_at_utc.strip())
            row = conn.execute(
                f"""
                SELECT id, account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                FROM wallet_snapshots
                WHERE account_id = ?
                  AND error IS NULL
                  AND captured_at_utc >= ?
                  {end_clause}
                ORDER BY balance_usdt DESC, captured_at_utc ASC, id ASC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
            return dict(row) if row is not None else None

    def get_lock_state(self, lock_name: str) -> Optional[Dict[str, Any]]:
        name = self._scoped_lock_name(lock_name)
        if not name:
            return None
        with self._connect_ctx() as conn:
            row = conn.execute(
                """
                SELECT holder
                FROM locks
                WHERE lock_name = ?
                LIMIT 1
                """,
                (name,),
            ).fetchone()
            if row is None:
                return None
            holder = row["holder"]
            if holder is None:
                return None
            try:
                loaded = json.loads(str(holder))
            except (TypeError, ValueError):
                return None
            return loaded if isinstance(loaded, dict) else None

    def set_lock_state(self, lock_name: str, state: Dict[str, Any]) -> None:
        name = self._scoped_lock_name(lock_name)
        if not name:
            return
        payload = json.dumps(state or {}, ensure_ascii=False)
        with self._connect_ctx() as conn:
            conn.execute(
                """
                INSERT INTO locks (lock_name, holder, updated_at_utc)
                VALUES (?, ?, ?)
                ON CONFLICT(lock_name) DO UPDATE SET
                    holder = excluded.holder,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (name, payload, utc_now_iso()),
            )

    def add_equity_recovery_event(
        self,
        cycle_key: str,
        cycle_min_captured_at_utc: str,
        cycle_min_equity_usdt: float,
        current_captured_at_utc: str,
        current_equity_usdt: float,
        trigger_pct: float,
        threshold_equity_usdt: float,
        reduce_ratio: float,
        open_positions: int,
        adjusted_positions: int,
        reduced_notional_usdt: float,
        error_count: int,
        details: Optional[Dict[str, Any]] = None,
    ) -> int:
        with self._connect_ctx() as conn:
            cursor = conn.execute(
                """
                INSERT INTO equity_recovery_events (
                    account_id, cycle_key, cycle_min_captured_at_utc, cycle_min_equity_usdt,
                    current_captured_at_utc, current_equity_usdt,
                    trigger_pct, threshold_equity_usdt, reduce_ratio,
                    open_positions, adjusted_positions, reduced_notional_usdt, error_count,
                    details_json, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.account_id,
                    (cycle_key or "").strip()[:64],
                    (cycle_min_captured_at_utc or "").strip(),
                    float(cycle_min_equity_usdt),
                    (current_captured_at_utc or "").strip(),
                    float(current_equity_usdt),
                    float(trigger_pct),
                    float(threshold_equity_usdt),
                    float(reduce_ratio),
                    int(open_positions),
                    int(adjusted_positions),
                    float(reduced_notional_usdt),
                    int(error_count),
                    json.dumps(details, ensure_ascii=False) if details is not None else None,
                    utc_now_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def get_earliest_wallet_snapshot_time(self) -> Optional[str]:
        with self._connect_ctx() as conn:
            row = conn.execute(
                """
                SELECT captured_at_utc
                FROM wallet_snapshots
                WHERE account_id = ? AND error IS NULL
                ORDER BY captured_at_utc ASC, id ASC
                LIMIT 1
                """,
                (self.account_id,),
            ).fetchone()
            return str(row["captured_at_utc"]) if row is not None else None

    def add_cashflow_event(
        self,
        event_time_utc: str,
        asset: str,
        amount: float,
        income_type: str,
        symbol: Optional[str] = None,
        tran_id: Optional[str] = None,
        info: Optional[str] = None,
        raw_json: Optional[Dict[str, Any]] = None,
    ) -> bool:
        normalized_asset = (asset or "").upper().strip()
        normalized_type = (income_type or "").upper().strip()
        normalized_symbol = (symbol or "").upper().strip() or None
        normalized_tran_id = (str(tran_id).strip() if tran_id is not None else "") or None
        normalized_info = (info or "").strip() or None
        raw_trade_id = str((raw_json or {}).get("tradeId") or "").strip()
        if normalized_tran_id:
            unique_source = f"{self.account_id}|tran|{normalized_tran_id}"
        elif raw_trade_id:
            unique_source = (
                f"{self.account_id}|trade|{normalized_symbol or ''}|{raw_trade_id}|{normalized_type}"
            )
        else:
            unique_source = "|".join(
                [
                    self.account_id,
                    event_time_utc,
                    normalized_asset,
                    f"{float(amount):.12f}",
                    normalized_type,
                    normalized_symbol or "",
                    normalized_info or "",
                ]
            )
        unique_key = hashlib.sha1(unique_source.encode("utf-8")).hexdigest()
        payload_json = json.dumps(raw_json, ensure_ascii=False) if raw_json is not None else None
        with self._connect_ctx() as conn:
            if normalized_tran_id:
                existing = conn.execute(
                    """
                    SELECT 1 FROM cashflow_events
                    WHERE account_id = ? AND tran_id = ?
                    LIMIT 1
                    """,
                    (self.account_id, normalized_tran_id),
                ).fetchone()
                if existing is not None:
                    return False
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO cashflow_events (
                    account_id, unique_key, event_time_utc, asset, amount, income_type,
                    symbol, tran_id, info, raw_json, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.account_id,
                    unique_key,
                    event_time_utc,
                    normalized_asset,
                    float(amount),
                    normalized_type,
                    normalized_symbol,
                    normalized_tran_id,
                    normalized_info,
                    payload_json,
                    utc_now_iso(),
                ),
            )
            return int(cursor.rowcount or 0) > 0

    def get_latest_cashflow_event_time(
        self,
        asset: str = "USDT",
        income_type: Optional[str] = None,
    ) -> Optional[str]:
        with self._connect_ctx() as conn:
            where = "account_id = ? AND asset = ?"
            params: List[Any] = [self.account_id, (asset or "USDT").upper()]
            if income_type:
                where += " AND income_type = ?"
                params.append(str(income_type).upper().strip())
            row = conn.execute(
                f"""
                SELECT event_time_utc
                FROM cashflow_events
                WHERE {where}
                ORDER BY event_time_utc DESC, id DESC
                LIMIT 1
                """,
                tuple(params),
            ).fetchone()
            return str(row["event_time_utc"]) if row is not None else None

    def replace_account_state(
        self,
        *,
        captured_at_utc: str,
        wallet_balance: float,
        unrealized_pnl: float,
        equity: float,
        available_balance: float,
        positions: List[Dict[str, Any]],
        raw_json: Optional[Dict[str, Any]] = None,
        stream_status: str = "REST",
    ) -> None:
        """Atomically replace the account's shared current snapshot."""
        payload_json = json.dumps(raw_json, ensure_ascii=False) if raw_json is not None else None
        with self._connect_ctx() as conn:
            conn.execute(
                """
                INSERT INTO account_state (
                    account_id, captured_at_utc, wallet_balance, unrealized_pnl,
                    equity, available_balance, stream_status, raw_json, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    captured_at_utc = excluded.captured_at_utc,
                    wallet_balance = excluded.wallet_balance,
                    unrealized_pnl = excluded.unrealized_pnl,
                    equity = excluded.equity,
                    available_balance = excluded.available_balance,
                    stream_status = excluded.stream_status,
                    raw_json = excluded.raw_json,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    self.account_id,
                    captured_at_utc,
                    float(wallet_balance),
                    float(unrealized_pnl),
                    float(equity),
                    float(available_balance),
                    str(stream_status or "REST").upper()[:24],
                    payload_json,
                    utc_now_iso(),
                ),
            )
            conn.execute(
                "DELETE FROM account_position_state WHERE account_id = ?",
                (self.account_id,),
            )
            for row in positions or []:
                self._upsert_account_position_row(conn, row, captured_at_utc)

    def upsert_account_position_updates(
        self,
        positions: List[Dict[str, Any]],
        *,
        captured_at_utc: Optional[str] = None,
    ) -> None:
        """Apply the changed-position subset carried by ACCOUNT_UPDATE."""
        captured_at = captured_at_utc or utc_now_iso()
        with self._connect_ctx() as conn:
            for row in positions or []:
                self._upsert_account_position_row(conn, row, captured_at)

    def apply_account_stream_update(
        self,
        *,
        balances: List[Dict[str, Any]],
        positions: List[Dict[str, Any]],
        captured_at_utc: str,
        asset: str = "USDT",
    ) -> None:
        """Merge the changed subset delivered by ACCOUNT_UPDATE."""
        self.upsert_account_position_updates(positions, captured_at_utc=captured_at_utc)
        normalized_asset = str(asset or "USDT").strip().upper()
        balance_row = next(
            (
                row
                for row in balances or []
                if str(row.get("asset") or row.get("a") or "").strip().upper() == normalized_asset
            ),
            None,
        )
        with self._connect_ctx() as conn:
            current = conn.execute(
                "SELECT * FROM account_state WHERE account_id = ?",
                (self.account_id,),
            ).fetchone()
            if current is None and balance_row is None:
                return
            try:
                wallet_balance = float(
                    (balance_row or {}).get("walletBalance")
                    or (balance_row or {}).get("wb")
                    or (current["wallet_balance"] if current is not None else 0.0)
                )
            except (TypeError, ValueError):
                wallet_balance = float(current["wallet_balance"] if current is not None else 0.0)
            pnl_row = conn.execute(
                """
                SELECT COALESCE(SUM(unrealized_pnl), 0) AS total
                FROM account_position_state
                WHERE account_id = ? AND ABS(position_amt) > 0.000000000001
                """,
                (self.account_id,),
            ).fetchone()
            unrealized_pnl = float(pnl_row["total"] or 0.0) if pnl_row is not None else 0.0
            available_balance = float(current["available_balance"] or 0.0) if current is not None else 0.0
            conn.execute(
                """
                INSERT INTO account_state (
                    account_id, captured_at_utc, wallet_balance, unrealized_pnl,
                    equity, available_balance, stream_status, raw_json, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, 'STREAM', NULL, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    captured_at_utc = excluded.captured_at_utc,
                    wallet_balance = excluded.wallet_balance,
                    unrealized_pnl = excluded.unrealized_pnl,
                    equity = excluded.equity,
                    stream_status = 'STREAM',
                    updated_at_utc = excluded.updated_at_utc
                """,
                (
                    self.account_id,
                    captured_at_utc,
                    wallet_balance,
                    unrealized_pnl,
                    wallet_balance + unrealized_pnl,
                    available_balance,
                    utc_now_iso(),
                ),
            )

    def _upsert_account_position_row(
        self,
        conn: sqlite3.Connection,
        row: Dict[str, Any],
        captured_at_utc: str,
    ) -> None:
        symbol = str(row.get("symbol") or row.get("s") or "").strip().upper()
        if not symbol:
            return
        position_side = str(row.get("positionSide") or row.get("ps") or "BOTH").strip().upper() or "BOTH"

        def number(*keys: str) -> Optional[float]:
            for key in keys:
                value = row.get(key)
                if value is None:
                    continue
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
            return None

        position_amt = number("positionAmt", "pa") or 0.0
        notional = number("notional")
        mark_price = number("markPrice")
        if (mark_price is None or mark_price <= 0) and notional is not None and abs(position_amt) > 1e-12:
            mark_price = abs(notional / position_amt)
        conn.execute(
            """
            INSERT INTO account_position_state (
                account_id, symbol, position_side, position_amt, entry_price,
                break_even_price, mark_price, unrealized_pnl, liquidation_price,
                leverage, notional, isolated_margin, initial_margin,
                captured_at_utc, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, symbol, position_side) DO UPDATE SET
                position_amt = excluded.position_amt,
                entry_price = COALESCE(excluded.entry_price, account_position_state.entry_price),
                break_even_price = COALESCE(excluded.break_even_price, account_position_state.break_even_price),
                mark_price = COALESCE(excluded.mark_price, account_position_state.mark_price),
                unrealized_pnl = COALESCE(excluded.unrealized_pnl, account_position_state.unrealized_pnl),
                liquidation_price = COALESCE(excluded.liquidation_price, account_position_state.liquidation_price),
                leverage = COALESCE(excluded.leverage, account_position_state.leverage),
                notional = COALESCE(excluded.notional, account_position_state.notional),
                isolated_margin = COALESCE(excluded.isolated_margin, account_position_state.isolated_margin),
                initial_margin = COALESCE(excluded.initial_margin, account_position_state.initial_margin),
                captured_at_utc = excluded.captured_at_utc,
                raw_json = excluded.raw_json
            """,
            (
                self.account_id,
                symbol,
                position_side,
                float(position_amt),
                number("entryPrice", "ep"),
                number("breakEvenPrice", "bep"),
                mark_price,
                number("unRealizedProfit", "unrealizedProfit", "up"),
                number("liquidationPrice"),
                number("leverage"),
                notional,
                number("isolatedMargin", "isolatedWallet", "iw"),
                number("positionInitialMargin", "initialMargin"),
                captured_at_utc,
                json.dumps(row, ensure_ascii=False),
            ),
        )

    def get_latest_account_state(self) -> Optional[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            row = conn.execute(
                "SELECT * FROM account_state WHERE account_id = ? LIMIT 1",
                (self.account_id,),
            ).fetchone()
            return dict(row) if row is not None else None

    def list_account_position_state(self, *, active_only: bool = False) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT * FROM account_position_state
                WHERE account_id = ?
                  AND (? = 0 OR ABS(position_amt) > 0.000000000001)
                ORDER BY symbol, position_side
                """,
                (self.account_id, 1 if active_only else 0),
            ).fetchall()
            return [dict(row) for row in rows]

    @staticmethod
    def _exchange_order_key(order: Dict[str, Any]) -> str:
        order_id = order.get("orderId") or order.get("algoId") or order.get("actualOrderId")
        if order_id not in (None, ""):
            return f"id:{order_id}"
        client_id = order.get("clientOrderId") or order.get("clientAlgoId") or order.get("c")
        if client_id not in (None, ""):
            return f"client:{client_id}"
        symbol = str(order.get("symbol") or order.get("s") or "").upper()
        event_time = order.get("updateTime") or order.get("time") or order.get("T") or order.get("E")
        return f"fallback:{symbol}:{event_time or 0}"

    def upsert_exchange_order_state(
        self,
        order: Dict[str, Any],
        *,
        source: str,
        event_time_utc: Optional[str] = None,
    ) -> None:
        event_time = event_time_utc or utc_now_iso()
        order_key = self._exchange_order_key(order)

        def number(*keys: str) -> Optional[float]:
            for key in keys:
                value = order.get(key)
                if value is None:
                    continue
                try:
                    return float(value)
                except (TypeError, ValueError):
                    continue
            return None

        def flag(*keys: str) -> Optional[int]:
            for key in keys:
                if key not in order:
                    continue
                value = order.get(key)
                if isinstance(value, bool):
                    return int(value)
                return int(str(value or "").strip().lower() in {"1", "true", "yes"})
            return None

        with self._connect_ctx() as conn:
            conn.execute(
                """
                INSERT INTO exchange_order_state (
                    account_id, order_key, symbol, order_id, client_order_id,
                    type, side, position_side, status, execution_type,
                    price, stop_price, avg_price, original_qty, executed_qty,
                    reduce_only, close_position, event_time_utc, source, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, order_key) DO UPDATE SET
                    symbol = excluded.symbol,
                    order_id = COALESCE(excluded.order_id, exchange_order_state.order_id),
                    client_order_id = COALESCE(excluded.client_order_id, exchange_order_state.client_order_id),
                    type = COALESCE(excluded.type, exchange_order_state.type),
                    side = COALESCE(excluded.side, exchange_order_state.side),
                    position_side = COALESCE(excluded.position_side, exchange_order_state.position_side),
                    status = COALESCE(excluded.status, exchange_order_state.status),
                    execution_type = COALESCE(excluded.execution_type, exchange_order_state.execution_type),
                    price = COALESCE(excluded.price, exchange_order_state.price),
                    stop_price = COALESCE(excluded.stop_price, exchange_order_state.stop_price),
                    avg_price = COALESCE(excluded.avg_price, exchange_order_state.avg_price),
                    original_qty = COALESCE(excluded.original_qty, exchange_order_state.original_qty),
                    executed_qty = COALESCE(excluded.executed_qty, exchange_order_state.executed_qty),
                    reduce_only = COALESCE(excluded.reduce_only, exchange_order_state.reduce_only),
                    close_position = COALESCE(excluded.close_position, exchange_order_state.close_position),
                    event_time_utc = excluded.event_time_utc,
                    source = excluded.source,
                    raw_json = excluded.raw_json
                """,
                (
                    self.account_id,
                    order_key,
                    str(order.get("symbol") or order.get("s") or "").strip().upper(),
                    str(order.get("orderId") or order.get("algoId") or order.get("i") or "").strip() or None,
                    str(order.get("clientOrderId") or order.get("clientAlgoId") or order.get("c") or "").strip() or None,
                    str(order.get("type") or order.get("orderType") or order.get("o") or "").strip().upper() or None,
                    str(order.get("side") or order.get("S") or "").strip().upper() or None,
                    str(order.get("positionSide") or order.get("ps") or "").strip().upper() or None,
                    str(order.get("status") or order.get("X") or order.get("algoStatus") or "").strip().upper() or None,
                    str(order.get("executionType") or order.get("x") or "").strip().upper() or None,
                    number("price", "p"),
                    number("stopPrice", "triggerPrice", "sp"),
                    number("avgPrice", "ap"),
                    number("origQty", "quantity", "q"),
                    number("executedQty", "z"),
                    flag("reduceOnly", "R"),
                    flag("closePosition", "cp"),
                    event_time,
                    str(source or "UNKNOWN").upper()[:32],
                    json.dumps(order, ensure_ascii=False),
                ),
            )

    def reconcile_open_order_state(self, orders: List[Dict[str, Any]]) -> None:
        returned_keys = {self._exchange_order_key(order) for order in orders or [] if isinstance(order, dict)}
        returned_by_id = {
            str(order.get("orderId") or order.get("algoId") or "").strip(): order
            for order in orders or []
            if isinstance(order, dict)
            and str(order.get("orderId") or order.get("algoId") or "").strip()
        }
        with self._connect_ctx() as conn:
            active_rows = conn.execute(
                """
                SELECT order_key, raw_json FROM exchange_order_state
                WHERE account_id = ? AND status IN (
                    'NEW', 'PENDING', 'ACTIVE', 'PARTIALLY_FILLED', 'TRIGGERING', 'TRIGGERED'
                )
                """,
                (self.account_id,),
            ).fetchall()
            missing: List[str] = []
            for row in active_rows:
                order_key = str(row["order_key"])
                if order_key in returned_keys:
                    continue
                linked_actual: Optional[Dict[str, Any]] = None
                try:
                    raw_payload = json.loads(str(row["raw_json"] or ""))
                except (TypeError, ValueError, json.JSONDecodeError):
                    raw_payload = {}
                actual_order_id = str(
                    raw_payload.get("actualOrderId")
                    or raw_payload.get("actualOrderID")
                    or raw_payload.get("aoid")
                    or ""
                ).strip()
                if actual_order_id:
                    linked_actual = returned_by_id.get(actual_order_id)
                if linked_actual is not None:
                    linked_status = str(linked_actual.get("status") or "TRIGGERED").upper()
                    conn.execute(
                        """
                        UPDATE exchange_order_state
                        SET status = ?, source = 'REST_VERIFY_LINK', event_time_utc = ?
                        WHERE account_id = ? AND order_key = ?
                        """,
                        (linked_status, utc_now_iso(), self.account_id, order_key),
                    )
                    continue
                missing.append(order_key)
            for order_key in missing:
                conn.execute(
                    """
                    UPDATE exchange_order_state
                    SET status = 'MISSING', source = 'REST_VERIFY', event_time_utc = ?
                    WHERE account_id = ? AND order_key = ?
                    """,
                    (utc_now_iso(), self.account_id, order_key),
                )
        for order in orders or []:
            if isinstance(order, dict):
                self.upsert_exchange_order_state(order, source="REST_VERIFY")

    def get_exchange_order_status(
        self,
        *,
        symbol: str,
        order_id: object = None,
        client_order_id: object = None,
    ) -> Optional[str]:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_order_id = str(order_id).strip() if order_id not in (None, "") else ""
        normalized_client_id = str(client_order_id).strip() if client_order_id not in (None, "") else ""
        with self._connect_ctx() as conn:
            row = conn.execute(
                """
                SELECT status FROM exchange_order_state
                WHERE account_id = ? AND symbol = ?
                  AND ((? != '' AND order_id = ?) OR (? != '' AND client_order_id = ?))
                ORDER BY event_time_utc DESC LIMIT 1
                """,
                (
                    self.account_id,
                    normalized_symbol,
                    normalized_order_id,
                    normalized_order_id,
                    normalized_client_id,
                    normalized_client_id,
                ),
            ).fetchone()
            return str(row["status"]).upper() if row is not None and row["status"] else None

    def get_exchange_order_state(
        self,
        *,
        symbol: str,
        order_id: object = None,
        client_order_id: object = None,
    ) -> Optional[Dict[str, Any]]:
        normalized_symbol = str(symbol or "").strip().upper()
        normalized_order_id = str(order_id).strip() if order_id not in (None, "") else ""
        normalized_client_id = str(client_order_id).strip() if client_order_id not in (None, "") else ""
        with self._connect_ctx() as conn:
            row = conn.execute(
                """
                SELECT * FROM exchange_order_state
                WHERE account_id = ? AND symbol = ?
                  AND ((? != '' AND order_id = ?) OR (? != '' AND client_order_id = ?))
                ORDER BY event_time_utc DESC LIMIT 1
                """,
                (
                    self.account_id,
                    normalized_symbol,
                    normalized_order_id,
                    normalized_order_id,
                    normalized_client_id,
                    normalized_client_id,
                ),
            ).fetchone()
            return dict(row) if row is not None else None

    def list_exchange_order_state(self, *, active_only: bool = False) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT * FROM exchange_order_state
                WHERE account_id = ?
                  AND (? = 0 OR status IN ('NEW', 'PENDING', 'ACTIVE', 'PARTIALLY_FILLED', 'TRIGGERING', 'TRIGGERED'))
                ORDER BY event_time_utc DESC
                """,
                (self.account_id, 1 if active_only else 0),
            ).fetchall()
            return [dict(row) for row in rows]

    def update_parent_algo_order_status(
        self,
        *,
        actual_order_id: object,
        status: str,
        event_time_utc: str,
    ) -> int:
        """Apply an actual matching-engine order result to its parent algo."""
        normalized_actual_id = str(actual_order_id or "").strip()
        normalized_status = str(status or "").strip().upper()
        if not normalized_actual_id or not normalized_status:
            return 0
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT order_key, raw_json
                FROM exchange_order_state
                WHERE account_id = ? AND raw_json IS NOT NULL
                """,
                (self.account_id,),
            ).fetchall()
            parent_keys: List[str] = []
            for row in rows:
                try:
                    payload = json.loads(str(row["raw_json"]))
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                candidate = (
                    payload.get("actualOrderId")
                    or payload.get("actualOrderID")
                    or payload.get("aoid")
                )
                if str(candidate or "").strip() == normalized_actual_id:
                    parent_keys.append(str(row["order_key"]))
            for order_key in parent_keys:
                conn.execute(
                    """
                    UPDATE exchange_order_state
                    SET status = ?, source = 'ORDER_TRADE_UPDATE_LINK', event_time_utc = ?
                    WHERE account_id = ? AND order_key = ?
                    """,
                    (normalized_status, event_time_utc, self.account_id, order_key),
                )
            return len(parent_keys)

    def add_binance_income_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        inserted: List[Dict[str, Any]] = []
        with self._connect_ctx() as conn:
            for record in records or []:
                tran_id = str(record.get("tranId") or "").strip()
                trade_id = str(record.get("tradeId") or "").strip()
                symbol = str(record.get("symbol") or "").strip().upper()
                income_type = str(record.get("incomeType") or "").strip().upper()
                try:
                    event_time_ms = int(record.get("time") or 0)
                    income = float(record.get("income") or 0.0)
                except (TypeError, ValueError):
                    continue
                if event_time_ms <= 0 or not income_type:
                    continue
                if tran_id:
                    unique_source = f"tran:{tran_id}"
                elif trade_id:
                    unique_source = f"trade:{symbol}:{trade_id}:{income_type}"
                else:
                    unique_source = (
                        f"fallback:{symbol}:{event_time_ms}:{income_type}:"
                        f"{record.get('asset') or ''}:{income:.12f}"
                    )
                unique_key = hashlib.sha1(unique_source.encode("utf-8")).hexdigest()
                if tran_id:
                    existing = conn.execute(
                        """
                        SELECT 1 FROM binance_income_records
                        WHERE account_id = ? AND tran_id = ?
                        LIMIT 1
                        """,
                        (self.account_id, tran_id),
                    ).fetchone()
                    if existing is not None:
                        continue
                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO binance_income_records (
                        account_id, unique_key, tran_id, trade_id, symbol,
                        income_type, asset, income, event_time_ms, raw_json, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.account_id,
                        unique_key,
                        tran_id or None,
                        trade_id or None,
                        symbol or None,
                        income_type,
                        str(record.get("asset") or "").strip().upper() or None,
                        income,
                        event_time_ms,
                        json.dumps(record, ensure_ascii=False),
                        utc_now_iso(),
                    ),
                )
                if int(cursor.rowcount or 0) > 0:
                    inserted.append(record)
        return inserted

    def add_binance_user_trades(self, trades: List[Dict[str, Any]]) -> int:
        inserted = 0
        with self._connect_ctx() as conn:
            for trade in trades or []:
                symbol = str(trade.get("symbol") or "").strip().upper()
                trade_id = str(trade.get("id") or trade.get("tradeId") or "").strip()
                try:
                    event_time_ms = int(trade.get("time") or 0)
                except (TypeError, ValueError):
                    continue
                if not symbol or not trade_id or event_time_ms <= 0:
                    continue

                def number(key: str) -> Optional[float]:
                    try:
                        value = trade.get(key)
                        return float(value) if value is not None else None
                    except (TypeError, ValueError):
                        return None

                cursor = conn.execute(
                    """
                    INSERT OR IGNORE INTO binance_user_trades (
                        account_id, symbol, trade_id, order_id, event_time_ms,
                        realized_pnl, commission, commission_asset, side,
                        qty, price, quote_qty, raw_json, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self.account_id,
                        symbol,
                        trade_id,
                        str(trade.get("orderId") or "").strip() or None,
                        event_time_ms,
                        number("realizedPnl") or 0.0,
                        number("commission") or 0.0,
                        str(trade.get("commissionAsset") or "").strip().upper() or None,
                        str(trade.get("side") or "").strip().upper() or None,
                        number("qty"),
                        number("price"),
                        number("quoteQty"),
                        json.dumps(trade, ensure_ascii=False),
                        utc_now_iso(),
                    ),
                )
                inserted += int(cursor.rowcount or 0)
        return inserted

    def load_binance_income_records(self, start_time_ms: int) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT * FROM binance_income_records
                WHERE account_id = ? AND event_time_ms >= ?
                ORDER BY event_time_ms, unique_key
                """,
                (self.account_id, int(start_time_ms)),
            ).fetchall()
            return [dict(row) for row in rows]

    def load_binance_user_trades(self, start_time_ms: int) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT * FROM binance_user_trades
                WHERE account_id = ? AND event_time_ms >= ?
                ORDER BY event_time_ms, symbol, trade_id
                """,
                (self.account_id, int(start_time_ms)),
            ).fetchall()
            return [dict(row) for row in rows]

    def prune_binance_trade_ledger(self, before_time_ms: int) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                "DELETE FROM binance_income_records WHERE account_id = ? AND event_time_ms < ?",
                (self.account_id, int(before_time_ms)),
            )
            conn.execute(
                "DELETE FROM binance_user_trades WHERE account_id = ? AND event_time_ms < ?",
                (self.account_id, int(before_time_ms)),
            )

    def put_daily_open_price(self, day_utc: str, symbol: str, price: float, source: str) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                INSERT INTO daily_open_prices (day_utc, symbol, open_price, source, updated_at_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(day_utc, symbol) DO UPDATE SET
                    open_price = excluded.open_price,
                    source = excluded.source,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (day_utc, str(symbol).upper(), float(price), str(source).upper()[:24], utc_now_iso()),
            )

    def get_daily_open_prices(self, day_utc: str) -> Dict[str, float]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                "SELECT symbol, open_price FROM daily_open_prices WHERE day_utc = ?",
                (day_utc,),
            ).fetchall()
            return {str(row["symbol"]): float(row["open_price"]) for row in rows}

    def put_market_data_cache(self, cache_key: str, payload: Any, expires_at_utc: str) -> None:
        with self._connect_ctx() as conn:
            conn.execute(
                """
                INSERT INTO market_data_cache (cache_key, payload_json, expires_at_utc, updated_at_utc)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    expires_at_utc = excluded.expires_at_utc,
                    updated_at_utc = excluded.updated_at_utc
                """,
                (cache_key, json.dumps(payload, ensure_ascii=False), expires_at_utc, utc_now_iso()),
            )

    def claim_market_data_attempt(self, cache_key: str, expires_at_utc: str) -> bool:
        """Atomically claim a once-per-cache-key external request."""
        with self._connect_ctx() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO market_data_cache (
                    cache_key, payload_json, expires_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    cache_key,
                    json.dumps({"attempted": True}),
                    expires_at_utc,
                    utc_now_iso(),
                ),
            )
            return int(cursor.rowcount or 0) == 1

    def get_market_data_cache(self, cache_key: str, *, now_utc: Optional[datetime] = None) -> Optional[Any]:
        now = (now_utc or datetime.now(timezone.utc)).replace(microsecond=0)
        with self._connect_ctx() as conn:
            row = conn.execute(
                "SELECT payload_json, expires_at_utc FROM market_data_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            if row is None:
                return None
            try:
                expires_at = datetime.fromisoformat(str(row["expires_at_utc"]))
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                if expires_at <= now:
                    return None
                return json.loads(str(row["payload_json"]))
            except (TypeError, ValueError, json.JSONDecodeError):
                return None

    def _scoped_lock_name(self, lock_name: str) -> str:
        raw = (lock_name or "").strip()
        if not raw:
            return ""
        return f"{self.account_id}:{raw}"

    def _insert_fill_from_order_event(
        self,
        conn: sqlite3.Connection,
        order_event_id: int,
        position_id: Optional[int],
        symbol: str,
        event_time_utc: str,
        order_payload: Dict[str, Any],
    ) -> None:
        status = str(order_payload.get("status") or "").strip().upper() or None
        executed_qty = _safe_float(order_payload.get("executedQty"))
        orig_qty = _safe_float(order_payload.get("origQty"))
        if executed_qty is None and status == "FILLED":
            executed_qty = orig_qty
        if executed_qty is None or executed_qty <= 0:
            return

        quote_qty = _safe_float(order_payload.get("cumQuote"))
        avg_price = _safe_float(order_payload.get("avgPrice"))
        if (avg_price is None or avg_price <= 0) and quote_qty is not None and quote_qty > 0:
            avg_price = quote_qty / executed_qty
        if avg_price is not None and avg_price <= 0:
            avg_price = None

        commission = _safe_float(order_payload.get("commission"))
        realized_pnl = _safe_float(order_payload.get("realizedPnl"))
        side = (str(order_payload.get("side") or "").strip().upper() or None)
        client_order_id = (str(order_payload.get("clientOrderId") or "").strip() or None)
        commission_asset = (str(order_payload.get("commissionAsset") or "").strip().upper() or None)
        reduce_only = _safe_bool_int(order_payload.get("reduceOnly"))

        conn.execute(
            """
            INSERT INTO fills (
                order_event_id, position_id, symbol,
                order_id, client_order_id, side, reduce_only, status,
                executed_qty, quote_qty, avg_price,
                realized_pnl, commission, commission_asset,
                event_time_utc, raw_json, created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_event_id) DO UPDATE SET
                position_id = COALESCE(excluded.position_id, fills.position_id),
                symbol = excluded.symbol,
                order_id = COALESCE(excluded.order_id, fills.order_id),
                client_order_id = COALESCE(excluded.client_order_id, fills.client_order_id),
                side = COALESCE(excluded.side, fills.side),
                reduce_only = COALESCE(excluded.reduce_only, fills.reduce_only),
                status = COALESCE(excluded.status, fills.status),
                executed_qty = excluded.executed_qty,
                quote_qty = COALESCE(excluded.quote_qty, fills.quote_qty),
                avg_price = COALESCE(excluded.avg_price, fills.avg_price),
                realized_pnl = COALESCE(excluded.realized_pnl, fills.realized_pnl),
                commission = COALESCE(excluded.commission, fills.commission),
                commission_asset = COALESCE(excluded.commission_asset, fills.commission_asset),
                event_time_utc = excluded.event_time_utc,
                raw_json = excluded.raw_json
            """,
            (
                int(order_event_id),
                int(position_id) if position_id is not None else None,
                (symbol or "").strip().upper(),
                _safe_int(order_payload.get("orderId")),
                client_order_id,
                side,
                reduce_only,
                status,
                float(executed_qty),
                quote_qty,
                avg_price,
                realized_pnl,
                commission,
                commission_asset,
                event_time_utc,
                json.dumps(order_payload, ensure_ascii=False),
                utc_now_iso(),
            ),
        )


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_bool_int(value: Any) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return 1 if float(value) != 0 else 0
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return 1
    return 0
