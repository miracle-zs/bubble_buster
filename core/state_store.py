import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass(frozen=True)
class RunState:
    run_id: str
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


class StateStore:
    def __init__(self, db_path: str, schema_path: Optional[str] = None):
        self.db_path = db_path
        self.schema_path = schema_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        if self.schema_path:
            with open(self.schema_path, "r", encoding="utf-8") as f:
                schema_sql = f.read()
        else:
            schema_sql = ""
        with self._connect() as conn:
            if schema_sql:
                conn.executescript(schema_sql)
            else:
                raise ValueError("schema_path is required for StateStore.init_schema")

    def create_run(self, trade_day_utc: str) -> Tuple[str, bool]:
        """Creates a run for a UTC trade day.

        Returns (run_id, created). If created is False, run_id is the existing run.
        """
        run_id = str(uuid.uuid4())
        started_at = utc_now_iso()
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO runs (run_id, trade_day_utc, started_at_utc, status, message)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (run_id, trade_day_utc, started_at, "RUNNING", None),
                )
                return run_id, True
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT run_id FROM runs WHERE trade_day_utc = ?",
                    (trade_day_utc,),
                ).fetchone()
                return str(row["run_id"]), False

    def finalize_run(self, run_id: str, status: str, message: Optional[str] = None) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE runs
                SET status = ?, message = ?, completed_at_utc = ?
                WHERE run_id = ?
                """,
                (status, message, utc_now_iso(), run_id),
            )

    def get_run(self, run_id: str) -> Optional[RunState]:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                return None
            return RunState(
                run_id=row["run_id"],
                trade_day_utc=row["trade_day_utc"],
                started_at_utc=row["started_at_utc"],
                completed_at_utc=row["completed_at_utc"],
                status=row["status"],
                reason=row["message"],
            )

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
        with self._connect() as conn:
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
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM positions WHERE status = 'OPEN'").fetchall()
            return [dict(row) for row in rows]

    def list_open_symbols(self) -> Set[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT symbol FROM positions WHERE status = 'OPEN'").fetchall()
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
        with self._connect() as conn:
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
        with self._connect() as conn:
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

    def set_position_qty(self, position_id: int, qty: float, entry_price: float) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE positions
                SET qty = ?, entry_price = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (qty, entry_price, utc_now_iso(), position_id),
            )

    def mark_position_closed(
        self,
        position_id: int,
        status: str,
        close_reason: Optional[str],
        close_order_id: Optional[int] = None,
    ) -> None:
        with self._connect() as conn:
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
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE positions
                SET last_error = ?, updated_at_utc = ?
                WHERE id = ?
                """,
                (error_message[:1000], utc_now_iso(), position_id),
            )

    def clear_position_error(self, position_id: int) -> None:
        with self._connect() as conn:
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
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO order_events (
                    position_id, symbol, order_id, client_order_id,
                    type, side, price, qty, status,
                    event_time_utc, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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

    def add_wallet_snapshot(
        self,
        captured_at_utc: str,
        balance_usdt: float,
        source: str = "API",
        error: Optional[str] = None,
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO wallet_snapshots (
                    captured_at_utc, balance_usdt, source, error, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    captured_at_utc,
                    float(balance_usdt),
                    source[:24],
                    (error or "")[:1000] or None,
                    utc_now_iso(),
                ),
            )
            return int(cursor.lastrowid)

    def get_latest_wallet_snapshot(self) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, captured_at_utc, balance_usdt, source, error, created_at_utc
                FROM wallet_snapshots
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            return dict(row) if row is not None else None


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
