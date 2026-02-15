import json
import os
import sqlite3
import uuid
import hashlib
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
    ) -> int:
        with self._connect() as conn:
            cursor = conn.execute(
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
            event_id = int(cursor.lastrowid)
            self._insert_fill_from_order_event(
                conn=conn,
                order_event_id=event_id,
                position_id=position_id,
                symbol=symbol,
                event_time_utc=event_time_utc,
                order_payload=order_payload,
            )
            return event_id

    def create_rebalance_cycle(
        self,
        run_id: Optional[str],
        reason_tag: str,
        mode: str,
        reduce_only: bool,
        target_count: int,
    ) -> int:
        now_iso = utc_now_iso()
        with self._connect() as conn:
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
        with self._connect() as conn:
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
        with self._connect() as conn:
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
        with self._connect() as conn:
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

    def get_earliest_wallet_snapshot_time(self) -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT captured_at_utc
                FROM wallet_snapshots
                ORDER BY captured_at_utc ASC, id ASC
                LIMIT 1
                """
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
        unique_source = "|".join(
            [
                event_time_utc,
                normalized_asset,
                f"{float(amount):.12f}",
                normalized_type,
                normalized_symbol or "",
                normalized_tran_id or "",
                normalized_info or "",
            ]
        )
        unique_key = hashlib.sha1(unique_source.encode("utf-8")).hexdigest()
        payload_json = json.dumps(raw_json, ensure_ascii=False) if raw_json is not None else None
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO cashflow_events (
                    unique_key, event_time_utc, asset, amount, income_type,
                    symbol, tran_id, info, raw_json, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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

    def get_latest_cashflow_event_time(self, asset: str = "USDT") -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT event_time_utc
                FROM cashflow_events
                WHERE asset = ?
                ORDER BY event_time_utc DESC, id DESC
                LIMIT 1
                """,
                ((asset or "USDT").upper(),),
            ).fetchone()
            return str(row["event_time_utc"]) if row is not None else None

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
            INSERT OR IGNORE INTO fills (
                order_event_id, position_id, symbol,
                order_id, client_order_id, side, reduce_only, status,
                executed_qty, quote_qty, avg_price,
                realized_pnl, commission, commission_asset,
                event_time_utc, raw_json, created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
