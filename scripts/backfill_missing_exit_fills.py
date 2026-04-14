#!/usr/bin/env python3
import argparse
import configparser
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_DB = ROOT / "state.db"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def parse_iso_utc(text: str) -> datetime:
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_fill_from_order_event_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if str(row.get("side") or "").upper() != "BUY":
        return None
    if str(row.get("status") or "").upper() != "FILLED":
        return None
    payload = json.loads(str(row.get("raw_json") or "{}"))
    executed_qty = safe_float(payload.get("executedQty"))
    if executed_qty is None or executed_qty <= 0:
        executed_qty = safe_float(row.get("qty"))
    if executed_qty is None or executed_qty <= 0:
        executed_qty = safe_float(payload.get("origQty"))
    if executed_qty is None or executed_qty <= 0:
        return None

    quote_qty = safe_float(payload.get("cumQuote"))
    avg_price = safe_float(payload.get("avgPrice"))
    if (avg_price is None or avg_price <= 0) and quote_qty and quote_qty > 0:
        avg_price = quote_qty / executed_qty
    if (avg_price is None or avg_price <= 0) and safe_float(row.get("price")) not in (None, 0.0):
        avg_price = safe_float(row.get("price"))
    if avg_price is None or avg_price <= 0:
        return None

    return {
        "position_id": safe_int(row.get("position_id")),
        "symbol": str(row.get("symbol") or "").upper(),
        "order_id": safe_int(row.get("order_id")) or safe_int(payload.get("orderId")),
        "client_order_id": str(row.get("client_order_id") or payload.get("clientOrderId") or "").strip() or None,
        "side": "BUY",
        "reduce_only": 1 if str(payload.get("reduceOnly")).lower() in {"1", "true"} else 0,
        "status": "FILLED",
        "executed_qty": float(executed_qty),
        "quote_qty": quote_qty,
        "avg_price": float(avg_price),
        "realized_pnl": safe_float(payload.get("realizedPnl")),
        "commission": safe_float(payload.get("commission")),
        "commission_asset": str(payload.get("commissionAsset") or "").upper() or None,
        "event_time_utc": str(row.get("event_time_utc") or utc_now_iso()),
        "type": str(row.get("type") or payload.get("type") or "MARKET").upper(),
        "raw_json": payload,
        "source": "local_order_event",
    }


def aggregate_trade_rows(position_id: int, symbol: str, trades: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    normalized = []
    for trade in trades:
        side = str(trade.get("side") or "").upper()
        qty = safe_float(trade.get("qty"))
        if side != "BUY" or qty is None or qty <= 0:
            continue
        normalized.append(trade)
    if not normalized:
        return None

    order_ids = [safe_int(trade.get("orderId")) for trade in normalized if safe_int(trade.get("orderId")) is not None]
    order_id = order_ids[0] if order_ids else None
    executed_qty = sum(float(trade.get("qty") or 0.0) for trade in normalized)
    quote_qty = sum(float(trade.get("quoteQty") or 0.0) for trade in normalized)
    avg_price = quote_qty / executed_qty if executed_qty > 0 and quote_qty > 0 else None
    if avg_price is None:
        weighted = sum(float(trade.get("qty") or 0.0) * float(trade.get("price") or 0.0) for trade in normalized)
        avg_price = weighted / executed_qty if executed_qty > 0 and weighted > 0 else None
    if avg_price is None or avg_price <= 0:
        return None

    event_time_ms = max(int(trade.get("time") or 0) for trade in normalized)
    event_time_utc = datetime.fromtimestamp(event_time_ms / 1000.0, tz=timezone.utc).replace(microsecond=0).isoformat()
    return {
        "position_id": int(position_id),
        "symbol": str(symbol or "").upper(),
        "order_id": order_id,
        "client_order_id": None,
        "side": "BUY",
        "reduce_only": 1,
        "status": "FILLED",
        "executed_qty": executed_qty,
        "quote_qty": quote_qty if quote_qty > 0 else None,
        "avg_price": avg_price,
        "realized_pnl": sum(float(trade.get("realizedPnl") or 0.0) for trade in normalized),
        "commission": sum(float(trade.get("commission") or 0.0) for trade in normalized),
        "commission_asset": str(normalized[-1].get("commissionAsset") or "").upper() or None,
        "event_time_utc": event_time_utc,
        "type": "MARKET",
        "raw_json": {"trades": list(normalized)},
        "source": "binance_user_trades",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill missing close BUY fills.")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite db path")
    parser.add_argument("--config", default="", help="Config path for Binance API lookups")
    parser.add_argument("--account", default="", help="Optional single account id to process")
    parser.add_argument("--limit", type=int, default=0, help="Optional max positions to inspect")
    parser.add_argument("--apply", action="store_true", help="Write recovered order_events/fills into db")
    parser.add_argument("--remote", action="store_true", help="Enable Binance history lookup; requires --config")
    return parser.parse_args()


def load_account_clients(config_path: str) -> Dict[str, Any]:
    if not config_path:
        return {}
    from core.runtime_components import create_components

    cfg = configparser.ConfigParser()
    read_ok = cfg.read(config_path)
    if not read_ok:
        raise FileNotFoundError(f"config not found or unreadable: {config_path}")
    _strategy, _manager, _sampler, _runtime, _service_cfg, account_runtimes = create_components(
        cfg=cfg,
        base_dir=str(Path(config_path).resolve().parent),
    )
    return {
        account_id: runtime["strategy"].client
        for account_id, runtime in account_runtimes.items()
    }


def load_target_positions(conn: sqlite3.Connection, account_id: str = "", limit: int = 0) -> List[sqlite3.Row]:
    sql = """
        SELECT
            p.id,
            p.symbol,
            p.status,
            p.close_reason,
            p.qty,
            p.close_order_id,
            p.tp_order_id,
            p.sl_order_id,
            p.tp_client_order_id,
            p.sl_client_order_id,
            p.opened_at_utc,
            p.closed_at_utc,
            r.account_id
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        WHERE p.status != 'OPEN'
          AND NOT EXISTS (
              SELECT 1 FROM fills f
              WHERE f.position_id = p.id
                AND f.side = 'BUY'
          )
    """
    params: List[Any] = []
    if account_id:
        sql += " AND r.account_id = ?"
        params.append(account_id)
    sql += " ORDER BY p.closed_at_utc ASC, p.id ASC"
    if limit > 0:
        sql += f" LIMIT {int(limit)}"
    return conn.execute(sql, params).fetchall()


def load_position_buy_order_events(conn: sqlite3.Connection, position_id: int) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, position_id, symbol, order_id, client_order_id, type, side, price, qty, status, event_time_utc, raw_json
        FROM order_events
        WHERE position_id = ?
          AND side = 'BUY'
        ORDER BY event_time_utc DESC, id DESC
        """,
        (position_id,),
    ).fetchall()


def has_buy_fill(conn: sqlite3.Connection, position_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM fills WHERE position_id = ? AND side = 'BUY' LIMIT 1",
        (position_id,),
    ).fetchone()
    return row is not None


def has_matching_buy_fill(conn: sqlite3.Connection, position_id: int, order_id: Optional[int]) -> bool:
    if order_id is None:
        return has_buy_fill(conn, position_id)
    row = conn.execute(
        """
        SELECT 1
        FROM fills
        WHERE position_id = ?
          AND side = 'BUY'
          AND order_id = ?
        LIMIT 1
        """,
        (position_id, int(order_id)),
    ).fetchone()
    return row is not None


def should_skip_recovered_fill(has_existing_buy_fill: bool, has_matching_buy_fill: bool) -> bool:
    return has_existing_buy_fill or has_matching_buy_fill


def recover_from_local_order_events(conn: sqlite3.Connection, position: sqlite3.Row) -> Optional[Dict[str, Any]]:
    for row in load_position_buy_order_events(conn, int(position["id"])):
        fill = build_fill_from_order_event_row(dict(row))
        if fill:
            return fill
    return None


def resolve_remote_order_candidates(position: sqlite3.Row) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    close_order_id = safe_int(position["close_order_id"])
    if close_order_id is not None:
        candidates.append({"order_id": close_order_id, "client_order_id": None, "reason": "close_order_id"})
    tp_order_id = safe_int(position["tp_order_id"])
    if tp_order_id is not None:
        candidates.append(
            {
                "order_id": tp_order_id,
                "client_order_id": str(position["tp_client_order_id"] or "").strip() or None,
                "reason": "tp_order_id",
            }
        )
    sl_order_id = safe_int(position["sl_order_id"])
    if sl_order_id is not None:
        candidates.append(
            {
                "order_id": sl_order_id,
                "client_order_id": str(position["sl_client_order_id"] or "").strip() or None,
                "reason": "sl_order_id",
            }
        )
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for item in candidates:
        key = (item["order_id"], item["client_order_id"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def recover_from_binance(position: sqlite3.Row, client: Any) -> Optional[Dict[str, Any]]:
    symbol = str(position["symbol"])
    position_id = int(position["id"])
    for candidate in resolve_remote_order_candidates(position):
        try:
            order = client.get_order(
                symbol=symbol,
                order_id=candidate["order_id"],
                orig_client_order_id=candidate["client_order_id"],
            )
        except Exception:
            continue
        if str(order.get("status") or "").upper() != "FILLED":
            continue
        actual_order_id = safe_int(order.get("actualOrderId")) or safe_int(order.get("orderId")) or candidate["order_id"]
        trades = client.get_user_trades(symbol=symbol, order_id=actual_order_id, limit=1000)
        agg = aggregate_trade_rows(position_id=position_id, symbol=symbol, trades=trades)
        if agg:
            agg["raw_json"] = {"order": order, "trades": trades, "reason": candidate["reason"]}
            return agg

    opened_at = parse_iso_utc(str(position["opened_at_utc"]))
    closed_at = parse_iso_utc(str(position["closed_at_utc"]))
    start_ms = int((opened_at - timedelta(minutes=5)).timestamp() * 1000)
    end_ms = int((closed_at + timedelta(minutes=5)).timestamp() * 1000)
    try:
        trades = client.get_user_trades(symbol=symbol, start_time=start_ms, end_time=end_ms, limit=1000)
    except Exception:
        return None
    buy_trades = [trade for trade in trades if str(trade.get("side") or "").upper() == "BUY"]
    if not buy_trades:
        return None
    target_qty = abs(float(position["qty"] or 0.0))
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for trade in buy_trades:
        order_id = safe_int(trade.get("orderId")) or -1
        grouped.setdefault(order_id, []).append(trade)
    scored = []
    for order_id, grouped_trades in grouped.items():
        qty = sum(float(item.get("qty") or 0.0) for item in grouped_trades)
        last_time = max(int(item.get("time") or 0) for item in grouped_trades)
        qty_gap = abs(qty - target_qty)
        time_gap = abs(last_time - int(closed_at.timestamp() * 1000))
        scored.append((qty_gap, time_gap, grouped_trades))
    scored.sort(key=lambda item: (item[0], item[1]))
    best_trades = scored[0][2]
    agg = aggregate_trade_rows(position_id=position_id, symbol=symbol, trades=best_trades)
    if agg:
        agg["raw_json"] = {"trades": best_trades, "reason": "time_window_match"}
    return agg


def insert_recovered_fill(conn: sqlite3.Connection, position: sqlite3.Row, fill: Dict[str, Any]) -> bool:
    position_id = int(position["id"])
    order_id = safe_int(fill.get("order_id"))
    if should_skip_recovered_fill(
        has_existing_buy_fill=has_buy_fill(conn, position_id),
        has_matching_buy_fill=has_matching_buy_fill(conn, position_id, order_id),
    ):
        return False
    row = None
    if order_id is not None:
        row = conn.execute(
            """
            SELECT id
            FROM order_events
            WHERE position_id = ?
              AND side = 'BUY'
              AND order_id = ?
              AND status = 'FILLED'
            ORDER BY id DESC
            LIMIT 1
            """,
            (position_id, int(order_id)),
        ).fetchone()
    if row is None:
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
                str(fill["symbol"]),
                order_id,
                fill.get("client_order_id"),
                str(fill.get("type") or "MARKET"),
                "BUY",
                safe_float(fill.get("avg_price")) or 0.0,
                float(fill.get("executed_qty") or 0.0),
                "FILLED",
                str(fill.get("event_time_utc") or utc_now_iso()),
                json.dumps(fill.get("raw_json") or {}, ensure_ascii=False),
            ),
        )
        order_event_id = int(cursor.lastrowid)
    else:
        order_event_id = int(row["id"])
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
            order_event_id,
            position_id,
            str(fill["symbol"]),
            order_id,
            fill.get("client_order_id"),
            "BUY",
            int(fill.get("reduce_only") or 0),
            "FILLED",
            float(fill.get("executed_qty") or 0.0),
            safe_float(fill.get("quote_qty")),
            float(fill.get("avg_price") or 0.0),
            safe_float(fill.get("realized_pnl")),
            safe_float(fill.get("commission")),
            fill.get("commission_asset"),
            str(fill.get("event_time_utc") or utc_now_iso()),
            json.dumps(fill.get("raw_json") or {}, ensure_ascii=False),
            utc_now_iso(),
        ),
    )
    return True


def run_backfill(args: argparse.Namespace) -> Dict[str, Any]:
    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    clients = load_account_clients(args.config) if args.remote and args.config else {}
    summary: Counter[str] = Counter()
    recovered_examples: List[str] = []
    try:
        positions = load_target_positions(conn, account_id=args.account, limit=args.limit)
        summary["inspected"] = len(positions)
        for position in positions:
            fill = recover_from_local_order_events(conn, position)
            source = "local"
            if fill is None and args.remote:
                client = clients.get(str(position["account_id"]))
                if client is not None:
                    fill = recover_from_binance(position, client)
                    source = "remote"
            if fill is None:
                summary["unresolved"] += 1
                summary[f"unresolved_{position['status']}"] += 1
                continue
            summary["recoverable"] += 1
            summary[f"recoverable_{source}"] += 1
            summary[f"recoverable_status_{position['status']}"] += 1
            recovered_examples.append(
                f"{position['account_id']}:{position['id']}:{position['symbol']}:{position['status']}:{source}"
            )
            if args.apply:
                inserted = insert_recovered_fill(conn, position, fill)
                if inserted:
                    summary["inserted"] += 1
                else:
                    summary["skipped_existing"] += 1
        if args.apply:
            conn.commit()
        else:
            conn.rollback()
    finally:
        conn.close()
    return {"summary": dict(summary), "examples": recovered_examples[:20]}


def main() -> None:
    args = parse_args()
    if args.remote and not args.config:
        raise SystemExit("--remote requires --config")
    result = run_backfill(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
