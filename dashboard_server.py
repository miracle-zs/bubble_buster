import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DashboardServerConfig:
    host: str
    port: int
    db_path: str
    log_file: str
    timezone_name: str
    entry_hour: int
    entry_minute: int
    refresh_sec: int
    curve_points: int = 600


class DashboardDataProvider:
    def __init__(
        self,
        db_path: str,
        log_file: str,
        timezone_name: str,
        entry_hour: int,
        entry_minute: int,
        balance_fetcher: Optional[Callable[[], float]] = None,
        close_price_fetcher: Optional[Callable[[str, int], Optional[float]]] = None,
        balance_cache_ttl_sec: int = 60,
        default_curve_points: int = 600,
    ):
        self.db_path = db_path
        self.log_file = log_file
        self.entry_hour = entry_hour % 24
        self.entry_minute = entry_minute % 60
        self.balance_fetcher = balance_fetcher
        self.close_price_fetcher = close_price_fetcher
        self.balance_cache_ttl_sec = max(5, int(balance_cache_ttl_sec))
        self.default_curve_points = max(100, min(5000, int(default_curve_points)))
        self._close_price_cache: Dict[Tuple[str, int], Optional[float]] = {}
        self._balance_cache_value: Optional[float] = None
        self._balance_cache_at: Optional[datetime] = None
        self._balance_last_attempt_at: Optional[datetime] = None
        self._balance_last_error: Optional[str] = None
        try:
            self.local_tz = ZoneInfo(timezone_name)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Invalid dashboard timezone=%s, fallback UTC", timezone_name)
            self.local_tz = timezone.utc

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _next_entry_local(self, now_local: datetime) -> datetime:
        target = now_local.replace(
            hour=self.entry_hour,
            minute=self.entry_minute,
            second=0,
            microsecond=0,
        )
        if now_local >= target:
            target += timedelta(days=1)
        return target

    def _tail_log(self, lines: int = 80) -> List[str]:
        if lines <= 0:
            return []
        if not os.path.exists(self.log_file):
            return []
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                return f.read().splitlines()[-lines:]
        except OSError:
            return []

    def _query_rows(self, conn: sqlite3.Connection, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _read_wallet_balance(self, now_utc: datetime) -> Dict[str, Any]:
        if self.balance_fetcher is None:
            return {"balance_usdt": None, "as_of_utc": None, "source": "DISABLED", "error": None}

        if self._balance_cache_at and self._balance_cache_value is not None:
            age_sec = (now_utc - self._balance_cache_at).total_seconds()
            if age_sec < self.balance_cache_ttl_sec:
                return {
                    "balance_usdt": round(self._balance_cache_value, 8),
                    "as_of_utc": self._balance_cache_at.replace(microsecond=0).isoformat(),
                    "source": "CACHE",
                    "error": None,
                }

        if self._balance_last_attempt_at is not None:
            attempt_age_sec = (now_utc - self._balance_last_attempt_at).total_seconds()
            if attempt_age_sec < self.balance_cache_ttl_sec:
                if self._balance_cache_value is not None and self._balance_cache_at is not None:
                    return {
                        "balance_usdt": round(self._balance_cache_value, 8),
                        "as_of_utc": self._balance_cache_at.replace(microsecond=0).isoformat(),
                        "source": "STALE",
                        "error": self._balance_last_error,
                    }
                return {
                    "balance_usdt": None,
                    "as_of_utc": None,
                    "source": "COOLDOWN",
                    "error": self._balance_last_error,
                }

        try:
            self._balance_last_attempt_at = now_utc
            balance = float(self.balance_fetcher())
            self._balance_cache_value = balance
            self._balance_cache_at = now_utc
            self._balance_last_error = None
            return {
                "balance_usdt": round(balance, 8),
                "as_of_utc": now_utc.replace(microsecond=0).isoformat(),
                "source": "API",
                "error": None,
            }
        except Exception as exc:  # noqa: BLE001
            self._balance_last_attempt_at = now_utc
            self._balance_last_error = str(exc)
            LOGGER.warning("Failed to fetch wallet balance for dashboard: %s", exc)
            if self._balance_cache_value is not None and self._balance_cache_at is not None:
                return {
                    "balance_usdt": round(self._balance_cache_value, 8),
                    "as_of_utc": self._balance_cache_at.replace(microsecond=0).isoformat(),
                    "source": "STALE",
                    "error": self._balance_last_error,
                }
            return {"balance_usdt": None, "as_of_utc": None, "source": "ERROR", "error": self._balance_last_error}

    def _extract_close_price(self, row: Dict[str, Any]) -> Optional[float]:
        payload: Dict[str, Any] = {}
        raw = row.get("close_raw_json")
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    payload = parsed
            except ValueError:
                payload = {}

        avg_price = self._safe_float(payload.get("avgPrice"))
        if avg_price and avg_price > 0:
            return avg_price

        cum_quote = self._safe_float(payload.get("cumQuote"))
        executed_qty = self._safe_float(payload.get("executedQty") or payload.get("origQty") or row.get("close_event_qty"))
        if cum_quote and executed_qty and executed_qty > 0 and cum_quote > 0:
            return cum_quote / executed_qty

        payload_price = self._safe_float(payload.get("price"))
        if payload_price and payload_price > 0:
            return payload_price

        event_price = self._safe_float(row.get("close_event_price"))
        if event_price and event_price > 0:
            return event_price

        status = str(row.get("status") or "").upper()
        close_reason = str(row.get("close_reason") or "").upper()
        if status == "CLOSED_TP" or close_reason == "TAKE_PROFIT_FILLED":
            tp_price = self._safe_float(row.get("tp_price"))
            if tp_price and tp_price > 0:
                return tp_price
        if status == "CLOSED_SL" or close_reason == "STOP_LOSS_FILLED":
            sl_price = self._safe_float(row.get("sl_price"))
            if sl_price and sl_price > 0:
                return sl_price

        if self.close_price_fetcher is not None:
            symbol = str(row.get("symbol") or "").upper().strip()
            order_id_raw = row.get("close_order_id")
            try:
                order_id = int(order_id_raw) if order_id_raw is not None else None
            except (TypeError, ValueError):
                order_id = None
            if symbol and order_id:
                cache_key = (symbol, order_id)
                if cache_key in self._close_price_cache:
                    return self._close_price_cache[cache_key]
                fetched_price: Optional[float] = None
                try:
                    fetched_price = self.close_price_fetcher(symbol, order_id)
                    if fetched_price is not None and fetched_price > 0:
                        fetched_price = float(fetched_price)
                    else:
                        fetched_price = None
                except Exception as exc:  # noqa: BLE001
                    LOGGER.debug("close_price_fetcher failed for %s order_id=%s: %s", symbol, order_id, exc)
                    fetched_price = None
                self._close_price_cache[cache_key] = fetched_price
                if fetched_price is not None:
                    return fetched_price

        return None

    def _insert_wallet_snapshot(
        self,
        conn: sqlite3.Connection,
        captured_at_utc: str,
        balance_usdt: float,
        source: str = "API",
        error: Optional[str] = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO wallet_snapshots (captured_at_utc, balance_usdt, source, error, created_at_utc)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                captured_at_utc,
                float(balance_usdt),
                source[:24],
                (error or "")[:1000] or None,
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            ),
        )

    def _get_latest_wallet_snapshot(self, conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
        try:
            row = conn.execute(
                """
                SELECT id, captured_at_utc, balance_usdt, source, error, created_at_utc
                FROM wallet_snapshots
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        except sqlite3.Error:
            return None
        if row is None:
            return None
        return dict(row)

    def _apply_drawdown(self, curve: List[Dict[str, Any]]) -> Dict[str, float]:
        peak_equity: Optional[float] = None
        max_drawdown = 0.0
        max_drawdown_pct = 0.0
        for point in curve:
            equity = self._safe_float(point.get("equity")) or 0.0
            if peak_equity is None or equity > peak_equity:
                peak_equity = equity
            drawdown = max(0.0, (peak_equity or 0.0) - equity)
            drawdown_pct = (drawdown / peak_equity * 100.0) if (peak_equity and peak_equity > 0) else 0.0
            point["drawdown"] = round(drawdown, 8)
            point["drawdown_pct"] = round(drawdown_pct, 6)
            if drawdown > max_drawdown:
                max_drawdown = drawdown
            if drawdown_pct > max_drawdown_pct:
                max_drawdown_pct = drawdown_pct
        current_drawdown = float(curve[-1]["drawdown"]) if curve else 0.0
        current_drawdown_pct = float(curve[-1]["drawdown_pct"]) if curve else 0.0
        return {
            "max_drawdown": round(max_drawdown, 8),
            "max_drawdown_pct": round(max_drawdown_pct, 6),
            "current_drawdown": round(current_drawdown, 8),
            "current_drawdown_pct": round(current_drawdown_pct, 6),
        }

    def _query_wallet_rows(
        self,
        conn: sqlite3.Connection,
        window_start_utc: Optional[str],
        max_points: int,
    ) -> List[Dict[str, Any]]:
        params: List[Any] = []
        where_sql = ""
        if window_start_utc:
            where_sql = "WHERE captured_at_utc >= ?"
            params.append(window_start_utc)
        params.append(max(1, int(max_points)))
        rows = self._query_rows(
            conn,
            f"""
            SELECT id, captured_at_utc, balance_usdt
            FROM wallet_snapshots
            {where_sql}
            ORDER BY captured_at_utc DESC, id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        rows.reverse()
        return rows

    def _build_balance_curve(
        self,
        conn: sqlite3.Connection,
        now_utc: datetime,
        wallet_balance_usdt: Optional[float],
        window_start_utc: Optional[str],
        max_points: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        wallet_rows = self._query_wallet_rows(
            conn=conn,
            window_start_utc=window_start_utc,
            max_points=max_points,
        )

        curve: List[Dict[str, Any]] = []
        if wallet_rows:
            base_balance = self._safe_float(wallet_rows[0].get("balance_usdt")) or 0.0
            for row in wallet_rows:
                balance = self._safe_float(row.get("balance_usdt"))
                if balance is None:
                    continue
                curve.append(
                    {
                        "t": row.get("captured_at_utc"),
                        "equity": round(balance, 8),
                        "pnl": 0.0,
                        "cum_pnl": round(balance - base_balance, 8),
                    }
                )
        elif wallet_balance_usdt is not None:
            curve = [
                {
                    "t": now_utc.replace(microsecond=0).isoformat(),
                    "equity": round(wallet_balance_usdt, 8),
                    "pnl": 0.0,
                    "cum_pnl": 0.0,
                }
            ]

        dd = self._apply_drawdown(curve)
        total_realized_pnl = float(curve[-1]["cum_pnl"]) if curve else 0.0

        stats = {
            "wallet_balance_usdt": round(wallet_balance_usdt, 8) if wallet_balance_usdt is not None else None,
            "total_realized_pnl": round(total_realized_pnl, 8),
            "closed_trades_priced": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "win_rate_pct": 0.0,
            "max_drawdown": dd["max_drawdown"],
            "max_drawdown_pct": dd["max_drawdown_pct"],
            "current_drawdown": dd["current_drawdown"],
            "current_drawdown_pct": dd["current_drawdown_pct"],
            "unpriced_closed_positions": 0,
            "equity_baseline": round((self._safe_float(curve[0].get("equity")) if curve else 0.0) or 0.0, 8),
        }
        return curve, stats

    def _load_trade_outcome_stats(self, conn: sqlite3.Connection, now_utc: datetime) -> Dict[str, Any]:
        rows = self._query_rows(
            conn,
            """
            SELECT
                p.id, p.symbol, p.side, p.qty, p.entry_price,
                p.tp_price, p.sl_price,
                p.status, p.close_reason,
                p.closed_at_utc, p.updated_at_utc, p.close_order_id,
                oe.event_time_utc AS close_event_time_utc,
                oe.price AS close_event_price,
                oe.qty AS close_event_qty,
                oe.raw_json AS close_raw_json
            FROM positions p
            LEFT JOIN order_events oe ON oe.id = (
                SELECT oe2.id
                FROM order_events oe2
                WHERE oe2.position_id = p.id
                  AND (
                    (p.close_order_id IS NOT NULL AND oe2.order_id = p.close_order_id)
                    OR (p.close_order_id IS NULL AND oe2.side = 'BUY' AND oe2.status = 'FILLED')
                  )
                ORDER BY oe2.id DESC
                LIMIT 1
            )
            WHERE p.status != 'OPEN'
            ORDER BY COALESCE(p.closed_at_utc, oe.event_time_utc, p.updated_at_utc) ASC, p.id ASC
            """,
        )

        cumulative_trade_pnl = 0.0
        wins = 0
        losses = 0
        breakeven = 0
        skipped_unpriced = 0

        for row in rows:
            side = str(row.get("side") or "").upper()
            if side and side != "SHORT":
                continue
            qty = self._safe_float(row.get("qty")) or 0.0
            entry_price = self._safe_float(row.get("entry_price")) or 0.0
            if qty <= 0 or entry_price <= 0:
                skipped_unpriced += 1
                continue
            close_price = self._extract_close_price(row)
            if close_price is None or close_price <= 0:
                skipped_unpriced += 1
                continue
            pnl = (entry_price - close_price) * qty
            cumulative_trade_pnl += pnl
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
            else:
                breakeven += 1

        priced_closed_count = wins + losses + breakeven
        win_rate_pct = (wins / priced_closed_count * 100.0) if priced_closed_count > 0 else 0.0
        return {
            "closed_trades_priced": priced_closed_count,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate_pct": round(win_rate_pct, 2),
            "unpriced_closed_positions": skipped_unpriced,
            "trade_realized_pnl": round(cumulative_trade_pnl, 8),
            "as_of_utc": now_utc.replace(microsecond=0).isoformat(),
        }

    def _list_unpriced_closed_positions(self, conn: sqlite3.Connection, limit: int = 80) -> List[Dict[str, Any]]:
        rows = self._query_rows(
            conn,
            """
            SELECT
                p.id, p.symbol, p.side, p.qty, p.entry_price,
                p.tp_price, p.sl_price,
                p.status, p.close_reason, p.close_order_id,
                p.closed_at_utc, p.updated_at_utc,
                oe.event_time_utc AS close_event_time_utc,
                oe.price AS close_event_price,
                oe.qty AS close_event_qty,
                oe.raw_json AS close_raw_json
            FROM positions p
            LEFT JOIN order_events oe ON oe.id = (
                SELECT oe2.id
                FROM order_events oe2
                WHERE oe2.position_id = p.id
                  AND (
                    (p.close_order_id IS NOT NULL AND oe2.order_id = p.close_order_id)
                    OR (p.close_order_id IS NULL AND oe2.side = 'BUY' AND oe2.status = 'FILLED')
                  )
                ORDER BY oe2.id DESC
                LIMIT 1
            )
            WHERE p.status != 'OPEN'
            ORDER BY COALESCE(p.closed_at_utc, oe.event_time_utc, p.updated_at_utc) DESC, p.id DESC
            LIMIT ?
            """,
            (max(1, int(limit)),),
        )
        items: List[Dict[str, Any]] = []
        for row in rows:
            side = str(row.get("side") or "").upper()
            if side and side != "SHORT":
                continue
            qty = self._safe_float(row.get("qty")) or 0.0
            entry_price = self._safe_float(row.get("entry_price")) or 0.0
            if qty <= 0 or entry_price <= 0:
                reason = "INVALID_POSITION_FIELDS"
            else:
                close_price = self._extract_close_price(row)
                if close_price is not None and close_price > 0:
                    continue
                reason = "MISSING_FILL_PRICE"
            items.append(
                {
                    "id": row.get("id"),
                    "symbol": row.get("symbol"),
                    "status": row.get("status"),
                    "close_reason": row.get("close_reason"),
                    "close_order_id": row.get("close_order_id"),
                    "detected_reason": reason,
                    "closed_at_utc": row.get("closed_at_utc") or row.get("close_event_time_utc") or row.get("updated_at_utc"),
                }
            )
        return items

    def _build_strategy_equity_curve(
        self,
        conn: sqlite3.Connection,
        now_utc: datetime,
        wallet_balance_usdt: Optional[float],
        window_start_utc: Optional[str],
        max_points: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        wallet_rows = self._query_wallet_rows(
            conn=conn,
            window_start_utc=window_start_utc,
            max_points=max_points,
        )
        cashflow_rows: List[Dict[str, Any]] = []
        try:
            cashflow_rows = self._query_rows(
                conn,
                """
                SELECT id, event_time_utc, amount
                FROM cashflow_events
                WHERE asset = 'USDT'
                ORDER BY event_time_utc ASC, id ASC
                LIMIT 5000
                """,
            )
        except sqlite3.Error:
            cashflow_rows = []

        curve: List[Dict[str, Any]] = []
        cum_cashflow = 0.0
        cf_idx = 0
        prev_cum_pnl = 0.0
        baseline_equity: Optional[float] = None
        cashflow_baseline: Optional[float] = None

        for row in wallet_rows:
            t = str(row.get("captured_at_utc") or "")
            balance = self._safe_float(row.get("balance_usdt"))
            if balance is None:
                continue
            while cf_idx < len(cashflow_rows):
                cf_t = str(cashflow_rows[cf_idx].get("event_time_utc") or "")
                if cf_t and cf_t <= t:
                    cum_cashflow += self._safe_float(cashflow_rows[cf_idx].get("amount")) or 0.0
                    cf_idx += 1
                    continue
                break

            if cashflow_baseline is None:
                # Align strategy-equity start point to account-equity start point:
                # only cashflow AFTER first wallet snapshot should shift strategy curve.
                cashflow_baseline = cum_cashflow
            effective_cashflow = cum_cashflow - (cashflow_baseline or 0.0)
            strategy_equity = balance - effective_cashflow
            if baseline_equity is None:
                baseline_equity = strategy_equity
            cum_pnl = strategy_equity - (baseline_equity or 0.0)
            pnl = cum_pnl - prev_cum_pnl
            prev_cum_pnl = cum_pnl

            curve.append(
                {
                    "t": t,
                    "equity": round(strategy_equity, 8),
                    "pnl": round(pnl, 8),
                    "cum_pnl": round(cum_pnl, 8),
                    "cum_cashflow": round(effective_cashflow, 8),
                }
            )

        if not curve and wallet_balance_usdt is not None:
            curve = [
                {
                    "t": now_utc.replace(microsecond=0).isoformat(),
                    "equity": round(wallet_balance_usdt, 8),
                    "pnl": 0.0,
                    "cum_pnl": 0.0,
                    "cum_cashflow": 0.0,
                }
            ]
            baseline_equity = wallet_balance_usdt

        if not curve:
            curve = [
                {
                    "t": now_utc.replace(microsecond=0).isoformat(),
                    "equity": 0.0,
                    "pnl": 0.0,
                    "cum_pnl": 0.0,
                    "cum_cashflow": 0.0,
                }
            ]
            baseline_equity = 0.0

        dd = self._apply_drawdown(curve)
        trade_stats = self._load_trade_outcome_stats(conn, now_utc)
        stats = {
            "wallet_balance_usdt": round(wallet_balance_usdt, 8) if wallet_balance_usdt is not None else None,
            "total_realized_pnl": round(float(curve[-1]["cum_pnl"]), 8),
            "closed_trades_priced": trade_stats["closed_trades_priced"],
            "wins": trade_stats["wins"],
            "losses": trade_stats["losses"],
            "breakeven": trade_stats["breakeven"],
            "win_rate_pct": trade_stats["win_rate_pct"],
            "max_drawdown": dd["max_drawdown"],
            "max_drawdown_pct": dd["max_drawdown_pct"],
            "current_drawdown": dd["current_drawdown"],
            "current_drawdown_pct": dd["current_drawdown_pct"],
            "unpriced_closed_positions": trade_stats["unpriced_closed_positions"],
            "equity_baseline": round((baseline_equity or 0.0), 8),
            "net_cashflow_usdt": round(float(curve[-1].get("cum_cashflow") or 0.0), 8),
            "trade_realized_pnl": trade_stats["trade_realized_pnl"],
        }
        return curve, stats

    def snapshot(
        self,
        log_lines: int = 80,
        window_hours: Optional[float] = None,
        curve_points: Optional[int] = None,
    ) -> Dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(self.local_tz)
        next_entry = self._next_entry_local(now_local)
        live_wallet = self._read_wallet_balance(now_utc)
        points_limit = max(100, min(5000, int(curve_points if curve_points is not None else self.default_curve_points)))
        window_hours_value: Optional[float] = None
        if window_hours is not None:
            try:
                parsed_hours = float(window_hours)
                if parsed_hours > 0:
                    window_hours_value = min(parsed_hours, 24.0 * 366.0)
            except (TypeError, ValueError):
                window_hours_value = None
        window_start_utc = (
            (now_utc - timedelta(hours=window_hours_value)).replace(microsecond=0).isoformat()
            if window_hours_value is not None
            else None
        )

        data: Dict[str, Any] = {
            "generated_at_utc": now_utc.replace(microsecond=0).isoformat(),
            "timezone": str(getattr(self.local_tz, "key", self.local_tz)),
            "now_local": now_local.replace(microsecond=0).isoformat(),
            "next_entry_local": next_entry.replace(microsecond=0).isoformat(),
            "seconds_to_next_entry": int((next_entry - now_local).total_seconds()),
            "curve_window_hours": window_hours_value,
            "curve_points": points_limit,
            "summary": {
                "open_positions": 0,
                "open_symbols": 0,
                "recent_errors": 0,
                "last_run_status": None,
                "wallet_balance_usdt": live_wallet["balance_usdt"],
                "net_cashflow_usdt": 0.0,
            },
            "wallet": live_wallet,
            "latest_run": None,
            "runs": [],
            "open_positions": [],
            "events": [],
            "cashflow_events": [],
            "unpriced_closed_details": [],
            "strategy_equity_curve": [],
            "balance_curve": [],
            "equity_curve": [],
            "drawdown_stats_strategy": {
                "wallet_balance_usdt": None,
                "total_realized_pnl": 0.0,
                "closed_trades_priced": 0,
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "win_rate_pct": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "current_drawdown": 0.0,
                "current_drawdown_pct": 0.0,
                "unpriced_closed_positions": 0,
                "equity_baseline": 0.0,
            },
            "drawdown_stats_balance": {
                "wallet_balance_usdt": live_wallet["balance_usdt"],
                "total_realized_pnl": 0.0,
                "closed_trades_priced": 0,
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "win_rate_pct": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "current_drawdown": 0.0,
                "current_drawdown_pct": 0.0,
                "unpriced_closed_positions": 0,
                "equity_baseline": live_wallet["balance_usdt"] if live_wallet["balance_usdt"] is not None else 0.0,
            },
            "drawdown_stats": {
                "wallet_balance_usdt": live_wallet["balance_usdt"],
                "total_realized_pnl": 0.0,
                "closed_trades_priced": 0,
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "win_rate_pct": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "current_drawdown": 0.0,
                "current_drawdown_pct": 0.0,
                "unpriced_closed_positions": 0,
                "equity_baseline": live_wallet["balance_usdt"] if live_wallet["balance_usdt"] is not None else 0.0,
            },
            "log_tail": self._tail_log(lines=log_lines),
        }

        if not os.path.exists(self.db_path):
            return data

        try:
            with self._connect() as conn:
                if live_wallet.get("source") == "API" and self._safe_float(live_wallet.get("balance_usdt")) is not None:
                    try:
                        self._insert_wallet_snapshot(
                            conn=conn,
                            captured_at_utc=str(live_wallet.get("as_of_utc") or now_utc.replace(microsecond=0).isoformat()),
                            balance_usdt=float(live_wallet["balance_usdt"]),
                            source="API",
                            error=None,
                        )
                    except sqlite3.Error as exc:
                        LOGGER.warning("Failed to persist wallet snapshot: %s", exc)

                latest_wallet_row = self._get_latest_wallet_snapshot(conn)
                if latest_wallet_row is not None:
                    data["wallet"] = {
                        "balance_usdt": round(float(latest_wallet_row["balance_usdt"]), 8),
                        "as_of_utc": latest_wallet_row["captured_at_utc"],
                        "source": "DB",
                        "error": live_wallet.get("error"),
                        "live_source": live_wallet.get("source"),
                    }
                    data["summary"]["wallet_balance_usdt"] = data["wallet"]["balance_usdt"]
                else:
                    data["wallet"] = live_wallet

                latest_run = conn.execute(
                    """
                    SELECT run_id, trade_day_utc, started_at_utc, completed_at_utc, status, message
                    FROM runs
                    ORDER BY started_at_utc DESC
                    LIMIT 1
                    """
                ).fetchone()
                if latest_run is not None:
                    data["latest_run"] = dict(latest_run)
                    data["summary"]["last_run_status"] = latest_run["status"]

                data["runs"] = self._query_rows(
                    conn,
                    """
                    SELECT run_id, trade_day_utc, started_at_utc, completed_at_utc, status, message
                    FROM runs
                    ORDER BY started_at_utc DESC
                    LIMIT 30
                    """,
                )

                data["open_positions"] = self._query_rows(
                    conn,
                    """
                    SELECT id, run_id, symbol, side, qty, entry_price,
                           liq_price_latest, tp_price, sl_price,
                           opened_at_utc, expire_at_utc, status, last_error
                    FROM positions
                    WHERE status = 'OPEN'
                    ORDER BY opened_at_utc DESC
                    LIMIT 100
                    """,
                )

                summary_row = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) AS open_positions,
                        COUNT(DISTINCT CASE WHEN status = 'OPEN' THEN symbol END) AS open_symbols,
                        SUM(CASE WHEN status = 'OPEN' AND last_error IS NOT NULL AND TRIM(last_error) != '' THEN 1 ELSE 0 END) AS recent_errors
                    FROM positions
                    """
                ).fetchone()
                if summary_row is not None:
                    data["summary"]["open_positions"] = int(summary_row["open_positions"] or 0)
                    data["summary"]["open_symbols"] = int(summary_row["open_symbols"] or 0)
                    data["summary"]["recent_errors"] = int(summary_row["recent_errors"] or 0)

                data["events"] = self._query_rows(
                    conn,
                    """
                    SELECT
                        oe.id, oe.position_id, oe.symbol, oe.order_id, oe.client_order_id,
                        oe.type, oe.side, oe.price, oe.qty, oe.status,
                        oe.event_time_utc,
                        p.status AS position_status,
                        p.close_reason AS position_close_reason
                    FROM order_events oe
                    LEFT JOIN positions p ON p.id = oe.position_id
                    ORDER BY oe.id DESC
                    LIMIT 120
                    """,
                )
                data["cashflow_events"] = self._query_rows(
                    conn,
                    """
                    SELECT id, event_time_utc, asset, amount, income_type, symbol, tran_id, info
                    FROM cashflow_events
                    ORDER BY event_time_utc DESC, id DESC
                    LIMIT 80
                    """,
                )
                data["unpriced_closed_details"] = self._list_unpriced_closed_positions(conn, limit=120)

                strategy_curve, strategy_stats = self._build_strategy_equity_curve(
                    conn=conn,
                    now_utc=now_utc,
                    wallet_balance_usdt=self._safe_float(data["wallet"].get("balance_usdt")),
                    window_start_utc=window_start_utc,
                    max_points=points_limit,
                )
                balance_curve, balance_stats = self._build_balance_curve(
                    conn=conn,
                    now_utc=now_utc,
                    wallet_balance_usdt=self._safe_float(data["wallet"].get("balance_usdt")),
                    window_start_utc=window_start_utc,
                    max_points=points_limit,
                )
                data["strategy_equity_curve"] = strategy_curve[-points_limit:]
                data["balance_curve"] = balance_curve[-points_limit:]
                data["drawdown_stats_strategy"] = strategy_stats
                data["drawdown_stats_balance"] = balance_stats
                data["summary"]["net_cashflow_usdt"] = strategy_stats.get("net_cashflow_usdt", 0.0)
                data["equity_curve"] = data["strategy_equity_curve"]
                data["drawdown_stats"] = data["drawdown_stats_balance"]
        except sqlite3.Error as exc:
            data["summary"]["last_run_status"] = "DB_ERROR"
            data["db_error"] = str(exc)

        return data


def render_dashboard_html(refresh_sec: int) -> str:
    return DASHBOARD_HTML.replace("__REFRESH_SEC__", str(max(2, refresh_sec)))


def _json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _make_handler(provider: DashboardDataProvider, cfg: DashboardServerConfig):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/":
                body = render_dashboard_html(cfg.refresh_sec).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/api/dashboard":
                params = parse_qs(parsed.query)
                lines = max(0, int(params.get("log_lines", ["80"])[0]))
                window_hours_raw = params.get("window_hours", [None])[0]
                curve_points_raw = params.get("curve_points", [None])[0]
                window_hours: Optional[float] = None
                curve_points: Optional[int] = None
                try:
                    if window_hours_raw not in (None, ""):
                        window_hours = float(window_hours_raw)
                except ValueError:
                    window_hours = None
                try:
                    if curve_points_raw not in (None, ""):
                        curve_points = int(curve_points_raw)
                except ValueError:
                    curve_points = None
                body = _json_bytes(
                    provider.snapshot(
                        log_lines=min(lines, 300),
                        window_hours=window_hours,
                        curve_points=curve_points,
                    )
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/healthz":
                body = _json_bytes({"ok": True})
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(404)
            self.end_headers()

        def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
            LOGGER.debug("dashboard_http: " + fmt, *args)

    return Handler


def run_dashboard_server(cfg: DashboardServerConfig) -> None:
    provider = DashboardDataProvider(
        db_path=cfg.db_path,
        log_file=cfg.log_file,
        timezone_name=cfg.timezone_name,
        entry_hour=cfg.entry_hour,
        entry_minute=cfg.entry_minute,
        default_curve_points=cfg.curve_points,
    )
    handler_cls = _make_handler(provider=provider, cfg=cfg)

    server = ThreadingHTTPServer((cfg.host, cfg.port), handler_cls)
    LOGGER.info(
        "dashboard server started: http://%s:%s (db=%s, log=%s)",
        cfg.host,
        cfg.port,
        cfg.db_path,
        cfg.log_file,
    )
    try:
        server.serve_forever()
    finally:
        server.server_close()


DASHBOARD_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Bubble Buster Console</title>
  <style>
    :root {
      --bg: #0a1118;
      --panel: #101c27cc;
      --line: #20445a;
      --text: #eaf6ff;
      --muted: #8db0c4;
      --ok: #26d07c;
      --warn: #ffb340;
      --bad: #ff5d5d;
      --accent: #4ec1ff;
      --accent-2: #14e0b7;
      --shadow: 0 10px 32px rgba(0, 0, 0, 0.32);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: "Avenir Next", "SF Pro Text", "PingFang SC", "Noto Sans SC", sans-serif;
      color: var(--text);
      background:
        radial-gradient(1200px 500px at 10% -10%, #0f2f40 0%, transparent 60%),
        radial-gradient(1000px 600px at 90% -20%, #183024 0%, transparent 60%),
        linear-gradient(180deg, #081018 0%, #050a0f 100%);
      min-height: 100vh;
      position: relative;
    }

    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background:
        linear-gradient(to right, rgba(255, 255, 255, 0.02) 1px, transparent 1px),
        linear-gradient(to bottom, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
      background-size: 44px 44px;
      opacity: 0.24;
    }

    .shell {
      max-width: 1240px;
      margin: 0 auto;
      padding: 24px 16px 32px;
      animation: rise 380ms ease-out;
      position: relative;
      z-index: 1;
    }

    @keyframes rise {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }

    .header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 16px;
      margin-bottom: 16px;
      padding: 14px 16px;
      border: 1px solid rgba(93, 158, 193, 0.24);
      border-radius: 16px;
      background: linear-gradient(135deg, rgba(16, 35, 48, 0.76), rgba(8, 22, 32, 0.84));
      box-shadow: var(--shadow);
    }

    .title {
      margin: 0;
      font-size: clamp(1.2rem, 2.6vw, 2rem);
      letter-spacing: 0.02em;
      text-shadow: 0 0 18px rgba(78, 193, 255, 0.24);
    }

    .subtitle {
      margin-top: 6px;
      font-size: 0.88rem;
      color: var(--muted);
    }

    .pill-row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }

    .pill {
      border: 1px solid var(--line);
      color: var(--muted);
      background: linear-gradient(180deg, #102230dd, #0a1a26cc);
      padding: 6px 10px;
      border-radius: 999px;
      font-size: 0.8rem;
      white-space: nowrap;
      box-shadow: inset 0 0 0 1px rgba(106, 182, 221, 0.08);
    }

    .pill-value {
      margin-left: 6px;
      display: inline;
      color: var(--muted);
      font-size: 0.8rem;
      font-weight: 700;
    }

    .pill-value.ok {
      color: var(--accent);
    }

    .pill-value.warn {
      color: var(--warn);
    }

    #serviceState.ok { color: var(--ok); }
    #serviceState.warn { color: var(--warn); }
    #serviceState.bad { color: var(--bad); }

    .pill span {
      font-weight: 700;
      letter-spacing: 0.02em;
    }

    .cards {
      display: grid;
      gap: 12px;
      margin-bottom: 10px;
    }

    .cards-runtime {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }

    .cards-performance {
      grid-template-columns: repeat(5, minmax(0, 1fr));
    }

    .card {
      position: relative;
      background: linear-gradient(165deg, rgba(19, 35, 48, 0.9), rgba(9, 20, 29, 0.92));
      border: 1px solid rgba(87, 151, 183, 0.34);
      border-radius: 16px;
      padding: 12px;
      backdrop-filter: blur(9px);
      box-shadow: var(--shadow);
      transition: transform 180ms ease, border-color 180ms ease;
      overflow: hidden;
    }

    .card::after {
      content: "";
      position: absolute;
      inset: auto -22% -62% -22%;
      height: 110px;
      background: radial-gradient(circle at 50% 0%, rgba(78, 193, 255, 0.26), transparent 64%);
      pointer-events: none;
    }

    .card:hover {
      transform: translateY(-2px);
      border-color: rgba(111, 188, 224, 0.46);
    }

    .k {
      font-size: 0.76rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .v {
      margin-top: 4px;
      font-size: 1.24rem;
      font-weight: 700;
    }

    .ok { color: var(--ok); }
    .warn { color: var(--warn); }
    .bad { color: var(--bad); }

    .grid {
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 12px;
    }

    .panel {
      background: linear-gradient(180deg, rgba(18, 32, 44, 0.86), rgba(10, 21, 30, 0.9));
      border: 1px solid rgba(80, 143, 175, 0.38);
      border-radius: 16px;
      overflow: hidden;
      box-shadow: var(--shadow);
    }

    .panel h2 {
      margin: 0;
      font-size: 0.82rem;
      letter-spacing: 0.09em;
      text-transform: uppercase;
      color: var(--accent);
      padding: 11px 12px;
      border-bottom: 1px solid rgba(80, 143, 175, 0.32);
      background: linear-gradient(90deg, rgba(20, 40, 55, 0.95), rgba(12, 27, 39, 0.92));
    }

    .table-wrap {
      overflow: auto;
      max-height: 290px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 600px;
      font-size: 0.84rem;
    }

    th, td {
      text-align: left;
      padding: 8px 10px;
      border-bottom: 1px solid rgba(53, 95, 122, 0.5);
      white-space: nowrap;
    }

    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: rgba(18, 36, 49, 0.96);
      color: var(--muted);
      font-weight: 600;
      backdrop-filter: blur(4px);
    }

    tbody tr:nth-child(odd) {
      background: rgba(14, 30, 42, 0.22);
    }

    tbody tr:hover {
      background: rgba(52, 104, 133, 0.24);
    }

    td.ok, td.warn, td.bad {
      font-weight: 700;
    }

    .mono {
      font-family: "SF Mono", "Menlo", "Consolas", monospace;
      font-size: 0.78rem;
    }

    .log {
      margin: 0;
      max-height: 190px;
      overflow: auto;
      padding: 10px 12px;
      background: rgba(9, 22, 31, 0.9);
      color: #cde7f5;
      font-size: 0.75rem;
      line-height: 1.45;
      border-top: 1px solid rgba(80, 143, 175, 0.32);
    }

    .chart-wrap {
      padding: 10px 12px 0;
    }

    .chart-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 8px;
    }

    .chart-title {
      font-size: 0.86rem;
      color: var(--muted);
      letter-spacing: 0.02em;
    }

    .tab-row {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
    }

    .window-row {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
      margin-top: 6px;
    }

    .tab-btn {
      border: 1px solid rgba(80, 143, 175, 0.6);
      background: rgba(10, 25, 35, 0.92);
      color: var(--muted);
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 0.76rem;
      cursor: pointer;
      transition: all 140ms ease;
    }

    .tab-btn:hover {
      color: var(--text);
      border-color: rgba(126, 199, 235, 0.8);
    }

    .tab-btn.active {
      color: #031018;
      background: linear-gradient(180deg, #68d8ff, #43b6ea);
      border-color: transparent;
      font-weight: 700;
    }

    .chart-canvas {
      width: 100%;
      height: 248px;
      display: block;
      border: 1px solid rgba(64, 120, 151, 0.6);
      border-radius: 10px;
      background: linear-gradient(180deg, rgba(11, 24, 34, 0.95), rgba(6, 14, 22, 0.98));
    }

    .stats-wrap {
      padding: 8px 12px 12px;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px 12px;
      font-size: 0.8rem;
    }

    .stat-item {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      border-bottom: 1px solid rgba(55, 99, 127, 0.5);
      padding: 6px 2px;
      color: #d7eefd;
    }

    .table-wrap::-webkit-scrollbar,
    .log::-webkit-scrollbar {
      width: 9px;
      height: 9px;
    }

    .table-wrap::-webkit-scrollbar-track,
    .log::-webkit-scrollbar-track {
      background: rgba(8, 19, 29, 0.64);
      border-radius: 8px;
    }

    .table-wrap::-webkit-scrollbar-thumb,
    .log::-webkit-scrollbar-thumb {
      background: linear-gradient(180deg, #2f607d, #224960);
      border-radius: 8px;
      border: 1px solid rgba(10, 21, 31, 0.8);
    }

    @media (max-width: 1020px) {
      .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .cards-runtime { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .cards-performance { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .grid { grid-template-columns: 1fr; }
      .header { padding: 12px; }
    }

    @media (max-width: 560px) {
      .cards,
      .cards-runtime,
      .cards-performance { grid-template-columns: 1fr; }
      .shell { padding: 14px 10px 20px; }
      .subtitle { font-size: 0.8rem; }
      .stats-wrap { grid-template-columns: 1fr; }
      .header {
        flex-direction: column;
        align-items: stretch;
      }
      .pill-row { width: 100%; }
      .pill {
        flex: 1 1 auto;
        text-align: center;
      }
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="header">
      <div>
        <h1 class="title">Bubble Buster Runtime Console</h1>
        <div class="subtitle" id="meta">loading...</div>
      </div>
      <div class="pill-row">
        <div class="pill">Auto refresh: <span id="refresh">__REFRESH_SEC__</span>s</div>
        <div class="pill">Next entry: <span id="nextEntry">--</span></div>
        <div class="pill">Service: <span id="serviceState">--</span></div>
      </div>
    </section>

    <section class="cards cards-runtime">
      <article class="card">
        <div class="k">Open Positions</div>
        <div class="v" id="openCount">0</div>
      </article>
      <article class="card">
        <div class="k">Open Symbols</div>
        <div class="v" id="symbolCount">0</div>
      </article>
      <article class="card">
        <div class="k">Recent Errors</div>
        <div class="v" id="errorCount">0</div>
      </article>
      <article class="card">
        <div class="k">Last Run Status</div>
        <div class="v" id="lastRunStatus">--</div>
      </article>
    </section>

    <section class="cards cards-performance">
      <article class="card">
        <div class="k">Account Equity (USDT)</div>
        <div class="v" id="walletBalance">--</div>
      </article>
      <article class="card">
        <div class="k">Equity Change (USDT)</div>
        <div class="v" id="realizedPnl">--</div>
      </article>
      <article class="card">
        <div class="k">Max Drawdown</div>
        <div class="v" id="maxDrawdown">--</div>
      </article>
      <article class="card">
        <div class="k">Win Rate</div>
        <div class="v" id="winRate">--</div>
      </article>
      <article class="card">
        <div class="k">Net Cashflow (USDT)</div>
        <div class="v" id="netCashflow">--</div>
      </article>
    </section>

    <section class="grid">
      <section class="panel">
        <h2>Equity Curve (USDT)</h2>
        <div class="chart-wrap">
          <div class="chart-head">
            <div class="chart-title" id="curveTitle">策略权益曲线（不含出入金）</div>
            <div class="tab-row">
              <button class="tab-btn active" id="tabStrategy" type="button">策略权益</button>
              <button class="tab-btn" id="tabBalance" type="button">账户权益</button>
            </div>
          </div>
          <div class="window-row" id="windowRow">
            <button class="tab-btn" data-window-hours="1" type="button">1H</button>
            <button class="tab-btn active" data-window-hours="24" type="button">1D</button>
            <button class="tab-btn" data-window-hours="168" type="button">1W</button>
            <button class="tab-btn" data-window-hours="720" type="button">1M</button>
            <button class="tab-btn" data-window-hours="8760" type="button">1Y</button>
          </div>
          <div class="chart-canvas" id="equityChart"></div>
        </div>
      </section>

      <section class="panel">
        <h2>Drawdown Stats</h2>
        <div class="stats-wrap mono" id="drawdownStats"></div>
      </section>

      <section class="panel">
        <h2>Open Positions</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th><th>Symbol</th><th>Qty</th><th>Entry</th><th>TP</th><th>SL</th><th>Expire</th><th>Error</th>
              </tr>
            </thead>
            <tbody id="positionsBody"></tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <h2>Recent Runs</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Trade Day</th><th>Status</th><th>Started(UTC)</th><th>Message</th>
              </tr>
            </thead>
            <tbody id="runsBody"></tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <h2>Recent Order Events</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th><th>Symbol</th><th>Type</th><th>Side</th><th>Status</th><th>Time(UTC)</th>
              </tr>
            </thead>
            <tbody id="eventsBody"></tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <h2>Recent Cashflow Events</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th><th>Type</th><th>Amount</th><th>Asset</th><th>Symbol</th><th>Time(UTC)</th>
              </tr>
            </thead>
            <tbody id="cashflowBody"></tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <h2>Closed w/o Fill Price</h2>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th><th>Symbol</th><th>Status</th><th>Close Reason</th><th>Detect</th><th>Order ID</th><th>Closed(UTC)</th>
              </tr>
            </thead>
            <tbody id="unpricedBody"></tbody>
          </table>
        </div>
      </section>

      <section class="panel">
        <h2>Strategy Log Tail</h2>
        <pre class="log mono" id="logTail"></pre>
      </section>
    </section>
  </main>

<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<script>
(function () {
  var refreshNode = document.getElementById("refresh");
  var REFRESH_SEC = Number((refreshNode && refreshNode.textContent) || "5");
  var pathPrefix = "/";
  if (window && window.location && typeof window.location.pathname === "string") {
    pathPrefix = window.location.pathname || "/";
  }
  pathPrefix = pathPrefix.replace(/\\/+$/, "");
  if (!pathPrefix) pathPrefix = "";
  var api = pathPrefix + "/api/dashboard";
  var equityChart = null;
  var currentCurveTab = "strategy";
  var currentWindowHours = 24;
  var latestData = null;

  var el = {
    meta: document.getElementById("meta"),
    nextEntry: document.getElementById("nextEntry"),
    serviceState: document.getElementById("serviceState"),
    openCount: document.getElementById("openCount"),
    symbolCount: document.getElementById("symbolCount"),
    errorCount: document.getElementById("errorCount"),
    lastRunStatus: document.getElementById("lastRunStatus"),
    walletBalance: document.getElementById("walletBalance"),
    realizedPnl: document.getElementById("realizedPnl"),
    maxDrawdown: document.getElementById("maxDrawdown"),
    winRate: document.getElementById("winRate"),
    netCashflow: document.getElementById("netCashflow"),
    curveTitle: document.getElementById("curveTitle"),
    tabStrategy: document.getElementById("tabStrategy"),
    tabBalance: document.getElementById("tabBalance"),
    windowRow: document.getElementById("windowRow"),
    equityChart: document.getElementById("equityChart"),
    drawdownStats: document.getElementById("drawdownStats"),
    positionsBody: document.getElementById("positionsBody"),
    runsBody: document.getElementById("runsBody"),
    eventsBody: document.getElementById("eventsBody"),
    cashflowBody: document.getElementById("cashflowBody"),
    unpricedBody: document.getElementById("unpricedBody"),
    logTail: document.getElementById("logTail")
  };

  function txt(v) {
    if (v === null || v === undefined || v === "") {
      return "--";
    }
    return String(v);
  }

  function escapeHtml(v) {
    return txt(v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function clsForStatus(status) {
    var s = (status || "").toUpperCase();
    if (s.indexOf("SUCCESS") >= 0 || s.indexOf("CLOSED_TP") >= 0) return "ok";
    if (s.indexOf("RUNNING") >= 0 || s.indexOf("SKIPPED") >= 0) return "warn";
    if (s.indexOf("FAILED") >= 0 || s.indexOf("ERROR") >= 0 || s.indexOf("CLOSED_SL") >= 0) return "bad";
    return "";
  }

  function setText(node, value) {
    if (node) {
      node.textContent = value;
    }
  }

  function toNum(v) {
    if (v === null || v === undefined || v === "") return null;
    var n = Number(v);
    if (Number.isNaN(n) || !Number.isFinite(n)) return null;
    return n;
  }

  function fmtNum(v, digits) {
    var n = toNum(v);
    if (n === null) return "--";
    return n.toFixed(digits);
  }

  function fmtSigned(v, digits) {
    var n = toNum(v);
    if (n === null) return "--";
    var prefix = n > 0 ? "+" : "";
    return prefix + n.toFixed(digits);
  }

  function fmtAxisTime(isoText) {
    var raw = txt(isoText);
    if (raw === "--") return raw;
    var d = new Date(raw);
    if (Number.isNaN(d.getTime())) return raw.slice(5, 16).replace("T", " ");
    var mm = String(d.getMonth() + 1).padStart(2, "0");
    var dd = String(d.getDate()).padStart(2, "0");
    var hh = String(d.getHours()).padStart(2, "0");
    var mi = String(d.getMinutes()).padStart(2, "0");
    return mm + "-" + dd + " " + hh + ":" + mi;
  }

  function fmtMetaTime(isoText) {
    var raw = txt(isoText);
    if (raw === "--") return raw;
    var d = new Date(raw);
    if (Number.isNaN(d.getTime())) return raw.slice(5, 19).replace("T", " ");
    var mm = String(d.getMonth() + 1).padStart(2, "0");
    var dd = String(d.getDate()).padStart(2, "0");
    var hh = String(d.getHours()).padStart(2, "0");
    var mi = String(d.getMinutes()).padStart(2, "0");
    var ss = String(d.getSeconds()).padStart(2, "0");
    return mm + "-" + dd + " " + hh + ":" + mi + ":" + ss;
  }

  function fmtDateOnly(isoText) {
    var raw = txt(isoText);
    if (raw === "--") return raw;
    var d = new Date(raw);
    if (Number.isNaN(d.getTime())) return raw.slice(0, 10);
    var yyyy = String(d.getFullYear());
    var mm = String(d.getMonth() + 1).padStart(2, "0");
    var dd = String(d.getDate()).padStart(2, "0");
    return yyyy + "-" + mm + "-" + dd;
  }

  function renderRows(target, rows, mapper, emptyCols) {
    if (!target) return;
    if (!rows || rows.length === 0) {
      target.innerHTML = '<tr><td colspan="' + emptyCols + '" class="mono">No data</td></tr>';
      return;
    }
    var html = "";
    for (var i = 0; i < rows.length; i += 1) {
      html += mapper(rows[i]);
    }
    target.innerHTML = html;
  }

  function fetchDashboard(callback) {
    var xhr = new XMLHttpRequest();
    var q = [
      "_=" + encodeURIComponent(String(new Date().getTime())),
      "window_hours=" + encodeURIComponent(String(currentWindowHours))
    ];
    xhr.open("GET", api + "?" + q.join("&"), true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      if (xhr.status < 200 || xhr.status >= 300) {
        callback(new Error("HTTP " + xhr.status));
        return;
      }
      try {
        callback(null, JSON.parse(xhr.responseText));
      } catch (err) {
        callback(err);
      }
    };
    xhr.onerror = function () {
      callback(new Error("Network error"));
    };
    xhr.send();
  }

  function renderEquityChart(curve) {
    if (!el.equityChart) return;
    if (typeof window.echarts === "undefined") {
      return;
    }
    if (!equityChart) {
      equityChart = window.echarts.init(el.equityChart, null, { renderer: "canvas" });
      window.addEventListener("resize", function () {
        if (equityChart) equityChart.resize();
      });
    }

    if (!curve || curve.length === 0) {
      equityChart.clear();
      return;
    }

    var points = [];
    var xData = [];
    var yData = [];
    for (var i = 0; i < curve.length; i += 1) {
      var item = curve[i] || {};
      var equity = toNum(item.equity);
      if (equity === null) continue;
      var ddPct = toNum(item.drawdown_pct) || 0;
      var p = {
        equity: equity,
        t: txt(item.t),
        cumPnl: toNum(item.cum_pnl) || 0,
        ddPct: ddPct
      };
      points.push(p);
      xData.push(p.t);
      yData.push(equity);
    }

    if (points.length === 0) {
      equityChart.clear();
      return;
    }

    var first = points[0];
    var last = points[points.length - 1];
    var lineColor = last.equity >= first.equity ? "#26d07c" : "#ff5d5d";

    var areaTop = last.equity >= first.equity ? "rgba(38,208,124,0.28)" : "rgba(255,93,93,0.24)";
    var areaBottom = last.equity >= first.equity ? "rgba(38,208,124,0.03)" : "rgba(255,93,93,0.03)";
    equityChart.setOption({
      animation: false,
      grid: { left: 54, right: 20, top: 16, bottom: 26 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "cross" },
        backgroundColor: "rgba(8, 20, 29, 0.96)",
        borderColor: "#173244",
        textStyle: { color: "#d7eefd", fontSize: 12 },
        formatter: function (params) {
          if (!params || params.length === 0) return "";
          var idx = params[0].dataIndex;
          var p = points[idx] || {};
          return [
            "<div>" + escapeHtml(fmtAxisTime(p.t || "--")) + "</div>",
            "<div>Equity: " + escapeHtml(fmtNum(p.equity, 4)) + "</div>",
            "<div>CumPnL: " + escapeHtml(fmtSigned(p.cumPnl, 4)) + "</div>",
            "<div>DD: " + escapeHtml(fmtNum(p.ddPct, 2)) + "%</div>"
          ].join("");
        }
      },
      dataZoom: [{ type: "inside", xAxisIndex: 0, filterMode: "none" }],
      xAxis: {
        type: "category",
        data: xData,
        boundaryGap: false,
        axisLabel: {
          color: "#8db0c4",
          hideOverlap: true,
          fontSize: 10,
          margin: 12,
          formatter: function (value) { return fmtAxisTime(value); }
        },
        axisLine: { lineStyle: { color: "#1a3647" } },
        axisTick: { show: false },
        splitNumber: 6
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: { color: "#8db0c4" },
        axisLine: { show: false },
        splitLine: { lineStyle: { color: "rgba(39, 73, 95, 0.46)" } }
      },
      series: [
        {
          name: "Equity",
          type: "line",
          showSymbol: points.length <= 1,
          symbolSize: 6,
          smooth: 0.18,
          data: yData,
          lineStyle: { width: 2.4, color: lineColor },
          areaStyle: {
            color: new window.echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: areaTop },
              { offset: 1, color: areaBottom }
            ])
          }
        }
      ]
    }, true);

  }

  function activeStats(data) {
    if (!data) return {};
    if (currentCurveTab === "balance") {
      return data.drawdown_stats_balance || data.drawdown_stats || {};
    }
    return data.drawdown_stats_strategy || data.drawdown_stats || {};
  }

  function activeCurve(data) {
    if (!data) return [];
    if (currentCurveTab === "balance") {
      return data.balance_curve || data.equity_curve || [];
    }
    return data.strategy_equity_curve || data.equity_curve || [];
  }

  function renderCurveTabState() {
    if (el.tabStrategy) {
      el.tabStrategy.classList.toggle("active", currentCurveTab === "strategy");
    }
    if (el.tabBalance) {
      el.tabBalance.classList.toggle("active", currentCurveTab === "balance");
    }
    if (el.curveTitle) {
      el.curveTitle.textContent = currentCurveTab === "strategy"
        ? "策略权益曲线（不含出入金）"
        : "账户权益曲线（含未实现盈亏/出入金）";
    }
    if (el.windowRow) {
      var buttons = el.windowRow.querySelectorAll("[data-window-hours]");
      for (var i = 0; i < buttons.length; i += 1) {
        var b = buttons[i];
        var h = Number(b.getAttribute("data-window-hours"));
        b.classList.toggle("active", h === currentWindowHours);
      }
    }
  }

  function rerenderFromLatest() {
    if (!latestData) return;
    var d = latestData;
    var summary = d.summary || {};
    var wallet = d.wallet || {};
    var stats = activeStats(d);

    var walletDisplay = stats.wallet_balance_usdt;
    if (walletDisplay === null || walletDisplay === undefined) walletDisplay = wallet.balance_usdt;
    setText(el.walletBalance, fmtNum(walletDisplay, 4));
    setText(el.realizedPnl, fmtSigned(stats.total_realized_pnl, 4));
    setText(el.maxDrawdown, fmtNum(stats.max_drawdown_pct, 2) + "%");
    var pricedTrades = Number(stats.closed_trades_priced || 0);
    if (currentCurveTab === "balance" || pricedTrades <= 0) {
      setText(el.winRate, "--");
    } else {
      setText(el.winRate, fmtNum(stats.win_rate_pct, 2) + "%");
    }
    setText(el.netCashflow, fmtSigned(stats.net_cashflow_usdt, 4));
    if (el.realizedPnl) {
      var pnl = toNum(stats.total_realized_pnl);
      el.realizedPnl.className = "v " + (pnl === null ? "" : (pnl > 0 ? "ok" : (pnl < 0 ? "bad" : "warn")));
    }
    if (el.maxDrawdown) {
      var dd = toNum(stats.max_drawdown_pct);
      el.maxDrawdown.className = "v " + (dd && dd > 0 ? "bad" : "");
    }
    if (el.winRate) {
      if (currentCurveTab === "balance" || pricedTrades <= 0) {
        el.winRate.className = "v";
      } else {
        var wr = toNum(stats.win_rate_pct);
        el.winRate.className = "v " + (wr === null ? "" : (wr >= 50 ? "ok" : "warn"));
      }
    }
    if (el.netCashflow) {
      var cf = toNum(stats.net_cashflow_usdt);
      el.netCashflow.className = "v " + (cf === null ? "" : (cf > 0 ? "warn" : (cf < 0 ? "bad" : "ok")));
    }
    renderCurveTabState();
    renderEquityChart(activeCurve(d));
    renderDrawdownStats(stats, wallet);
    setText(el.openCount, txt(summary.open_positions));
    setText(el.symbolCount, txt(summary.open_symbols));
    setText(el.errorCount, txt(summary.recent_errors));
  }

  function renderDrawdownStats(stats, wallet) {
    if (!el.drawdownStats) return;
    var s = stats || {};
    var w = wallet || {};
    var walletBalance = toNum(s.wallet_balance_usdt);
    if (walletBalance === null) walletBalance = toNum(w.balance_usdt);

    var rows = [
      ["Account Equity", walletBalance === null ? "--" : fmtNum(walletBalance, 4) + " USDT"],
      ["Equity Change", fmtSigned(s.total_realized_pnl, 4) + " USDT"],
      ["Net Cashflow", fmtSigned(s.net_cashflow_usdt, 4) + " USDT"],
      ["Trade Realized", fmtSigned(s.trade_realized_pnl, 4) + " USDT"],
      ["Max Drawdown", fmtNum(s.max_drawdown, 4) + " (" + fmtNum(s.max_drawdown_pct, 2) + "%)"],
      ["Current Drawdown", fmtNum(s.current_drawdown, 4) + " (" + fmtNum(s.current_drawdown_pct, 2) + "%)"],
      ["Win Rate", (currentCurveTab === "balance" || Number(s.closed_trades_priced || 0) <= 0) ? "--" : (fmtNum(s.win_rate_pct, 2) + "%")],
      ["Closed Trades (Priced)", txt(s.closed_trades_priced)],
      ["Closed w/o Fill Price", txt(s.unpriced_closed_positions)],
      ["Balance Source", txt(w.source)]
    ];

    var html = "";
    for (var i = 0; i < rows.length; i += 1) {
      html += '<div class="stat-item"><span>' + escapeHtml(rows[i][0]) + '</span><span>' + escapeHtml(rows[i][1]) + '</span></div>';
    }
    el.drawdownStats.innerHTML = html;
  }

  function refresh() {
    fetchDashboard(function (err, d) {
      if (err) {
        setText(el.meta, "dashboard fetch error: " + err);
        return;
      }

      latestData = d;
      var summary = d.summary || {};
      var wallet = d.wallet || {};
      var stats = activeStats(d);
      var svc = d.service || {};
      var svcStatus = "DISABLED";
      if (svc.enabled) {
        if (svc.running) {
          svcStatus = "RUNNING";
        } else if (svc.error) {
          svcStatus = "ERROR (" + svc.error + ")";
        } else {
          svcStatus = "STOPPED";
        }
      }

      setText(
        el.meta,
        "Updated " + fmtMetaTime(d.generated_at_utc) +
          " · TZ " + txt(d.timezone) +
          " · Balance " + txt(wallet.source)
      );
      setText(el.nextEntry, fmtAxisTime(d.next_entry_local));
      setText(el.serviceState, svcStatus);
      if (el.serviceState) {
        var svcClass = "";
        if (svcStatus.indexOf("RUNNING") >= 0) svcClass = "ok";
        else if (svcStatus.indexOf("ERROR") >= 0) svcClass = "bad";
        else if (svcStatus.indexOf("STOPPED") >= 0 || svcStatus.indexOf("DISABLED") >= 0) svcClass = "warn";
        el.serviceState.className = svcClass;
      }
      setText(el.openCount, txt(summary.open_positions));
      setText(el.symbolCount, txt(summary.open_symbols));
      setText(el.errorCount, txt(summary.recent_errors));
      setText(el.lastRunStatus, txt(summary.last_run_status));
      if (el.lastRunStatus) {
        el.lastRunStatus.className = "v " + clsForStatus(summary.last_run_status);
      }
      rerenderFromLatest();

      renderRows(el.positionsBody, d.open_positions || [], function (p) {
        var errClass = p.last_error ? " bad" : "";
        return (
          "<tr>" +
          '<td class="mono">' + escapeHtml(p.id) + "</td>" +
          "<td>" + escapeHtml(p.symbol) + "</td>" +
          "<td>" + escapeHtml(p.qty) + "</td>" +
          "<td>" + escapeHtml(p.entry_price) + "</td>" +
          "<td>" + escapeHtml(p.tp_price) + "</td>" +
          "<td>" + escapeHtml(p.sl_price) + "</td>" +
          '<td class="mono">' + escapeHtml(fmtAxisTime(p.expire_at_utc)) + "</td>" +
          '<td class="mono' + errClass + '">' + escapeHtml(p.last_error) + "</td>" +
          "</tr>"
        );
      }, 8);

      renderRows(el.runsBody, d.runs || [], function (r) {
        return (
          "<tr>" +
          '<td class="mono">' + escapeHtml(fmtDateOnly(r.trade_day_utc)) + "</td>" +
          '<td class="' + clsForStatus(r.status) + '">' + escapeHtml(r.status) + "</td>" +
          '<td class="mono">' + escapeHtml(fmtAxisTime(r.started_at_utc)) + "</td>" +
          "<td>" + escapeHtml(r.message) + "</td>" +
          "</tr>"
        );
      }, 4);

      renderRows(el.eventsBody, d.events || [], function (e) {
        return (
          "<tr>" +
          '<td class="mono">' + escapeHtml(e.id) + "</td>" +
          "<td>" + escapeHtml(e.symbol) + "</td>" +
          "<td>" + escapeHtml(e.type) + "</td>" +
          "<td>" + escapeHtml(e.side) + "</td>" +
          '<td class="' + clsForStatus(e.status) + '">' + escapeHtml(e.status) + "</td>" +
          '<td class="mono">' + escapeHtml(fmtAxisTime(e.event_time_utc)) + "</td>" +
          "</tr>"
        );
      }, 6);

      renderRows(el.cashflowBody, d.cashflow_events || [], function (c) {
        var amount = toNum(c.amount);
        var amountClass = amount === null ? "" : (amount > 0 ? "ok" : (amount < 0 ? "bad" : "warn"));
        return (
          "<tr>" +
          '<td class="mono">' + escapeHtml(c.id) + "</td>" +
          "<td>" + escapeHtml(c.income_type) + "</td>" +
          '<td class="' + amountClass + '">' + escapeHtml(fmtSigned(c.amount, 4)) + "</td>" +
          "<td>" + escapeHtml(c.asset) + "</td>" +
          "<td>" + escapeHtml(c.symbol) + "</td>" +
          '<td class="mono">' + escapeHtml(fmtAxisTime(c.event_time_utc)) + "</td>" +
          "</tr>"
        );
      }, 6);

      renderRows(el.unpricedBody, d.unpriced_closed_details || [], function (u) {
        return (
          "<tr>" +
          '<td class="mono">' + escapeHtml(u.id) + "</td>" +
          "<td>" + escapeHtml(u.symbol) + "</td>" +
          '<td class="' + clsForStatus(u.status) + '">' + escapeHtml(u.status) + "</td>" +
          "<td>" + escapeHtml(u.close_reason) + "</td>" +
          "<td>" + escapeHtml(u.detected_reason) + "</td>" +
          '<td class="mono">' + escapeHtml(u.close_order_id) + "</td>" +
          '<td class="mono">' + escapeHtml(fmtAxisTime(u.closed_at_utc)) + "</td>" +
          "</tr>"
        );
      }, 7);

      if (el.logTail) {
        var logLines = d.log_tail || [];
        if (Object.prototype.toString.call(logLines) !== "[object Array]") {
          logLines = [];
        }
        el.logTail.textContent = logLines.join("\\n") || "No log lines";
      }
    });
  }

  refresh();
  if (el.tabStrategy) {
    el.tabStrategy.addEventListener("click", function () {
      currentCurveTab = "strategy";
      rerenderFromLatest();
    });
  }
  if (el.tabBalance) {
    el.tabBalance.addEventListener("click", function () {
      currentCurveTab = "balance";
      rerenderFromLatest();
    });
  }
  if (el.windowRow) {
    el.windowRow.addEventListener("click", function (evt) {
      var t = evt.target;
      if (!t || !t.getAttribute) return;
      var raw = t.getAttribute("data-window-hours");
      if (!raw) return;
      var parsed = Number(raw);
      if (!Number.isFinite(parsed) || parsed <= 0) return;
      currentWindowHours = parsed;
      renderCurveTabState();
      refresh();
    });
  }
  setInterval(refresh, Math.max(2000, REFRESH_SEC * 1000));
})();
</script>
</body>
</html>
"""
