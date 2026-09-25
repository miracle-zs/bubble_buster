import ast
import copy
import json
import logging
import os
import sqlite3
import glob
import re
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from core.state_store import SQLITE_BUSY_TIMEOUT_MS
from core.task_status import format_task_status, task_status_template
from core.template_loader import load_template

LOGGER = logging.getLogger(__name__)

# The daily portfolio take-profit feature was introduced in commit 2dc257c
# at 2026-08-10 09:32:10 Asia/Shanghai. Comparisons must not use snapshots
# from before that strategy existed.
PORTFOLIO_TAKE_PROFIT_LAUNCH_AT_UTC = "2026-08-10T01:32:10+00:00"
PORTFOLIO_LOSS_CUT_LOCK_NAME = "portfolio_loss_cut_v1"


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
        account_strategy_notes: Optional[Dict[str, str]] = None,
        account_modes: Optional[Dict[str, str]] = None,
        account_equity_recovery_enabled: Optional[Dict[str, bool]] = None,
        overview_account_ids: Optional[List[str]] = None,
        equity_comparison_account_ids: Optional[List[str]] = None,
        live_wallet_account_id: str = "default",
        trade_stats_fetchers: Optional[Dict[str, Any]] = None,
        live_position_clients: Optional[Dict[str, Any]] = None,
        live_position_cache_ttl_sec: int = 30,
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
        self._task_status_cache_key: Optional[Tuple[Tuple[str, int, int], ...]] = None
        self._task_status_cache_value: Optional[Dict[str, Dict[str, Dict[str, Any]]]] = None
        self._task_log_file_identity: Optional[Tuple[Tuple[str, int], ...]] = None
        self._task_log_parse_state: Dict[str, Tuple[int, int]] = {}
        self._task_status_lock = threading.RLock()
        self._accounts_summary_cache_lock = threading.RLock()
        self._accounts_summary_refresh_lock = threading.Lock()
        self._accounts_summary_fast_compute_lock = threading.Lock()
        self._accounts_summary_cache_value: Optional[Dict[str, Any]] = None
        self._accounts_summary_fast_cache_value: Optional[Dict[str, Any]] = None
        self.account_strategy_notes = {
            str(k).strip(): str(v).strip()
            for k, v in (account_strategy_notes or {}).items()
            if str(k).strip()
        }
        self.account_modes = {
            str(k).strip(): str(v).strip().lower() or "full"
            for k, v in (account_modes or {}).items()
            if str(k).strip()
        }
        self.account_equity_recovery_enabled = {
            str(k).strip(): bool(v)
            for k, v in (account_equity_recovery_enabled or {}).items()
            if str(k).strip()
        }
        overview_ids = [str(x).strip() for x in (overview_account_ids or []) if str(x).strip()]
        self.overview_account_ids: Optional[Set[str]] = set(overview_ids) if overview_ids else None
        comparison_ids = [
            str(x).strip()
            for x in (equity_comparison_account_ids or [])
            if str(x).strip()
        ]
        self.equity_comparison_account_ids: Optional[Set[str]] = (
            set(comparison_ids) if comparison_ids else None
        )
        self.live_wallet_account_id = (live_wallet_account_id or "").strip() or "default"
        self.trade_stats_fetchers = trade_stats_fetchers or {}
        self.live_position_clients = {
            str(k).strip(): v
            for k, v in (live_position_clients or {}).items()
            if str(k).strip() and v is not None
        }
        self.live_position_cache_ttl_sec = max(5, int(live_position_cache_ttl_sec))
        self._readonly_position_cache: Dict[str, Tuple[datetime, List[Dict[str, Any]]]] = {}
        self._readonly_position_cache_errors: Dict[str, Optional[str]] = {}
        self._balance_cache_value: Optional[float] = None
        self._balance_cache_at: Optional[datetime] = None
        self._balance_last_attempt_at: Optional[datetime] = None
        self._balance_last_error: Optional[str] = None
        try:
            self.local_tz = ZoneInfo(timezone_name)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Invalid dashboard timezone=%s, fallback UTC", timezone_name)
            self.local_tz = timezone.utc

    def set_live_position_clients(self, clients: Optional[Dict[str, Any]]) -> None:
        """Attach account-scoped exchange clients without persisting live risk data."""
        self.live_position_clients = {
            str(k).strip(): v
            for k, v in (clients or {}).items()
            if str(k).strip() and v is not None
        }

    @staticmethod
    def _live_order_status(
        orders: List[Dict[str, Any]],
        order_id: Any,
        client_order_id: Any,
        configured: bool,
    ) -> str:
        if not configured:
            return "NOT_SET"

        wanted_id = str(order_id).strip() if order_id not in (None, "") else ""
        wanted_client_id = str(client_order_id).strip() if client_order_id not in (None, "") else ""
        for order in orders:
            current_id = str(order.get("orderId") or order.get("algoId") or "").strip()
            current_client_id = str(
                order.get("clientOrderId") or order.get("clientAlgoId") or ""
            ).strip()
            if (wanted_id and current_id == wanted_id) or (
                wanted_client_id and current_client_id == wanted_client_id
            ):
                return str(order.get("status") or "UNKNOWN").strip().upper() or "UNKNOWN"
        return "MISSING"

    @classmethod
    def _position_margin_details(
        cls,
        risk: Dict[str, Any],
        account_position: Optional[Dict[str, Any]],
        notional: Optional[float],
    ) -> Tuple[Optional[float], Optional[str]]:
        """Resolve the exchange-reported initial margin used for position ROI."""
        account_position = account_position if isinstance(account_position, dict) else {}
        for payload, keys, source in (
            (
                account_position,
                ("positionInitialMargin", "initialMargin"),
                "ACCOUNT_POSITION_INITIAL_MARGIN",
            ),
            (
                risk,
                ("positionInitialMargin", "initialMargin"),
                "POSITION_RISK_INITIAL_MARGIN",
            ),
            (
                risk,
                ("isolatedMargin", "isolatedWallet"),
                "POSITION_RISK_ISOLATED_MARGIN",
            ),
        ):
            for key in keys:
                value = cls._safe_float(payload.get(key))
                if value is not None and value > 0:
                    return value, source

        leverage = cls._safe_float(risk.get("leverage"))
        if notional is not None and notional > 0 and leverage is not None and leverage > 0:
            return notional / leverage, "NOTIONAL_DIV_LEVERAGE"
        return None, None

    @classmethod
    def _live_return_metrics(
        cls,
        risk: Dict[str, Any],
        account_position: Optional[Dict[str, Any]],
        qty: Optional[float],
        entry_price: Optional[float],
        mark_price: Optional[float],
    ) -> Dict[str, Any]:
        pnl = cls._safe_float(risk.get("unRealizedProfit"))
        notional = cls._safe_float(risk.get("notional"))
        if notional is None or abs(notional) <= 0:
            fallback_price = mark_price if mark_price is not None and mark_price > 0 else entry_price
            if qty is not None and qty > 0 and fallback_price is not None and fallback_price > 0:
                notional = qty * fallback_price
        if notional is not None:
            notional = abs(notional)

        actual_margin, actual_margin_source = cls._position_margin_details(
            risk=risk,
            account_position=account_position,
            notional=notional,
        )
        return {
            "unrealized_pnl": pnl,
            "notional": notional,
            "actual_margin": actual_margin,
            "actual_margin_source": actual_margin_source,
            "unrealized_pnl_notional_pct": (
                round(pnl / notional * 100.0, 4)
                if pnl is not None and notional is not None and notional > 0
                else None
            ),
            "unrealized_pnl_margin_pct": (
                round(pnl / actual_margin * 100.0, 4)
                if pnl is not None and actual_margin is not None and actual_margin > 0
                else None
            ),
        }

    @classmethod
    def _ensure_position_notional(cls, position: Dict[str, Any]) -> None:
        """Fill a displayable current exposure when exchange notional is absent."""
        current_notional = cls._safe_float(position.get("notional"))
        if current_notional is not None and current_notional > 0:
            if not str(position.get("notional_source") or "").strip():
                mark_price = cls._safe_float(position.get("mark_price"))
                position["notional_source"] = (
                    "MARK_PRICE" if mark_price is not None and mark_price > 0 else "ENTRY_PRICE_ESTIMATE"
                )
            return

        qty = cls._safe_float(position.get("qty"))
        mark_price = cls._safe_float(position.get("mark_price"))
        entry_price = cls._safe_float(position.get("entry_price"))
        reference_price = mark_price if mark_price is not None and mark_price > 0 else entry_price
        if qty is None or qty <= 0 or reference_price is None or reference_price <= 0:
            return

        position["notional"] = round(abs(qty) * reference_price, 8)
        position["notional_source"] = (
            "MARK_PRICE" if mark_price is not None and mark_price > 0 else "ENTRY_PRICE_ESTIMATE"
        )

    @classmethod
    def _readonly_order_price(cls, order: Dict[str, Any]) -> Optional[float]:
        for key in ("stopPrice", "triggerPrice", "price", "activatePrice"):
            value = cls._safe_float(order.get(key))
            if value is not None and value > 0:
                return value
        return None

    @staticmethod
    def _readonly_order_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes", "y"}

    @classmethod
    def _readonly_exit_orders(
        cls,
        orders: List[Dict[str, Any]],
        side: str,
        position_side: str,
        entry_price: Optional[float],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Match active exchange exit orders to one readonly exchange position."""
        expected_order_side = "SELL" if side == "LONG" else "BUY"
        matched: Dict[str, List[Dict[str, Any]]] = {"tp": [], "sl": []}
        normalized_position_side = str(position_side or "").strip().upper()

        for order in orders or []:
            if not isinstance(order, dict):
                continue
            order_side = str(order.get("side") or "").strip().upper()
            if order_side and order_side != expected_order_side:
                continue
            order_position_side = str(order.get("positionSide") or "").strip().upper()
            if order_position_side in {"LONG", "SHORT"} and normalized_position_side in {"LONG", "SHORT"}:
                if order_position_side != normalized_position_side:
                    continue

            order_type = str(order.get("type") or order.get("orderType") or "").strip().upper()
            is_conditional = (
                "STOP" in order_type
                or "TAKE_PROFIT" in order_type
                or order_type == "TRAILING_STOP_MARKET"
            )
            is_reduce_only = cls._readonly_order_bool(order.get("reduceOnly")) or cls._readonly_order_bool(
                order.get("closePosition")
            )
            if not is_conditional and not is_reduce_only:
                continue

            order_price = cls._readonly_order_price(order)
            kind: Optional[str] = None
            if "TAKE_PROFIT" in order_type:
                kind = "tp"
            elif "STOP" in order_type or order_type == "TRAILING_STOP_MARKET":
                kind = "sl"
            elif order_type == "LIMIT" and order_price is not None and entry_price is not None and entry_price > 0:
                is_profit_price = order_price > entry_price if side == "LONG" else order_price < entry_price
                kind = "tp" if is_profit_price else "sl"
            if kind is None:
                continue

            matched[kind].append(
                {
                    "order_id": order.get("orderId") or order.get("algoId"),
                    "client_order_id": order.get("clientOrderId") or order.get("clientAlgoId"),
                    "type": order_type or None,
                    "status": str(order.get("status") or "NEW").strip().upper() or "NEW",
                    "price": order_price,
                    "limit_price": cls._safe_float(order.get("price")),
                    "trigger_price": cls._safe_float(order.get("stopPrice") or order.get("triggerPrice")),
                    "side": order_side or None,
                    "position_side": order_position_side or None,
                }
            )
        return matched

    @staticmethod
    def _select_readonly_order(orders: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not orders:
            return None
        active_statuses = {
            "NEW",
            "PENDING",
            "ACTIVE",
            "PARTIALLY_FILLED",
            "TRIGGERING",
            "TRIGGERED",
        }
        return sorted(
            orders,
            key=lambda order: (
                0 if str(order.get("status") or "").upper() in active_statuses else 1,
                -int(order.get("order_id") or 0) if str(order.get("order_id") or "").isdigit() else 0,
            ),
        )[0]

    @staticmethod
    def _account_position_index(account_payload: Any) -> Dict[Tuple[str, str], Dict[str, Any]]:
        if not isinstance(account_payload, dict):
            return {}
        result: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in account_payload.get("positions") or []:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            position_side = str(row.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            result[(symbol, position_side)] = row
        return result

    def _local_exchange_risks(self, account_id: str) -> List[Dict[str, Any]]:
        """Read the latest persisted account snapshot in Binance-shaped rows."""
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT * FROM account_position_state
                WHERE account_id = ? AND ABS(position_amt) > 0.000000000001
                ORDER BY symbol, position_side
                """,
                (str(account_id).strip(),),
            ).fetchall()
        return [
            {
                "symbol": row["symbol"],
                "positionSide": row["position_side"],
                "positionAmt": row["position_amt"],
                "entryPrice": row["entry_price"],
                "breakEvenPrice": row["break_even_price"],
                "markPrice": row["mark_price"],
                "unRealizedProfit": row["unrealized_pnl"],
                "liquidationPrice": row["liquidation_price"],
                "leverage": row["leverage"],
                "notional": row["notional"],
                "isolatedMargin": row["isolated_margin"],
                "positionInitialMargin": row["initial_margin"],
                "capturedAtUtc": row["captured_at_utc"],
            }
            for row in rows
        ]

    def _local_exchange_orders(self, account_id: str) -> List[Dict[str, Any]]:
        with self._connect_ctx() as conn:
            rows = conn.execute(
                """
                SELECT * FROM exchange_order_state
                WHERE account_id = ?
                  AND status IN ('NEW', 'PENDING', 'ACTIVE', 'PARTIALLY_FILLED', 'TRIGGERING', 'TRIGGERED')
                ORDER BY event_time_utc DESC
                """,
                (str(account_id).strip(),),
            ).fetchall()
        return [
            {
                "symbol": row["symbol"],
                "orderId": self._safe_int(row["order_id"]) if row["order_id"] not in (None, "") else None,
                "clientOrderId": row["client_order_id"],
                "type": row["type"],
                "side": row["side"],
                "positionSide": row["position_side"],
                "status": row["status"],
                "price": row["price"],
                "stopPrice": row["stop_price"],
                "avgPrice": row["avg_price"],
                "origQty": row["original_qty"],
                "executedQty": row["executed_qty"],
                "reduceOnly": bool(row["reduce_only"]) if row["reduce_only"] is not None else None,
                "closePosition": bool(row["close_position"]) if row["close_position"] is not None else None,
                "eventTimeUtc": row["event_time_utc"],
            }
            for row in rows
        ]

    def _enrich_open_positions_with_live_data(
        self,
        positions: List[Dict[str, Any]],
        account_id: Optional[str],
    ) -> None:
        if not positions or not account_id:
            return

        try:
            risks = self._local_exchange_risks(str(account_id).strip())
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Local position state read failed account=%s: %s", account_id, exc)
            for position in positions:
                position["live_data_available"] = False
                position["live_data_error"] = "实时仓位暂不可用"
            return

        order_fetch_error = None
        try:
            open_orders = self._local_exchange_orders(str(account_id).strip())
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Local order state read failed account=%s: %s", account_id, exc)
            open_orders = []
            order_fetch_error = "实时挂单暂不可用"

        risk_by_symbol: Dict[str, Dict[str, Any]] = {}
        risk_by_symbol_side: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for risk in risks or []:
            symbol = str(risk.get("symbol") or "").strip().upper()
            if symbol:
                risk_by_symbol[symbol] = risk
                position_side = str(risk.get("positionSide") or "").strip().upper()
                if position_side:
                    risk_by_symbol_side[(symbol, position_side)] = risk

        orders_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for order in open_orders or []:
            symbol = str(order.get("symbol") or "").strip().upper()
            if symbol:
                orders_by_symbol.setdefault(symbol, []).append(order)

        captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        for position in positions:
            symbol = str(position.get("symbol") or "").strip().upper()
            configured_side = str(position.get("side") or "").strip().upper()
            risk = risk_by_symbol_side.get((symbol, configured_side)) or risk_by_symbol.get(symbol)
            symbol_orders = orders_by_symbol.get(symbol, [])
            position["live_data_as_of_utc"] = captured_at
            position["live_data_available"] = risk is not None
            position["live_data_error"] = None if risk is not None else "交易所未返回该仓位"
            position["order_data_error"] = order_fetch_error
            position["tp_order_status"] = self._live_order_status(
                symbol_orders,
                position.get("tp_order_id", position.get("_tp_order_id")),
                position.get("tp_client_order_id", position.get("_tp_client_order_id")),
                configured=position.get("tp_price") not in (None, ""),
            )
            position["sl_order_status"] = self._live_order_status(
                symbol_orders,
                position.get("sl_order_id", position.get("_sl_order_id")),
                position.get("sl_client_order_id", position.get("_sl_client_order_id")),
                configured=position.get("sl_price") not in (None, ""),
            )
            if risk is None:
                self._ensure_position_notional(position)
                continue

            position["position_side"] = str(risk.get("positionSide") or position.get("side") or "").upper()
            position["mark_price"] = self._safe_float(risk.get("markPrice"))
            position["unrealized_pnl"] = self._safe_float(risk.get("unRealizedProfit"))
            position["live_liq_price"] = self._safe_float(risk.get("liquidationPrice"))
            return_metrics = self._live_return_metrics(
                risk=risk,
                account_position=None,
                qty=self._safe_float(position.get("qty")),
                entry_price=self._safe_float(position.get("entry_price")),
                mark_price=position["mark_price"],
            )
            position["notional"] = return_metrics["notional"]
            position["actual_margin"] = return_metrics["actual_margin"]
            position["actual_margin_source"] = return_metrics["actual_margin_source"]
            position["unrealized_pnl_notional_pct"] = return_metrics["unrealized_pnl_notional_pct"]
            position["unrealized_pnl_margin_pct"] = return_metrics["unrealized_pnl_margin_pct"]
            isolated_margin = self._safe_float(risk.get("isolatedMargin"))
            pnl = self._safe_float(risk.get("unRealizedProfit"))
            position["unrealized_pnl_pct"] = (
                round(pnl / isolated_margin * 100.0, 4)
                if pnl is not None and isolated_margin is not None and isolated_margin > 0
                else None
            )
            position["leverage"] = self._safe_float(risk.get("leverage"))
            raw_notional = self._safe_float(risk.get("notional"))
            if raw_notional is not None and abs(raw_notional) > 0:
                position["notional_source"] = "EXCHANGE_NOTIONAL"
            else:
                self._ensure_position_notional(position)

    def _is_readonly_account(self, account_id: Optional[str]) -> bool:
        normalized = str(account_id or "").strip()
        return bool(normalized) and self.account_modes.get(normalized, "full") == "readonly"

    def _readonly_live_positions(self, account_id: str) -> List[Dict[str, Any]]:
        """Return current exchange positions for a readonly account without persisting them."""
        normalized_account_id = str(account_id or "").strip()
        if not self._is_readonly_account(normalized_account_id):
            return []

        now_utc = datetime.now(timezone.utc)
        cached = self._readonly_position_cache.get(normalized_account_id)
        if cached is not None:
            cached_at, cached_rows = cached
            age_sec = (now_utc - cached_at).total_seconds()
            if age_sec < self.live_position_cache_ttl_sec:
                return [dict(row) for row in cached_rows]

        try:
            risks = self._local_exchange_risks(normalized_account_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Readonly local position read failed account=%s: %s", normalized_account_id, exc)
            self._readonly_position_cache_errors[normalized_account_id] = str(exc)
            self._readonly_position_cache[normalized_account_id] = (now_utc, [])
            return []

        order_fetch_error = None
        try:
            open_orders = self._local_exchange_orders(normalized_account_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Readonly local order read failed account=%s: %s", normalized_account_id, exc)
            open_orders = []
            order_fetch_error = "实时订单暂不可用"

        account_position_index: Dict[Tuple[str, str], Dict[str, Any]] = {
            (
                str(row.get("symbol") or "").strip().upper(),
                str(row.get("positionSide") or "BOTH").strip().upper() or "BOTH",
            ): row
            for row in risks
            if str(row.get("symbol") or "").strip()
        }
        account_fetch_error = None

        orders_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for order in open_orders:
            if not isinstance(order, dict):
                continue
            symbol = str(order.get("symbol") or "").strip().upper()
            if symbol:
                orders_by_symbol.setdefault(symbol, []).append(order)

        captured_at = now_utc.replace(microsecond=0).isoformat()
        rows: List[Dict[str, Any]] = []
        for risk in risks or []:
            symbol = str(risk.get("symbol") or "").strip().upper()
            position_amt = self._safe_float(risk.get("positionAmt"))
            if not symbol or position_amt is None or abs(position_amt) <= 1e-12:
                continue

            exchange_position_side = str(risk.get("positionSide") or "").strip().upper()
            if exchange_position_side in {"LONG", "SHORT"}:
                side = exchange_position_side
            else:
                side = "LONG" if position_amt > 0 else "SHORT"
            position_key = exchange_position_side or side
            entry_price = self._safe_float(risk.get("entryPrice"))
            mark_price = self._safe_float(risk.get("markPrice"))
            account_position = account_position_index.get((symbol, exchange_position_side or "BOTH"))
            if account_position is None:
                account_position = next(
                    (
                        row
                        for (account_symbol, _account_side), row in account_position_index.items()
                        if account_symbol == symbol
                    ),
                    None,
                )
            return_metrics = self._live_return_metrics(
                risk=risk,
                account_position=account_position,
                qty=abs(position_amt),
                entry_price=entry_price,
                mark_price=mark_price,
            )
            exit_orders = self._readonly_exit_orders(
                orders=orders_by_symbol.get(symbol, []),
                side=side,
                position_side=exchange_position_side or side,
                entry_price=entry_price,
            )
            selected_tp = self._select_readonly_order(exit_orders["tp"])
            selected_sl = self._select_readonly_order(exit_orders["sl"])
            if order_fetch_error:
                tp_order_status = "READ_ERROR"
                sl_order_status = "READ_ERROR"
            else:
                tp_order_status = str((selected_tp or {}).get("status") or "NOT_FOUND").upper()
                sl_order_status = str((selected_sl or {}).get("status") or "NOT_FOUND").upper()
            margin_data_error = None
            if return_metrics["actual_margin"] is None:
                margin_data_error = account_fetch_error or "实际保证金暂不可用"
            rows.append(
                {
                    "id": f"readonly:{normalized_account_id}:{symbol}:{position_key}",
                    "run_id": None,
                    "symbol": symbol,
                    "side": side,
                    "position_side": exchange_position_side or side,
                    "qty": abs(position_amt),
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "unrealized_pnl": return_metrics["unrealized_pnl"],
                    "unrealized_pnl_pct": return_metrics["unrealized_pnl_margin_pct"],
                    "unrealized_pnl_notional_pct": return_metrics["unrealized_pnl_notional_pct"],
                    "unrealized_pnl_margin_pct": return_metrics["unrealized_pnl_margin_pct"],
                    "liq_price_latest": self._safe_float(risk.get("liquidationPrice")),
                    "isolated_margin": self._safe_float(risk.get("isolatedMargin")),
                    "notional": return_metrics["notional"],
                    "notional_source": (
                        "EXCHANGE_NOTIONAL"
                        if self._safe_float(risk.get("notional")) not in (None, 0.0)
                        else (
                            "MARK_PRICE"
                            if self._safe_float(risk.get("markPrice")) not in (None, 0.0)
                            else "ENTRY_PRICE_ESTIMATE"
                        )
                    ),
                    "actual_margin": return_metrics["actual_margin"],
                    "actual_margin_source": return_metrics["actual_margin_source"],
                    "leverage": self._safe_float(risk.get("leverage")),
                    "tp_price": (selected_tp or {}).get("price"),
                    "sl_price": (selected_sl or {}).get("price"),
                    "tp_order_id": (selected_tp or {}).get("order_id"),
                    "sl_order_id": (selected_sl or {}).get("order_id"),
                    "tp_client_order_id": (selected_tp or {}).get("client_order_id"),
                    "sl_client_order_id": (selected_sl or {}).get("client_order_id"),
                    "tp_order_status": tp_order_status,
                    "sl_order_status": sl_order_status,
                    "tp_orders": exit_orders["tp"],
                    "sl_orders": exit_orders["sl"],
                    "order_data_source": "LOCAL_USER_STREAM",
                    "order_data_error": order_fetch_error,
                    "margin_data_error": margin_data_error,
                    "opened_at_utc": None,
                    "expire_at_utc": None,
                    "status": "LIVE_READONLY",
                    "last_error": None,
                    "live_data_available": True,
                    "live_data_as_of_utc": captured_at,
                    "readonly_live": True,
                    "live_position_source": "LOCAL_ACCOUNT_STATE",
                }
            )

        rows.sort(key=lambda row: (str(row.get("symbol") or ""), str(row.get("position_side") or "")))
        self._readonly_position_cache_errors[normalized_account_id] = None
        self._readonly_position_cache[normalized_account_id] = (now_utc, rows)
        return [dict(row) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
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

    def _entry_cycle_date(self, now_local: datetime) -> str:
        cycle_start = now_local.replace(
            hour=self.entry_hour,
            minute=self.entry_minute,
            second=0,
            microsecond=0,
        )
        if now_local < cycle_start:
            cycle_start -= timedelta(days=1)
        return cycle_start.date().isoformat()

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

    @staticmethod
    def _task_status_template() -> Dict[str, Dict[str, Any]]:
        return task_status_template()

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return default

    @staticmethod
    def _status_from_error_count(errors: int, successes: int) -> str:
        if errors <= 0:
            return "SUCCESS"
        if successes > 0:
            return "PARTIAL"
        return "FAILED"

    @staticmethod
    def _format_symbol_field(value: Any) -> str:
        if isinstance(value, str):
            symbols = [x.strip().upper() for x in value.split(",") if x.strip()]
        elif isinstance(value, list):
            symbols = [str(x).strip().upper() for x in value if str(x).strip()]
        else:
            symbols = []
        if not symbols:
            return "-"
        unique: List[str] = []
        for sym in symbols:
            if sym not in unique:
                unique.append(sym)
        return ",".join(unique)

    @staticmethod
    def _append_summary_part(parts: List[str], key: str, value: Any) -> None:
        text = str(value).strip()
        if text == "" or text == "-":
            return
        parts.append(f"{key}={text}")

    @staticmethod
    def _log_time_from_line(line: str) -> Optional[str]:
        if len(line) < 19:
            return None
        candidate = line[:19]
        try:
            datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S")
            return candidate
        except ValueError:
            return None

    def _task_status_from_payload(
        self,
        task_key: str,
        payload: Dict[str, Any],
        time_local: Optional[str],
    ) -> Dict[str, Any]:
        return format_task_status(task_key, payload, time_local)

    @staticmethod
    def _read_log_lines(path: str, max_lines: int = 25000) -> List[str]:
        if not os.path.exists(path):
            return []
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()
            if max_lines > 0 and len(lines) > max_lines:
                return lines[-max_lines:]
            return lines
        except OSError:
            return []

    @staticmethod
    def _read_task_log_lines(path: str, markers: Tuple[str, ...]) -> List[str]:
        if not os.path.exists(path):
            return []
        lines: List[str] = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if any(marker in line for marker in markers):
                        lines.append(line.rstrip("\n"))
        except OSError:
            return []
        return lines

    @staticmethod
    def _read_task_log_lines_from_offset(
        path: str,
        start_offset: int,
        markers: Tuple[str, ...],
    ) -> Tuple[List[str], int]:
        """Read only the newly appended portion of a task log.

        The dashboard log is append-only during normal operation. Keeping the
        byte offset avoids rescanning tens of megabytes on every overview
        refresh while still allowing callers to reset to offset zero after a
        rotation or truncation.
        """
        if not os.path.exists(path):
            return [], 0
        lines: List[str] = []
        try:
            with open(path, "rb") as f:
                file_size = os.fstat(f.fileno()).st_size
                offset = max(0, int(start_offset))
                if offset > file_size:
                    offset = 0
                f.seek(offset)
                while True:
                    line = f.readline()
                    if not line:
                        break
                    text = line.decode("utf-8", errors="replace")
                    if any(marker in text for marker in markers):
                        lines.append(text.rstrip("\r\n"))
                end_offset = f.tell()
        except OSError:
            return [], 0
        return lines, end_offset

    def _task_log_files(self) -> List[str]:
        files: List[str] = []
        rotated = sorted(glob.glob(f"{self.log_file}.*"))
        if rotated:
            files.append(rotated[-1])
        files.append(self.log_file)
        seen = set()
        deduped: List[str] = []
        for path in files:
            if not path or path in seen or not os.path.exists(path):
                continue
            deduped.append(path)
            seen.add(path)
        return deduped

    def _task_status_cache_signature(self, files: List[str]) -> Tuple[Tuple[str, int, int], ...]:
        signature: List[Tuple[str, int, int]] = []
        for path in files:
            try:
                stat = os.stat(path)
                signature.append((path, int(stat.st_ino), int(stat.st_size)))
            except OSError:
                continue
        return tuple(signature)

    def _parse_task_statuses_from_logs(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        with self._task_status_lock:
            return self._parse_task_statuses_from_logs_locked()

    def _parse_task_statuses_from_logs_locked(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        file_paths = self._task_log_files()
        signature = self._task_status_cache_signature(file_paths)
        if self._task_status_cache_key == signature and self._task_status_cache_value is not None:
            return self._task_status_cache_value

        markers = {
            "entry": "service entry result:",
            "daily_loss_cut": "service daily loss-cut result:",
            "noon_protection": "service noon protection result:",
            "manage": "service manage summary:",
        }
        log_markers = tuple(markers.values()) + (
            "service equity recovery take-profit ",
            "service portfolio take-profit ",
        )

        current_identity = tuple((path, inode) for path, inode, _size in signature)
        rebuild = self._task_status_cache_value is None or self._task_log_file_identity != current_identity
        if not rebuild:
            for path, inode, size in signature:
                previous = self._task_log_parse_state.get(path)
                if previous is None or previous[0] != inode or size < previous[1]:
                    rebuild = True
                    break

        if rebuild:
            statuses: Dict[str, Dict[str, Dict[str, Any]]] = {}
            self._task_log_parse_state = {}
        else:
            # The parser owns this dictionary under _task_status_lock. Reusing
            # it preserves the latest status while merging only new lines.
            statuses = self._task_status_cache_value or {}

        for path in file_paths:
            stat_entry = next((item for item in signature if item[0] == path), None)
            if stat_entry is None:
                continue
            inode = stat_entry[1]
            start_offset = 0
            if not rebuild:
                previous = self._task_log_parse_state.get(path)
                if previous is not None and previous[0] == inode:
                    start_offset = previous[1]
            lines, end_offset = self._read_task_log_lines_from_offset(
                path,
                start_offset,
                log_markers,
            )
            self._task_log_parse_state[path] = (inode, end_offset)
            for line in lines:
                if "service portfolio take-profit " in line:
                    time_local = self._log_time_from_line(line)
                    matched_result = re.search(
                        r"service portfolio take-profit account=([A-Za-z0-9_.-]+)\s+result=(\{.*\})",
                        line,
                    )
                    if matched_result:
                        aid = matched_result.group(1).strip()
                        payload_raw = matched_result.group(2).strip()
                        try:
                            parsed = ast.literal_eval(payload_raw)
                        except (SyntaxError, ValueError):
                            parsed = None
                        if aid and isinstance(parsed, dict):
                            statuses.setdefault(aid, self._task_status_template())["equity_recovery_take_profit"] = (
                                self._task_status_from_payload(
                                    "equity_recovery_take_profit",
                                    parsed,
                                    time_local,
                                )
                            )
                            continue
                    matched_error = re.search(
                        r"service portfolio take-profit failed account=([A-Za-z0-9_.-]+):\s*(.*)$",
                        line,
                    )
                    if matched_error:
                        aid = matched_error.group(1).strip()
                        err = matched_error.group(2).strip()
                        if aid:
                            statuses.setdefault(aid, self._task_status_template())["equity_recovery_take_profit"] = {
                                "status": "FAILED",
                                "time_local": time_local,
                                "summary": f"error={err[:80]}",
                            }
                        continue

                if "service equity recovery take-profit " in line:
                    time_local = self._log_time_from_line(line)
                    matched_result = re.search(
                        r"service equity recovery take-profit account=([A-Za-z0-9_.-]+)\s+result:\s+(\{.*\})",
                        line,
                    )
                    if matched_result:
                        aid = matched_result.group(1).strip()
                        payload_raw = matched_result.group(2).strip()
                        try:
                            parsed = ast.literal_eval(payload_raw)
                        except (SyntaxError, ValueError):
                            parsed = None
                        if aid and isinstance(parsed, dict):
                            statuses.setdefault(aid, self._task_status_template())["equity_recovery_take_profit"] = (
                                self._task_status_from_payload("equity_recovery_take_profit", parsed, time_local)
                            )
                            continue
                    matched_error = re.search(
                        r"service equity recovery take-profit failed account=([A-Za-z0-9_.-]+):\s*(.*)$",
                        line,
                    )
                    if matched_error:
                        aid = matched_error.group(1).strip()
                        err = matched_error.group(2).strip()
                        if aid:
                            statuses.setdefault(aid, self._task_status_template())["equity_recovery_take_profit"] = {
                                "status": "FAILED",
                                "time_local": time_local,
                                "summary": f"error={err[:80]}",
                            }
                        continue

                task_key = None
                marker = None
                for maybe_task, maybe_marker in markers.items():
                    if maybe_marker in line:
                        task_key = maybe_task
                        marker = maybe_marker
                        break
                if task_key is None or marker is None:
                    continue

                payload_raw = line.split(marker, 1)[1].strip()
                if not payload_raw.startswith("{"):
                    continue
                try:
                    parsed = ast.literal_eval(payload_raw)
                except (SyntaxError, ValueError):
                    continue
                if not isinstance(parsed, dict):
                    continue

                time_local = self._log_time_from_line(line)

                # Single-account legacy shape: manage summary and task summaries without account map.
                if task_key == "manage" and "summary" in parsed and "account_id" in parsed:
                    account_id = str(parsed.get("account_id") or "").strip()
                    if account_id:
                        statuses.setdefault(account_id, self._task_status_template())[task_key] = (
                            self._task_status_from_payload(task_key, parsed, time_local)
                        )
                    continue
                if task_key in {"daily_loss_cut", "noon_protection"} and "total" in parsed:
                    statuses.setdefault("__GLOBAL__", self._task_status_template())[task_key] = (
                        self._task_status_from_payload(task_key, parsed, time_local)
                    )
                    continue

                for account_id, account_payload in parsed.items():
                    aid = str(account_id or "").strip()
                    if not aid or not isinstance(account_payload, dict):
                        continue
                    statuses.setdefault(aid, self._task_status_template())[task_key] = (
                        self._task_status_from_payload(task_key, account_payload, time_local)
                    )

        self._task_log_file_identity = current_identity
        self._task_status_cache_key = signature
        self._task_status_cache_value = statuses
        return statuses

    def _latest_task_statuses_for_accounts(
        self,
        account_ids: List[str],
        conn: Optional[sqlite3.Connection] = None,
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        normalized_ids = [str(aid).strip() for aid in account_ids if str(aid).strip()]
        payload = {aid: self._task_status_template() for aid in normalized_ids}
        if not normalized_ids:
            return payload

        parsed = self._parse_task_statuses_from_logs()
        global_fallback = parsed.get("__GLOBAL__")
        for aid in normalized_ids:
            tasks = payload[aid]
            if global_fallback:
                for key, value in global_fallback.items():
                    if isinstance(value, dict):
                        tasks[key] = dict(value)
            account_tasks = parsed.get(aid)
            if not account_tasks:
                continue
            for key, value in account_tasks.items():
                if key in tasks and isinstance(value, dict):
                    tasks[key] = dict(value)

        if conn is not None:
            self._merge_persisted_task_statuses(conn, normalized_ids, payload)
        elif os.path.exists(self.db_path):
            try:
                with self._connect_ctx() as ctx_conn:
                    self._merge_persisted_task_statuses(ctx_conn, normalized_ids, payload)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to query task statuses from DB: %s", exc)
        return payload

    def _merge_persisted_task_statuses(
        self,
        conn: sqlite3.Connection,
        account_ids: List[str],
        payload: Dict[str, Dict[str, Dict[str, Any]]],
    ) -> None:
        persisted_entries = self._latest_entry_statuses_from_db(conn, account_ids)
        for aid, persisted_entry in persisted_entries.items():
            if aid in payload:
                current_entry = payload[aid].get("entry", {})
                if self._task_status_is_newer(persisted_entry, current_entry):
                    payload[aid]["entry"] = persisted_entry

        persisted_tasks = self._latest_task_executions_from_db(conn, account_ids)
        for aid, tasks in persisted_tasks.items():
            if aid not in payload:
                continue
            for task_key, task_data in tasks.items():
                if task_key in payload[aid]:
                    current_task = payload[aid][task_key]
                    if self._task_status_is_newer(task_data, current_task):
                        payload[aid][task_key] = task_data

    def _latest_task_executions_from_db(
        self,
        conn: sqlite3.Connection,
        account_ids: List[str],
    ) -> Dict[str, Dict[str, Dict[str, Any]]]:
        if not account_ids:
            return {}
        try:
            table_check = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='task_executions'"
            ).fetchone()
            if table_check is None:
                return {}
            placeholders = ",".join("?" for _ in account_ids)
            rows = self._query_rows(
                conn,
                f"""
                SELECT t.account_id, t.task_name, t.status, t.summary, t.time_local
                FROM task_executions t
                INNER JOIN (
                    SELECT account_id, task_name, MAX(id) AS max_id
                    FROM task_executions
                    WHERE account_id IN ({placeholders})
                    GROUP BY account_id, task_name
                ) m ON t.id = m.max_id
                """,
                tuple(account_ids),
            )
            result: Dict[str, Dict[str, Dict[str, Any]]] = {}
            for row in rows:
                aid = str(row.get("account_id") or "").strip()
                tname = str(row.get("task_name") or "").strip()
                if not aid or not tname:
                    continue
                result.setdefault(aid, {})[tname] = {
                    "status": str(row.get("status") or "UNKNOWN"),
                    "time_local": row.get("time_local"),
                    "summary": str(row.get("summary") or "--"),
                }
            return result
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Error querying task_executions from db: %s", exc)
            return {}

    def _task_status_is_newer(self, candidate: Dict[str, Any], current: Dict[str, Any]) -> bool:
        if str(current.get("status") or "UNKNOWN").upper() == "UNKNOWN":
            return True

        def parse_local(value: Any) -> Optional[datetime]:
            text = str(value or "").strip()
            if not text:
                return None
            try:
                parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
            return parsed.replace(tzinfo=self.local_tz)

        candidate_time = parse_local(candidate.get("time_local"))
        current_time = parse_local(current.get("time_local"))
        if candidate_time is None:
            return False
        if current_time is None:
            return True
        return candidate_time >= current_time

    def _latest_entry_statuses_from_db(
        self,
        conn: sqlite3.Connection,
        account_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        if not account_ids:
            return {}
        placeholders = ",".join("?" for _ in account_ids)
        rows = self._query_rows(
            conn,
            f"""
            SELECT
                r.run_id, r.account_id, r.started_at_utc, r.completed_at_utc,
                r.status, r.message, COUNT(p.id) AS position_count
            FROM runs r
            INNER JOIN (
                SELECT account_id, MAX(started_at_utc) AS max_started_at_utc
                FROM runs
                WHERE account_id IN ({placeholders})
                GROUP BY account_id
            ) latest
                ON latest.account_id = r.account_id
               AND latest.max_started_at_utc = r.started_at_utc
            LEFT JOIN positions p ON p.run_id = r.run_id
            GROUP BY
                r.run_id, r.account_id, r.started_at_utc, r.completed_at_utc,
                r.status, r.message
            """,
            tuple(account_ids),
        )
        wait_state_by_account = self._entry_wait_states_from_db(conn, account_ids)
        return {
            str(row.get("account_id") or "").strip(): self._entry_status_from_run_row(
                row,
                wait_state=wait_state_by_account.get(str(row.get("account_id") or "").strip()),
            )
            for row in rows
            if str(row.get("account_id") or "").strip()
        }

    def _entry_progresses_from_db(
        self,
        conn: sqlite3.Connection,
        account_ids: List[str],
        now_local: Optional[datetime] = None,
    ) -> Dict[str, Dict[str, Any]]:
        if not account_ids:
            return {}
        placeholders = ",".join("?" for _ in account_ids)
        run_rows = self._query_rows(
            conn,
            f"""
            SELECT
                r.run_id, r.account_id, r.trade_day_utc, r.started_at_utc,
                r.completed_at_utc, r.status, r.message
            FROM runs r
            INNER JOIN (
                SELECT account_id, MAX(started_at_utc) AS max_started_at_utc
                FROM runs
                WHERE account_id IN ({placeholders})
                GROUP BY account_id
            ) latest
                ON latest.account_id = r.account_id
               AND latest.max_started_at_utc = r.started_at_utc
            """,
            tuple(account_ids),
        )
        if not run_rows:
            return {}

        run_ids = [str(row.get("run_id") or "") for row in run_rows if row.get("run_id")]
        positions_by_run: Dict[str, List[Dict[str, Any]]] = {run_id: [] for run_id in run_ids}
        if run_ids:
            run_placeholders = ",".join("?" for _ in run_ids)
            position_rows = self._query_rows(
                conn,
                f"""
                SELECT run_id, symbol, status, opened_at_utc, entry_price, sl_price
                FROM positions
                WHERE run_id IN ({run_placeholders})
                ORDER BY opened_at_utc ASC, id ASC
                """,
                tuple(run_ids),
            )
            for position in position_rows:
                positions_by_run.setdefault(str(position.get("run_id") or ""), []).append(position)

        wait_states = self._entry_wait_states_from_db(conn, account_ids)
        entry_actions_by_account = self._entry_actions_from_db(conn, run_ids)
        current_cycle_date = self._entry_cycle_date(now_local or datetime.now(self.local_tz))
        progress_by_account: Dict[str, Dict[str, Any]] = {}
        for run in run_rows:
            account_id = str(run.get("account_id") or "").strip()
            run_id = str(run.get("run_id") or "").strip()
            if not account_id or not run_id:
                continue

            positions = positions_by_run.get(run_id, [])
            opened_symbols = [
                {
                    "symbol": str(position.get("symbol") or "").strip(),
                    "opened_at_local": self._format_utc_as_local(position.get("opened_at_utc")),
                    "entry_price": self._safe_float(position.get("entry_price")),
                    "sl_price": self._safe_float(position.get("sl_price")),
                    "position_status": str(position.get("status") or "UNKNOWN").strip().upper(),
                }
                for position in positions
                if str(position.get("symbol") or "").strip()
            ]

            wait_state = wait_states.get(account_id) or {}
            if str(wait_state.get("run_id") or "") != run_id:
                wait_state = {}
            raw_pending = wait_state.get("pending")
            pending_items = raw_pending.values() if isinstance(raw_pending, dict) else []
            waiting_symbols: List[Dict[str, Any]] = []
            next_checks: List[datetime] = []
            for item in pending_items:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol") or "").strip()
                if not symbol:
                    continue
                hour_open_raw = str(item.get("hour_open_utc") or "").strip()
                next_check: Optional[datetime] = None
                if hour_open_raw:
                    try:
                        parsed_hour_open = datetime.fromisoformat(hour_open_raw)
                        if parsed_hour_open.tzinfo is None:
                            parsed_hour_open = parsed_hour_open.replace(tzinfo=timezone.utc)
                        next_check = parsed_hour_open.astimezone(timezone.utc) + timedelta(hours=1)
                    except ValueError:
                        next_check = None
                if next_check is not None:
                    next_checks.append(next_check)
                entry_phase = str(item.get("phase") or "INITIAL").strip().upper()
                waiting_symbols.append(
                    {
                        "symbol": symbol,
                        "signal_time_local": self._format_utc_as_local(item.get("signal_time_utc")),
                        "observing_hour_local": self._format_utc_as_local(hour_open_raw),
                        "next_check_local": self._format_utc_as_local(next_check.isoformat()) if next_check else None,
                        "entry_phase": entry_phase,
                        "bullish_seen": bool(item.get("bullish_seen", False)),
                    }
                )

            message = str(run.get("message") or "").strip()
            persisted_opened = self._run_message_count(message, "opened", default=len(opened_symbols)) or 0
            position_count = len(opened_symbols)
            legacy_opened_count = max(position_count, persisted_opened)
            failed_count = self._run_message_count(message, "failed", default=0) or 0
            entry_failed_count = self._run_message_count(message, "entry_failed", default=None)
            exit_setup_failed_count = self._run_message_count(message, "exit_setup_failed", default=0) or 0
            if entry_failed_count is None:
                entry_failed_count = max(0, failed_count - exit_setup_failed_count)
            skipped_count = self._run_message_count(message, "skipped_existing", default=0) or 0
            entry_deferred_count = self._run_message_count(message, "entry_deferred", default=0) or 0
            scale_in_failed_count = self._run_message_count(message, "scale_in_failed", default=0) or 0
            scale_in_skipped_count = self._run_message_count(message, "scale_in_skipped", default=0) or 0
            waiting_count = len(waiting_symbols)
            # Exit-setup failures already had an entry fill and are included in opened_count.
            legacy_target_count = legacy_opened_count + waiting_count + entry_failed_count + skipped_count
            entry_actions = entry_actions_by_account.get(account_id, [])
            initial_actions = [
                action for action in entry_actions
                if str(action.get("entry_stage") or "INITIAL").upper() == "INITIAL"
            ]
            scale_in_actions = [
                action for action in entry_actions
                if str(action.get("entry_stage") or "").upper() == "SCALE_IN"
            ]
            scale_in_mode = str(
                self._run_message_value(message, "entry_scale_in_mode", default="")
                or wait_state.get("entry_scale_in_mode")
                or "none"
            ).strip().lower()
            action_data_available = bool(entry_actions)
            if action_data_available:
                pending_initial_count = sum(
                    1
                    for item in waiting_symbols
                    if str(item.get("entry_phase") or "INITIAL").upper()
                    in {"INITIAL", "POST_INITIAL_CANDLE"}
                )
                planned_initial_count = max(
                    len(initial_actions)
                    + pending_initial_count
                    + entry_failed_count
                    + skipped_count,
                    len(initial_actions),
                )
                has_scale_in_stage = bool(
                    scale_in_actions
                    or scale_in_failed_count
                    or scale_in_skipped_count
                    or scale_in_mode not in {"", "none", "off", "disabled"}
                )
                planned_action_count = planned_initial_count * (2 if has_scale_in_stage else 1)
                action_completed_count = len(entry_actions)
                action_failed_count = entry_failed_count + scale_in_failed_count
                action_skipped_count = skipped_count + scale_in_skipped_count
                action_target_count = max(
                    planned_action_count,
                    action_completed_count + action_failed_count + action_skipped_count,
                )
                opened_count = action_completed_count
                target_count = action_target_count
            else:
                action_completed_count = legacy_opened_count
                action_failed_count = entry_failed_count
                action_skipped_count = skipped_count
                action_target_count = legacy_target_count
                opened_count = legacy_opened_count
                target_count = legacy_target_count
            run_status = str(run.get("status") or "UNKNOWN").strip().upper()
            if waiting_count > 0:
                progress_status = "WAITING"
            elif run_status == "FAILED":
                progress_status = "FAILED"
            elif failed_count > 0:
                progress_status = "PARTIAL"
            elif run_status == "SUCCESS":
                progress_status = "COMPLETED"
            elif run_status == "SKIPPED":
                progress_status = "SKIPPED"
            else:
                progress_status = "RUNNING"

            deadline_raw = str(wait_state.get("deadline_utc") or "").strip()
            updated_raw = str(wait_state.get("updated_at_utc") or "").strip()
            event_time = updated_raw or run.get("completed_at_utc") or run.get("started_at_utc")
            started_at_local = self._format_utc_as_local(run.get("started_at_utc"))
            is_today = bool(
                started_at_local
                and started_at_local[:10] == current_cycle_date
            )
            progress_by_account[account_id] = {
                "run_id": run_id,
                "trade_day_utc": run.get("trade_day_utc"),
                "is_today": is_today,
                "status": progress_status,
                "target_count": target_count,
                "opened_count": opened_count,
                "position_count": position_count,
                "waiting_count": waiting_count,
                "failed_count": failed_count,
                "entry_failed_count": entry_failed_count,
                "exit_setup_failed_count": exit_setup_failed_count,
                "skipped_count": skipped_count,
                "opened_symbols": opened_symbols,
                "entry_actions": entry_actions,
                "entry_action_completed_count": action_completed_count,
                "entry_action_target_count": action_target_count,
                "entry_action_pending_count": max(
                    0,
                    action_target_count
                    - action_completed_count
                    - action_failed_count
                    - action_skipped_count,
                ),
                "entry_action_failed_count": action_failed_count,
                "entry_action_skipped_count": action_skipped_count,
                "entry_action_initial_count": len(initial_actions),
                "entry_action_scale_in_count": len(scale_in_actions),
                "entry_action_scale_in_mode": scale_in_mode,
                "waiting_symbols": waiting_symbols,
                "next_check_local": self._format_utc_as_local(min(next_checks).isoformat()) if next_checks else None,
                "deadline_local": self._format_utc_as_local(deadline_raw),
                "started_at_local": started_at_local,
                "updated_at_local": self._format_utc_as_local(event_time),
            }
        return progress_by_account

    @staticmethod
    def _entry_wait_states_from_db(
        conn: sqlite3.Connection,
        account_ids: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        lock_name_by_account = {
            aid: f"{aid}:bearish_hour_entry_wait_v1"
            for aid in account_ids
        }
        if not lock_name_by_account:
            return {}
        placeholders = ",".join("?" for _ in lock_name_by_account)
        rows = conn.execute(
            f"SELECT lock_name, holder FROM locks WHERE lock_name IN ({placeholders})",
            tuple(lock_name_by_account.values()),
        ).fetchall()
        account_by_lock_name = {name: aid for aid, name in lock_name_by_account.items()}
        states: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            account_id = account_by_lock_name.get(str(row["lock_name"] or ""))
            if not account_id:
                continue
            try:
                parsed = json.loads(str(row["holder"] or "{}"))
            except (TypeError, ValueError):
                continue
            if isinstance(parsed, dict):
                states[account_id] = parsed
        return states

    def _entry_actions_from_db(
        self,
        conn: sqlite3.Connection,
        run_ids: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Load unique initial and scale-in entry orders for the latest runs."""
        normalized_run_ids = [str(run_id).strip() for run_id in run_ids if str(run_id).strip()]
        if not normalized_run_ids:
            return {}
        placeholders = ",".join("?" for _ in normalized_run_ids)
        rows = self._query_rows(
            conn,
            f"""
            SELECT
                oe.id, r.account_id, p.run_id, oe.position_id, oe.symbol,
                oe.client_order_id, oe.event_time_utc, oe.raw_json
            FROM order_events oe
            INNER JOIN positions p ON p.id = oe.position_id
            INNER JOIN runs r ON r.run_id = p.run_id
            WHERE p.run_id IN ({placeholders})
              AND (
                    LOWER(COALESCE(oe.client_order_id, '')) LIKE 't10s-ent-%'
                 OR LOWER(COALESCE(oe.client_order_id, '')) LIKE 't10s-add-%'
              )
            ORDER BY oe.event_time_utc ASC, oe.id ASC
            """,
            tuple(normalized_run_ids),
        )
        actions_by_account: Dict[str, List[Dict[str, Any]]] = {}
        seen: Set[Tuple[str, str, str]] = set()
        for row in rows:
            account_id = str(row.get("account_id") or "").strip()
            run_id = str(row.get("run_id") or "").strip()
            symbol = str(row.get("symbol") or "").strip().upper()
            client_order_id = str(row.get("client_order_id") or "").strip()
            if not account_id or not run_id or not symbol:
                continue
            action_stage = "SCALE_IN" if client_order_id.lower().startswith("t10s-add-") else "INITIAL"
            action_key = (account_id, run_id, client_order_id or f"event-{row.get('id')}")
            if action_key in seen:
                continue
            seen.add(action_key)

            entry_audit: Dict[str, Any] = {}
            try:
                raw_payload = json.loads(str(row.get("raw_json") or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                raw_payload = {}
            if isinstance(raw_payload, dict) and isinstance(raw_payload.get("entry_audit"), dict):
                entry_audit = raw_payload["entry_audit"]
            independent_raw = entry_audit.get("signal_independent", False)
            independent = independent_raw is True or str(independent_raw).strip().lower() == "true"
            actions_by_account.setdefault(account_id, []).append(
                {
                    "action_id": client_order_id or f"order_event_{row.get('id')}",
                    "symbol": symbol,
                    "opened_at_local": self._format_utc_as_local(row.get("event_time_utc")),
                    "entry_stage": action_stage,
                    "signal_independent": independent,
                }
            )
        return actions_by_account

    def _entry_status_from_run_row(
        self,
        row: Dict[str, Any],
        wait_state: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        status = str(row.get("status") or "UNKNOWN").strip().upper()
        if status not in {"SUCCESS", "FAILED", "RUNNING", "SKIPPED"}:
            status = "UNKNOWN"
        message = str(row.get("message") or "").strip()
        position_count = self._safe_int(row.get("position_count"), 0)
        opened = self._run_message_count(message, "opened", default=position_count)
        failed = self._run_message_count(message, "failed", default=0)
        skipped = self._run_message_count(message, "skipped_existing", default=0)

        parts = [f"opened={opened}", f"failed={failed}", f"skipped={skipped}"]
        if status == "RUNNING":
            pending_count = 0
            if isinstance(wait_state, dict) and str(wait_state.get("run_id") or "") == str(row.get("run_id") or ""):
                pending = wait_state.get("pending")
                pending_count = len(pending) if isinstance(pending, dict) else 0
            if pending_count > 0:
                parts.append(f"waiting={pending_count}")
        elif message and self._run_message_count(message, "opened", default=None) is None:
            parts.append(f"reason={message[:80]}")

        event_time = row.get("started_at_utc") if status == "RUNNING" else (
            row.get("completed_at_utc") or row.get("started_at_utc")
        )
        return {
            "status": status,
            "time_local": self._format_utc_as_local(event_time),
            "summary": " ".join(parts),
        }

    @staticmethod
    def _run_message_count(message: str, key: str, default: Optional[int]) -> Optional[int]:
        matched = re.search(rf"(?:^|[,\s]){re.escape(key)}=(\d+)", message)
        if matched is None:
            return default
        return int(matched.group(1))

    @staticmethod
    def _run_message_value(message: str, key: str, default: Optional[str]) -> Optional[str]:
        matched = re.search(rf"(?:^|[,\s]){re.escape(key)}=([^,\s]+)", message)
        if matched is None:
            return default
        return matched.group(1)

    def _format_utc_as_local(self, value: Any) -> Optional[str]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(self.local_tz).strftime("%Y-%m-%d %H:%M:%S")

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

    def _load_portfolio_loss_cut_state(
        self,
        conn: sqlite3.Connection,
        *,
        account_id: Optional[str],
        current_equity_usdt: Any,
        open_positions: int,
    ) -> Optional[Dict[str, Any]]:
        """Return the strategy's persisted loss-cut state for one account.

        The curve is useful for presentation, but it is not authoritative for
        whether the manager has actually latched and completed a portfolio
        loss-cut. Keep those states tied to the same scoped lock used by the
        position manager.
        """
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id:
            return None
        try:
            row = conn.execute(
                """
                SELECT holder, updated_at_utc
                FROM locks
                WHERE lock_name = ?
                LIMIT 1
                """,
                (f"{normalized_account_id}:{PORTFOLIO_LOSS_CUT_LOCK_NAME}",),
            ).fetchone()
        except sqlite3.Error:
            return None
        if row is None or row["holder"] is None:
            return None
        try:
            state = json.loads(str(row["holder"]))
        except (TypeError, ValueError):
            return None
        if not isinstance(state, dict):
            return None

        baseline_equity = self._safe_float(state.get("baseline_equity_usdt"))
        threshold_equity = self._safe_float(state.get("threshold_equity_usdt"))
        current_equity = self._safe_float(current_equity_usdt)
        if current_equity is None:
            current_equity = self._safe_float(state.get("current_equity_usdt"))
        loss_pct = self._safe_float(state.get("loss_pct"))
        if (
            loss_pct is None
            and baseline_equity is not None
            and baseline_equity > 0
            and threshold_equity is not None
        ):
            loss_pct = (1.0 - threshold_equity / baseline_equity) * 100.0

        current_return_pct: Optional[float] = None
        distance_pct: Optional[float] = None
        threshold_reached = False
        if baseline_equity is not None and baseline_equity > 0 and current_equity is not None:
            current_return_pct = ((current_equity - baseline_equity) / baseline_equity) * 100.0
            if threshold_equity is not None:
                distance_pct = ((current_equity - threshold_equity) / baseline_equity) * 100.0
                threshold_eps = max(1e-9, abs(threshold_equity) * 1e-12)
                threshold_reached = current_equity + threshold_eps <= threshold_equity

        tracked_open_positions = max(0, int(open_positions or 0))
        triggered = bool(state.get("triggered"))
        reported_close_complete = bool(state.get("close_complete"))
        close_complete = triggered and reported_close_complete and tracked_open_positions == 0
        state_inconsistent = reported_close_complete and not close_complete
        if triggered:
            status = "TRIGGERED_COMPLETE" if close_complete else "CLOSING"
        elif threshold_reached:
            status = "THRESHOLD_REACHED"
        elif baseline_equity is None or threshold_equity is None:
            status = "UNAVAILABLE"
        else:
            status = "MONITORING"

        def rounded(value: Optional[float]) -> Optional[float]:
            return round(value, 8) if value is not None else None

        return {
            "status": status,
            "source": "BACKEND_LOCK",
            "cycle_date": state.get("cycle_date"),
            "baseline_equity": rounded(baseline_equity),
            "baseline_captured_at_utc": state.get("baseline_captured_at_utc"),
            "current_equity": rounded(current_equity),
            "threshold_equity": rounded(threshold_equity),
            "loss_pct": rounded(loss_pct),
            "current_return_pct": rounded(current_return_pct),
            "distance_pct": rounded(distance_pct),
            "threshold_reached": threshold_reached,
            "triggered": triggered,
            "reported_close_complete": reported_close_complete,
            "close_complete": close_complete,
            "state_inconsistent": state_inconsistent,
            "triggered_at_utc": state.get("triggered_at_utc"),
            "updated_at_utc": state.get("updated_at_utc") or row["updated_at_utc"],
            "open_positions": tracked_open_positions,
        }

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
            candidate_order_ids: List[int] = []

            for order_id_raw in (
                row.get("close_order_id"),
                row.get("tp_order_id"),
                row.get("sl_order_id"),
            ):
                try:
                    parsed_order_id = int(order_id_raw) if order_id_raw is not None else None
                except (TypeError, ValueError):
                    parsed_order_id = None
                if parsed_order_id and parsed_order_id > 0 and parsed_order_id not in candidate_order_ids:
                    candidate_order_ids.append(parsed_order_id)

            if symbol and candidate_order_ids:
                for order_id in candidate_order_ids:
                    cache_key = (symbol, order_id)
                    if cache_key in self._close_price_cache:
                        cached = self._close_price_cache[cache_key]
                        if cached is not None:
                            return cached
                        continue

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
        account_id: str = "default",
        source: str = "API",
        error: Optional[str] = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO wallet_snapshots (account_id, captured_at_utc, balance_usdt, source, error, created_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                captured_at_utc,
                float(balance_usdt),
                source[:24],
                (error or "")[:1000] or None,
                datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            ),
        )

    def _get_latest_wallet_snapshot(
        self,
        conn: sqlite3.Connection,
        account_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        try:
            if account_id:
                row = conn.execute(
                    """
                    SELECT id, account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                    FROM wallet_snapshots
                    WHERE account_id = ? AND error IS NULL
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (account_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT id, account_id, captured_at_utc, balance_usdt, source, error, created_at_utc
                    FROM wallet_snapshots
                    WHERE error IS NULL
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

    def _resample_curve(
        self,
        curve: List[Dict[str, Any]],
        max_points: int,
    ) -> List[Dict[str, Any]]:
        target = max(2, int(max_points))
        total_points = len(curve)
        if total_points <= target:
            return [dict(point) for point in curve]
        if target == 2:
            return [dict(curve[0]), dict(curve[-1])]

        interior_count = total_points - 2
        # Bucket by time and keep first/min/max/last in each bucket to preserve shape.
        bucket_count = max(1, target // 4)
        selected: List[int] = [0]
        for bucket in range(bucket_count):
            start = 1 + int(bucket * interior_count / bucket_count)
            end = 1 + int((bucket + 1) * interior_count / bucket_count)
            if end <= start:
                continue
            indices = list(range(start, end))
            min_idx = min(indices, key=lambda idx: self._safe_float(curve[idx].get("equity")) or 0.0)
            max_idx = max(indices, key=lambda idx: self._safe_float(curve[idx].get("equity")) or 0.0)
            selected.extend(sorted({indices[0], min_idx, max_idx, indices[-1]}))
        selected.append(total_points - 1)
        selected = sorted(set(selected))
        if len(selected) <= target:
            return [dict(curve[idx]) for idx in selected]

        interior = selected[1:-1]
        keep_interior = max(0, target - 2)
        sampled_interior: List[int] = []
        if keep_interior > 0 and interior:
            if keep_interior >= len(interior):
                sampled_interior = interior
            elif keep_interior == 1:
                sampled_interior = [interior[len(interior) // 2]]
            else:
                step = (len(interior) - 1) / float(keep_interior - 1)
                used = set()
                for i in range(keep_interior):
                    pick = interior[int(round(i * step))]
                    if pick in used:
                        continue
                    sampled_interior.append(pick)
                    used.add(pick)
                if len(sampled_interior) < keep_interior:
                    for pick in interior:
                        if pick in used:
                            continue
                        sampled_interior.append(pick)
                        used.add(pick)
                        if len(sampled_interior) >= keep_interior:
                            break
                sampled_interior = sorted(sampled_interior[:keep_interior])

        final_indices = [0] + sampled_interior + [total_points - 1]
        return [dict(curve[idx]) for idx in final_indices]

    def _query_wallet_rows(
        self,
        conn: sqlite3.Connection,
        window_start_utc: Optional[str],
        account_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: List[Any] = []
        where_sql = "WHERE error IS NULL"
        if account_id:
            where_sql += " AND account_id = ?"
            params.append(account_id)
        if window_start_utc:
            where_sql += " AND captured_at_utc >= ?"
            params.append(window_start_utc)
        return self._query_rows(
            conn,
            f"""
            SELECT id, captured_at_utc, balance_usdt
            FROM wallet_snapshots
            {where_sql}
            ORDER BY captured_at_utc ASC, id ASC
            """,
            tuple(params),
        )

    def _load_all_time_account_pnl(
        self,
        conn: sqlite3.Connection,
        account_id: Optional[str] = None,
    ) -> Dict[str, float]:
        params: List[Any] = []
        where_sql = "WHERE error IS NULL"
        if account_id:
            where_sql += " AND account_id = ?"
            params.append(account_id)
        first_row = conn.execute(
            f"""
            SELECT captured_at_utc, balance_usdt
            FROM wallet_snapshots
            {where_sql}
            ORDER BY captured_at_utc ASC, id ASC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        latest_row = conn.execute(
            f"""
            SELECT captured_at_utc, balance_usdt
            FROM wallet_snapshots
            {where_sql}
            ORDER BY captured_at_utc DESC, id DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        if first_row is None or latest_row is None:
            return {
                "all_time_account_pnl": 0.0,
                "all_time_account_cashflow_usdt": 0.0,
                "all_time_account_baseline_usdt": 0.0,
            }

        baseline = float(first_row["balance_usdt"])
        latest = float(latest_row["balance_usdt"])
        cashflow_params: List[Any] = [str(first_row["captured_at_utc"]), str(latest_row["captured_at_utc"])]
        cashflow_where = "WHERE asset = 'USDT' AND event_time_utc >= ? AND event_time_utc <= ?"
        if account_id:
            cashflow_where += " AND account_id = ?"
            cashflow_params.append(account_id)
        cashflow_row = conn.execute(
            f"""
            SELECT COALESCE(SUM(amount), 0) AS amount
            FROM (
                SELECT MAX(amount) AS amount
                FROM cashflow_events
                {cashflow_where}
                GROUP BY account_id, COALESCE(NULLIF(tran_id, ''), unique_key)
            )
            """,
            tuple(cashflow_params),
        ).fetchone()
        cashflow = float(cashflow_row["amount"] if cashflow_row is not None else 0.0)
        return {
            "all_time_account_pnl": round(latest - baseline - cashflow, 8),
            "all_time_account_cashflow_usdt": round(cashflow, 8),
            "all_time_account_baseline_usdt": round(baseline, 8),
        }

    def _build_balance_curve(
        self,
        conn: sqlite3.Connection,
        now_utc: datetime,
        wallet_balance_usdt: Optional[float],
        window_start_utc: Optional[str],
        max_points: int,
        account_id: Optional[str] = None,
        wallet_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        if wallet_rows is None:
            wallet_rows = self._query_wallet_rows(
                conn=conn,
                window_start_utc=window_start_utc,
                account_id=account_id,
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

        curve = self._resample_curve(curve, max_points)
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
            "gross_profit": 0.0,
            "gross_loss_abs": 0.0,
            "avg_win": 0.0,
            "avg_loss_abs": 0.0,
            "profit_factor": None,
            "avg_win_loss_ratio": None,
            "max_drawdown": dd["max_drawdown"],
            "max_drawdown_pct": dd["max_drawdown_pct"],
            "current_drawdown": dd["current_drawdown"],
            "current_drawdown_pct": dd["current_drawdown_pct"],
            "unpriced_closed_positions": 0,
            "equity_baseline": round((self._safe_float(curve[0].get("equity")) if curve else 0.0) or 0.0, 8),
        }
        stats.update(self._load_all_time_account_pnl(conn, account_id=account_id))
        return curve, stats

    def _load_trade_outcome_stats(
        self,
        conn: sqlite3.Connection,
        now_utc: datetime,
        account_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: List[Any] = []
        where_sql = "WHERE f.realized_pnl IS NOT NULL"
        if account_id:
            where_sql += " AND r.account_id = ?"
            params.append(account_id)
        rows = self._query_rows(
            conn,
            f"""
            SELECT
                f.id, f.realized_pnl, f.commission, f.commission_asset
            FROM fills f
            JOIN positions p ON p.id = f.position_id
            JOIN runs r ON r.run_id = p.run_id
            {where_sql}
            ORDER BY f.event_time_utc ASC, f.id ASC
            """,
            tuple(params),
        )

        cumulative_trade_pnl = 0.0
        trading_fees_usdt = 0.0
        wins = 0
        losses = 0
        breakeven = 0
        gross_profit = 0.0
        gross_loss_abs = 0.0

        for row in rows:
            pnl = self._safe_float(row.get("realized_pnl")) or 0.0
            cumulative_trade_pnl += pnl
            commission_asset = str(row.get("commission_asset") or "").upper()
            commission = self._safe_float(row.get("commission")) or 0.0
            if commission_asset == "USDT":
                trading_fees_usdt += commission
            if pnl > 0:
                wins += 1
                gross_profit += pnl
            elif pnl < 0:
                losses += 1
                gross_loss_abs += abs(pnl)
            else:
                breakeven += 1

        realized_fill_count = wins + losses + breakeven
        win_rate_pct = (wins / realized_fill_count * 100.0) if realized_fill_count > 0 else 0.0
        avg_win = (gross_profit / wins) if wins > 0 else 0.0
        avg_loss_abs = (gross_loss_abs / losses) if losses > 0 else 0.0
        profit_factor = (gross_profit / gross_loss_abs) if gross_loss_abs > 0 else None
        avg_win_loss_ratio = (avg_win / avg_loss_abs) if (avg_loss_abs > 0 and avg_win > 0) else None
        missing_realized_params: List[Any] = []
        missing_realized_where = "WHERE p.status NOT IN ('OPEN', 'PENDING_EXIT_SETUP')"
        if account_id:
            missing_realized_where += " AND r.account_id = ?"
            missing_realized_params.append(account_id)
        missing_realized_row = conn.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM positions p
            JOIN runs r ON r.run_id = p.run_id
            {missing_realized_where}
              AND NOT EXISTS (
                  SELECT 1
                  FROM fills f
                  WHERE f.position_id = p.id
                    AND f.realized_pnl IS NOT NULL
              )
            """,
            tuple(missing_realized_params),
        ).fetchone()
        missing_realized_positions = int(missing_realized_row["count"] if missing_realized_row else 0)
        return {
            "closed_trades_priced": realized_fill_count,
            "realized_fill_count": realized_fill_count,
            "wins": wins,
            "losses": losses,
            "breakeven": breakeven,
            "win_rate_pct": round(win_rate_pct, 2),
            "gross_profit": round(gross_profit, 8),
            "gross_loss_abs": round(gross_loss_abs, 8),
            "avg_win": round(avg_win, 8),
            "avg_loss_abs": round(avg_loss_abs, 8),
            "profit_factor": round(profit_factor, 6) if profit_factor is not None else None,
            "avg_win_loss_ratio": round(avg_win_loss_ratio, 6) if avg_win_loss_ratio is not None else None,
            "trading_fees_usdt": round(trading_fees_usdt, 8),
            "net_trade_pnl": round(cumulative_trade_pnl - trading_fees_usdt, 8),
            "unpriced_closed_positions": missing_realized_positions,
            "trade_realized_pnl": round(cumulative_trade_pnl, 8),
            "as_of_utc": now_utc.replace(microsecond=0).isoformat(),
        }

    def _list_unpriced_closed_positions(
        self,
        conn: sqlite3.Connection,
        limit: int = 80,
        account_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params: List[Any] = []
        where_sql = "WHERE p.status NOT IN ('OPEN', 'PENDING_EXIT_SETUP')"
        if account_id:
            where_sql += " AND r.account_id = ?"
            params.append(account_id)
        params.append(max(1, int(limit)))
        rows = self._query_rows(
            conn,
            f"""
            SELECT
                p.id, p.symbol, p.side, p.qty, p.entry_price,
                p.tp_price, p.sl_price,
                p.tp_order_id, p.sl_order_id,
                p.status, p.close_reason, p.close_order_id,
                p.closed_at_utc, p.updated_at_utc,
                oe.event_time_utc AS close_event_time_utc,
                oe.price AS close_event_price,
                oe.qty AS close_event_qty,
                oe.raw_json AS close_raw_json
            FROM positions p
            LEFT JOIN runs r ON r.run_id = p.run_id
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
            {where_sql}
              AND NOT EXISTS (
                  SELECT 1
                  FROM fills f
                  WHERE f.position_id = p.id
                    AND f.realized_pnl IS NOT NULL
              )
            ORDER BY COALESCE(p.closed_at_utc, oe.event_time_utc, p.updated_at_utc) DESC, p.id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        items: List[Dict[str, Any]] = []
        for row in rows:
            side = str(row.get("side") or "").upper()
            if side and side != "SHORT":
                continue
            items.append(
                {
                    "id": row.get("id"),
                    "symbol": row.get("symbol"),
                    "status": row.get("status"),
                    "close_reason": row.get("close_reason"),
                    "close_order_id": row.get("close_order_id"),
                    "detected_reason": "MISSING_EXCHANGE_REALIZED_PNL",
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
        account_id: Optional[str] = None,
        include_trade_stats: bool = True,
        wallet_rows: Optional[List[Dict[str, Any]]] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        if wallet_rows is None:
            wallet_rows = self._query_wallet_rows(
                conn=conn,
                window_start_utc=window_start_utc,
                account_id=account_id,
            )
        cashflow_rows: List[Dict[str, Any]] = []
        params: List[Any] = []
        where_sql = "WHERE asset = 'USDT'"
        if account_id:
            where_sql += " AND account_id = ?"
            params.append(account_id)
        try:
            cashflow_rows = self._query_rows(
                conn,
                f"""
                SELECT MIN(id) AS id, MAX(event_time_utc) AS event_time_utc, MAX(amount) AS amount
                FROM cashflow_events
                {where_sql}
                GROUP BY account_id, COALESCE(NULLIF(tran_id, ''), unique_key)
                ORDER BY event_time_utc ASC, id ASC
                LIMIT 5000
                """,
                tuple(params),
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

        curve = self._resample_curve(curve, max_points)
        dd = self._apply_drawdown(curve)
        trade_stats = {
            "closed_trades_priced": 0,
            "realized_fill_count": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "win_rate_pct": 0.0,
            "gross_profit": 0.0,
            "gross_loss_abs": 0.0,
            "avg_win": 0.0,
            "avg_loss_abs": 0.0,
            "profit_factor": None,
            "avg_win_loss_ratio": None,
            "trade_realized_pnl": 0.0,
            "trading_fees_usdt": 0.0,
            "net_trade_pnl": 0.0,
            "unpriced_closed_positions": 0,
        }
        if include_trade_stats:
            trade_stats = self._load_trade_outcome_stats(conn, now_utc, account_id=account_id)
        stats = {
            "wallet_balance_usdt": round(wallet_balance_usdt, 8) if wallet_balance_usdt is not None else None,
            "total_realized_pnl": round(float(curve[-1]["cum_pnl"]), 8),
            "closed_trades_priced": trade_stats["closed_trades_priced"],
            "wins": trade_stats["wins"],
            "losses": trade_stats["losses"],
            "breakeven": trade_stats["breakeven"],
            "win_rate_pct": trade_stats["win_rate_pct"],
            "gross_profit": trade_stats["gross_profit"],
            "gross_loss_abs": trade_stats["gross_loss_abs"],
            "avg_win": trade_stats["avg_win"],
            "avg_loss_abs": trade_stats["avg_loss_abs"],
            "profit_factor": trade_stats["profit_factor"],
            "avg_win_loss_ratio": trade_stats["avg_win_loss_ratio"],
            "realized_fill_count": trade_stats["realized_fill_count"],
            "max_drawdown": dd["max_drawdown"],
            "max_drawdown_pct": dd["max_drawdown_pct"],
            "current_drawdown": dd["current_drawdown"],
            "current_drawdown_pct": dd["current_drawdown_pct"],
            "unpriced_closed_positions": trade_stats["unpriced_closed_positions"],
            "equity_baseline": round((baseline_equity or 0.0), 8),
            "net_cashflow_usdt": round(float(curve[-1].get("cum_cashflow") or 0.0), 8),
            "trade_realized_pnl": trade_stats["trade_realized_pnl"],
            "trading_fees_usdt": trade_stats["trading_fees_usdt"],
            "net_trade_pnl": trade_stats["net_trade_pnl"],
        }
        stats.update(self._load_all_time_account_pnl(conn, account_id=account_id))
        return curve, stats

    def snapshot(
        self,
        log_lines: int = 80,
        window_hours: Optional[float] = None,
        curve_points: Optional[int] = None,
        account_id: Optional[str] = None,
        include_details: bool = True,
        include_log: bool = True,
        include_curves: bool = True,
        include_balance_curve: bool = True,
        include_trade_stats: bool = True,
    ) -> Dict[str, Any]:
        now_utc = datetime.now(timezone.utc)
        now_local = now_utc.astimezone(self.local_tz)
        next_entry = self._next_entry_local(now_local)
        scoped_account = (account_id or "").strip() or None
        live_wallet = (
            self._read_wallet_balance(now_utc)
            if scoped_account is None
            else {"balance_usdt": None, "as_of_utc": None, "source": "ACCOUNT_SCOPED", "error": None}
        )
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
            "account_id": scoped_account,
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
            "portfolio_loss_cut": None,
            "latest_run": None,
            "runs": [],
            "open_positions": [],
            "events": [],
            "cashflow_events": [],
            "unpriced_closed_details": [],
            "trade_outcome_stats": None,
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
                "gross_profit": 0.0,
                "gross_loss_abs": 0.0,
                "avg_win": 0.0,
                "avg_loss_abs": 0.0,
                "profit_factor": None,
                "avg_win_loss_ratio": None,
                "realized_fill_count": 0,
                "trading_fees_usdt": 0.0,
                "net_trade_pnl": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "current_drawdown": 0.0,
                "current_drawdown_pct": 0.0,
                "unpriced_closed_positions": 0,
                "equity_baseline": 0.0,
                "all_time_account_pnl": 0.0,
                "all_time_account_cashflow_usdt": 0.0,
                "all_time_account_baseline_usdt": 0.0,
            },
            "drawdown_stats_balance": {
                "wallet_balance_usdt": live_wallet["balance_usdt"],
                "total_realized_pnl": 0.0,
                "closed_trades_priced": 0,
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "win_rate_pct": 0.0,
                "gross_profit": 0.0,
                "gross_loss_abs": 0.0,
                "avg_win": 0.0,
                "avg_loss_abs": 0.0,
                "profit_factor": None,
                "avg_win_loss_ratio": None,
                "realized_fill_count": 0,
                "trading_fees_usdt": 0.0,
                "net_trade_pnl": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "current_drawdown": 0.0,
                "current_drawdown_pct": 0.0,
                "unpriced_closed_positions": 0,
                "equity_baseline": live_wallet["balance_usdt"] if live_wallet["balance_usdt"] is not None else 0.0,
                "all_time_account_pnl": 0.0,
                "all_time_account_cashflow_usdt": 0.0,
                "all_time_account_baseline_usdt": 0.0,
            },
            "drawdown_stats": {
                "wallet_balance_usdt": live_wallet["balance_usdt"],
                "total_realized_pnl": 0.0,
                "closed_trades_priced": 0,
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "win_rate_pct": 0.0,
                "gross_profit": 0.0,
                "gross_loss_abs": 0.0,
                "avg_win": 0.0,
                "avg_loss_abs": 0.0,
                "profit_factor": None,
                "avg_win_loss_ratio": None,
                "realized_fill_count": 0,
                "trading_fees_usdt": 0.0,
                "net_trade_pnl": 0.0,
                "max_drawdown": 0.0,
                "max_drawdown_pct": 0.0,
                "current_drawdown": 0.0,
                "current_drawdown_pct": 0.0,
                "unpriced_closed_positions": 0,
                "equity_baseline": live_wallet["balance_usdt"] if live_wallet["balance_usdt"] is not None else 0.0,
                "all_time_account_pnl": 0.0,
                "all_time_account_cashflow_usdt": 0.0,
                "all_time_account_baseline_usdt": 0.0,
            },
            "log_tail": self._tail_log(lines=log_lines) if include_log else [],
        }

        readonly_live_positions: List[Dict[str, Any]] = []
        if self._is_readonly_account(scoped_account):
            readonly_live_positions = self._readonly_live_positions(scoped_account)
            data["live_position_source"] = "LOCAL_ACCOUNT_STATE"
            data["live_position_error"] = self._readonly_position_cache_errors.get(scoped_account)
            data["summary"]["open_positions"] = len(readonly_live_positions)
            data["summary"]["open_symbols"] = len(
                {str(row.get("symbol") or "").strip() for row in readonly_live_positions if row.get("symbol")}
            )
            if data["live_position_error"]:
                data["summary"]["recent_errors"] = 1
            if include_details:
                data["open_positions"] = [dict(row) for row in readonly_live_positions]

        if not os.path.exists(self.db_path):
            return data

        try:
            with self._connect_ctx() as conn:
                if live_wallet.get("source") == "API" and self._safe_float(live_wallet.get("balance_usdt")) is not None:
                    try:
                        self._insert_wallet_snapshot(
                            conn=conn,
                            captured_at_utc=str(live_wallet.get("as_of_utc") or now_utc.replace(microsecond=0).isoformat()),
                            balance_usdt=float(live_wallet["balance_usdt"]),
                            account_id=self.live_wallet_account_id,
                            source="API",
                            error=None,
                        )
                    except sqlite3.Error as exc:
                        LOGGER.warning("Failed to persist wallet snapshot: %s", exc)

                latest_wallet_row = self._get_latest_wallet_snapshot(conn, account_id=scoped_account)
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
                    (
                        """
                        SELECT run_id, account_id, trade_day_utc, started_at_utc, completed_at_utc, status, message
                        FROM runs
                        WHERE account_id = ?
                        ORDER BY started_at_utc DESC
                        LIMIT 1
                        """
                        if scoped_account
                        else """
                        SELECT run_id, account_id, trade_day_utc, started_at_utc, completed_at_utc, status, message
                        FROM runs
                        ORDER BY started_at_utc DESC
                        LIMIT 1
                        """
                    ),
                    ((scoped_account,) if scoped_account else ()),
                ).fetchone()
                if latest_run is not None:
                    data["latest_run"] = dict(latest_run)
                    data["summary"]["last_run_status"] = latest_run["status"]

                summary_row = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN p.status = 'OPEN' THEN 1 ELSE 0 END) AS open_positions,
                        COUNT(DISTINCT CASE WHEN p.status = 'OPEN' THEN p.symbol END) AS open_symbols,
                        SUM(CASE WHEN p.status = 'OPEN' AND p.last_error IS NOT NULL AND TRIM(p.last_error) != '' THEN 1 ELSE 0 END) AS recent_errors
                    FROM positions p
                    LEFT JOIN runs r ON r.run_id = p.run_id
                    WHERE (? IS NULL OR r.account_id = ?)
                    """,
                    (scoped_account, scoped_account),
                ).fetchone()
                if summary_row is not None:
                    data["summary"]["open_positions"] = int(summary_row["open_positions"] or 0)
                    data["summary"]["open_symbols"] = int(summary_row["open_symbols"] or 0)
                    data["summary"]["recent_errors"] = int(summary_row["recent_errors"] or 0)
                if self._is_readonly_account(scoped_account):
                    data["summary"]["open_positions"] = len(readonly_live_positions)
                    data["summary"]["open_symbols"] = len(
                        {
                            str(row.get("symbol") or "").strip()
                            for row in readonly_live_positions
                            if row.get("symbol")
                        }
                    )
                    if data.get("live_position_error"):
                        data["summary"]["recent_errors"] = max(data["summary"]["recent_errors"], 1)

                if scoped_account and not self._is_readonly_account(scoped_account):
                    data["portfolio_loss_cut"] = self._load_portfolio_loss_cut_state(
                        conn,
                        account_id=scoped_account,
                        current_equity_usdt=data["wallet"].get("balance_usdt"),
                        open_positions=data["summary"]["open_positions"],
                    )

                if include_details:
                    data["runs"] = self._query_rows(
                        conn,
                        (
                            """
                            SELECT run_id, account_id, trade_day_utc, started_at_utc, completed_at_utc, status, message
                            FROM runs
                            WHERE account_id = ?
                            ORDER BY started_at_utc DESC
                            LIMIT 30
                            """
                            if scoped_account
                            else """
                            SELECT run_id, account_id, trade_day_utc, started_at_utc, completed_at_utc, status, message
                            FROM runs
                            ORDER BY started_at_utc DESC
                            LIMIT 30
                            """
                        ),
                        ((scoped_account,) if scoped_account else ()),
                    )

                    if self._is_readonly_account(scoped_account):
                        data["open_positions"] = [dict(row) for row in readonly_live_positions]
                    else:
                        data["open_positions"] = self._query_rows(
                            conn,
                            """
                            SELECT p.id, p.run_id, r.account_id AS _live_account_id,
                                   p.symbol, p.side, p.qty, p.entry_price,
                                   p.liq_price_latest, p.tp_price, p.sl_price,
                                   p.tp_order_id AS _tp_order_id,
                                   p.sl_order_id AS _sl_order_id,
                                   p.tp_client_order_id AS _tp_client_order_id,
                                   p.sl_client_order_id AS _sl_client_order_id,
                                   p.opened_at_utc, p.expire_at_utc, p.status, p.last_error
                            FROM positions p
                            LEFT JOIN runs r ON r.run_id = p.run_id
                            WHERE p.status = 'OPEN'
                              AND (? IS NULL OR r.account_id = ?)
                            ORDER BY p.opened_at_utc DESC
                            LIMIT 100
                            """,
                            (scoped_account, scoped_account),
                        )
                    if scoped_account and not self._is_readonly_account(scoped_account):
                        self._enrich_open_positions_with_live_data(
                            data["open_positions"],
                            account_id=scoped_account,
                        )
                    elif not scoped_account:
                        positions_by_account: Dict[str, List[Dict[str, Any]]] = {}
                        for position in data["open_positions"]:
                            position_account_id = str(
                                position.get("_live_account_id") or self.live_wallet_account_id or ""
                            ).strip()
                            if position_account_id:
                                positions_by_account.setdefault(position_account_id, []).append(position)
                        for position_account_id, positions in positions_by_account.items():
                            self._enrich_open_positions_with_live_data(
                                positions,
                                account_id=position_account_id,
                            )
                    for position in data["open_positions"]:
                        self._ensure_position_notional(position)
                        position.pop("_live_account_id", None)
                        position.pop("_tp_order_id", None)
                        position.pop("_sl_order_id", None)
                        position.pop("_tp_client_order_id", None)
                        position.pop("_sl_client_order_id", None)

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
                        LEFT JOIN runs r ON r.run_id = p.run_id
                        WHERE (? IS NULL OR r.account_id = ?)
                        ORDER BY oe.id DESC
                        LIMIT 120
                        """,
                        (scoped_account, scoped_account),
                    )
                    data["cashflow_events"] = self._query_rows(
                        conn,
                        """
                        SELECT MIN(id) AS id,
                               MAX(event_time_utc) AS event_time_utc,
                               asset,
                               MAX(amount) AS amount,
                               income_type,
                               symbol,
                               tran_id,
                               info
                        FROM cashflow_events
                        WHERE (? IS NULL OR account_id = ?)
                        GROUP BY account_id, COALESCE(NULLIF(tran_id, ''), unique_key)
                        ORDER BY event_time_utc DESC, id DESC
                        LIMIT 80
                        """,
                        (scoped_account, scoped_account),
                    )
                    data["unpriced_closed_details"] = self._list_unpriced_closed_positions(
                        conn,
                        limit=120,
                        account_id=scoped_account,
                    )

                if include_trade_stats and not include_curves:
                    data["trade_outcome_stats"] = self._load_trade_outcome_stats(
                        conn,
                        now_utc,
                        account_id=scoped_account,
                    )

                if include_curves:
                    wallet_rows = self._query_wallet_rows(
                        conn=conn,
                        window_start_utc=window_start_utc,
                        account_id=scoped_account,
                    )
                    strategy_curve, strategy_stats = self._build_strategy_equity_curve(
                        conn=conn,
                        now_utc=now_utc,
                        wallet_balance_usdt=self._safe_float(data["wallet"].get("balance_usdt")),
                        window_start_utc=window_start_utc,
                        max_points=points_limit,
                        account_id=scoped_account,
                        include_trade_stats=include_trade_stats,
                        wallet_rows=wallet_rows,
                    )
                    data["strategy_equity_curve"] = strategy_curve[-points_limit:]
                    data["drawdown_stats_strategy"] = strategy_stats
                    data["summary"]["net_cashflow_usdt"] = strategy_stats.get("net_cashflow_usdt", 0.0)
                    data["equity_curve"] = data["strategy_equity_curve"]
                    if include_balance_curve:
                        balance_curve, balance_stats = self._build_balance_curve(
                            conn=conn,
                            now_utc=now_utc,
                            wallet_balance_usdt=self._safe_float(data["wallet"].get("balance_usdt")),
                            window_start_utc=window_start_utc,
                            max_points=points_limit,
                            account_id=scoped_account,
                            wallet_rows=wallet_rows,
                        )
                        data["balance_curve"] = balance_curve[-points_limit:]
                        data["drawdown_stats_balance"] = balance_stats
                        data["drawdown_stats"] = data["drawdown_stats_balance"]
                    else:
                        data["balance_curve"] = []
                        data["drawdown_stats_balance"] = dict(strategy_stats)
                        data["drawdown_stats"] = data["drawdown_stats_strategy"]
        except sqlite3.Error as exc:
            data["summary"]["last_run_status"] = "DB_ERROR"
            data["db_error"] = str(exc)

        return data

    def accounts_equity_comparison(
        self,
        window_hours: Optional[float] = 168.0,
        curve_points: int = 600,
    ) -> Dict[str, Any]:
        """Return normalized wallet-equity curves starting at portfolio TP launch."""
        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
        points_limit = max(100, min(5000, int(curve_points)))
        launch_start_utc = datetime.fromisoformat(PORTFOLIO_TAKE_PROFIT_LAUNCH_AT_UTC)
        parsed_window_hours: Optional[float] = None
        if window_hours is not None:
            try:
                parsed_window_hours = min(24.0 * 366.0, max(0.001, float(window_hours)))
            except (TypeError, ValueError):
                parsed_window_hours = 168.0
        requested_start_utc = (
            now_utc - timedelta(hours=parsed_window_hours)
            if parsed_window_hours is not None
            else launch_start_utc
        )
        effective_start_utc = max(launch_start_utc, requested_start_utc)
        window_start_utc = effective_start_utc.isoformat()

        payload: Dict[str, Any] = {
            "generated_at_utc": now_utc.isoformat(),
            "timezone": str(getattr(self.local_tz, "key", self.local_tz)),
            "window_hours": parsed_window_hours,
            "curve_points": points_limit,
            "comparison_start_at_utc": PORTFOLIO_TAKE_PROFIT_LAUNCH_AT_UTC,
            "baseline": "first_valid_snapshot_at_or_after_portfolio_take_profit_launch",
            "series": [],
            "events": [],
        }
        if not os.path.exists(self.db_path):
            return payload

        try:
            with self._connect_ctx() as conn:
                if self.equity_comparison_account_ids is not None:
                    configured_ids = self.equity_comparison_account_ids
                    if self.overview_account_ids is not None:
                        configured_ids = configured_ids & self.overview_account_ids
                    account_ids = sorted(configured_ids)
                elif self.overview_account_ids is not None:
                    account_ids = sorted(
                        account_id
                        for account_id in self.overview_account_ids
                        if self.account_modes.get(account_id, "full") != "readonly"
                    )
                else:
                    account_rows = self._query_rows(
                        conn,
                        """
                        SELECT DISTINCT account_id
                        FROM wallet_snapshots
                        WHERE error IS NULL
                        ORDER BY account_id ASC
                        """,
                    )
                    account_ids = sorted(
                        {
                            str(row.get("account_id") or "").strip()
                            for row in account_rows
                            if str(row.get("account_id") or "").strip()
                            and self.account_modes.get(str(row.get("account_id") or "").strip(), "full")
                            != "readonly"
                        }
                    )

                for account_id in account_ids:
                    wallet_rows = self._query_wallet_rows(
                        conn=conn,
                        window_start_utc=window_start_utc,
                        account_id=account_id,
                    )
                    raw_curve: List[Dict[str, Any]] = []
                    for row in wallet_rows:
                        balance = self._safe_float(row.get("balance_usdt"))
                        captured_at = str(row.get("captured_at_utc") or "").strip()
                        if balance is None or not captured_at:
                            continue
                        raw_curve.append(
                            {
                                "t": captured_at,
                                "equity": round(balance, 8),
                            }
                        )

                    curve = self._resample_curve(raw_curve, points_limit)
                    baseline = self._safe_float(curve[0].get("equity")) if curve else None
                    if baseline is None or baseline <= 0:
                        payload["series"].append(
                            {
                                "account_id": account_id,
                                "status": "NO_DATA",
                                "points": [],
                                "baseline_equity": None,
                                "baseline_at_utc": None,
                                "latest_equity": None,
                                "latest_at_utc": None,
                                "change_pct": None,
                                "min_change_pct": None,
                                "max_change_pct": None,
                            }
                        )
                        continue

                    points: List[Dict[str, Any]] = []
                    for point in curve:
                        equity = self._safe_float(point.get("equity")) or 0.0
                        points.append(
                            {
                                "t": point.get("t"),
                                "equity": round(equity, 8),
                                "relative_pct": round((equity / baseline - 1.0) * 100.0, 6),
                            }
                        )

                    latest = points[-1]
                    relative_values = [float(point["relative_pct"]) for point in points]
                    payload["series"].append(
                        {
                            "account_id": account_id,
                            "status": "OK",
                            "points": points,
                            "baseline_equity": round(baseline, 8),
                            "baseline_at_utc": points[0].get("t"),
                            "latest_equity": latest.get("equity"),
                            "latest_at_utc": latest.get("t"),
                            "change_pct": latest.get("relative_pct"),
                            "min_change_pct": round(min(relative_values), 6),
                            "max_change_pct": round(max(relative_values), 6),
                        }
                    )
        except sqlite3.Error as exc:
            payload["db_error"] = str(exc)
            return payload

        comparison_account_ids = set(account_ids)
        payload["events"] = [
            event
            for event in self._portfolio_take_profit_comparison_events(
                window_start_utc=window_start_utc,
                end_utc=now_utc.isoformat(),
            )
            if str(event.get("account_id") or "").strip() in comparison_account_ids
        ]
        return payload

    def _portfolio_take_profit_comparison_events(
        self,
        window_start_utc: Optional[str],
        end_utc: str,
    ) -> List[Dict[str, Any]]:
        """Extract compact portfolio take-profit lifecycle events from strategy logs.

        The runtime checks an already-triggered plan once per minute.  Showing
        every ``TRIGGERED_RETRY`` warning as a separate dashboard event makes a
        single pending exit look like a stream of new triggers, so retries are
        coalesced by account and portfolio cycle here.  A completed retry is
        emitted separately to make the waiting-to-complete transition visible.
        """
        raw_events: List[Dict[str, Any]] = []
        seen: Set[Tuple[str, str]] = set()
        for path in self._task_log_files():
            for line in self._read_task_log_lines(path, ("service portfolio take-profit ",)):
                matched = re.search(
                    r"service portfolio take-profit account=([A-Za-z0-9_.-]+)\s+result=(\{.*\})",
                    line,
                )
                if not matched:
                    continue
                account_id = matched.group(1).strip()
                try:
                    parsed = ast.literal_eval(matched.group(2).strip())
                except (SyntaxError, ValueError):
                    continue
                if not isinstance(parsed, dict):
                    continue
                status = str(parsed.get("status") or "").upper()
                if status not in {"TRIGGERED", "TRIGGERED_RETRY", "ALREADY_TRIGGERED"}:
                    continue
                time_local = self._log_time_from_line(line)
                if not time_local:
                    continue
                try:
                    local_dt = datetime.strptime(time_local, "%Y-%m-%d %H:%M:%S").replace(
                        tzinfo=self.local_tz
                    )
                    event_dt = local_dt.astimezone(timezone.utc)
                except (TypeError, ValueError):
                    continue
                event_utc = event_dt.replace(microsecond=0).isoformat()
                if window_start_utc and event_utc < window_start_utc:
                    continue
                if event_utc > end_utc:
                    continue
                key = (account_id, event_utc)
                if key in seen:
                    continue
                seen.add(key)
                cycle_key = str(parsed.get("cycle_date") or time_local[:10]).strip() or time_local[:10]
                close_complete_raw = parsed.get("close_complete")
                close_complete = (
                    close_complete_raw is True
                    or close_complete_raw == 1
                    or str(close_complete_raw).strip().lower() == "true"
                )
                raw_events.append(
                    {
                        "account_id": account_id,
                        "cycle_key": cycle_key,
                        "t_utc": event_utc,
                        "status": status,
                        "actual_profit_pct": self._safe_float(parsed.get("actual_profit_pct")),
                        "closed_take_profit": self._safe_int(parsed.get("closed_take_profit"), 0),
                        "adjusted_take_profit": self._safe_int(parsed.get("adjusted_take_profit"), 0),
                        "close_complete": close_complete,
                        "pending": self._safe_int(parsed.get("pending"), 0),
                        "errors": self._safe_int(parsed.get("errors"), 0),
                    }
                )

        # Once a cycle is complete, runtime_service returns ALREADY_TRIGGERED
        # and intentionally does not emit the warning above.  The manage
        # summary still contains that latched state, so use it as the
        # completion edge for accounts such as acc03 that have no final
        # TRIGGERED_RETRY warning in the log.
        for path in self._task_log_files():
            for line in self._read_task_log_lines(path, ("service manage summary: ",)):
                if "ALREADY_TRIGGERED" not in line or "close_complete': True" not in line:
                    continue
                matched = re.search(r"service manage summary:\s+(\{.*\})", line)
                if not matched:
                    continue
                try:
                    parsed_summary = ast.literal_eval(matched.group(1).strip())
                except (SyntaxError, ValueError):
                    continue
                if not isinstance(parsed_summary, dict):
                    continue
                time_local = self._log_time_from_line(line)
                if not time_local:
                    continue
                try:
                    event_dt = datetime.strptime(time_local, "%Y-%m-%d %H:%M:%S").replace(
                        tzinfo=self.local_tz
                    )
                    event_utc = event_dt.astimezone(timezone.utc).replace(microsecond=0).isoformat()
                except (TypeError, ValueError):
                    continue
                if window_start_utc and event_utc < window_start_utc:
                    continue
                if event_utc > end_utc:
                    continue
                for account_id, account_summary in parsed_summary.items():
                    if not isinstance(account_summary, dict):
                        continue
                    portfolio_state = account_summary.get("portfolio_take_profit")
                    if not isinstance(portfolio_state, dict):
                        continue
                    status = str(portfolio_state.get("status") or "").upper()
                    if status != "ALREADY_TRIGGERED" or not portfolio_state.get("close_complete"):
                        continue
                    account_id = str(account_id).strip()
                    if not account_id:
                        continue
                    key = (account_id, event_utc)
                    if key in seen:
                        continue
                    seen.add(key)
                    cycle_key = str(portfolio_state.get("cycle_date") or time_local[:10]).strip() or time_local[:10]
                    raw_events.append(
                        {
                            "account_id": account_id,
                            "cycle_key": cycle_key,
                            "t_utc": event_utc,
                            "status": "ALREADY_TRIGGERED",
                            "actual_profit_pct": self._safe_float(portfolio_state.get("actual_profit_pct")),
                            "closed_take_profit": self._safe_int(portfolio_state.get("closed_take_profit"), 0),
                            "adjusted_take_profit": self._safe_int(portfolio_state.get("adjusted_take_profit"), 0),
                            "close_complete": True,
                            "pending": 0,
                            "errors": 0,
                        }
                    )

        grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for raw_event in sorted(
            raw_events,
            key=lambda item: (
                str(item.get("account_id") or ""),
                str(item.get("cycle_key") or ""),
                str(item.get("t_utc") or ""),
            ),
        ):
            group_key = (
                str(raw_event.get("account_id") or ""),
                str(raw_event.get("cycle_key") or ""),
            )
            grouped.setdefault(group_key, []).append(raw_event)

        events: List[Dict[str, Any]] = []

        def public_event(raw_event: Dict[str, Any]) -> Dict[str, Any]:
            return {
                "account_id": raw_event["account_id"],
                "t_utc": raw_event["t_utc"],
                "status": raw_event["status"],
                "actual_profit_pct": raw_event["actual_profit_pct"],
                "closed_take_profit": raw_event["closed_take_profit"],
                "adjusted_take_profit": raw_event["adjusted_take_profit"],
                "close_complete": raw_event["close_complete"],
                "pending": raw_event["pending"],
                "errors": raw_event["errors"],
            }

        for group_events in grouped.values():
            initial_events = [item for item in group_events if item["status"] == "TRIGGERED"]
            retry_events = [item for item in group_events if item["status"] == "TRIGGERED_RETRY"]
            if initial_events:
                initial_event = public_event(initial_events[0])
                initial_event["label"] = "组合止盈首次触发"
                events.append(initial_event)

            incomplete_retries = [item for item in retry_events if not item["close_complete"]]
            if incomplete_retries:
                first_retry = incomplete_retries[0]
                last_retry = incomplete_retries[-1]
                retry_event = public_event(first_retry)
                retry_event.update(
                    {
                        "label": "组合止盈重试",
                        "retry_count": len(incomplete_retries),
                        "first_retry_at_utc": first_retry["t_utc"],
                        "last_retry_at_utc": last_retry["t_utc"],
                        "actual_profit_pct": last_retry["actual_profit_pct"],
                        "closed_take_profit": last_retry["closed_take_profit"],
                        "adjusted_take_profit": last_retry["adjusted_take_profit"],
                        "pending": last_retry["pending"],
                        "errors": last_retry["errors"],
                        "close_complete": False,
                    }
                )
                events.append(retry_event)

            completion_events = [
                item
                for item in group_events
                if item["status"] in {"TRIGGERED_RETRY", "ALREADY_TRIGGERED"}
                and item["close_complete"]
            ]
            if completion_events:
                completed_event_raw = completion_events[0]
                completed_event = public_event(completed_event_raw)
                completed_event.update(
                    {
                        "status": "ALREADY_TRIGGERED",
                        "label": "组合止盈完成",
                        "retry_count": len(retry_events),
                        "first_retry_at_utc": retry_events[0]["t_utc"] if retry_events else None,
                        "last_retry_at_utc": retry_events[-1]["t_utc"] if retry_events else None,
                        "close_complete": True,
                        "pending": 0,
                    }
                )
                events.append(completed_event)

        events.sort(key=lambda item: (str(item.get("t_utc") or ""), str(item.get("account_id") or "")))
        return events

    def accounts_summary(self, *, include_details: bool = True) -> Dict[str, Any]:
        now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        payload = {"generated_at_utc": now_utc, "accounts": []}
        if not os.path.exists(self.db_path):
            return payload

        try:
            with self._connect_ctx() as conn:
                if self.overview_account_ids is not None:
                    rows = self._configured_accounts_summary_rows(conn, sorted(self.overview_account_ids))
                else:
                    rows = self._query_rows(
                        conn,
                        """
                        WITH accounts AS (
                            SELECT DISTINCT account_id FROM runs
                            UNION
                            SELECT DISTINCT account_id FROM wallet_snapshots
                            UNION
                            SELECT DISTINCT account_id FROM cashflow_events
                        ),
                        latest_runs AS (
                            SELECT r.account_id, r.status, r.started_at_utc
                            FROM runs r
                            INNER JOIN (
                                SELECT account_id, MAX(started_at_utc) AS max_started
                                FROM runs
                                GROUP BY account_id
                            ) x ON x.account_id = r.account_id AND x.max_started = r.started_at_utc
                        ),
                        open_pos AS (
                            SELECT r.account_id, COUNT(*) AS open_positions
                            FROM positions p
                            INNER JOIN runs r ON r.run_id = p.run_id
                            WHERE p.status = 'OPEN'
                            GROUP BY r.account_id
                        ),
                        latest_wallet AS (
                            SELECT ws.account_id, ws.balance_usdt
                            FROM wallet_snapshots ws
                            INNER JOIN (
                                SELECT account_id, MAX(id) AS max_id
                                FROM wallet_snapshots
                                WHERE error IS NULL
                                GROUP BY account_id
                            ) x ON x.account_id = ws.account_id AND x.max_id = ws.id
                        )
                        SELECT
                            a.account_id,
                            COALESCE(op.open_positions, 0) AS open_positions,
                            lr.status AS last_run_status,
                            lw.balance_usdt AS wallet_balance_usdt
                        FROM accounts a
                        LEFT JOIN open_pos op ON op.account_id = a.account_id
                        LEFT JOIN latest_runs lr ON lr.account_id = a.account_id
                        LEFT JOIN latest_wallet lw ON lw.account_id = a.account_id
                        ORDER BY a.account_id ASC
                        """
                    )
                by_account: Dict[str, Dict[str, Any]] = {}
                for row in rows:
                    aid = str(row.get("account_id") or "").strip()
                    if not aid:
                        continue
                    if self.overview_account_ids is not None and aid not in self.overview_account_ids:
                        continue
                    row["strategy_note"] = self.account_strategy_notes.get(aid, "")
                    row["mode"] = self.account_modes.get(aid, "full")
                    row["equity_recovery_take_profit_enabled"] = bool(
                        self.account_equity_recovery_enabled.get(aid, False)
                    )
                    by_account[aid] = row

                # Ensure configured accounts can appear in overview even when DB has no rows yet.
                configured_ids = sorted(self.overview_account_ids) if self.overview_account_ids is not None else sorted(
                    self.account_strategy_notes.keys()
                )
                for aid in configured_ids:
                    if aid in by_account:
                        continue
                    note = self.account_strategy_notes.get(aid, "")
                    by_account[aid] = {
                        "account_id": aid,
                        "open_positions": 0,
                        "last_run_status": None,
                        "wallet_balance_usdt": None,
                        "strategy_note": note,
                        "mode": self.account_modes.get(aid, "full"),
                        "equity_recovery_take_profit_enabled": bool(
                            self.account_equity_recovery_enabled.get(aid, False)
                        ),
                    }

                for aid, row in by_account.items():
                    if not self._is_readonly_account(aid):
                        row["portfolio_loss_cut"] = self._load_portfolio_loss_cut_state(
                            conn,
                            account_id=aid,
                            current_equity_usdt=row.get("wallet_balance_usdt"),
                            open_positions=int(row.get("open_positions") or 0),
                        )
                        continue
                    row["portfolio_loss_cut"] = None
                    live_positions = self._readonly_live_positions(aid)
                    row["open_positions"] = len(live_positions)
                    row["open_symbols"] = len(
                        {str(position.get("symbol") or "").strip() for position in live_positions if position.get("symbol")}
                    )
                    row["live_position_source"] = "LOCAL_ACCOUNT_STATE"
                    row["live_position_as_of_utc"] = (
                        live_positions[0].get("live_data_as_of_utc") if live_positions else None
                    )
                    live_position_error = self._readonly_position_cache_errors.get(aid)
                    if live_position_error:
                        row["live_position_error"] = live_position_error

                task_statuses = {}
                entry_progresses = {}
                if include_details:
                    task_statuses = self._latest_task_statuses_for_accounts(
                        list(by_account.keys()),
                        conn=conn,
                    )
                    entry_progresses = self._entry_progresses_from_db(
                        conn,
                        list(by_account.keys()),
                    )
                for aid, row in by_account.items():
                    if include_details:
                        row["tasks"] = task_statuses.get(aid, self._task_status_template())
                        row["entry_progress"] = entry_progresses.get(aid)
                    # 为 readonly 账户添加交易统计
                    if self.account_modes.get(aid, "full") == "readonly":
                        fetcher = self.trade_stats_fetchers.get(aid)
                        if fetcher is not None:
                            try:
                                stats = fetcher.get_cached_stats(account_id=aid, lookback_days=30)
                                if stats is not None:
                                    row["trade_stats"] = {
                                        "total_realized_pnl": stats.total_realized_pnl,
                                        "net_realized_pnl": stats.net_realized_pnl,
                                        "commission_usdt": stats.commission_usdt,
                                        "funding_fee_usdt": stats.funding_fee_usdt,
                                        "total_trades": stats.total_trades,
                                        "win_count": stats.win_count,
                                        "loss_count": stats.loss_count,
                                        "win_rate_pct": stats.win_rate_pct,
                                        "gross_profit": stats.gross_profit,
                                        "gross_loss": stats.gross_loss,
                                        "profit_factor": stats.profit_factor,
                                        "avg_win": stats.avg_win,
                                        "avg_loss": stats.avg_loss,
                                        "last_updated_utc": stats.last_updated_utc,
                                    }
                            except Exception as exc:  # noqa: BLE001
                                LOGGER.warning("Failed to fetch trade stats for account=%s: %s", aid, exc)

                payload["accounts"] = [by_account[k] for k in sorted(by_account.keys())]
                return payload
        except sqlite3.Error as exc:
            payload["db_error"] = str(exc)
            return payload

    @staticmethod
    def _summary_without_task_details(payload: Dict[str, Any]) -> Dict[str, Any]:
        compact = copy.deepcopy(payload)
        for row in compact.get("accounts", []):
            if not isinstance(row, dict):
                continue
            row.pop("tasks", None)
            row.pop("entry_progress", None)
        return compact

    def refresh_accounts_summary_cache(self) -> Dict[str, Any]:
        """Refresh the complete overview snapshot outside the HTTP request path."""
        if not self._accounts_summary_refresh_lock.acquire(blocking=False):
            return self.cached_accounts_summary()
        try:
            full_payload = self.accounts_summary(include_details=True)
            fast_payload = self._summary_without_task_details(full_payload)
            with self._accounts_summary_cache_lock:
                self._accounts_summary_cache_value = full_payload
                self._accounts_summary_fast_cache_value = fast_payload
            return copy.deepcopy(full_payload)
        finally:
            self._accounts_summary_refresh_lock.release()

    def cached_accounts_summary_fast(self) -> Dict[str, Any]:
        """Return the lightweight account snapshot without log-derived details."""
        with self._accounts_summary_cache_lock:
            cached = self._accounts_summary_fast_cache_value
            full = self._accounts_summary_cache_value
            if cached is None and full is not None:
                cached = self._summary_without_task_details(full)
                self._accounts_summary_fast_cache_value = cached
            if cached is not None:
                return copy.deepcopy(cached)

        # This path is only used before the first background refresh. It still
        # avoids the expensive task-log parser and is safe for a fast first UI.
        with self._accounts_summary_fast_compute_lock:
            with self._accounts_summary_cache_lock:
                cached = self._accounts_summary_fast_cache_value
                if cached is not None:
                    return copy.deepcopy(cached)
            fast_payload = self.accounts_summary(include_details=False)
            with self._accounts_summary_cache_lock:
                if self._accounts_summary_fast_cache_value is None:
                    self._accounts_summary_fast_cache_value = fast_payload
                cached = self._accounts_summary_fast_cache_value
            return copy.deepcopy(cached or fast_payload)

    def cached_accounts_summary(self) -> Dict[str, Any]:
        """Return the latest complete snapshot, falling back to fast data on cold start."""
        with self._accounts_summary_cache_lock:
            cached = self._accounts_summary_cache_value
            if cached is not None:
                return copy.deepcopy(cached)

        fast_payload = self.cached_accounts_summary_fast()
        fast_payload["details_pending"] = True
        return fast_payload

    def cached_accounts_summary_details(self) -> Dict[str, Any]:
        """Return only task/progress fields from the latest complete snapshot."""
        with self._accounts_summary_cache_lock:
            cached = self._accounts_summary_cache_value
            if cached is None:
                return {
                    "ready": False,
                    "generated_at_utc": None,
                    "accounts": [],
                }
            full_payload = copy.deepcopy(cached)

        details = {
            "ready": True,
            "generated_at_utc": full_payload.get("generated_at_utc"),
            "accounts": [
                {
                    "account_id": row.get("account_id"),
                    "tasks": row.get("tasks", self._task_status_template()),
                    "entry_progress": row.get("entry_progress"),
                }
                for row in full_payload.get("accounts", [])
                if isinstance(row, dict)
            ],
        }
        if full_payload.get("db_error"):
            details["db_error"] = full_payload["db_error"]
        return details

    def _configured_accounts_summary_rows(
        self,
        conn: sqlite3.Connection,
        account_ids: List[str],
    ) -> List[Dict[str, Any]]:
        account_ids = [str(aid).strip() for aid in account_ids if str(aid).strip()]
        if not account_ids:
            return []
        placeholders = ",".join("?" for _ in account_ids)
        open_rows = self._query_rows(
            conn,
            f"""
            SELECT r.account_id, COUNT(*) AS open_positions
            FROM positions p
            INNER JOIN runs r ON r.run_id = p.run_id
            WHERE p.status = 'OPEN' AND r.account_id IN ({placeholders})
            GROUP BY r.account_id
            """,
            tuple(account_ids),
        )
        latest_run_rows = self._query_rows(
            conn,
            f"""
            SELECT r.account_id, r.status, r.started_at_utc
            FROM runs r
            INNER JOIN (
                SELECT account_id, MAX(started_at_utc) AS max_started
                FROM runs
                WHERE account_id IN ({placeholders})
                GROUP BY account_id
            ) x ON x.account_id = r.account_id AND x.max_started = r.started_at_utc
            """,
            tuple(account_ids),
        )
        latest_wallet_rows = self._query_rows(
            conn,
            f"""
            SELECT account_id, balance_usdt
            FROM (
                SELECT account_id, balance_usdt,
                       ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY id DESC) AS rn
                FROM wallet_snapshots
                WHERE account_id IN ({placeholders}) AND error IS NULL
            )
            WHERE rn = 1
            """,
            tuple(account_ids),
        )
        open_by_account = {
            str(row.get("account_id") or ""): self._safe_int(row.get("open_positions"), 0)
            for row in open_rows
        }
        run_by_account = {
            str(row.get("account_id") or ""): row
            for row in latest_run_rows
        }
        wallet_by_account = {
            str(row.get("account_id") or ""): row
            for row in latest_wallet_rows
        }
        rows: List[Dict[str, Any]] = []
        for aid in account_ids:
            latest_run = run_by_account.get(aid, {})
            latest_wallet = wallet_by_account.get(aid, {})
            rows.append(
                {
                    "account_id": aid,
                    "open_positions": open_by_account.get(aid, 0),
                    "last_run_status": latest_run.get("status"),
                    "wallet_balance_usdt": latest_wallet.get("balance_usdt"),
                }
            )
        return rows


def _inline_script_json(value: Any) -> str:
    """Serialize a value for an inline script without allowing tag termination."""
    return (
        json.dumps(value, ensure_ascii=False)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def get_dashboard_html() -> str:
    return load_template("dashboard.html")


def get_accounts_overview_html() -> str:
    return load_template("accounts_overview.html")


DASHBOARD_HTML = get_dashboard_html()
ACCOUNTS_OVERVIEW_HTML = get_accounts_overview_html()


def _render_dashboard_template(
    refresh_sec: int,
    echarts_src: str,
    *,
    account_id: str = "",
    account_mode: str = "full",
    strategy_note: str = "",
    portfolio_loss_cut_enabled: bool = False,
    portfolio_loss_cut_pct: float = 3.5,
    portfolio_loss_cut_hour: int = 8,
    portfolio_loss_cut_minute: int = 0,
) -> str:
    safe_stop_pct = min(100.0, max(0.001, float(portfolio_loss_cut_pct)))
    return (
        get_dashboard_html().replace("__REFRESH_SEC__", str(max(2, refresh_sec)))
        .replace("__ECHARTS_SRC__", echarts_src or "https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js")
        .replace("__ACCOUNT_ID_JSON__", _inline_script_json(account_id))
        .replace("__ACCOUNT_MODE_JSON__", _inline_script_json(account_mode))
        .replace("__STRATEGY_NOTE_JSON__", _inline_script_json(strategy_note))
        .replace("__PORTFOLIO_STOP_ENABLED__", "true" if portfolio_loss_cut_enabled else "false")
        .replace("__PORTFOLIO_STOP_PCT__", f"{safe_stop_pct:g}")
        .replace("__PORTFOLIO_STOP_HOUR__", str(int(portfolio_loss_cut_hour) % 24))
        .replace("__PORTFOLIO_STOP_MINUTE__", str(int(portfolio_loss_cut_minute) % 60))
    )


def render_dashboard_html(
    refresh_sec: int,
    echarts_src: str = "https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js",
) -> str:
    return _render_dashboard_template(refresh_sec, echarts_src)


def render_account_dashboard_html(
    refresh_sec: int,
    account_id: str,
    echarts_src: str = "https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js",
    account_mode: str = "full",
    strategy_note: str = "",
    portfolio_loss_cut_enabled: bool = False,
    portfolio_loss_cut_pct: float = 3.5,
    portfolio_loss_cut_hour: int = 8,
    portfolio_loss_cut_minute: int = 0,
) -> str:
    safe_account_id = (account_id or "").strip()
    if not safe_account_id:
        return render_dashboard_html(refresh_sec, echarts_src=echarts_src)
    safe_account_mode = (account_mode or "full").strip().lower()
    if safe_account_mode not in {"full", "readonly", "loss_cut_only"}:
        safe_account_mode = "full"
    return _render_dashboard_template(
        refresh_sec,
        echarts_src,
        account_id=safe_account_id,
        account_mode=safe_account_mode,
        strategy_note=(strategy_note or "").strip(),
        portfolio_loss_cut_enabled=portfolio_loss_cut_enabled,
        portfolio_loss_cut_pct=portfolio_loss_cut_pct,
        portfolio_loss_cut_hour=portfolio_loss_cut_hour,
        portfolio_loss_cut_minute=portfolio_loss_cut_minute,
    )


def render_accounts_overview_html(
    refresh_sec: int,
    entry_hour: int = 7,
    entry_minute: int = 40,
    portfolio_loss_cut_enabled: bool = False,
    portfolio_loss_cut_pct: float = 3.5,
    portfolio_loss_cut_hour: int = 8,
    portfolio_loss_cut_minute: int = 0,
) -> str:
    safe_entry_hour = int(entry_hour) % 24
    safe_entry_minute = int(entry_minute) % 60
    safe_stop_pct = min(100.0, max(0.001, float(portfolio_loss_cut_pct)))
    safe_stop_hour = int(portfolio_loss_cut_hour) % 24
    safe_stop_minute = int(portfolio_loss_cut_minute) % 60
    stop_enabled = bool(portfolio_loss_cut_enabled)
    stop_label = f"-{safe_stop_pct:g}% 已启用" if stop_enabled else "未启用"
    stop_class = "" if stop_enabled else "is-disabled"
    return (
        get_accounts_overview_html().replace("__REFRESH_SEC__", str(max(15, refresh_sec)))
        .replace("__ENTRY_TIME__", f"{safe_entry_hour:02d}:{safe_entry_minute:02d}")
        .replace("__PORTFOLIO_STOP_ENABLED__", "true" if stop_enabled else "false")
        .replace("__PORTFOLIO_STOP_PCT__", f"{safe_stop_pct:g}")
        .replace("__PORTFOLIO_STOP_HOUR__", str(safe_stop_hour))
        .replace("__PORTFOLIO_STOP_MINUTE__", str(safe_stop_minute))
        .replace("__PORTFOLIO_STOP_LABEL__", stop_label)
        .replace("__PORTFOLIO_STOP_CLASS__", stop_class)
    )


def _json_bytes(payload: Dict[str, Any]) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


_CURVE_PAYLOAD_KEYS = {
    "strategy_equity_curve",
    "balance_curve",
    "equity_curve",
    "drawdown_stats_strategy",
    "drawdown_stats_balance",
    "drawdown_stats",
}


def _strip_curve_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    for key in _CURVE_PAYLOAD_KEYS:
        payload.pop(key, None)
    return payload


def _safe_query_int(
    raw_value: Optional[str],
    default: int,
    min_value: int,
    max_value: int,
) -> int:
    try:
        value = int(raw_value) if raw_value not in (None, "") else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(min_value, min(max_value, value))


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

            if path in {"/api/dashboard", "/api/dashboard/core", "/api/dashboard/details"}:
                params = parse_qs(parsed.query)
                lines = _safe_query_int(
                    params.get("log_lines", ["80"])[0],
                    default=80,
                    min_value=0,
                    max_value=300,
                )
                window_hours_raw = params.get("window_hours", [None])[0]
                curve_points_raw = params.get("curve_points", [None])[0]
                include_details = path != "/api/dashboard/core"
                include_log = path != "/api/dashboard/core"
                include_curves = path != "/api/dashboard/details"
                include_balance_curve = path != "/api/dashboard/core"
                include_trade_stats = path != "/api/dashboard/core"
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
                        include_details=include_details,
                        include_log=include_log,
                        include_curves=include_curves,
                        include_balance_curve=include_balance_curve,
                        include_trade_stats=include_trade_stats,
                    )
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path == "/api/accounts/summary":
                body = _json_bytes(provider.accounts_summary())
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if path.startswith("/api/account/"):
                account_suffix = path[len("/api/account/") :].strip()
                include_details = True
                include_log = True
                include_curves = True
                endpoint_kind = ""
                account_id = ""
                if account_suffix.endswith("/snapshot"):
                    endpoint_kind = "snapshot"
                    account_id = account_suffix[: -len("/snapshot")].strip().strip("/")
                elif account_suffix.endswith("/core"):
                    endpoint_kind = "core"
                    account_id = account_suffix[: -len("/core")].strip().strip("/")
                    include_details = False
                    include_log = False
                    include_curves = False
                elif account_suffix.endswith("/details"):
                    endpoint_kind = "details"
                    account_id = account_suffix[: -len("/details")].strip().strip("/")
                    include_curves = False
                elif account_suffix.endswith("/curve"):
                    endpoint_kind = "curve"
                    account_id = account_suffix[: -len("/curve")].strip().strip("/")
                    include_details = False
                    include_log = False
                else:
                    account_id = ""
                if not account_id:
                    self.send_response(404)
                    self.end_headers()
                    return
                params = parse_qs(parsed.query)
                lines = _safe_query_int(
                    params.get("log_lines", ["80"])[0],
                    default=80,
                    min_value=0,
                    max_value=300,
                )
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
                payload = provider.snapshot(
                    log_lines=min(lines, 300),
                    window_hours=window_hours,
                    curve_points=curve_points,
                    account_id=account_id or None,
                    include_details=include_details,
                    include_log=include_log,
                    include_curves=include_curves,
                    include_balance_curve=include_curves,
                    include_trade_stats=endpoint_kind in {"snapshot", "core"},
                )
                if endpoint_kind == "core":
                    payload = _strip_curve_payload(payload)
                body = _json_bytes(payload)
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

