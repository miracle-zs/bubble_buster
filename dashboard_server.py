import ast
import json
import logging
import os
import sqlite3
import glob
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlparse
from zoneinfo import ZoneInfo

from core.state_store import SQLITE_BUSY_TIMEOUT_MS

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
        return {
            "entry": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
            "daily_loss_cut": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
            "noon_protection": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
            "manage": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
            "equity_recovery_take_profit": {"status": "UNKNOWN", "time_local": None, "summary": "--"},
        }

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
        status = "UNKNOWN"
        summary = "--"
        if task_key == "entry":
            opened = self._safe_int(payload.get("opened"), 0)
            failed = self._safe_int(payload.get("failed"), 0)
            skipped = self._safe_int(payload.get("skipped"), 0)
            status_raw = str(payload.get("status") or "").upper()
            if status_raw in {"SUCCESS", "FAILED", "SKIPPED", "RUNNING"}:
                status = status_raw
            else:
                status = self._status_from_error_count(failed, opened)
            entry_failed_symbols = self._format_symbol_field(payload.get("entry_failed_symbols"))
            skipped_symbols = self._format_symbol_field(payload.get("skipped_symbols"))
            parts = [f"opened={opened}", f"failed={failed}", f"skipped={skipped}"]
            self._append_summary_part(parts, "failed_symbols", entry_failed_symbols)
            self._append_summary_part(parts, "skipped_symbols", skipped_symbols)
            summary = " ".join(parts)
        elif task_key == "daily_loss_cut":
            total = self._safe_int(payload.get("total"), 0)
            closed_loss_cut = self._safe_int(payload.get("closed_loss_cut"), 0)
            errors = self._safe_int(payload.get("errors"), 0)
            status = self._status_from_error_count(errors, max(0, total - errors))
            closed_symbols = self._format_symbol_field(payload.get("closed_symbols"))
            failed_symbols = self._format_symbol_field(payload.get("failed_symbols"))
            parts = [f"total={total}", f"closed={closed_loss_cut}", f"errors={errors}"]
            self._append_summary_part(parts, "closed_symbols", closed_symbols)
            self._append_summary_part(parts, "failed_symbols", failed_symbols)
            summary = " ".join(parts)
        elif task_key == "noon_protection":
            total = self._safe_int(payload.get("total"), 0)
            updated_sl = self._safe_int(payload.get("updated_sl"), 0)
            skipped = self._safe_int(payload.get("skipped"), 0)
            errors = self._safe_int(payload.get("errors"), 0)
            status = self._status_from_error_count(errors, max(0, updated_sl + skipped))
            failed_symbols = self._format_symbol_field(payload.get("failed_symbols"))
            parts = [f"total={total}", f"updated={updated_sl}", f"skipped={skipped}", f"errors={errors}"]
            self._append_summary_part(parts, "failed_symbols", failed_symbols)
            summary = " ".join(parts)
        elif task_key == "manage":
            if payload.get("skipped"):
                status = "SKIPPED"
                reason = str(payload.get("reason") or "SKIPPED").strip() or "SKIPPED"
                summary = f"reason={reason}"
            elif payload.get("error"):
                status = "FAILED"
                summary = f"error={str(payload.get('error'))[:80]}"
            else:
                manage_summary = payload.get("summary")
                if isinstance(manage_summary, dict):
                    total = self._safe_int(manage_summary.get("total"), 0)
                    closed_tp = self._safe_int(manage_summary.get("closed_tp"), 0)
                    closed_sl = self._safe_int(manage_summary.get("closed_sl"), 0)
                    closed_timeout = self._safe_int(manage_summary.get("closed_timeout"), 0)
                    updated_sl = self._safe_int(manage_summary.get("updated_sl"), 0)
                    errors = self._safe_int(manage_summary.get("errors"), 0)
                    status = self._status_from_error_count(
                        errors,
                        max(0, total + closed_tp + closed_sl + closed_timeout + updated_sl),
                    )
                    summary = (
                        f"total={total} tp={closed_tp} sl={closed_sl} "
                        f"timeout={closed_timeout} updated={updated_sl} errors={errors}"
                    )
                else:
                    status = "SUCCESS"
                    summary = "ok"
        elif task_key == "equity_recovery_take_profit":
            status_raw = str(payload.get("status") or "").upper()
            if any(
                key in payload
                for key in ("baseline_equity", "threshold_equity", "closed_take_profit", "cycle_date")
            ):
                if status_raw in {"TRIGGERED", "TRIGGERED_RETRY", "ALREADY_TRIGGERED"}:
                    status = "SUCCESS"
                elif status_raw in {"MONITORING", "SKIPPED", "DISABLED"}:
                    status = "SKIPPED"
                elif status_raw in {"FAILED", "ERROR"}:
                    status = "FAILED"
                else:
                    status = "UNKNOWN"
                closed = self._safe_int(payload.get("closed_take_profit"), 0)
                adjusted = self._safe_int(payload.get("adjusted_take_profit"), closed)
                errors = self._safe_int(payload.get("errors"), 0)
                current_equity = self._safe_float(payload.get("current_equity")) or 0.0
                threshold_equity = self._safe_float(payload.get("threshold_equity")) or 0.0
                actual_profit_pct = self._safe_float(payload.get("actual_profit_pct")) or 0.0
                summary = (
                    f"equity={current_equity:.2f}/{threshold_equity:.2f} "
                    f"profit={actual_profit_pct:.2f}% adjusted={adjusted} "
                    f"closed={closed} errors={errors}"
                )
                return {
                    "status": status,
                    "time_local": time_local,
                    "summary": summary,
                }
            if status_raw in {"TRIGGERED", "PARTIAL"}:
                status = "SUCCESS"
            elif status_raw in {"NOT_TRIGGERED", "SKIPPED", "DISABLED"}:
                status = "SKIPPED"
            elif status_raw in {"FAILED", "ERROR"}:
                status = "FAILED"
            else:
                status = "UNKNOWN"
            adjusted = self._safe_int(payload.get("adjusted"), 0)
            errors = self._safe_int(payload.get("errors"), 0)
            reduced_notional = self._safe_float(payload.get("reduced_notional")) or 0.0
            summary = f"adjusted={adjusted} errors={errors} reduced={reduced_notional:.2f}"

        return {
            "status": status,
            "time_local": time_local,
            "summary": summary,
        }

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
                signature.append((path, int(stat.st_mtime), int(stat.st_size)))
            except OSError:
                continue
        return tuple(signature)

    def _parse_task_statuses_from_logs(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        file_paths = self._task_log_files()
        signature = self._task_status_cache_signature(file_paths)
        if self._task_status_cache_key == signature and self._task_status_cache_value is not None:
            return self._task_status_cache_value

        statuses: Dict[str, Dict[str, Dict[str, Any]]] = {}
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
        for path in file_paths:
            for line in self._read_task_log_lines(path, log_markers):
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
            persisted_entries = self._latest_entry_statuses_from_db(conn, normalized_ids)
            for aid, persisted_entry in persisted_entries.items():
                current_entry = payload[aid]["entry"]
                if self._task_status_is_newer(persisted_entry, current_entry):
                    payload[aid]["entry"] = persisted_entry
        return payload

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
                waiting_symbols.append(
                    {
                        "symbol": symbol,
                        "signal_time_local": self._format_utc_as_local(item.get("signal_time_utc")),
                        "observing_hour_local": self._format_utc_as_local(hour_open_raw),
                        "next_check_local": self._format_utc_as_local(next_check.isoformat()) if next_check else None,
                    }
                )

            message = str(run.get("message") or "").strip()
            persisted_opened = self._run_message_count(message, "opened", default=len(opened_symbols)) or 0
            opened_count = max(len(opened_symbols), persisted_opened)
            failed_count = self._run_message_count(message, "failed", default=0) or 0
            entry_failed_count = self._run_message_count(message, "entry_failed", default=None)
            exit_setup_failed_count = self._run_message_count(message, "exit_setup_failed", default=0) or 0
            if entry_failed_count is None:
                entry_failed_count = max(0, failed_count - exit_setup_failed_count)
            skipped_count = self._run_message_count(message, "skipped_existing", default=0) or 0
            waiting_count = len(waiting_symbols)
            # Exit-setup failures already had an entry fill and are included in opened_count.
            target_count = opened_count + waiting_count + entry_failed_count + skipped_count
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
                "waiting_count": waiting_count,
                "failed_count": failed_count,
                "entry_failed_count": entry_failed_count,
                "exit_setup_failed_count": exit_setup_failed_count,
                "skipped_count": skipped_count,
                "opened_symbols": opened_symbols,
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
                if self.overview_account_ids is not None:
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
        """Extract actual portfolio take-profit trigger moments from strategy logs."""
        events: List[Dict[str, Any]] = []
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
                if status not in {"TRIGGERED", "TRIGGERED_RETRY"}:
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
                events.append(
                    {
                        "account_id": account_id,
                        "t_utc": event_utc,
                        "status": status,
                        "label": (
                            "组合止盈重试"
                            if status == "TRIGGERED_RETRY"
                            else "组合止盈首次触发"
                        ),
                        "actual_profit_pct": self._safe_float(parsed.get("actual_profit_pct")),
                        "closed_take_profit": self._safe_int(parsed.get("closed_take_profit"), 0),
                        "adjusted_take_profit": self._safe_int(parsed.get("adjusted_take_profit"), 0),
                    }
                )
        events.sort(key=lambda item: (str(item.get("t_utc") or ""), str(item.get("account_id") or "")))
        return events

    def accounts_summary(self) -> Dict[str, Any]:
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

                task_statuses = self._latest_task_statuses_for_accounts(
                    list(by_account.keys()),
                    conn=conn,
                )
                entry_progresses = self._entry_progresses_from_db(
                    conn,
                    list(by_account.keys()),
                )
                for aid, row in by_account.items():
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
        open_by_account = {
            str(row.get("account_id") or ""): self._safe_int(row.get("open_positions"), 0)
            for row in open_rows
        }
        run_by_account = {
            str(row.get("account_id") or ""): row
            for row in latest_run_rows
        }
        rows: List[Dict[str, Any]] = []
        for aid in account_ids:
            wallet_rows = self._query_rows(
                conn,
                """
                SELECT balance_usdt
                FROM wallet_snapshots
                WHERE account_id = ? AND error IS NULL
                ORDER BY id DESC
                LIMIT 1
                """,
                (aid,),
            )
            latest_run = run_by_account.get(aid, {})
            latest_wallet = wallet_rows[0] if wallet_rows else {}
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
        DASHBOARD_HTML.replace("__REFRESH_SEC__", str(max(2, refresh_sec)))
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
        ACCOUNTS_OVERVIEW_HTML.replace("__REFRESH_SEC__", str(max(15, refresh_sec)))
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
  <style>
    /* Account detail — restrained operations sheet, shared with the overview. */
    html { background: #071019; }
    body {
      min-height: 100vh;
      color: #eaf6ff;
      background:
        linear-gradient(rgba(17, 39, 53, 0.11) 1px, transparent 1px),
        #071019;
      background-size: 100% 48px;
      letter-spacing: 0;
    }
    body::before { display: none; }
    button, a, summary { font: inherit; }
    button, summary { -webkit-tap-highlight-color: transparent; }

    .detail-shell {
      width: 100%;
      max-width: 1680px;
      margin: 0 auto;
      padding: 12px 16px 32px;
    }
    .account-command-bar {
      min-height: 72px;
      display: grid;
      grid-template-columns: minmax(360px, 1.65fr) repeat(3, minmax(150px, 0.62fr));
      align-items: stretch;
      border: 1px solid #24485d;
      border-radius: 12px 12px 0 0;
      background: #0c1923;
      overflow: hidden;
    }
    .account-command-main {
      min-width: 0;
      display: flex;
      align-items: center;
      gap: 16px;
      padding: 12px 18px;
    }
    .overview-link {
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      min-height: 30px;
      padding-right: 15px;
      border-right: 1px solid #24485d;
      color: #8fb1c4;
      text-decoration: none;
      font-size: 12px;
      font-weight: 700;
    }
    .overview-link:hover,
    .overview-link:focus-visible { color: #4ec1ff; outline: none; }
    .account-heading { min-width: 0; }
    .account-title-row { display: flex; align-items: center; flex-wrap: wrap; gap: 9px; }
    .account-title {
      margin: 0;
      color: #f2f8fc;
      font-size: 21px;
      line-height: 1.2;
      letter-spacing: 0;
    }
    .mode-badge {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 7px;
      border: 1px solid #31586d;
      border-radius: 4px;
      color: #bad5e4;
      background: #10232f;
      font-size: 10px;
      font-weight: 800;
    }
    .mode-badge.mode-readonly { border-color: #654c6f; color: #e1b8f1; background: #211726; }
    .mode-badge.mode-loss-cut { border-color: #7c5c28; color: #f1cc87; background: #241d12; }
    .account-meta {
      margin-top: 5px;
      color: #7798ab;
      font-size: 11px;
      line-height: 1.35;
    }
    .command-cell {
      min-width: 0;
      padding: 12px 16px;
      border-left: 1px solid #203b4c;
      display: flex;
      flex-direction: column;
      justify-content: center;
      gap: 4px;
    }
    .command-cell-label { color: #7899ac; font-size: 10px; }
    .command-cell-value {
      color: #dcebf4;
      font-size: 13px;
      font-weight: 800;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .command-cell-value.ok, .status-ok { color: #26d07c; }
    .command-cell-value.warn, .status-warn { color: #ffb340; }
    .command-cell-value.bad, .status-bad { color: #ff6b6b; }
    .strategy-strip {
      min-height: 42px;
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 7px 18px;
      border: 1px solid #24485d;
      border-top: 0;
      border-radius: 0 0 12px 12px;
      background: #09151e;
    }
    .strategy-strip-label {
      flex: 0 0 auto;
      color: #7899ac;
      font-size: 10px;
      font-weight: 800;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .strategy-tags { min-width: 0; display: flex; align-items: center; flex-wrap: wrap; gap: 5px; }
    .strategy-chip {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 7px;
      border: 1px solid #2b4b5c;
      border-radius: 4px;
      color: #9db8c8;
      background: #0f202b;
      font-size: 10px;
      font-weight: 700;
    }
    .strategy-chip.primary { border-color: #316b89; color: #c9eaff; background: #102939; }
    .strategy-chip.off { border-color: #744146; color: #ff9a9a; background: #29191d; }
    .refresh-note { margin-left: auto; color: #66889b; font-size: 10px; white-space: nowrap; }

    .summary-rail {
      display: grid;
      grid-template-columns: 1.3fr repeat(3, minmax(0, 1fr)) 0.78fr 0.95fr;
      margin-top: 14px;
      border: 1px solid #24485d;
      border-radius: 12px;
      background: #0c1923;
      overflow: hidden;
    }
    .summary-metric {
      min-width: 0;
      min-height: 94px;
      padding: 15px 16px;
      border-left: 1px solid #203b4c;
      display: flex;
      flex-direction: column;
      justify-content: center;
      gap: 7px;
    }
    .summary-metric:first-child { border-left: 0; }
    .summary-metric.primary { background: #0e1e29; box-shadow: inset 3px 0 0 #4ec1ff; }
    .summary-label { color: #7899ac; font-size: 11px; }
    .summary-value {
      color: #edf7fd;
      font-family: ui-monospace, Menlo, Monaco, Consolas, monospace;
      font-size: 21px;
      line-height: 1.1;
      font-weight: 750;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .summary-metric.primary .summary-value { font-size: 27px; }
    .summary-help { color: #628397; font-size: 9px; line-height: 1.35; }

    .analytics-grid {
      display: grid;
      grid-template-columns: minmax(0, 2fr) minmax(340px, 0.82fr);
      gap: 14px;
      margin-top: 14px;
    }
    .surface {
      min-width: 0;
      border: 1px solid #24485d;
      border-radius: 12px;
      background: #0b1822;
      overflow: hidden;
    }
    .surface-head {
      min-height: 49px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 9px 14px;
      border-bottom: 1px solid #203b4c;
      background: #0d1c27;
    }
    .surface-heading { min-width: 0; }
    .surface-title { margin: 0; color: #dcebf4; font-size: 14px; line-height: 1.25; }
    .surface-subtitle { margin-top: 3px; color: #6f91a4; font-size: 10px; }
    .surface-count {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 24px;
      min-height: 20px;
      padding: 1px 7px;
      border: 1px solid #315469;
      border-radius: 999px;
      color: #9db8c8;
      font-size: 10px;
      font-weight: 800;
    }
    .chart-toolbar { display: flex; align-items: center; flex-wrap: wrap; justify-content: flex-end; gap: 12px; }
    .tab-row, .window-row { display: flex; align-items: center; gap: 4px; flex-wrap: wrap; }
    .window-row { margin: 0; }
    .tab-btn {
      min-height: 27px;
      padding: 3px 9px;
      border: 1px solid #29495b;
      border-radius: 4px;
      color: #8eacbd;
      background: #0a1720;
      font-size: 10px;
      font-weight: 750;
      cursor: pointer;
    }
    .tab-btn:hover, .tab-btn:focus-visible { color: #eaf6ff; border-color: #4e829e; outline: none; }
    .tab-btn.active {
      color: #06131b;
      border-color: #69c8f3;
      background: #69c8f3;
      box-shadow: none;
    }
    .chart-stage { position: relative; padding: 12px 14px 14px; }
    .chart-canvas {
      width: 100%;
      height: 348px;
      border: 0;
      border-radius: 0;
      background: #08141d;
    }
    .chart-empty {
      position: absolute;
      inset: 12px 14px 14px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #67889b;
      background: #08141d;
      font-size: 12px;
      pointer-events: none;
    }
    .chart-empty[hidden] { display: none; }

    .risk-panel { display: flex; flex-direction: column; }
    .risk-body { flex: 1 1 auto; padding: 18px; display: flex; flex-direction: column; }
    .risk-state-line { display: flex; align-items: flex-start; justify-content: space-between; gap: 14px; }
    .risk-state-copy { min-width: 0; }
    .risk-eyebrow { color: #6f91a4; font-size: 10px; }
    .risk-state {
      display: block;
      margin-top: 5px;
      color: #dcebf4;
      font-size: 22px;
      line-height: 1.2;
      font-weight: 800;
    }
    .risk-threshold {
      flex: 0 0 auto;
      padding: 5px 8px;
      border: 1px solid #684044;
      border-radius: 4px;
      color: #ff9a9a;
      background: #24171b;
      font-family: ui-monospace, Menlo, Monaco, Consolas, monospace;
      font-size: 10px;
      font-weight: 800;
    }
    .risk-description { min-height: 34px; margin: 13px 0 0; color: #8ba8b9; font-size: 11px; line-height: 1.55; }
    .risk-meter { margin-top: 17px; }
    .risk-meter-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; color: #7899ac; font-size: 10px; }
    .risk-meter-track { position: relative; height: 7px; margin-top: 8px; background: #1b303d; overflow: hidden; }
    .risk-meter-fill { display: block; width: 0; height: 100%; background: #26d07c; transition: width 180ms ease; }
    .risk-meter-fill.warn { background: #ffb340; }
    .risk-meter-fill.bad { background: #ff5d5d; }
    .risk-meter-scale { display: flex; justify-content: space-between; margin-top: 6px; color: #59798c; font-size: 9px; }
    .risk-facts {
      display: grid;
      grid-template-columns: 1fr 1fr;
      margin-top: 20px;
      border-top: 1px solid #203b4c;
      border-left: 1px solid #203b4c;
    }
    .risk-fact { min-width: 0; min-height: 66px; padding: 11px; border-right: 1px solid #203b4c; border-bottom: 1px solid #203b4c; }
    .risk-fact-label { display: block; color: #6f91a4; font-size: 9px; }
    .risk-fact-value { display: block; margin-top: 6px; color: #dcebf4; font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size: 13px; font-weight: 800; }

    .section-block { margin-top: 14px; }
    .table-wrap {
      width: 100%;
      max-height: 430px;
      overflow: auto;
      background: #09151e;
      scrollbar-color: #31566c #0a1720;
      scrollbar-width: thin;
    }
    .detail-table {
      width: 100%;
      min-width: 820px;
      border-collapse: collapse;
      table-layout: auto;
      color: #cfe1eb;
      font-size: 12px;
    }
    .detail-table th, .detail-table td {
      padding: 10px 13px;
      border-bottom: 1px solid rgba(42, 76, 96, 0.62);
      text-align: left;
      vertical-align: top;
      white-space: nowrap;
    }
    .detail-table th {
      position: sticky;
      top: 0;
      z-index: 2;
      color: #7899ac;
      background: #0e202b;
      font-size: 10px;
      font-weight: 750;
      letter-spacing: 0.03em;
    }
    .detail-table tbody tr:nth-child(odd) { background: rgba(15, 33, 45, 0.34); }
    .detail-table tbody tr:hover { background: rgba(31, 65, 84, 0.35); }
    .detail-table td.numeric { font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-variant-numeric: tabular-nums; }
    .symbol-cell strong { display: block; color: #edf7fd; font-size: 12px; }
    .symbol-cell small { display: block; margin-top: 3px; color: #5e8093; font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size: 9px; }
    .position-stack { display: flex; flex-direction: column; gap: 3px; min-width: 92px; }
    .position-primary { color: #dcebf4; font-weight: 750; }
    .position-secondary { color: #6f91a4; font-size: 10px; }
    .position-secondary strong { color: #a9c5d5; font-weight: 700; }
    .live-pnl { min-width: 104px; }
    .live-pnl.positive .position-primary { color: #65dfa0; }
    .live-pnl.negative .position-primary { color: #ff8989; }
    .live-pnl.neutral .position-primary { color: #dcebf4; }
    .live-unavailable { color: #6f91a4; font-size: 10px; }
    .exit-stack { display: flex; flex-direction: column; gap: 4px; min-width: 138px; }
    .exit-line { display: flex; align-items: center; gap: 5px; min-height: 20px; }
    .exit-key { width: 20px; color: #7899ac; font-size: 9px; font-weight: 850; }
    .exit-key.tp { color: #65dfa0; }
    .exit-key.sl { color: #ff8989; }
    .exit-price { min-width: 56px; color: #cfe1eb; font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size: 10px; }
    .order-state { display: inline-flex; align-items: center; min-height: 18px; padding: 1px 5px; border: 1px solid #315469; border-radius: 999px; color: #9db8c8; background: #0f202b; font-size: 8px; font-weight: 800; }
    .order-state.ok { border-color: #236944; color: #65dfa0; background: #10271e; }
    .order-state.warn { border-color: #755822; color: #ffc86d; background: #271f10; }
    .order-state.bad { border-color: #74383b; color: #ff8989; background: #291719; }
    .live-sync { display: block; margin-top: 4px; color: #5e8093; font-size: 9px; }
    .cell-muted { color: #7899ac; }
    .status-badge {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 7px;
      border: 1px solid #315469;
      border-radius: 999px;
      color: #9db8c8;
      background: #0f202b;
      font-size: 9px;
      font-weight: 850;
    }
    .status-badge.ok { border-color: #236944; color: #65dfa0; background: #10271e; }
    .status-badge.warn { border-color: #755822; color: #ffc86d; background: #271f10; }
    .status-badge.bad { border-color: #74383b; color: #ff8989; background: #291719; }
    .status-badge.readonly { border-color: #694d77; color: #d7a0f0; background: #21162a; }
    .readonly-protection-note { color: #d7a0f0; font-size: 10px; white-space: nowrap; }
    .error-cell { max-width: 320px; white-space: normal !important; color: #ff8989; line-height: 1.45; }
    .empty-row { padding: 24px 13px !important; color: #68899b; text-align: center !important; }

    .activity-tabs {
      display: flex;
      align-items: center;
      gap: 2px;
      padding: 8px 10px 0;
      border-bottom: 1px solid #203b4c;
      background: #0d1c27;
    }
    .activity-tab {
      position: relative;
      min-height: 36px;
      padding: 6px 12px 9px;
      border: 0;
      color: #7899ac;
      background: transparent;
      font-size: 11px;
      font-weight: 800;
      cursor: pointer;
    }
    .activity-tab::after { content: ""; position: absolute; left: 10px; right: 10px; bottom: -1px; height: 2px; background: transparent; }
    .activity-tab:hover, .activity-tab:focus-visible { color: #dcebf4; outline: none; }
    .activity-tab.active { color: #eaf6ff; }
    .activity-tab.active::after { background: #4ec1ff; }
    .tab-count { margin-left: 5px; color: #66889b; font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size: 9px; }
    .activity-view[hidden] { display: none; }
    .message-cell { min-width: 360px; max-width: 760px; white-space: normal !important; }
    .message-summary { color: #a9c1cf; line-height: 1.45; }
    .row-details { margin-top: 5px; }
    .row-details summary { color: #628ca3; font-size: 9px; cursor: pointer; list-style: none; }
    .row-details summary::-webkit-details-marker { display: none; }
    .row-details summary::before { content: "+ "; }
    .row-details[open] summary::before { content: "− "; }
    .raw-message { margin-top: 6px; padding: 8px; color: #7596a8; background: #07131b; font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size: 9px; line-height: 1.45; word-break: break-word; }

    .diagnostics-panel { margin-top: 14px; border: 1px solid #263f4f; border-radius: 12px; background: #09151e; overflow: hidden; }
    .diagnostics-panel > summary {
      min-height: 48px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 9px 14px;
      color: #a8bfcc;
      background: #0b1923;
      font-size: 12px;
      font-weight: 800;
      cursor: pointer;
      list-style: none;
    }
    .diagnostics-panel > summary::-webkit-details-marker { display: none; }
    .diagnostics-panel > summary::after { content: "展开"; color: #68899b; font-size: 9px; font-weight: 700; }
    .diagnostics-panel[open] > summary::after { content: "收起"; }
    .diagnostics-intro { color: #66889b; font-size: 10px; font-weight: 500; }
    .diagnostics-grid { display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr); border-top: 1px solid #203b4c; }
    .diagnostic-block { min-width: 0; }
    .diagnostic-block + .diagnostic-block { border-left: 1px solid #203b4c; }
    .diagnostic-title { margin: 0; padding: 10px 13px; border-bottom: 1px solid #203b4c; color: #8facbd; background: #0c1a24; font-size: 10px; }
    .log {
      margin: 0;
      height: 322px;
      max-height: none;
      overflow: auto;
      padding: 11px 13px;
      border: 0;
      color: #8eacbd;
      background: #07131b;
      font-size: 10px;
      line-height: 1.55;
    }
    .mono { font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; font-size: inherit; }

    @media (max-width: 1180px) {
      .account-command-bar { grid-template-columns: minmax(340px, 1.4fr) repeat(3, minmax(130px, 0.62fr)); }
      .summary-rail { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .summary-metric:nth-child(4) { border-left: 0; border-top: 1px solid #203b4c; }
      .summary-metric:nth-child(n+5) { border-top: 1px solid #203b4c; }
      .analytics-grid { grid-template-columns: 1fr; }
      .risk-facts { grid-template-columns: repeat(4, 1fr); }
    }
    @media (max-width: 820px) {
      .detail-shell { padding: 10px 10px 24px; }
      .account-command-bar { grid-template-columns: 1fr 1fr; }
      .account-command-main { grid-column: 1 / -1; border-bottom: 1px solid #203b4c; }
      .command-cell:nth-child(2) { border-left: 0; }
      .command-cell:nth-child(4) { grid-column: 1 / -1; border-left: 0; border-top: 1px solid #203b4c; }
      .strategy-strip { align-items: flex-start; flex-wrap: wrap; }
      .refresh-note { width: 100%; margin-left: 0; }
      .summary-rail { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .summary-metric { min-height: 82px; border-top: 1px solid #203b4c; }
      .summary-metric:nth-child(odd) { border-left: 0; }
      .summary-metric:nth-child(-n+2) { border-top: 0; }
      .summary-metric.primary { grid-column: auto; }
      .surface-head { align-items: flex-start; flex-direction: column; }
      .chart-toolbar { width: 100%; justify-content: space-between; }
      .chart-canvas { height: 280px; }
      .risk-facts { grid-template-columns: 1fr 1fr; }
      .diagnostics-grid { grid-template-columns: 1fr; }
      .diagnostic-block + .diagnostic-block { border-left: 0; border-top: 1px solid #203b4c; }
    }
    @media (max-width: 540px) {
      .account-command-main { align-items: flex-start; flex-direction: column; gap: 9px; }
      .overview-link { min-height: 20px; padding-right: 0; border-right: 0; }
      .account-title { font-size: 19px; }
      .command-cell { min-height: 62px; padding: 10px 12px; }
      .strategy-strip { padding: 9px 12px; }
      .summary-metric { padding: 13px 12px; }
      .summary-value { font-size: 17px; }
      .summary-metric.primary .summary-value { font-size: 22px; }
      .chart-toolbar { align-items: flex-start; flex-direction: column; }
      .chart-canvas { height: 246px; }
      .chart-stage { padding: 9px; }
      .chart-empty { inset: 9px; }
      .risk-body { padding: 14px; }
      .risk-state { font-size: 19px; }
      .activity-tabs { overflow-x: auto; }
      .activity-tab { flex: 0 0 auto; }
      .detail-table th, .detail-table td { padding: 9px 10px; }
    }
    @media (prefers-reduced-motion: reduce) {
      *, *::before, *::after { scroll-behavior: auto !important; transition: none !important; animation: none !important; }
    }
  </style>
</head>
<body>
  <main class="detail-shell">
    <header class="account-command-bar">
      <div class="account-command-main">
        <a class="overview-link" id="overviewLink" href="/">← 账户总览</a>
        <div class="account-heading">
          <div class="account-title-row">
            <h1 class="account-title" id="accountTitle">账户详情</h1>
            <span class="mode-badge" id="accountMode">加载中</span>
          </div>
          <div class="account-meta" id="meta">正在读取账户状态...</div>
        </div>
      </div>
      <div class="command-cell">
        <span class="command-cell-label">服务状态</span>
        <strong class="command-cell-value" id="serviceState">检测中</strong>
      </div>
      <div class="command-cell">
        <span class="command-cell-label">下次开单</span>
        <strong class="command-cell-value mono" id="nextEntry">--</strong>
      </div>
      <div class="command-cell">
        <span class="command-cell-label">数据更新时间</span>
        <strong class="command-cell-value mono" id="updatedAt">--</strong>
      </div>
    </header>

    <section class="strategy-strip" aria-label="账户策略配置">
      <span class="strategy-strip-label">Strategy</span>
      <div class="strategy-tags" id="strategyTags"><span class="strategy-chip">未配置策略说明</span></div>
      <span class="refresh-note">自动刷新 <span id="refresh">__REFRESH_SEC__</span>s · 页面只读</span>
    </section>

    <section class="summary-rail" aria-label="账户核心指标">
      <article class="summary-metric primary">
        <span class="summary-label">策略权益 (USDT)</span>
        <strong class="summary-value" id="walletBalance">--</strong>
        <span class="summary-help">扣除窗口内出入金影响</span>
      </article>
      <article class="summary-metric">
        <span class="summary-label" id="cycleReturnLabel">周期收益</span>
        <strong class="summary-value" id="cycleReturn">--</strong>
        <span class="summary-help" id="cycleReturnHelp">按组合止损周期计算</span>
      </article>
      <article class="summary-metric">
        <span class="summary-label">距周期高点回撤</span>
        <strong class="summary-value" id="currentDrawdown">--</strong>
        <span class="summary-help">当前权益相对周期内高点</span>
      </article>
      <article class="summary-metric">
        <span class="summary-label">距组合止损</span>
        <strong class="summary-value" id="stopDistance">--</strong>
        <span class="summary-help" id="stopDistanceHelp">阈值计算中</span>
      </article>
      <article class="summary-metric">
        <span class="summary-label">持仓</span>
        <strong class="summary-value" id="openCount">0</strong>
        <span class="summary-help"><span id="symbolCount">0</span> 个交易对</span>
      </article>
      <article class="summary-metric">
        <span class="summary-label">最近任务</span>
        <strong class="summary-value" id="lastRunStatus">--</strong>
        <span class="summary-help"><span id="errorCount">0</span> 个近期异常</span>
      </article>
    </section>

    <section class="analytics-grid">
      <section class="surface chart-surface" aria-labelledby="curveHeading">
        <header class="surface-head">
          <div class="surface-heading">
            <h2 class="surface-title" id="curveHeading">权益曲线</h2>
            <div class="surface-subtitle" id="curveTitle">策略权益（不含出入金）</div>
          </div>
          <div class="chart-toolbar">
            <div class="tab-row" aria-label="权益口径">
              <button class="tab-btn active" id="tabStrategy" type="button" aria-pressed="true">策略权益</button>
              <button class="tab-btn" id="tabBalance" type="button" aria-pressed="false">账户权益</button>
            </div>
            <div class="window-row" id="windowRow" aria-label="曲线时间范围">
              <button class="tab-btn" data-window-hours="1" type="button">1H</button>
              <button class="tab-btn active" data-window-hours="24" type="button">1D</button>
              <button class="tab-btn" data-window-hours="168" type="button">1W</button>
              <button class="tab-btn" data-window-hours="720" type="button">1M</button>
              <button class="tab-btn" data-window-hours="8760" type="button">1Y</button>
            </div>
          </div>
        </header>
        <div class="chart-stage">
          <div class="chart-canvas" id="equityChart"></div>
          <div class="chart-empty" id="chartEmpty">曲线加载中...</div>
        </div>
      </section>

      <aside class="surface risk-panel" aria-labelledby="riskHeading">
        <header class="surface-head">
          <div class="surface-heading">
            <h2 class="surface-title" id="riskHeading">周期风险</h2>
            <div class="surface-subtitle" id="riskCycleStart">按每日止损周期计算</div>
          </div>
        </header>
        <div class="risk-body">
          <div class="risk-state-line">
            <div class="risk-state-copy">
              <span class="risk-eyebrow">当前状态</span>
              <strong class="risk-state" id="riskState">计算中</strong>
            </div>
            <span class="risk-threshold" id="riskThreshold">-3.5%</span>
          </div>
          <p class="risk-description" id="riskDescription">正在根据周期权益曲线计算风险距离。</p>
          <div class="risk-meter">
            <div class="risk-meter-head"><span>止损预算使用</span><strong id="riskMeterLabel">--</strong></div>
            <div class="risk-meter-track"><span class="risk-meter-fill" id="riskMeterFill"></span></div>
            <div class="risk-meter-scale"><span>周期起点</span><span id="riskMeterLimit">止损 -3.5%</span></div>
          </div>
          <div class="risk-facts" id="drawdownStats"></div>
        </div>
      </aside>
    </section>

    <section class="surface section-block positions-panel" aria-labelledby="positionsHeading">
      <header class="surface-head">
        <div class="surface-heading">
          <h2 class="surface-title" id="positionsHeading">当前持仓</h2>
          <div class="surface-subtitle">名义金额优先按交易所敞口或标记价计算；缺失时按入场价估算，数量保留用于核对</div>
        </div>
        <span class="surface-count" id="positionsCount">0</span>
      </header>
      <div class="table-wrap">
        <table class="detail-table positions-table">
          <thead><tr><th>交易对 / 方向</th><th>名义金额 / 数量</th><th>入场价 / 标记价</th><th>未实现盈亏</th><th>止盈 / 止损</th><th>到期时间</th><th>状态</th></tr></thead>
          <tbody id="positionsBody"><tr><td colspan="7" class="empty-row">持仓加载中...</td></tr></tbody>
        </table>
      </div>
    </section>

    <section class="surface section-block activity-panel" aria-labelledby="activityHeading">
      <header class="surface-head">
        <div class="surface-heading">
          <h2 class="surface-title" id="activityHeading">账户活动</h2>
          <div class="surface-subtitle">运行、订单与资金变动按同一时区展示</div>
        </div>
      </header>
      <div class="activity-tabs" role="tablist" aria-label="账户活动分类">
        <button class="activity-tab active" type="button" role="tab" aria-selected="true" data-activity-target="activity-runs">运行记录 <span class="tab-count" id="runsCount">0</span></button>
        <button class="activity-tab" type="button" role="tab" aria-selected="false" data-activity-target="activity-events">订单事件 <span class="tab-count" id="eventsCount">0</span></button>
        <button class="activity-tab" type="button" role="tab" aria-selected="false" data-activity-target="activity-cashflow">资金流水 <span class="tab-count" id="cashflowCount">0</span></button>
      </div>
      <div class="activity-view" id="activity-runs" role="tabpanel">
        <div class="table-wrap"><table class="detail-table"><thead><tr><th>交易日</th><th>状态</th><th>开始时间</th><th>执行摘要</th></tr></thead><tbody id="runsBody"></tbody></table></div>
      </div>
      <div class="activity-view" id="activity-events" role="tabpanel" hidden>
        <div class="table-wrap"><table class="detail-table"><thead><tr><th>时间</th><th>交易对</th><th>订单类型</th><th>方向</th><th>状态</th><th>事件 ID</th></tr></thead><tbody id="eventsBody"></tbody></table></div>
      </div>
      <div class="activity-view" id="activity-cashflow" role="tabpanel" hidden>
        <div class="table-wrap"><table class="detail-table"><thead><tr><th>时间</th><th>类型</th><th>金额</th><th>资产</th><th>交易对</th><th>事件 ID</th></tr></thead><tbody id="cashflowBody"></tbody></table></div>
      </div>
    </section>

    <details class="diagnostics-panel">
      <summary><span>诊断信息 <span class="diagnostics-intro">仅在核对异常时展开</span></span><span class="surface-count" id="unpricedCount">0</span></summary>
      <div class="diagnostics-grid">
        <section class="diagnostic-block">
          <h2 class="diagnostic-title">待核对平仓</h2>
          <div class="table-wrap"><table class="detail-table"><thead><tr><th>交易对 / ID</th><th>状态</th><th>平仓原因</th><th>检测结果</th><th>订单 ID</th><th>平仓时间</th></tr></thead><tbody id="unpricedBody"></tbody></table></div>
        </section>
        <section class="diagnostic-block">
          <h2 class="diagnostic-title">策略日志尾部</h2>
          <pre class="log mono" id="logTail">日志按需加载...</pre>
        </section>
      </div>
    </details>
  </main>

<script>
(function () {
  var refreshNode = document.getElementById("refresh");
  var REFRESH_SEC = Number((refreshNode && refreshNode.textContent) || "5");
  var ECHARTS_SRC = "__ECHARTS_SRC__";
  var accountId = __ACCOUNT_ID_JSON__;
  var accountMode = __ACCOUNT_MODE_JSON__;
  var accountStrategyNote = __STRATEGY_NOTE_JSON__;
  var portfolioStopEnabled = __PORTFOLIO_STOP_ENABLED__;
  var portfolioStopPct = Number("__PORTFOLIO_STOP_PCT__") || 3.5;
  var portfolioStopHour = Number("__PORTFOLIO_STOP_HOUR__") || 0;
  var portfolioStopMinute = Number("__PORTFOLIO_STOP_MINUTE__") || 0;
  var isMobile = !!(window.matchMedia && window.matchMedia("(max-width: 900px)").matches);
  var pathPrefix = "/";
  if (window && window.location && typeof window.location.pathname === "string") {
    pathPrefix = window.location.pathname || "/";
  }
  pathPrefix = pathPrefix.replace(/\\/+$/, "");
  if (!pathPrefix) pathPrefix = "";
  var apiBase = pathPrefix.replace(/\\/legacy$/, "").replace(/\\/account\\/[^/]+$/, "");
  var api = accountId
    ? apiBase + "/api/account/" + encodeURIComponent(accountId)
    : pathPrefix + "/api/dashboard";
  var equityChart = null;
  var currentCurveTab = accountMode === "readonly" ? "balance" : "strategy";
  var currentWindowHours = 24;
  var displayTimeZone = "Asia/Shanghai";
  var riskSnapshot = null;
  var latestData = null;
  var refreshTick = 0;
  var fullLoadedOnce = false;
  var detailsLoaded = false;
  var detailsInFlight = false;
  var detailsRefreshPending = false;

  var el = {
    overviewLink: document.getElementById("overviewLink"),
    accountTitle: document.getElementById("accountTitle"),
    accountMode: document.getElementById("accountMode"),
    meta: document.getElementById("meta"),
    updatedAt: document.getElementById("updatedAt"),
    strategyTags: document.getElementById("strategyTags"),
    nextEntry: document.getElementById("nextEntry"),
    serviceState: document.getElementById("serviceState"),
    openCount: document.getElementById("openCount"),
    symbolCount: document.getElementById("symbolCount"),
    errorCount: document.getElementById("errorCount"),
    lastRunStatus: document.getElementById("lastRunStatus"),
    walletBalance: document.getElementById("walletBalance"),
    cycleReturn: document.getElementById("cycleReturn"),
    cycleReturnLabel: document.getElementById("cycleReturnLabel"),
    cycleReturnHelp: document.getElementById("cycleReturnHelp"),
    currentDrawdown: document.getElementById("currentDrawdown"),
    stopDistance: document.getElementById("stopDistance"),
    stopDistanceHelp: document.getElementById("stopDistanceHelp"),
    curveTitle: document.getElementById("curveTitle"),
    tabStrategy: document.getElementById("tabStrategy"),
    tabBalance: document.getElementById("tabBalance"),
    windowRow: document.getElementById("windowRow"),
    equityChart: document.getElementById("equityChart"),
    chartEmpty: document.getElementById("chartEmpty"),
    riskCycleStart: document.getElementById("riskCycleStart"),
    riskState: document.getElementById("riskState"),
    riskThreshold: document.getElementById("riskThreshold"),
    riskDescription: document.getElementById("riskDescription"),
    riskMeterLabel: document.getElementById("riskMeterLabel"),
    riskMeterFill: document.getElementById("riskMeterFill"),
    riskMeterLimit: document.getElementById("riskMeterLimit"),
    drawdownStats: document.getElementById("drawdownStats"),
    positionsCount: document.getElementById("positionsCount"),
    runsCount: document.getElementById("runsCount"),
    eventsCount: document.getElementById("eventsCount"),
    cashflowCount: document.getElementById("cashflowCount"),
    unpricedCount: document.getElementById("unpricedCount"),
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

  function formatStrategyNote(note) {
    var raw = String(note || "").trim();
    if (!raw) return '<span class="strategy-chip">未配置策略说明</span>';
    var parts = raw.split("/").map(function (part) {
      return String(part || "").trim();
    }).filter(Boolean);
    var html = "";
    for (var i = 0; i < parts.length; i += 1) {
      var part = parts[i];
      var upper = part.toUpperCase();
      var cls = "strategy-chip";
      if (upper.indexOf("OFF") >= 0) {
        cls += " off";
      } else if (
        upper.indexOf("TP") >= 0
        || part.indexOf("减仓") >= 0
        || part.indexOf("清仓") >= 0
        || part.indexOf("组合止盈") >= 0
      ) {
        cls += " primary";
      }
      html += '<span class="' + cls + '">' + escapeHtml(part) + "</span>";
    }
    return html || '<span class="strategy-chip">未配置策略说明</span>';
  }

  function applyAccountContext() {
    var displayId = accountId || "运行账户";
    var modeText = "完整策略";
    var modeClass = "mode-badge";
    if (accountMode === "readonly") {
      modeText = "只读监控";
      modeClass += " mode-readonly";
    } else if (accountMode === "loss_cut_only") {
      modeText = "止损监控";
      modeClass += " mode-loss-cut";
    }
    setText(el.accountTitle, displayId + " 账户详情");
    if (el.accountMode) {
      el.accountMode.textContent = modeText;
      el.accountMode.className = modeClass;
    }
    if (el.strategyTags) el.strategyTags.innerHTML = formatStrategyNote(accountStrategyNote);
    if (el.overviewLink) el.overviewLink.href = (apiBase || "") + "/";
    document.title = displayId + " · Bubble Buster";

    var cycleText = String(portfolioStopHour).padStart(2, "0") + ":" + String(portfolioStopMinute).padStart(2, "0");
    if (accountMode === "readonly") {
      setText(el.cycleReturnLabel, "窗口收益");
      setText(el.cycleReturnHelp, "跟随所选曲线范围");
      setText(el.stopDistanceHelp, "只读账户不执行组合止损");
      setText(el.riskCycleStart, "只读账户 · 仅观察余额与交易结果");
      setText(el.riskThreshold, "不适用");
      setText(el.riskMeterLimit, "不适用");
      if (el.tabStrategy) el.tabStrategy.hidden = true;
    } else {
      setText(el.cycleReturnLabel, cycleText + " 以来收益");
      setText(el.cycleReturnHelp, "按 Asia/Shanghai 止损周期计算");
      setText(el.stopDistanceHelp, portfolioStopEnabled ? "距离 -" + portfolioStopPct + "% 阈值" : "组合止损未启用");
      setText(el.riskCycleStart, "每日 " + cycleText + " 重置 · Asia/Shanghai");
      setText(el.riskThreshold, portfolioStopEnabled ? "-" + portfolioStopPct + "%" : "未启用");
      setText(el.riskMeterLimit, portfolioStopEnabled ? "止损 -" + portfolioStopPct + "%" : "未启用");
    }
  }

  function clsForStatus(status) {
    var s = (status || "").toUpperCase();
    if (s.indexOf("SUCCESS") >= 0 || s.indexOf("CLOSED_TP") >= 0) return "ok";
    if (s.indexOf("RUNNING") >= 0 || s.indexOf("SKIPPED") >= 0) return "warn";
    if (s.indexOf("FAILED") >= 0 || s.indexOf("ERROR") >= 0 || s.indexOf("CLOSED_SL") >= 0) return "bad";
    return "";
  }

  function statusLabel(status) {
    var s = String(status || "").toUpperCase();
    var labels = {
      SUCCESS: "成功",
      RUNNING: "运行中",
      FAILED: "失败",
      PARTIAL: "部分完成",
      SKIPPED: "已跳过",
      NEW: "挂单中",
      FILLED: "已成交",
      CANCELED: "已撤销",
      CANCELLED: "已撤销",
      EXPIRED: "已过期",
      REJECTED: "已拒绝",
      CLOSED_TP: "止盈平仓",
      CLOSED_SL: "止损平仓",
      CLOSED_TIMEOUT: "到期平仓",
      CLOSED_EXTERNAL: "外部平仓"
    };
    return labels[s] || s || "--";
  }

  function statusBadge(status) {
    var cls = clsForStatus(status);
    return '<span class="status-badge ' + cls + '">' + escapeHtml(statusLabel(status)) + "</span>";
  }

  function orderTypeLabel(value) {
    var s = String(value || "").toUpperCase();
    var labels = { MARKET: "市价", LIMIT: "限价", STOP: "止损", STOP_MARKET: "止损市价", TAKE_PROFIT: "止盈", TAKE_PROFIT_MARKET: "止盈市价" };
    return labels[s] || s || "--";
  }

  function sideLabel(value) {
    var s = String(value || "").toUpperCase();
    if (s === "BUY") return "买入";
    if (s === "SELL") return "卖出";
    if (s === "SHORT") return "空单";
    if (s === "LONG") return "多单";
    if (s === "BOTH") return "双向";
    return s || "--";
  }

  function liveOrderLabel(value) {
    var s = String(value || "").toUpperCase();
    var labels = {
      ACTIVE: "挂单中",
      NEW: "挂单中",
      PARTIALLY_FILLED: "部分成交",
      FILLED: "已成交",
      CANCELED: "已撤销",
      CANCELLED: "已撤销",
      EXPIRED: "已过期",
      REJECTED: "已拒绝",
      READ_ERROR: "读取失败",
      NOT_FOUND: "未发现",
      MISSING: "缺失",
      NOT_SET: "未设置"
    };
    return labels[s] || s || "待同步";
  }

  function liveOrderClass(value) {
    var s = String(value || "").toUpperCase();
    if (s === "NEW" || s === "ACTIVE" || s === "PARTIALLY_FILLED") return "ok";
    if (s === "READ_ERROR" || s === "MISSING" || s === "CANCELED" || s === "CANCELLED" || s === "EXPIRED" || s === "REJECTED") return "bad";
    return "warn";
  }

  function liveOrderBadge(value) {
    var label = liveOrderLabel(value);
    var cls = liveOrderClass(value);
    return '<span class="order-state ' + cls + '">' + escapeHtml(label) + "</span>";
  }

  function cashflowTypeLabel(value) {
    var s = String(value || "").toUpperCase();
    var labels = { TRANSFER: "划转", REALIZED_PNL: "已实现盈亏", COMMISSION: "手续费", FUNDING_FEE: "资金费", INSURANCE_CLEAR: "保险清算" };
    return labels[s] || s || "--";
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

  function trimFixed(value, digits) {
    var n = toNum(value);
    if (n === null) return "--";
    return n.toFixed(digits).replace(/(\\.\\d*?[1-9])0+$/, "$1").replace(/\\.0+$/, "");
  }

  function fmtAdaptive(value) {
    var n = toNum(value);
    if (n === null) return "--";
    var magnitude = Math.abs(n);
    var digits = magnitude >= 100 ? 2 : (magnitude >= 1 ? 4 : (magnitude >= 0.01 ? 6 : 8));
    return trimFixed(n, digits);
  }

  function fmtQuantity(value) {
    var n = toNum(value);
    if (n === null) return "--";
    var digits = Math.abs(n) >= 100 ? 2 : (Math.abs(n) >= 1 ? 4 : 8);
    return trimFixed(n, digits);
  }

  function notionalSourceLabel(value) {
    var labels = {
      EXCHANGE_NOTIONAL: "交易所实时",
      MARK_PRICE: "按标记价",
      ENTRY_PRICE_ESTIMATE: "按入场价估算"
    };
    return labels[String(value || "").toUpperCase()] || "";
  }

  function zonedFormat(isoText, options, fallbackSlice) {
    var raw = txt(isoText);
    if (raw === "--") return raw;
    var d = new Date(raw);
    if (Number.isNaN(d.getTime())) return raw.slice(0, fallbackSlice || 16).replace("T", " ");
    try {
      return new Intl.DateTimeFormat("zh-CN", Object.assign({ timeZone: displayTimeZone, hour12: false }, options)).format(d).replace(/\\//g, "-");
    } catch (err) {
      return raw.slice(5, fallbackSlice || 16).replace("T", " ");
    }
  }

  function fmtAxisTime(isoText) {
    return zonedFormat(isoText, { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" }, 16);
  }

  function fmtMetaTime(isoText) {
    return zonedFormat(isoText, { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit" }, 19);
  }

  function fmtDateOnly(isoText) {
    return zonedFormat(isoText, { year: "numeric", month: "2-digit", day: "2-digit" }, 10);
  }

  function renderRows(target, rows, mapper, emptyCols) {
    if (!target) return;
    if (!rows || rows.length === 0) {
      target.innerHTML = '<tr><td colspan="' + emptyCols + '" class="empty-row">暂无数据</td></tr>';
      return;
    }
    var html = "";
    for (var i = 0; i < rows.length; i += 1) {
      html += mapper(rows[i]);
    }
    target.innerHTML = html;
  }

  function curvePointsForWindow(hours) {
    var h = Number(hours);
    if (!Number.isFinite(h) || h <= 0) return 600;
    // Wallet snapshots are typically per-minute; request more points for longer windows.
    var estimatedPoints = Math.ceil(h * 60);
    var capped = Math.max(600, Math.min(5000, estimatedPoints));
    if (isMobile) {
      return Math.max(320, Math.min(1200, Math.ceil(capped * 0.5)));
    }
    return capped;
  }

  function fetchDashboard(apiUrl, options, callback) {
    var opts = options || {};
    var lite = !!opts.lite;
    var xhr = new XMLHttpRequest();
    var curvePoints = curvePointsForWindow(currentWindowHours);
    if (!lite && currentWindowHours >= 24) {
      curvePoints = Math.max(curvePoints, 1200);
    }
    var q = [
      "_=" + encodeURIComponent(String(new Date().getTime())),
      "window_hours=" + encodeURIComponent(String(currentWindowHours)),
      "curve_points=" + encodeURIComponent(String(curvePoints)),
      "log_lines=" + encodeURIComponent(lite ? "0" : "80")
    ];
    xhr.open("GET", apiUrl + "?" + q.join("&"), true);
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

  function mergeLatest(partial) {
    if (!partial || typeof partial !== "object") return;
    if (!latestData || typeof latestData !== "object") {
      latestData = {};
    }
    var keys = Object.keys(partial);
    for (var i = 0; i < keys.length; i += 1) {
      latestData[keys[i]] = partial[keys[i]];
    }
    applyTradeOutcomeStats();
  }

  function mergeObjectFields(base, extra) {
    var merged = {};
    var k;
    if (base && typeof base === "object") {
      var baseKeys = Object.keys(base);
      for (var i = 0; i < baseKeys.length; i += 1) {
        k = baseKeys[i];
        merged[k] = base[k];
      }
    }
    if (extra && typeof extra === "object") {
      var extraKeys = Object.keys(extra);
      for (var j = 0; j < extraKeys.length; j += 1) {
        k = extraKeys[j];
        merged[k] = extra[k];
      }
    }
    return merged;
  }

  function applyTradeOutcomeStats() {
    if (!latestData || !latestData.trade_outcome_stats || typeof latestData.trade_outcome_stats !== "object") return;
    latestData.drawdown_stats_strategy = mergeObjectFields(
      latestData.drawdown_stats_strategy || {},
      latestData.trade_outcome_stats
    );
    if (currentCurveTab === "strategy") {
      latestData.drawdown_stats = latestData.drawdown_stats_strategy;
    }
  }

  function mergeCurveOnly(curvePayload) {
    if (!curvePayload || typeof curvePayload !== "object") return;
    if (!latestData || typeof latestData !== "object") {
      latestData = {};
    }
    latestData.curve_window_hours = curvePayload.curve_window_hours;
    latestData.curve_points = curvePayload.curve_points;
    latestData.strategy_equity_curve = curvePayload.strategy_equity_curve || [];
    latestData.balance_curve = curvePayload.balance_curve || [];
    latestData.equity_curve = curvePayload.equity_curve || latestData.strategy_equity_curve || [];
    if (curvePayload.drawdown_stats_strategy) {
      latestData.drawdown_stats_strategy = curvePayload.drawdown_stats_strategy;
    }
    if (curvePayload.drawdown_stats_balance) {
      latestData.drawdown_stats_balance = curvePayload.drawdown_stats_balance;
    }
    if (curvePayload.drawdown_stats) {
      latestData.drawdown_stats = curvePayload.drawdown_stats;
    }
    applyTradeOutcomeStats();
  }

  function showChartEmpty(message) {
    if (!el.chartEmpty) return;
    el.chartEmpty.textContent = message || "暂无曲线";
    el.chartEmpty.hidden = false;
  }

  function renderEquityChart(curve) {
    if (!el.equityChart) return;
    if (typeof window.echarts === "undefined") {
      showChartEmpty("图表组件加载中...");
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
      showChartEmpty("所选范围暂无权益曲线");
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
      showChartEmpty("所选范围暂无有效数据");
      return;
    }
    if (el.chartEmpty) el.chartEmpty.hidden = true;

    var first = points[0];
    var last = points[points.length - 1];
    var lineColor = last.equity >= first.equity ? "#26d07c" : "#ff5d5d";

    var areaTop = last.equity >= first.equity ? "rgba(38,208,124,0.20)" : "rgba(255,93,93,0.17)";
    var areaBottom = last.equity >= first.equity ? "rgba(38,208,124,0.01)" : "rgba(255,93,93,0.01)";
    equityChart.setOption({
      animation: false,
      grid: { left: 58, right: 18, top: 18, bottom: 30 },
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "line", lineStyle: { color: "#426b80" } },
        backgroundColor: "rgba(7, 19, 27, 0.97)",
        borderColor: "#31566c",
        textStyle: { color: "#d7eefd", fontSize: 11 },
        formatter: function (params) {
          if (!params || params.length === 0) return "";
          var idx = params[0].dataIndex;
          var p = points[idx] || {};
          return [
            "<div>" + escapeHtml(fmtAxisTime(p.t || "--")) + "</div>",
            "<div>权益：" + escapeHtml(fmtNum(p.equity, 4)) + " USDT</div>",
            "<div>窗口变化：" + escapeHtml(fmtSigned(p.cumPnl, 4)) + " USDT</div>",
            "<div>回撤：" + escapeHtml(fmtNum(p.ddPct, 2)) + "%</div>"
          ].join("");
        }
      },
      dataZoom: [{ type: "inside", xAxisIndex: 0, filterMode: "none" }],
      xAxis: {
        type: "category",
        data: xData,
        boundaryGap: false,
        axisLabel: {
          color: "#6f91a4",
          hideOverlap: true,
          fontSize: 10,
          margin: 12,
          formatter: function (value) { return fmtAxisTime(value); }
        },
        axisLine: { lineStyle: { color: "#234354" } },
        axisTick: { show: false },
        splitNumber: 6
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: { color: "#6f91a4", formatter: function (value) { return fmtAdaptive(value); } },
        axisLine: { show: false },
        splitLine: { lineStyle: { color: "rgba(42, 76, 96, 0.42)" } }
      },
      series: [
        {
          name: "权益",
          type: "line",
          showSymbol: points.length <= 1,
          symbolSize: 6,
          smooth: 0.12,
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

  function ensureEcharts() {
    if (typeof window.echarts !== "undefined") return;
    if (window.__bb_echarts_loading) return;
    window.__bb_echarts_loading = true;
    var script = document.createElement("script");
    var resolvedSrc = ECHARTS_SRC || "https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js";
    // When deployed behind a path prefix (e.g. /bubble), map absolute local static
    // path to prefixed path so echarts can be loaded from the same upstream.
    if (resolvedSrc.charAt(0) === "/" && resolvedSrc.indexOf("://") < 0) {
      var basePrefix = pathPrefix.replace(/\\/legacy$/, "").replace(/\\/account\\/[^/]+$/, "");
      if (basePrefix && basePrefix !== "/") {
        resolvedSrc = basePrefix + resolvedSrc;
      }
    }
    script.src = resolvedSrc;
    script.async = true;
    script.onload = function () {
      window.__bb_echarts_loading = false;
      rerenderFromLatest();
    };
    script.onerror = function () {
      window.__bb_echarts_loading = false;
      showChartEmpty("图表组件加载失败");
    };
    document.head.appendChild(script);
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

  function setSummaryMetric(node, value, cls) {
    if (!node) return;
    node.textContent = value;
    node.className = "summary-value" + (cls ? " " + cls : "");
  }

  function cycleStartMs(referenceMs) {
    var shanghaiOffsetMs = 8 * 60 * 60 * 1000;
    var local = new Date(referenceMs + shanghaiOffsetMs);
    var start = Date.UTC(
      local.getUTCFullYear(),
      local.getUTCMonth(),
      local.getUTCDate(),
      portfolioStopHour,
      portfolioStopMinute
    ) - shanghaiOffsetMs;
    if (referenceMs < start) start -= 24 * 60 * 60 * 1000;
    return start;
  }

  function calculateCurveMetrics(rawCurve, useCycle) {
    var points = (rawCurve || []).filter(function (point) {
      return point && Number.isFinite(Number(point.equity));
    });
    if (points.length < 2) return null;
    var startIndex = 0;
    if (useCycle) {
      var latestMs = new Date(points[points.length - 1].t || "").getTime();
      if (!Number.isFinite(latestMs)) return null;
      var targetMs = cycleStartMs(latestMs);
      var afterIndex = -1;
      for (var i = 0; i < points.length; i += 1) {
        var pointMs = new Date(points[i].t || "").getTime();
        if (!Number.isFinite(pointMs)) continue;
        if (afterIndex < 0 && pointMs >= targetMs) afterIndex = i;
      }
      if (afterIndex < 0) return null;
      startIndex = afterIndex;
    }
    var scoped = points.slice(startIndex);
    if (scoped.length < 2) return null;
    var baseline = Number(scoped[0].equity);
    if (!Number.isFinite(baseline) || baseline === 0) return null;
    var currentEquity = Number(scoped[scoped.length - 1].equity);
    var peakEquity = baseline;
    for (var j = 1; j < scoped.length; j += 1) {
      var candidate = Number(scoped[j].equity);
      if (Number.isFinite(candidate) && candidate > peakEquity) peakEquity = candidate;
    }
    var currentReturnPct = ((currentEquity - baseline) / baseline) * 100;
    var currentDrawdownPct = peakEquity > 0 ? ((currentEquity - peakEquity) / peakEquity) * 100 : 0;
    return {
      currentReturnPct: currentReturnPct,
      currentDrawdownPct: currentDrawdownPct,
      stopDistancePct: currentReturnPct + portfolioStopPct
    };
  }

  function renderRiskMetrics(data) {
    var useCycle = accountMode !== "readonly";
    var sourceCurve = useCycle
      ? ((data && data.strategy_equity_curve) || [])
      : activeCurve(data || {});
    var calculated = calculateCurveMetrics(sourceCurve, useCycle);
    if (calculated) {
      riskSnapshot = calculated;
    } else if (sourceCurve.length) {
      riskSnapshot = null;
    }
    var metrics = calculated || riskSnapshot || {};
    var stop = data && data.portfolio_loss_cut;
    var backendReturn = stop ? toNum(stop.current_return_pct) : null;
    var currentReturn = backendReturn !== null ? backendReturn : toNum(metrics.currentReturnPct);
    var currentDrawdown = toNum(metrics.currentDrawdownPct);
    var returnCls = currentReturn === null ? "" : (currentReturn > 0 ? "ok" : (currentReturn < 0 ? "warn" : ""));
    var drawdownCls = currentDrawdown === null
      ? ""
      : (currentDrawdown <= -(portfolioStopPct * 0.7) ? "bad" : (currentDrawdown < 0 ? "warn" : "ok"));
    setSummaryMetric(el.cycleReturn, currentReturn === null ? "--" : fmtSigned(currentReturn, 2) + "%", returnCls);
    setSummaryMetric(el.currentDrawdown, currentDrawdown === null ? "--" : fmtSigned(currentDrawdown, 2) + "%", drawdownCls);

    if (accountMode === "readonly") {
      setSummaryMetric(el.stopDistance, "不适用", "");
      if (el.riskState) el.riskState.className = "risk-state";
      setText(el.riskState, "只读监控");
      setText(el.riskDescription, "该账户不执行自动开仓与组合止损，当前页面仅用于观察余额、交易结果与账户活动。");
      setText(el.riskMeterLabel, "不适用");
      if (el.riskMeterFill) {
        el.riskMeterFill.style.width = "0%";
        el.riskMeterFill.className = "risk-meter-fill";
      }
      return;
    }

    if (!portfolioStopEnabled) {
      setSummaryMetric(el.stopDistance, "未启用", "warn");
      if (el.riskState) el.riskState.className = "risk-state warn";
      setText(el.riskState, "组合止损未启用");
      setText(el.riskDescription, "周期收益与回撤仍会持续计算，但当前账户不会按组合阈值触发整体平仓。");
      setText(el.riskMeterLabel, "未启用");
      if (el.riskMeterFill) {
        el.riskMeterFill.style.width = "0%";
        el.riskMeterFill.className = "risk-meter-fill warn";
      }
      return;
    }

    if (!stop || !stop.status) {
      setSummaryMetric(el.stopDistance, "状态待同步", "warn");
      if (el.riskState) el.riskState.className = "risk-state warn";
      setText(el.riskState, "等待后端止损状态");
      setText(el.riskDescription, "后端尚未写入本周期组合止损状态；页面不会仅凭权益曲线判定已经触发。");
      setText(el.riskMeterLabel, "待同步");
      if (el.riskMeterFill) {
        el.riskMeterFill.style.width = "0%";
        el.riskMeterFill.className = "risk-meter-fill warn";
      }
      return;
    }

    var status = String(stop.status || "").toUpperCase();
    var distance = toNum(stop.distance_pct);
    var configuredLossPct = toNum(stop.loss_pct);
    if (configuredLossPct === null || configuredLossPct <= 0) configuredLossPct = portfolioStopPct;
    var riskCls = "ok";
    var distanceText = distance === null ? "--" : fmtNum(distance, 2) + "%";
    var stateText = "风险正常";
    var description = currentReturn === null
      ? "后端正在监控本周期组合止损。"
      : "当前周期收益 " + fmtSigned(currentReturn, 2) + "% ，距离 -" + fmtNum(configuredLossPct, 2) + "% 止损阈值仍有 " + fmtNum(distance, 2) + " 个百分点。";

    if (status === "TRIGGERED_COMPLETE") {
      riskCls = "bad";
      distanceText = "已完成";
      stateText = "组合止损已完成";
      description = "后端已完成本周期组合止损，当前受管持仓为 0。";
    } else if (status === "CLOSING") {
      riskCls = "bad";
      distanceText = "执行中";
      stateText = "组合止损执行中";
      description = stop.state_inconsistent
        ? "后端已记录平仓完成，但本地仍有 " + txt(stop.open_positions) + " 个受管持仓，状态正在等待对账。"
        : "后端已经触发组合止损，正在处理剩余 " + txt(stop.open_positions) + " 个受管持仓。";
    } else if (status === "THRESHOLD_REACHED") {
      riskCls = "bad";
      distanceText = distance === null ? "已到阈值" : "已超出 " + fmtNum(Math.abs(distance), 2) + "%";
      stateText = "已到组合止损阈值";
      description = "最新权益已达到止损阈值，正在等待策略管理器确认并执行；当前尚未标记为已触发。";
    } else if (status === "MONITORING" && distance !== null) {
      riskCls = distance <= 1 ? "warn" : "ok";
      stateText = distance <= 1 ? "接近组合止损" : "风险正常";
    } else {
      riskCls = "warn";
      distanceText = "状态待同步";
      stateText = "等待后端止损状态";
      description = "后端组合止损基准尚未就绪；页面不会仅凭权益曲线判定已经触发。";
    }

    setSummaryMetric(el.stopDistance, distanceText, riskCls);
    if (el.riskState) el.riskState.className = "risk-state " + riskCls;
    setText(el.riskState, stateText);
    setText(el.riskDescription, description);

    var usedPct = status === "TRIGGERED_COMPLETE" || status === "CLOSING" || status === "THRESHOLD_REACHED"
      ? 100
      : (currentReturn !== null && currentReturn < 0
        ? Math.min(100, Math.abs(currentReturn) / configuredLossPct * 100)
        : 0);
    setText(el.riskMeterLabel, usedPct <= 0 ? "未消耗" : fmtNum(usedPct, 0) + "%");
    if (el.riskMeterFill) {
      el.riskMeterFill.style.width = usedPct.toFixed(2) + "%";
      el.riskMeterFill.className = "risk-meter-fill " + riskCls;
    }
  }

  function renderCurveTabState() {
    if (el.tabStrategy) {
      el.tabStrategy.classList.toggle("active", currentCurveTab === "strategy");
      el.tabStrategy.setAttribute("aria-pressed", currentCurveTab === "strategy" ? "true" : "false");
    }
    if (el.tabBalance) {
      el.tabBalance.classList.toggle("active", currentCurveTab === "balance");
      el.tabBalance.setAttribute("aria-pressed", currentCurveTab === "balance" ? "true" : "false");
    }
    if (el.curveTitle) {
      el.curveTitle.textContent = currentCurveTab === "strategy"
        ? "策略权益（不含出入金）"
        : "账户权益（含未实现盈亏与出入金）";
    }
    if (el.windowRow) {
      var buttons = el.windowRow.querySelectorAll("[data-window-hours]");
      for (var i = 0; i < buttons.length; i += 1) {
        var b = buttons[i];
        var h = Number(b.getAttribute("data-window-hours"));
        b.classList.toggle("active", h === currentWindowHours);
        b.setAttribute("aria-pressed", h === currentWindowHours ? "true" : "false");
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
    renderCurveTabState();
    renderEquityChart(activeCurve(d));
    renderRiskMetrics(d);
    renderDrawdownStats(stats, wallet);
    setText(el.openCount, txt(summary.open_positions));
    setText(el.symbolCount, txt(summary.open_symbols));
    setText(el.errorCount, txt(summary.recent_errors));
  }

  function renderDrawdownStats(stats, wallet) {
    if (!el.drawdownStats) return;
    var s = stats || {};
    var w = wallet || {};
    var rows = [
      ["窗口权益变化", fmtSigned(s.total_realized_pnl, 4) + " USDT"],
      ["全部权益变化", fmtSigned(s.all_time_account_pnl, 4) + " USDT"],
      ["窗口最大回撤", fmtNum(s.max_drawdown_pct, 2) + "%"],
      ["窗口出入金", fmtSigned(s.net_cashflow_usdt, 4) + " USDT"]
    ];

    var html = "";
    for (var i = 0; i < rows.length; i += 1) {
      html += '<div class="risk-fact"><span class="risk-fact-label">' + escapeHtml(rows[i][0]) + '</span><strong class="risk-fact-value">' + escapeHtml(rows[i][1]) + '</strong></div>';
    }
    el.drawdownStats.innerHTML = html;
  }

  function refreshCore(options) {
    var opts = options || {};
    var lite = !!opts.lite;
    fetchDashboard(api + "/core", { lite: lite }, function (err, d) {
      if (err) {
        setText(el.meta, "账户数据加载失败：" + err);
        return;
      }

      var previousOpenCount = latestData && latestData.summary
        ? toNum(latestData.summary.open_positions)
        : null;
      mergeLatest(d);
      d = latestData || d;
      var summary = d.summary || {};
      var wallet = d.wallet || {};
      var stats = activeStats(d);
      var svc = d.service || {};
      if (d.timezone) displayTimeZone = String(d.timezone);
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
        "Binance Futures · " + txt(d.timezone) + " · 余额来源 " + txt(wallet.source)
      );
      setText(el.updatedAt, fmtMetaTime(d.generated_at_utc));
      setText(el.nextEntry, fmtAxisTime(d.next_entry_local));
      var svcLabel = svcStatus === "RUNNING" ? "运行中" : (svcStatus === "DISABLED" ? "未启用" : (svcStatus === "STOPPED" ? "已停止" : "异常"));
      setText(el.serviceState, svcLabel);
      if (el.serviceState) {
        var svcClass = "";
        if (svcStatus.indexOf("RUNNING") >= 0) svcClass = "ok";
        else if (svcStatus.indexOf("ERROR") >= 0) svcClass = "bad";
        else if (svcStatus.indexOf("STOPPED") >= 0 || svcStatus.indexOf("DISABLED") >= 0) svcClass = "warn";
        el.serviceState.className = "command-cell-value " + svcClass;
      }
      setText(el.openCount, txt(summary.open_positions));
      setText(el.symbolCount, txt(summary.open_symbols));
      setText(el.errorCount, txt(summary.recent_errors));
      setText(el.lastRunStatus, statusLabel(summary.last_run_status));
      if (el.lastRunStatus) {
        el.lastRunStatus.className = "summary-value " + clsForStatus(summary.last_run_status);
      }
      rerenderFromLatest();
      var currentOpenCount = toNum(summary.open_positions);
      if (
        previousOpenCount !== null
        && currentOpenCount !== null
        && previousOpenCount !== currentOpenCount
      ) {
        refreshDetails();
      }

    });
  }

  function refreshCurveFast() {
    fetchDashboard(api + "/curve", { lite: true }, function (err, d) {
      if (err) {
        if (el.curveTitle) el.curveTitle.textContent = "权益曲线（加载失败，请稍后重试）";
        return;
      }
      mergeCurveOnly(d);
      rerenderFromLatest();
    });
  }

  function summarizeRunMessage(message) {
    var raw = String(message || "").trim();
    if (!raw) return "无执行摘要";
    var values = {};
    raw.split(",").forEach(function (part) {
      var text = String(part || "").trim();
      var eq = text.indexOf("=");
      if (eq <= 0) return;
      values[text.slice(0, eq)] = text.slice(eq + 1);
    });
    var items = [];
    if (values.opened !== undefined) items.push("开仓 " + values.opened);
    if (values.failed !== undefined) items.push("失败 " + values.failed);
    if (values.entry_failed !== undefined && Number(values.entry_failed) > 0) items.push("入场失败 " + values.entry_failed);
    if (values.skipped_existing !== undefined && Number(values.skipped_existing) > 0) items.push("已有持仓 " + values.skipped_existing);
    if (values.planned !== undefined) items.push("计划 " + values.planned);
    if (values.adjusted !== undefined) items.push("调整 " + values.adjusted);
    if (values.errors !== undefined && Number(values.errors) > 0) items.push("异常 " + values.errors);
    return items.length ? items.join(" · ") : raw.slice(0, 140);
  }

  function renderDetails(d) {
      var positions = d.open_positions || [];
      var runs = d.runs || [];
      var events = d.events || [];
      var cashflows = d.cashflow_events || [];
      var unpriced = d.unpriced_closed_details || [];
      setText(el.positionsCount, String(positions.length));
      setText(el.runsCount, String(runs.length));
      setText(el.eventsCount, String(events.length));
      setText(el.cashflowCount, String(cashflows.length));
      setText(el.unpricedCount, String(unpriced.length));

      renderRows(el.positionsBody, positions, function (p) {
        var errorText = String(p.last_error || "").trim();
        var liveAvailable = p.live_data_available === true;
        var readonlyLive = p.readonly_live === true;
        var liveError = String(p.live_data_error || p.order_data_error || p.margin_data_error || "").trim();
        var pnl = toNum(p.unrealized_pnl);
        var pnlPct = toNum(p.unrealized_pnl_pct);
        var notionalPnlPct = toNum(p.unrealized_pnl_notional_pct);
        var marginPnlPct = toNum(p.unrealized_pnl_margin_pct);
        var notional = toNum(p.notional);
        var leverage = toNum(p.leverage);
        var notionalSub = "数量 " + fmtQuantity(p.qty);
        if (leverage !== null) notionalSub += " · 杠杆 " + fmtAdaptive(leverage) + "x";
        var notionalSource = notionalSourceLabel(p.notional_source);
        if (notionalSource) notionalSub += " · " + notionalSource;
        var pnlClass = pnl === null ? "neutral" : (pnl > 0 ? "positive" : (pnl < 0 ? "negative" : "neutral"));
        var pnlMain = pnl === null ? "实时不可用" : fmtSigned(pnl, 2) + " USDT";
        var pnlSub = readonlyLive
          ? "名义收益率 " + (notionalPnlPct === null ? "--" : fmtSigned(notionalPnlPct, 2) + "%") +
            " · 实际保证金收益率 " + (marginPnlPct === null ? "--" : fmtSigned(marginPnlPct, 2) + "%")
          : (pnlPct === null ? "收益率 --" : "收益率 " + fmtSigned(pnlPct, 2) + "%");
        var stateHtml = errorText
          ? '<span class="status-badge bad">异常</span><div class="error-cell">' + escapeHtml(errorText) + "</div>"
          : (readonlyLive
            ? '<span class="status-badge readonly">只读实时</span>'
            : '<span class="status-badge ok">正常</span>');
        if (liveError) {
          stateHtml += '<small class="live-unavailable">' + escapeHtml(liveError) + "</small>";
        } else if (liveAvailable) {
          stateHtml += '<small class="live-sync">' + (readonlyLive ? "交易所实时" : "实时已同步") + "</small>";
        }
        var sourceLabel = readonlyLive ? "交易所实时" : "#" + p.id;
        var protectionHtml = '<div class="exit-stack"><div class="exit-line"><span class="exit-key tp">TP</span><span class="exit-price">' + escapeHtml(fmtAdaptive(p.tp_price)) + '</span>' + liveOrderBadge(p.tp_order_status) + '</div><div class="exit-line"><span class="exit-key sl">SL</span><span class="exit-price">' + escapeHtml(fmtAdaptive(p.sl_price)) + '</span>' + liveOrderBadge(p.sl_order_status) + "</div></div>";
        if (readonlyLive) {
          protectionHtml += '<small class="readonly-protection-note">交易所订单读取</small>';
        }
        return (
          "<tr>" +
          '<td class="symbol-cell"><strong>' + escapeHtml(p.symbol) + '</strong><small>' + escapeHtml(sideLabel(p.side)) + ' · ' + escapeHtml(sourceLabel) + "</small></td>" +
          '<td class="numeric"><div class="position-stack"><strong class="position-primary">' + escapeHtml(notional === null ? "--" : fmtAdaptive(notional) + " USDT") + '</strong><small class="position-secondary">' + escapeHtml(notionalSub) + "</small></div></td>" +
          '<td class="numeric"><div class="position-stack"><strong class="position-primary">' + escapeHtml(fmtAdaptive(p.entry_price)) + '</strong><small class="position-secondary">标记 <strong>' + escapeHtml(liveAvailable ? fmtAdaptive(p.mark_price) : "--") + "</strong></small></div></td>" +
          '<td class="numeric live-pnl ' + pnlClass + '"><div class="position-stack"><strong class="position-primary">' + escapeHtml(pnlMain) + '</strong><small class="position-secondary">' + escapeHtml(pnlSub) + "</small></div></td>" +
          "<td>" + protectionHtml + "</td>" +
          '<td class="numeric">' + escapeHtml(fmtAxisTime(p.expire_at_utc)) + "</td>" +
          "<td>" + stateHtml + "</td>" +
          "</tr>"
        );
      }, 7);

      renderRows(el.runsBody, runs, function (r) {
        var rawMessage = String(r.message || "");
        return (
          "<tr>" +
          '<td class="numeric">' + escapeHtml(fmtDateOnly(r.trade_day_utc)) + "</td>" +
          "<td>" + statusBadge(r.status) + "</td>" +
          '<td class="numeric">' + escapeHtml(fmtAxisTime(r.started_at_utc)) + "</td>" +
          '<td class="message-cell"><div class="message-summary">' + escapeHtml(summarizeRunMessage(rawMessage)) + '</div><details class="row-details"><summary>原始记录</summary><div class="raw-message">' + escapeHtml(rawMessage) + "</div></details></td>" +
          "</tr>"
        );
      }, 4);

      renderRows(el.eventsBody, events, function (e) {
        return (
          "<tr>" +
          '<td class="numeric">' + escapeHtml(fmtAxisTime(e.event_time_utc)) + "</td>" +
          "<td>" + escapeHtml(e.symbol) + "</td>" +
          "<td>" + escapeHtml(orderTypeLabel(e.type)) + "</td>" +
          "<td>" + escapeHtml(sideLabel(e.side)) + "</td>" +
          "<td>" + statusBadge(e.status) + "</td>" +
          '<td class="numeric cell-muted">#' + escapeHtml(e.id) + "</td>" +
          "</tr>"
        );
      }, 6);

      renderRows(el.cashflowBody, cashflows, function (c) {
        var amount = toNum(c.amount);
        var amountClass = amount === null ? "" : (amount > 0 ? "ok" : (amount < 0 ? "bad" : "warn"));
        return (
          "<tr>" +
          '<td class="numeric">' + escapeHtml(fmtAxisTime(c.event_time_utc)) + "</td>" +
          "<td>" + escapeHtml(cashflowTypeLabel(c.income_type)) + "</td>" +
          '<td class="numeric ' + amountClass + '">' + escapeHtml(fmtSigned(c.amount, 4)) + "</td>" +
          "<td>" + escapeHtml(c.asset) + "</td>" +
          "<td>" + escapeHtml(c.symbol) + "</td>" +
          '<td class="numeric cell-muted">#' + escapeHtml(c.id) + "</td>" +
          "</tr>"
        );
      }, 6);

      renderRows(el.unpricedBody, unpriced, function (u) {
        return (
          "<tr>" +
          '<td class="symbol-cell"><strong>' + escapeHtml(u.symbol) + '</strong><small>#' + escapeHtml(u.id) + "</small></td>" +
          "<td>" + statusBadge(u.status) + "</td>" +
          "<td>" + escapeHtml(u.close_reason) + "</td>" +
          "<td>" + escapeHtml(u.detected_reason) + "</td>" +
          '<td class="numeric">' + escapeHtml(u.close_order_id) + "</td>" +
          '<td class="numeric">' + escapeHtml(fmtAxisTime(u.closed_at_utc)) + "</td>" +
          "</tr>"
        );
      }, 6);

      if (el.logTail) {
        var logLines = d.log_tail || [];
        if (Object.prototype.toString.call(logLines) !== "[object Array]") {
          logLines = [];
        }
        el.logTail.textContent = logLines.join("\\n") || "暂无日志";
      }
      fullLoadedOnce = true;
  }

  function refreshDetails() {
    if (detailsInFlight) {
      detailsRefreshPending = true;
      return;
    }
    detailsInFlight = true;
    detailsRefreshPending = false;
    fetchDashboard(api + "/details", { lite: false }, function (err, d) {
      detailsInFlight = false;
      if (err) {
        if (el.positionsBody) {
          el.positionsBody.innerHTML = '<tr><td colspan="7" class="empty-row bad">详情加载失败，请稍后重试</td></tr>';
        }
        return;
      }
      renderDetails(d || {});
      detailsLoaded = true;
      if (detailsRefreshPending) refreshDetails();
    });
  }

  function setupDetailsLazyLoad() {
    var targets = [];
    if (el.positionsBody && el.positionsBody.closest) {
      var panel = el.positionsBody.closest(".positions-panel");
      if (panel) targets.push(panel);
    }
    if (el.logTail && el.logTail.closest) {
      var logPanel = el.logTail.closest(".diagnostics-panel");
      if (logPanel) targets.push(logPanel);
    }

    setTimeout(function () {
      if (!detailsLoaded) refreshDetails();
    }, isMobile ? 1800 : 1200);

    if (!targets.length || !window.IntersectionObserver) {
      return;
    }

    var loaded = false;
    var obs = new IntersectionObserver(function (entries) {
      if (loaded) return;
      for (var i = 0; i < entries.length; i += 1) {
        if (entries[i].isIntersecting) {
          loaded = true;
          refreshDetails();
          obs.disconnect();
          return;
        }
      }
    }, { rootMargin: "240px 0px" });
    for (var j = 0; j < targets.length; j += 1) {
      obs.observe(targets[j]);
    }

    setTimeout(function () {
      if (!loaded && !detailsLoaded) {
        loaded = true;
        refreshDetails();
        obs.disconnect();
      }
    }, isMobile ? 4500 : 3500);
  }

  function setupActivityTabs() {
    var tabs = document.querySelectorAll("[data-activity-target]");
    for (var i = 0; i < tabs.length; i += 1) {
      tabs[i].addEventListener("click", function (evt) {
        var targetId = evt.currentTarget.getAttribute("data-activity-target");
        if (!targetId) return;
        for (var j = 0; j < tabs.length; j += 1) {
          var tab = tabs[j];
          var active = tab.getAttribute("data-activity-target") === targetId;
          tab.classList.toggle("active", active);
          tab.setAttribute("aria-selected", active ? "true" : "false");
          var view = document.getElementById(tab.getAttribute("data-activity-target"));
          if (view) view.hidden = !active;
        }
      });
    }
  }

  applyAccountContext();
  setupActivityTabs();
  renderCurveTabState();
  refreshCurveFast();
  refreshCore({ lite: true });
  ensureEcharts();
  setupDetailsLazyLoad();
  setTimeout(function () {
    refreshCurveFast();
    refreshCore({ lite: false });
  }, isMobile ? 1400 : 500);
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
      refreshCurveFast();
    });
  }
  setInterval(function () {
    refreshTick += 1;
    if (isMobile) {
      var fullEvery = 3;
      refreshCurveFast();
      refreshCore({ lite: (refreshTick % fullEvery) !== 0 });
      if (fullLoadedOnce && (refreshTick % fullEvery) === 0) refreshDetails();
      return;
    }
    refreshCurveFast();
    refreshCore({ lite: false });
    if (fullLoadedOnce && (refreshTick % 2) === 0) refreshDetails();
  }, Math.max(2000, REFRESH_SEC * 1000));
})();
</script>
</body>
</html>
"""


ACCOUNTS_OVERVIEW_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Bubble Buster Overview</title>
  <style>
    :root { --bg:#0a1118; --panel:#101c27cc; --line:#20445a; --text:#eaf6ff; --muted:#8db0c4; --ok:#26d07c; --warn:#ffb340; --bad:#ff5d5d; --accent:#4ec1ff; --spark:#18d29f; --spark-fill:rgba(24,210,159,0.14); }
    * { box-sizing: border-box; }
    body { margin:0; font-family:"Avenir Next","SF Pro Text","PingFang SC","Noto Sans SC",sans-serif; color:var(--text); background:linear-gradient(180deg,#081018 0%,#050a0f 100%); }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 24px; }
    .title { font-size: 24px; font-weight: 700; margin: 0 0 6px; }
    .sub { color: var(--muted); margin: 0 0 18px; }
    .section-title { margin: 22px 0 10px; color:#c7dff0; font-size:16px; font-weight:700; letter-spacing:0.02em; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit,minmax(240px,1fr)); gap: 12px; }
    .card { background: var(--panel); border:1px solid var(--line); border-radius:12px; padding:14px; box-shadow:0 8px 20px rgba(0,0,0,0.25); }
    .row { display:flex; align-items:center; justify-content:space-between; margin-top: 8px; }
    .row.note { align-items:flex-start; margin-top: 6px; }
    .aid { font-size:18px; font-weight:700; color: var(--accent); }
    .label { color: var(--muted); font-size: 12px; }
    .val { font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; }
    .val.text { font-family: "Avenir Next","SF Pro Text","PingFang SC","Noto Sans SC",sans-serif; text-align: right; white-space: normal; max-width: 62%; line-height: 1.35; font-size: 11px; font-weight: 500; color: #b8cddd; display:flex; flex-direction:column; gap:2px; }
    .note-line { display:block; }
    .status-ok { color: var(--ok); }
    .status-warn { color: var(--warn); }
    .status-bad { color: var(--bad); }
    .spark-block { margin-top: 10px; padding-top: 10px; border-top:1px dashed #1e3e52; }
    .spark-title { display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; }
    .spark-box { height:76px; border:1px solid #1d3f53; border-radius:8px; background:linear-gradient(180deg,rgba(11,23,33,0.75) 0%, rgba(9,18,27,0.88) 100%); overflow:hidden; }
    .spark-box.spark-up { --spark:var(--ok); --spark-fill:rgba(38,208,124,0.11); }
    .spark-box.spark-down { --spark:var(--bad); --spark-fill:rgba(255,93,93,0.07); }
    .spark-box.spark-flat { --spark:var(--accent); --spark-fill:rgba(78,193,255,0.12); }
    .spark-svg { width:100%; height:100%; display:block; }
    .spark-path { fill:none; stroke:var(--spark); stroke-width:2; vector-effect:non-scaling-stroke; stroke-linejoin:round; stroke-linecap:round; }
    .spark-area { fill:var(--spark-fill); }
    .spark-empty { display:flex; align-items:center; justify-content:center; height:100%; color:var(--muted); font-size:12px; }
    .spark-up { color: var(--ok); }
    .spark-down { color: var(--bad); }
    .spark-flat { color: var(--accent); }
    .task-panel { margin-top: 22px; }
    .task-panel-head { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; margin-bottom:12px; }
    .task-panel-meta { display:flex; flex-direction:column; align-items:flex-end; gap:8px; }
    .task-updated { color:var(--muted); font-size:12px; }
    .task-filter-bar { display:flex; flex-wrap:wrap; gap:8px; }
    .task-filter-chip { appearance:none; border:1px solid #29536a; background:rgba(12,27,38,0.72); color:#b8d2e3; border-radius:999px; padding:5px 10px; font-size:12px; font-weight:600; cursor:pointer; transition:all 140ms ease; }
    .task-filter-chip:hover { border-color:#4ea6d0; color:#eff8ff; }
    .task-filter-chip.active { background:rgba(78,193,255,0.16); color:#ecf8ff; border-color:#4ec1ff; box-shadow:0 0 0 1px rgba(78,193,255,0.14) inset; }
    .task-board-list { display:block; }
    .task-account-card { background:linear-gradient(180deg,rgba(16,28,39,0.95) 0%, rgba(10,20,30,0.98) 100%); border:1px solid var(--line); border-radius:14px; padding:14px; box-shadow:0 12px 28px rgba(0,0,0,0.24); }
    .task-account-card + .task-account-card { margin-top:12px; }
    .task-account-card.task-account-anomaly { border-color:#5e4032; box-shadow:0 14px 30px rgba(42,15,10,0.22); }
    .task-account-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:10px; }
    .task-account-title { display:flex; align-items:center; flex-wrap:wrap; gap:8px; }
    .task-account-id { color: var(--accent); font-size:17px; font-weight:700; }
    .task-mode-badge, .task-feature-badge { display:inline-flex; align-items:center; border:1px solid #294e62; border-radius:999px; padding:2px 8px; font-size:11px; font-weight:700; letter-spacing:0.02em; color:#d7ecfa; background:rgba(20,41,54,0.8); }
    .task-feature-badge { border-color:#5b6e29; color:#eff5cf; background:rgba(65,83,17,0.26); }
    .task-mode-badge.mode-readonly { border-color:#5b4e62; color:#f0d7fa; background:rgba(65,41,54,0.4); }
    .task-last-run { font-size:12px; }
    .task-table { border:1px solid #1f3f53; border-radius:10px; overflow:hidden; background:linear-gradient(180deg,rgba(8,24,35,0.72) 0%, rgba(6,17,26,0.92) 100%); }
    .task-head, .task-row-main { display:grid; grid-template-columns: minmax(170px,1.1fr) 120px 180px minmax(260px,1.6fr); align-items:start; column-gap:10px; }
    .task-head { background:rgba(14,33,47,0.8); border-bottom:1px solid #21465c; padding:8px 10px; }
    .task-row { border-bottom:1px solid rgba(35,71,92,0.55); padding:9px 10px; }
    .task-row:last-child { border-bottom: none; }
    .task-row-main { min-height: 32px; }
    .task-col-h { color:#84a8bd; font-size:10px; letter-spacing:0.08em; text-transform:uppercase; }
    .task-name-wrap { display:flex; flex-direction:column; gap:3px; min-width:0; }
    .task-name { color:#d9ebf8; font-size:13px; font-weight:700; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .task-task-tag { color:#8db0c4; font-size:11px; }
    .task-result { min-width:0; }
    .task-result-lines { display:flex; flex-direction:column; gap:4px; }
    .task-stat-line, .task-symbol-line { color:#b6d0e0; font-size:11px; line-height:1.4; }
    .task-stat-line { color:#d9ecf8; }
    .task-symbol-line { color:#8fb4c9; display:flex; align-items:flex-start; gap:6px; }
    .task-symbol-label { color:#7fa2b7; flex:0 0 auto; }
    .task-symbol-text { min-width:0; word-break:break-word; }
    .task-meta { display:flex; align-items:center; justify-content:flex-start; }
    .task-badge { border:1px solid #2e5065; border-radius:999px; padding:3px 8px; font-size:11px; font-weight:800; line-height:1.2; min-width:72px; justify-content:center; display:inline-flex; }
    .task-time { color:var(--muted); font-size:11px; font-family: ui-monospace, Menlo, Monaco, Consolas, monospace; padding-top:4px; white-space:nowrap; }
    .task-badge.status-ok { border-color:#1f7148; background:rgba(38,208,124,0.15); }
    .task-badge.status-warn { border-color:#8a6521; background:rgba(255,179,64,0.15); }
    .task-badge.status-bad { border-color:#8d3535; background:rgba(255,93,93,0.15); }
    @media (max-width: 900px) {
      .task-panel-head { flex-direction:column; align-items:flex-start; }
      .task-panel-meta { align-items:flex-start; }
      .task-head, .task-row-main { grid-template-columns: minmax(120px,1fr) 92px 160px minmax(180px,1.2fr); }
    }
    @media (max-width: 640px) {
      .task-account-head { flex-direction:column; align-items:flex-start; }
      .task-head { display:none; }
      .task-row-main { grid-template-columns: 1fr; row-gap:6px; }
      .task-meta, .task-time { padding-top:0; }
      .task-result { border-top:1px dashed rgba(39,75,96,0.7); padding-top:6px; }
    }
    .actions { display:flex; gap:8px; margin-top: 12px; }
    .btn { text-decoration:none; color:#081018; background:var(--accent); border-radius:8px; padding:6px 10px; font-size:12px; font-weight:700; }
    .btn.alt { background: transparent; border: 1px solid var(--line); color: var(--text); }
    .entry-progress-panel { margin-top:22px; }
    .entry-progress-heading { display:flex; align-items:flex-end; justify-content:space-between; gap:16px; margin-bottom:10px; }
    .entry-progress-heading .section-title { margin:0; }
    .entry-progress-updated { color:var(--muted); font-size:12px; }
    .entry-progress-shell { border-top:1px solid #28556d; border-bottom:1px solid #18384c; background:rgba(8,21,31,0.58); }
    .entry-progress-overview { display:flex; align-items:center; gap:28px; min-height:62px; padding:10px 16px; border-bottom:1px solid rgba(35,71,92,0.66); }
    .entry-progress-overview-primary { min-width:170px; }
    .entry-progress-overview-primary strong { display:block; color:#f0f8ff; font-size:20px; line-height:1.2; }
    .entry-progress-overview-primary span { color:var(--muted); font-size:11px; }
    .entry-progress-stat { display:flex; align-items:baseline; gap:6px; color:var(--muted); font-size:11px; }
    .entry-progress-stat strong { color:#dcecf6; font-size:15px; }
    .entry-progress-stat.ok strong { color:var(--ok); }
    .entry-progress-stat.warn strong { color:var(--warn); }
    .entry-progress-stat.bad strong { color:var(--bad); }
    .entry-progress-timeline { display:grid; grid-template-columns:110px minmax(0,1fr); gap:14px; padding:13px 16px 14px; border-bottom:1px solid rgba(35,71,92,0.66); }
    .entry-progress-timeline-label { color:#7fa5bb; font-size:11px; padding-top:2px; }
    .entry-progress-timeline-track { display:flex; align-items:flex-start; gap:0; min-width:0; }
    .entry-progress-timeline-event { position:relative; flex:1 1 0; min-width:0; padding:0 14px 0 18px; border-left:1px solid #2d6078; }
    .entry-progress-timeline-event::before { content:""; position:absolute; left:-4px; top:4px; width:7px; height:7px; border-radius:50%; background:var(--accent); box-shadow:0 0 0 3px #0a1b26; }
    .entry-progress-timeline-event time { display:block; color:#dff1fc; font-size:12px; font-weight:700; margin-bottom:4px; }
    .entry-progress-timeline-event span { display:block; color:#9dbbcb; font-size:10px; line-height:1.45; word-break:break-word; }
    .entry-progress-header, .entry-progress-row { display:grid; grid-template-columns:110px minmax(230px,0.85fr) 130px 150px minmax(210px,1fr); column-gap:18px; align-items:center; }
    .entry-progress-header { min-height:34px; padding:0 16px; color:#7195aa; background:rgba(17,38,52,0.5); font-size:10px; font-weight:700; }
    .entry-progress-row { min-height:68px; padding:10px 16px; border-top:1px solid rgba(35,71,92,0.52); }
    .entry-progress-account { display:flex; flex-direction:column; gap:3px; }
    .entry-progress-account strong { color:var(--accent); font-size:14px; }
    .entry-progress-account span { color:var(--muted); font-size:9px; }
    .entry-progress-meter-wrap { min-width:0; }
    .entry-progress-counts { display:flex; align-items:baseline; gap:10px; margin-bottom:6px; color:#9eb9c9; font-size:10px; }
    .entry-progress-counts strong { color:#f0f8ff; font-size:16px; }
    .entry-progress-meter { display:flex; width:100%; height:5px; overflow:hidden; background:#142a38; }
    .entry-progress-meter-opened { background:var(--ok); }
    .entry-progress-meter-waiting { background:var(--warn); }
    .entry-progress-meter-failed { background:var(--bad); }
    .entry-progress-state { display:flex; flex-direction:column; align-items:flex-start; gap:3px; }
    .entry-progress-status { font-size:11px; font-weight:800; }
    .entry-progress-status.status-warn { color:var(--warn); }
    .entry-progress-next, .entry-progress-window { color:var(--muted); font-size:9px; line-height:1.4; }
    .entry-progress-detail { color:#9db8c8; font-size:10px; line-height:1.5; word-break:break-word; }
    .entry-progress-detail.ok { color:#7898aa; }
    .entry-progress-detail.warn { color:#ffd08a; }
    .entry-progress-detail.bad { color:#ff8c8c; }
    .entry-progress-empty { padding:20px 16px; color:var(--muted); font-size:12px; }
    @media (max-width: 900px) {
      .entry-progress-overview { gap:18px; }
      .entry-progress-header, .entry-progress-row { grid-template-columns:90px minmax(190px,1fr) 110px minmax(180px,1fr); }
      .entry-progress-window, .entry-progress-header span:nth-child(4) { display:none; }
      .entry-progress-timeline-event { padding-right:8px; }
    }
    @media (max-width: 640px) {
      .entry-progress-heading { align-items:flex-start; flex-direction:column; gap:5px; }
      .entry-progress-overview { display:grid; grid-template-columns:1fr 1fr; gap:10px 18px; padding:13px 2px; }
      .entry-progress-overview-primary { grid-column:1 / -1; min-width:0; }
      .entry-progress-timeline { grid-template-columns:1fr; gap:9px; padding:13px 2px; }
      .entry-progress-timeline-track { display:grid; grid-template-columns:1fr 1fr; gap:12px 0; }
      .entry-progress-timeline-event { min-height:44px; }
      .entry-progress-header { display:none; }
      .entry-progress-row { grid-template-columns:1fr; row-gap:8px; min-height:0; padding:13px 2px; }
      .entry-progress-account { flex-direction:row; align-items:baseline; justify-content:space-between; }
      .entry-progress-state { flex-direction:row; align-items:center; justify-content:space-between; }
      .entry-progress-window { display:block; }
    }
  </style>
  <style>
    html { background:#071019; }
    body {
      min-height:100vh;
      background:#071019;
      color:var(--text);
      letter-spacing:0;
    }
    button, a { font:inherit; }
    .wrap {
      width:100%;
      max-width:1680px;
      margin:0 auto;
      padding:12px 16px 28px;
    }
    .command-bar {
      min-height:58px;
      display:grid;
      grid-template-columns:minmax(290px,1.5fr) repeat(4,minmax(130px,0.72fr)) minmax(210px,0.9fr);
      align-items:center;
      border:1px solid #24485d;
      border-radius:12px;
      background:linear-gradient(135deg,#0d1b26,#0b1720);
      overflow:hidden;
    }
    .command-brand {
      min-width:0;
      padding:10px 18px;
    }
    .command-brand-title-row {
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:14px;
    }
    .command-brand .title {
      margin:0;
      font-size:20px;
      line-height:1.2;
      color:#f2f8fc;
      letter-spacing:0;
    }
    .command-brand .sub {
      margin:4px 0 0;
      color:#7899ac;
      font-size:11px;
    }
    .command-brand-link {
      flex:0 0 auto;
      color:#bfe9ff;
      text-decoration:none;
      border:1px solid #2d6078;
      border-radius:5px;
      padding:5px 8px;
      background:#102938;
      font-size:11px;
      font-weight:800;
      transition:color 140ms ease, border-color 140ms ease, background 140ms ease;
    }
    .command-brand-link:hover,
    .command-brand-link:focus-visible {
      color:#f2fbff;
      border-color:#4ec1ff;
      background:#173d52;
      outline:none;
    }
    .command-item {
      min-width:0;
      min-height:34px;
      padding:2px 16px;
      border-left:1px solid #203b4c;
      display:flex;
      flex-direction:column;
      justify-content:center;
      gap:3px;
    }
    .command-label {
      color:#7899ac;
      font-size:11px;
      line-height:1.2;
    }
    .command-value {
      color:#dcebf4;
      font-size:13px;
      font-weight:700;
      line-height:1.25;
      white-space:nowrap;
      overflow:hidden;
      text-overflow:ellipsis;
    }
    .command-value.status-ok { color:var(--ok); }
    .command-value.status-warn { color:var(--warn); }
    .command-value.status-bad { color:var(--bad); }
    .portfolio-stop-command {
      align-self:stretch;
      display:flex;
      align-items:center;
      justify-content:center;
      gap:7px;
      padding:0 18px;
      border-left:1px solid #4d3033;
      color:var(--bad);
      background:#15171d;
      font-size:13px;
      font-weight:800;
      white-space:nowrap;
    }
    .portfolio-stop-command.is-disabled {
      color:var(--muted);
      border-left-color:#203b4c;
      background:#0c1822;
    }
    .status-dot {
      display:inline-block;
      width:7px;
      height:7px;
      margin-right:7px;
      border-radius:50%;
      background:currentColor;
      vertical-align:1px;
    }
    .accounts-area { margin-top:14px; }
    .managed-grid {
      display:grid;
      width:100%;
      max-width:none;
      margin:0;
      grid-template-columns:repeat(4,minmax(0,1fr));
      gap:14px;
    }
    .account-card {
      min-width:0;
      position:relative;
      z-index:1;
      border:1px solid #24485d;
      border-radius:14px;
      background:linear-gradient(165deg,#101d28,#0d1923);
      box-shadow:0 8px 22px rgba(0,0,0,0.22);
      overflow:visible;
    }
    .account-card:hover,
    .account-card:focus-within { z-index:8; }
    .account-card-head {
      position:relative;
      min-height:50px;
      padding:0 16px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:10px;
      border-bottom:1px solid rgba(55,101,127,0.56);
      border-radius:13px 13px 0 0;
      background:rgba(13,25,35,0.72);
    }
    .account-card-head .aid {
      font-size:17px;
      line-height:1;
    }
    .account-head-actions {
      display:flex;
      align-items:center;
      gap:12px;
      min-width:0;
    }
    .venue-state {
      color:#8ba8b9;
      font-size:10px;
      white-space:nowrap;
    }
    .venue-state.status-ok,
    .venue-state.status-warn,
    .venue-state.status-bad { color:inherit; }
    .venue-state .status-dot { color:var(--ok); margin-left:6px; margin-right:0; }
    .venue-state.status-bad .status-dot { color:var(--bad); }
    .venue-state.status-warn .status-dot { color:var(--warn); }
    .detail-link {
      color:#a9c9db;
      text-decoration:none;
      font-size:11px;
      border-left:1px solid rgba(53,91,111,0.7);
      padding-left:10px;
    }
    .detail-link:hover { color:var(--accent); }
    .strategy-popover-wrap {
      position:static;
      flex:0 0 auto;
    }
    .strategy-trigger {
      appearance:none;
      min-height:24px;
      padding:3px 7px;
      border:1px solid #2b5065;
      border-radius:4px;
      background:#10212c;
      color:#b4cfdf;
      font-size:10px;
      font-weight:700;
      cursor:pointer;
    }
    .strategy-trigger:hover,
    .strategy-trigger:focus-visible,
    .strategy-popover-wrap.is-open .strategy-trigger {
      border-color:var(--accent);
      color:#edf8ff;
      outline:none;
    }
    .strategy-popover {
      position:absolute;
      top:calc(100% + 7px);
      right:14px;
      width:290px;
      padding:10px;
      border:1px solid #35657d;
      border-radius:9px;
      background:#09141d;
      box-shadow:0 16px 36px rgba(0,0,0,0.38);
      opacity:0;
      visibility:hidden;
      transform:translateY(-4px);
      pointer-events:none;
      transition:opacity 120ms ease, transform 120ms ease, visibility 120ms ease;
    }
    .strategy-popover::before {
      content:"";
      position:absolute;
      left:0;
      right:0;
      top:-8px;
      height:8px;
    }
    .strategy-popover-wrap:hover .strategy-popover,
    .strategy-popover-wrap:focus-within .strategy-popover,
    .strategy-popover-wrap.is-open .strategy-popover {
      opacity:1;
      visibility:visible;
      transform:translateY(0);
      pointer-events:auto;
    }
    .strategy-popover-head {
      display:flex;
      align-items:baseline;
      justify-content:space-between;
      gap:10px;
      margin-bottom:8px;
    }
    .strategy-popover-title {
      color:#e4f2fa;
      font-size:11px;
      font-weight:800;
    }
    .strategy-popover-subtitle {
      color:#6f91a4;
      font-size:9px;
    }
    .strategy-popover-tags {
      min-width:0;
      display:flex;
      flex-wrap:wrap;
      align-content:flex-start;
      gap:4px;
    }
    .strategy-tag {
      display:inline-flex;
      align-items:center;
      min-height:20px;
      padding:2px 6px;
      border:1px solid #29485a;
      border-radius:3px;
      background:#101f2a;
      color:#9cb7c7;
      font-size:9px;
      font-weight:700;
      line-height:1.25;
      white-space:nowrap;
    }
    .strategy-tag-primary {
      border-color:#316b89;
      background:#102939;
      color:#c9eaff;
    }
    .strategy-tag-protection {
      border-color:#344954;
      background:#111d25;
      color:#9fb3be;
    }
    .strategy-tag-off {
      border-color:#744146;
      background:#29191d;
      color:#ff9a9a;
    }
    .strategy-tag-empty {
      border-color:transparent;
      background:transparent;
      color:#6d8999;
      padding-left:0;
    }
    .account-card-body {
      padding:17px 16px 15px;
      border-radius:0 0 13px 13px;
      background:rgba(13,25,35,0.72);
    }
    .account-primary {
      display:grid;
      grid-template-columns:minmax(0,1.25fr) minmax(0,1fr);
      gap:24px;
      align-items:end;
      padding-bottom:16px;
    }
    .metric-label {
      display:block;
      color:#7899ac;
      font-size:12px;
      line-height:1.25;
      margin-bottom:6px;
    }
    .metric-value {
      display:block;
      color:#edf7fd;
      font-family:ui-monospace,Menlo,Monaco,Consolas,monospace;
      font-size:22px;
      line-height:1.15;
      font-weight:700;
      white-space:nowrap;
    }
    .metric-value.return-value { font-size:23px; }
    .metric-value.status-ok { color:var(--ok); }
    .metric-value.status-warn { color:var(--warn); }
    .metric-value.status-bad { color:var(--bad); }
    .account-secondary {
      display:grid;
      grid-template-columns:repeat(3,minmax(0,1fr));
      border-top:1px solid rgba(55,101,127,0.56);
      border-bottom:0;
      background:rgba(7,20,29,0.22);
    }
    .account-secondary .metric {
      min-width:0;
      padding:12px 10px 11px 0;
    }
    .account-secondary .metric + .metric {
      padding-left:10px;
      border-left:1px solid rgba(55,101,127,0.42);
    }
    .account-secondary .metric-value { font-size:16px; }
    .risk-summary {
      min-height:48px;
      display:flex;
      align-items:center;
      justify-content:space-between;
      gap:12px;
      color:#8ba8b9;
      font-size:12px;
    }
    .risk-summary strong {
      color:#dcebf4;
      font-size:13px;
    }
    .risk-summary strong.status-ok { color:var(--ok); }
    .risk-summary strong.status-warn { color:var(--warn); }
    .risk-summary strong.status-bad { color:var(--bad); }
    .spark-block {
      margin-top:0;
      padding-top:13px;
      border-top:1px solid rgba(55,101,127,0.42);
    }
    .spark-title { margin-bottom:8px; }
    .spark-title .label { font-size:12px; }
    .spark-box {
      height:124px;
      border:0;
      border-radius:0;
      background:#0a151e;
      overflow:hidden;
    }
    .spark-zero-line {
      stroke:#294757;
      stroke-width:1;
      vector-effect:non-scaling-stroke;
    }
    .spark-stop-line {
      stroke:rgba(255,93,93,0.62);
      stroke-width:1;
      stroke-dasharray:4 3;
      vector-effect:non-scaling-stroke;
    }
    .stop-meter {
      margin-top:11px;
      display:grid;
      grid-template-columns:auto minmax(60px,1fr) auto;
      align-items:center;
      gap:8px;
      color:#7899ac;
      font-size:10px;
    }
    .stop-meter-track {
      height:3px;
      background:#213440;
      overflow:hidden;
    }
    .stop-meter-fill {
      display:block;
      width:0;
      height:100%;
      background:var(--ok);
      transition:width 180ms ease;
    }
    .stop-meter-fill.status-warn { background:var(--warn); }
    .stop-meter-fill.status-bad { background:var(--bad); }
    .stop-meter-label.status-warn { color:var(--warn); }
    .stop-meter-label.status-bad { color:var(--bad); }
    .readonly-strip {
      margin-top:14px;
      min-height:96px;
      display:grid;
      grid-template-columns:minmax(190px,1.25fr) repeat(5,minmax(105px,0.85fr)) minmax(280px,2.3fr) auto;
      align-items:center;
      border:1px solid #584561;
      border-radius:14px;
      background:linear-gradient(165deg,#121b29,#101722);
      box-shadow:0 8px 22px rgba(0,0,0,0.18);
      overflow:hidden;
    }
    .readonly-identity,
    .readonly-metric,
    .readonly-chart,
    .readonly-action { min-width:0; padding:15px 16px; }
    .readonly-metric,
    .readonly-chart,
    .readonly-action { border-left:1px solid rgba(74,59,89,0.72); }
    .readonly-id {
      color:#d7a0f0;
      font-size:17px;
      font-weight:800;
    }
    .readonly-badge {
      display:inline-flex;
      margin-left:6px;
      padding:2px 6px;
      border:1px solid #694d77;
      border-radius:4px;
      color:#d7a0f0;
      font-size:10px;
      font-weight:700;
      vertical-align:2px;
    }
    .readonly-source {
      margin-top:5px;
      color:#9b85a5;
      font-size:10px;
    }
    .readonly-metric .metric-value { font-size:15px; }
    .readonly-metric .metric-value.pnl-value { font-size:16px; }
    .readonly-chart {
      display:grid;
      grid-template-columns:minmax(0,1fr) auto;
      gap:10px;
      align-items:center;
    }
    .readonly-chart .spark-box { height:50px; }
    .readonly-chart-meta .metric-label { margin-bottom:3px; }
    .readonly-chart-meta .spark-delta { font-size:15px; }
    .readonly-action .detail-link {
      display:block;
      padding:7px 9px;
      border:1px solid #5a4464;
      border-radius:5px;
      color:#d7b5e5;
      white-space:nowrap;
    }
    .entry-progress-panel, .task-panel { margin-top:12px; }
    .entry-progress-heading, .task-panel-head {
      min-height:36px;
      margin-bottom:0;
      padding:0 2px 8px;
      align-items:center;
    }
    .section-title {
      margin:0;
      color:#dcebf4;
      font-size:14px;
      letter-spacing:0;
    }
    .section-heading-actions {
      display:flex;
      align-items:center;
      gap:12px;
    }
    .entry-progress-updated, .task-updated { font-size:11px; }
    .section-toggle,
    .task-filter-chip {
      appearance:none;
      min-height:28px;
      border:1px solid #2b5065;
      border-radius:5px;
      padding:4px 9px;
      background:#0d1b25;
      color:#a9c6d7;
      font-size:11px;
      font-weight:700;
      cursor:pointer;
    }
    .section-toggle:hover,
    .task-filter-chip:hover { color:#edf8ff; border-color:#4c8cac; }
    .task-filter-chip.active {
      color:#eaf7ff;
      border-color:var(--accent);
      background:#173042;
      box-shadow:none;
    }
    .entry-progress-shell {
      border:1px solid #24485d;
      border-radius:12px;
      background:#0b1721;
      overflow:hidden;
    }
    .entry-progress-overview {
      min-height:58px;
      padding:9px 14px;
      gap:24px;
      border-bottom:1px solid #1f3e51;
    }
    .entry-progress-overview-primary strong { font-size:17px; }
    .entry-progress-overview-primary span,
    .entry-progress-stat { font-size:10px; }
    .entry-progress-stat strong { font-size:14px; }
    .entry-progress-timeline {
      padding:11px 14px 12px;
      border-bottom:0;
    }
    .entry-progress-timeline-event time { font-size:11px; }
    .entry-progress-timeline-event span { font-size:10px; }
    .entry-progress-details {
      border-top:1px solid #1f3e51;
    }
    .entry-progress-details.is-collapsed { display:none; }
    .entry-progress-header { background:#102330; }
    .entry-progress-row { min-height:62px; }
    .task-panel-meta {
      flex-direction:row;
      align-items:center;
      gap:12px;
    }
    .task-filter-bar {
      gap:4px;
      padding:2px;
      border:1px solid #203f52;
      border-radius:6px;
      background:#0a151e;
    }
    .task-filter-chip { border-color:transparent; }
    .task-board-list {
      border:1px solid #24485d;
      border-radius:12px;
      background:#0b1721;
      overflow:hidden;
    }
    .task-account-card {
      border:0;
      border-radius:0;
      background:#0b1721;
      box-shadow:none;
    }
    .task-account-card + .task-account-card {
      margin-top:0;
      border-top:1px solid #24485d;
    }
    .task-account-card.task-account-anomaly {
      border-color:var(--bad);
      box-shadow:none;
      background:#17171d;
    }
    .task-table { background:#09141d; }
    .task-empty {
      min-height:150px;
      display:flex;
      flex-direction:column;
      align-items:center;
      justify-content:center;
      gap:6px;
      color:#7899ac;
      text-align:center;
    }
    .task-empty strong {
      color:#dcebf4;
      font-size:15px;
    }
    .task-empty span { font-size:11px; }
    @media (max-width: 1260px) {
      .command-bar {
        grid-template-columns:minmax(260px,1.25fr) repeat(2,minmax(130px,0.7fr)) minmax(190px,0.9fr);
      }
      .command-item.command-secondary { display:none; }
      .managed-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
      .readonly-strip {
        grid-template-columns:minmax(180px,1.2fr) repeat(5,minmax(90px,0.8fr));
      }
      .readonly-chart,
      .readonly-action {
        grid-column:1 / -1;
        border-left:0;
        border-top:1px solid #302b3c;
      }
    }
    @media (max-width: 760px) {
      .wrap { padding:8px 10px 20px; }
      .command-bar { grid-template-columns:1fr 1fr; }
      .command-brand { grid-column:1 / -1; border-bottom:1px solid #203b4c; }
      .command-brand-title-row { align-items:flex-start; }
      .command-item { border-left:0; padding:8px 14px; }
      .command-item:nth-of-type(even) { border-left:1px solid #203b4c; }
      .portfolio-stop-command {
        min-height:42px;
        border-left:1px solid #4d3033;
      }
      .managed-grid { grid-template-columns:1fr; }
      .account-card-head { min-height:46px; }
      .strategy-popover {
        width:min(290px,calc(100vw - 40px));
      }
      .spark-box { height:118px; }
      .readonly-strip { grid-template-columns:1fr 1fr; }
      .readonly-identity { grid-column:1 / -1; border-bottom:1px solid #302b3c; }
      .readonly-metric,
      .readonly-chart,
      .readonly-action { border-left:0; border-top:1px solid #302b3c; }
      .readonly-chart { grid-column:1 / -1; }
      .readonly-action { grid-column:1 / -1; }
      .entry-progress-heading, .task-panel-head {
        align-items:flex-start;
        flex-direction:column;
        gap:8px;
      }
      .section-heading-actions, .task-panel-meta {
        width:100%;
        justify-content:space-between;
      }
      .entry-progress-overview { padding:12px 10px; }
      .entry-progress-timeline { padding:12px 10px; }
      .task-filter-bar { overflow-x:auto; max-width:100%; }
    }
  </style>
</head>
<body>
  <main class="wrap">
    <header class="command-bar">
      <div class="command-brand">
        <div class="command-brand-title-row">
          <h1 class="title">Bubble Buster 账户总览</h1>
          <a class="command-brand-link" href="accounts/comparison/">权益对比 ↗</a>
        </div>
        <p class="sub">账户、开仓进度与风险状态</p>
      </div>
      <div class="command-item">
        <span class="command-label">服务</span>
        <strong id="service-state" class="command-value status-warn"><span class="status-dot"></span>检测中</strong>
      </div>
      <div class="command-item">
        <span class="command-label">更新时间</span>
        <strong id="overview-updated-at" class="command-value">--</strong>
      </div>
      <div class="command-item command-secondary">
        <span class="command-label">异常</span>
        <strong id="overview-anomaly-count" class="command-value status-ok">0</strong>
      </div>
      <div class="command-item command-secondary">
        <span class="command-label">开单任务</span>
        <strong class="command-value">每天 __ENTRY_TIME__</strong>
      </div>
      <div id="portfolio-stop-command" class="portfolio-stop-command __PORTFOLIO_STOP_CLASS__">
        组合止损 __PORTFOLIO_STOP_LABEL__
      </div>
    </header>
    <span hidden><span id="refresh">__REFRESH_SEC__</span></span>
    <section id="cards" class="accounts-area"></section>
    <section class="entry-progress-panel">
      <div class="entry-progress-heading">
        <h2 class="section-title">今日开单进度</h2>
        <div class="section-heading-actions">
          <div id="entry-progress-updated" class="entry-progress-updated">数据更新时间 --</div>
          <button id="entry-progress-toggle" class="section-toggle" type="button">查看账号明细</button>
        </div>
      </div>
      <div class="entry-progress-shell">
        <div id="entry-progress-board"></div>
      </div>
    </section>
    <section class="task-panel">
      <div class="task-panel-head">
        <div>
          <h2 class="section-title">异常与任务（优先处理）</h2>
        </div>
        <div class="task-panel-meta">
          <div id="task-updated-at" class="task-updated">数据更新时间 --</div>
          <div class="task-filter-bar">
            <button id="task-filter-anomaly" class="task-filter-chip active" type="button">仅异常</button>
            <button id="task-filter-all" class="task-filter-chip" type="button">全部</button>
            <button id="task-filter-symbols" class="task-filter-chip" type="button">仅有symbol明细</button>
          </div>
        </div>
      </div>
      <div id="task-board" class="task-board-list"></div>
    </section>
  </main>
<script>
(function () {
  var refreshSec = Number(document.getElementById("refresh").textContent || "5");
  var pathPrefix = (window.location.pathname || "/").replace(/[/]+$/, "");
  if (!pathPrefix) pathPrefix = "";
  var summaryApi = pathPrefix + "/api/accounts/summary";
  var healthApi = pathPrefix + "/healthz";
  var portfolioStopEnabled = "__PORTFOLIO_STOP_ENABLED__" === "true";
  var portfolioStopPct = Number("__PORTFOLIO_STOP_PCT__") || 3.5;
  var portfolioStopHour = Number("__PORTFOLIO_STOP_HOUR__") || 0;
  var portfolioStopMinute = Number("__PORTFOLIO_STOP_MINUTE__") || 0;
  var cards = document.getElementById("cards");
  var entryProgressBoard = document.getElementById("entry-progress-board");
  var entryProgressUpdated = document.getElementById("entry-progress-updated");
  var entryProgressToggle = document.getElementById("entry-progress-toggle");
  var taskBoard = document.getElementById("task-board");
  var taskUpdatedAt = document.getElementById("task-updated-at");
  var serviceState = document.getElementById("service-state");
  var overviewUpdatedAt = document.getElementById("overview-updated-at");
  var overviewAnomalyCount = document.getElementById("overview-anomaly-count");
  var curveCache = {};
  var curveInFlight = {};
  var curveObserver = null;
  var accountModes = {};
  var portfolioStopStates = {};
  var summaryRows = [];
  var taskFilter = "anomaly";
  var entryDetailsExpanded = false;
  var curveTtlMs = Math.max(60000, Math.max(15, refreshSec) * 3000);

  function escapeHtml(text) {
    return String(text || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function statusCls(status) {
    var s = String(status || "").toUpperCase();
    if (s.indexOf("SUCCESS") >= 0 || s.indexOf("RUNNING") >= 0) return "status-ok";
    if (s.indexOf("SKIPPED") >= 0 || s === "") return "status-warn";
    return "status-bad";
  }

  function taskStatusText(status) {
    var s = String(status || "UNKNOWN").toUpperCase();
    if (s === "SUCCESS") return "SUCCESS";
    if (s === "FAILED") return "FAILED";
    if (s === "PARTIAL") return "PARTIAL";
    if (s === "SKIPPED") return "SKIPPED";
    if (s === "RUNNING") return "RUNNING";
    return "UNKNOWN";
  }

  function taskStatusCls(status) {
    var s = String(status || "UNKNOWN").toUpperCase();
    if (s === "SUCCESS" || s === "RUNNING") return "status-ok";
    if (s === "PARTIAL" || s === "SKIPPED" || s === "UNKNOWN") return "status-warn";
    return "status-bad";
  }

  function localTimestamp(value) {
    var raw = String(value || "");
    if (!raw) return "--";
    var date = new Date(raw);
    if (!Number.isFinite(date.getTime())) return raw;
    try {
      return new Intl.DateTimeFormat("zh-CN", {
        timeZone: "Asia/Shanghai",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hour12: false
      }).format(date).replace(/\\//g, "-");
    } catch (e) {
      return raw.slice(5, 19).replace("T", " ");
    }
  }

  function anomalyTaskCount(rows) {
    var total = 0;
    for (var i = 0; i < (rows || []).length; i += 1) {
      var tasks = ((rows[i] || {}).tasks) || {};
      var keys = Object.keys(tasks);
      for (var j = 0; j < keys.length; j += 1) {
        var status = String(((tasks[keys[j]] || {}).status) || "").toUpperCase();
        if (status === "FAILED" || status === "PARTIAL") total += 1;
      }
    }
    return total;
  }

  function renderCommandBar(payload) {
    if (overviewUpdatedAt) {
      overviewUpdatedAt.textContent = localTimestamp((payload || {}).generated_at_utc);
    }
    if (overviewAnomalyCount) {
      var count = anomalyTaskCount(summaryRows);
      overviewAnomalyCount.textContent = String(count);
      overviewAnomalyCount.className = "command-value " + (count > 0 ? "status-bad" : "status-ok");
    }
  }

  function fetchHealth() {
    if (!serviceState) return;
    var xhr = new XMLHttpRequest();
    xhr.open("GET", healthApi + "?_=" + Date.now(), true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      if (xhr.status < 200 || xhr.status >= 300) {
        serviceState.innerHTML = '<span class="status-dot"></span>未知';
        serviceState.className = "command-value status-warn";
        return;
      }
      var payload = {};
      try { payload = JSON.parse(xhr.responseText || "{}"); } catch (e) {}
      var running = !!payload.service_running;
      var enabled = !!payload.service_enabled;
      serviceState.innerHTML = '<span class="status-dot"></span>'
        + (running ? "RUNNING" : (enabled ? "STOPPED" : "未启用"));
      serviceState.className = "command-value " + (running ? "status-ok" : (enabled ? "status-bad" : "status-warn"));
    };
    xhr.send();
  }

  function parseSummaryPairs(summary) {
    var raw = String(summary || "--").trim();
    var pairs = {};
    if (!raw || raw === "--") return pairs;
    var parts = raw.split(/\\s+/);
    for (var i = 0; i < parts.length; i += 1) {
      var part = String(parts[i] || "");
      if (!part) continue;
      var eq = part.indexOf("=");
      if (eq > 0 && eq < part.length - 1) {
        var key = part.slice(0, eq);
        var val = part.slice(eq + 1);
        pairs[key] = val;
      }
    }
    return pairs;
  }

  function taskTimeValue(task) {
    var raw = String((task && task.time_local) || "");
    return raw ? raw : "";
  }

  function latestTaskTimeForAccount(row) {
    var tasks = (row && row.tasks) || {};
    var keys = Object.keys(tasks);
    var best = "";
    for (var i = 0; i < keys.length; i += 1) {
      var val = taskTimeValue(tasks[keys[i]]);
      if (val && (!best || val > best)) best = val;
    }
    return best;
  }

  function latestTaskTime(rows) {
    var best = "";
    for (var i = 0; i < rows.length; i += 1) {
      var val = latestTaskTimeForAccount(rows[i]);
      if (val && (!best || val > best)) best = val;
    }
    return best;
  }

  function hasAnomalyTask(row) {
    var tasks = (row && row.tasks) || {};
    var keys = Object.keys(tasks);
    for (var i = 0; i < keys.length; i += 1) {
      var status = String(((tasks[keys[i]] || {}).status) || "UNKNOWN").toUpperCase();
      if (status === "FAILED" || status === "PARTIAL") return true;
    }
    return false;
  }

  function hasSymbolDetails(row) {
    var tasks = (row && row.tasks) || {};
    var keys = Object.keys(tasks);
    for (var i = 0; i < keys.length; i += 1) {
      var pairs = parseSummaryPairs((tasks[keys[i]] || {}).summary || "--");
      var pairKeys = Object.keys(pairs);
      for (var j = 0; j < pairKeys.length; j += 1) {
        if (pairKeys[j].indexOf("_symbols") > 0) return true;
      }
    }
    return false;
  }

  function sortTaskAccounts(rows) {
    return (rows || []).slice();
  }

  function visibleTaskAccounts(rows) {
    var sorted = sortTaskAccounts(rows || []);
    if (taskFilter === "anomaly") {
      return sorted.filter(hasAnomalyTask);
    }
    if (taskFilter === "symbols") {
      return sorted.filter(hasSymbolDetails);
    }
    return sorted;
  }

  function taskMetaLabel(taskKey) {
    if (taskKey === "equity_recovery_take_profit") return "巡检内触发";
    if (taskKey === "manage") return "例行巡检";
    return "";
  }

  function symbolList(rawValue) {
    var raw = String(rawValue || "").trim();
    if (!raw || raw === "-" || raw === "--") return [];
    return raw.split(",").map(function (item) {
      return String(item || "").trim();
    }).filter(Boolean);
  }

  function formatTaskResultLines(taskKey, task) {
    var t = task || {};
    var pairs = parseSummaryPairs(String(t.summary || "--"));
    var statParts = [];
    var symbolLines = [];
    function addSymbols(label, key) {
      var items = symbolList(pairs[key]);
      if (!items.length) return;
      symbolLines.push({ label: label, key: key, symbols: items });
    }
    if (taskKey === "entry") {
      statParts.push("opened=" + (pairs.opened || "0"));
      statParts.push("failed=" + (pairs.failed || "0"));
      statParts.push("skipped=" + (pairs.skipped || "0"));
      addSymbols("failed", "failed_symbols");
      addSymbols("skipped", "skipped_symbols");
    } else if (taskKey === "daily_loss_cut") {
      statParts.push("total=" + (pairs.total || "0"));
      statParts.push("closed=" + (pairs.closed || "0"));
      statParts.push("errors=" + (pairs.errors || "0"));
      addSymbols("closed", "closed_symbols");
      addSymbols("failed", "failed_symbols");
    } else if (taskKey === "noon_protection") {
      statParts.push("total=" + (pairs.total || "0"));
      statParts.push("updated=" + (pairs.updated || "0"));
      statParts.push("skipped=" + (pairs.skipped || "0"));
      statParts.push("errors=" + (pairs.errors || "0"));
      addSymbols("failed", "failed_symbols");
    } else if (taskKey === "manage") {
      if (pairs.reason) statParts.push("reason=" + pairs.reason);
      if (pairs.error) statParts.push("error=" + pairs.error);
      if (pairs.total) statParts.push("total=" + pairs.total);
      if (pairs.tp) statParts.push("tp=" + pairs.tp);
      if (pairs.sl) statParts.push("sl=" + pairs.sl);
      if (pairs.timeout) statParts.push("timeout=" + pairs.timeout);
      if (pairs.updated) statParts.push("updated=" + pairs.updated);
      if (pairs.errors) statParts.push("errors=" + pairs.errors);
    } else if (taskKey === "equity_recovery_take_profit") {
      statParts.push("adjusted=" + (pairs.adjusted || "0"));
      statParts.push("errors=" + (pairs.errors || "0"));
      if (pairs.reduced) statParts.push("reduced=" + pairs.reduced);
    } else {
      var keys = Object.keys(pairs);
      for (var i = 0; i < keys.length; i += 1) {
        statParts.push(keys[i] + "=" + pairs[keys[i]]);
      }
    }
    var fullText = statParts.join(" | ");
    for (var j = 0; j < symbolLines.length; j += 1) {
      fullText += (fullText ? "\\n" : "") + symbolLines[j].label + ": " + symbolLines[j].symbols.join(", ");
    }
    return {
      statLine: statParts.join(" | ") || "--",
      symbolLines: symbolLines,
      tooltip: fullText || "--",
    };
  }

  function taskResultHtml(accountId, taskKey, task) {
    var formatted = formatTaskResultLines(taskKey, task);
    var html = '<div class="task-result-lines">'
      + '<div class="task-stat-line">' + escapeHtml(formatted.statLine) + "</div>";
    for (var i = 0; i < formatted.symbolLines.length; i += 1) {
      var line = formatted.symbolLines[i];
      html += '<div class="task-symbol-line">'
        + '<span class="task-symbol-label">' + escapeHtml(line.label + ":") + "</span>"
        + '<span class="task-symbol-text">' + escapeHtml(line.symbols.join(", ")) + "</span>"
        + "</div>";
    }
    html += "</div>";
    return html;
  }

  function taskRowHtml(accountId, taskKey, name, task) {
    var t = task || {};
    var status = String(t.status || "UNKNOWN");
    var statusText = taskStatusText(status);
    var cls = taskStatusCls(status);
    var timeRaw = String(t.time_local || "");
    var timeText = timeRaw || "--";
    var metaText = taskMetaLabel(taskKey);
    return '<div class="task-row">'
      + '<div class="task-row-main">'
      + '<div class="task-name-wrap"><span class="task-name">' + escapeHtml(name) + '</span>'
      + (metaText ? '<span class="task-task-tag">' + escapeHtml(metaText) + "</span>" : "")
      + "</div>"
      + '<span class="task-meta"><span class="task-badge ' + cls + '">' + escapeHtml(statusText) + "</span></span>"
      + '<span class="task-time">' + escapeHtml(timeText) + "</span>"
      + '<span class="task-result">' + taskResultHtml(accountId, taskKey, t) + "</span>"
      + "</div>"
      + "</div>";
  }

  function taskRowsForAccount(row) {
    var mode = String((row && row.mode) || "full").toLowerCase();
    var tasks = (row && row.tasks) || {};
    var rows = [];
    if (mode === "readonly") {
      // readonly 模式显示交易统计
      var stats = row && row.trade_stats;
      if (stats) {
        var profitFactor = stats.profit_factor != null ? stats.profit_factor.toFixed(2) : "--";
        var statsHtml = '<div class="task-row">'
          + '<div class="task-row-main" style="grid-template-columns: minmax(170px,1.1fr) 120px 180px minmax(260px,1.6fr);">'
          + '<div class="task-name-wrap"><span class="task-name">交易统计(近30日)</span>'
          + '<span class="task-task-tag">只读监控</span></div>'
          + '<span class="task-meta"><span class="task-badge status-ok">READONLY</span></span>'
          + '<span class="task-time">' + escapeHtml(String(stats.last_updated_utc || "").slice(0, 16)) + '</span>'
          + '<div class="task-result-lines">'
          + '<div class="task-stat-line">总盈亏=' + fmt(stats.total_realized_pnl, 4) + ' | 胜率=' + fmt(stats.win_rate_pct, 1) + '% | 盈亏比=' + profitFactor + '</div>'
          + '<div class="task-stat-line">胜=' + stats.win_count + ' 负=' + stats.loss_count + ' | 平均盈利=' + fmt(stats.avg_win, 4) + ' | 平均亏损=' + fmt(stats.avg_loss, 4) + '</div>'
          + '</div></div></div>';
        return statsHtml;
      }
      // 没有统计数据时显示简化信息
      return '<div class="task-row">'
        + '<div class="task-row-main" style="grid-template-columns: minmax(170px,1.1fr) 120px 180px minmax(260px,1.6fr);">'
        + '<div class="task-name-wrap"><span class="task-name">账户监控</span>'
        + '<span class="task-task-tag">只读监控</span></div>'
        + '<span class="task-meta"><span class="task-badge status-warn">READONLY</span></span>'
        + '<span class="task-time">--</span>'
        + '<div class="task-result-lines"><div class="task-stat-line">暂无交易统计数据</div></div>'
        + '</div></div>';
    }
    if (mode === "full") {
      rows.push({ key: "entry", name: "开仓(entry)", task: tasks.entry });
    }
    rows.push({ key: "daily_loss_cut", name: "浮亏砍仓", task: tasks.daily_loss_cut });
    rows.push({ key: "noon_protection", name: "中午保护", task: tasks.noon_protection });
    if (mode === "full") {
      rows.push({ key: "manage", name: "巡检(manage)", task: tasks.manage });
      if (row && row.equity_recovery_take_profit_enabled) {
        rows.push({ key: "equity_recovery_take_profit", name: "组合止盈监控", task: tasks.equity_recovery_take_profit });
      }
    }
    var html = "";
    for (var i = 0; i < rows.length; i += 1) {
      html += taskRowHtml(String((row && row.account_id) || ""), rows[i].key, rows[i].name, rows[i].task);
    }
    return html;
  }

  function fmt(v, digits) {
    if (v === null || v === undefined || v === "") return "--";
    var n = Number(v);
    if (!Number.isFinite(n)) return String(v);
    return n.toFixed(digits);
  }

  function formatStrategyNote(note) {
    var raw = String(note || "").trim();
    if (!raw) return '<span class="strategy-tag strategy-tag-empty">未配置策略说明</span>';
    var parts = raw.split("/").map(function (s) { return String(s || "").trim(); }).filter(Boolean);
    var html = "";
    for (var i = 0; i < parts.length; i += 1) {
      var part = parts[i];
      var upper = part.toUpperCase();
      var cls = "strategy-tag strategy-tag-protection";
      if (upper.indexOf("OFF") >= 0) {
        cls = "strategy-tag strategy-tag-off";
      } else if (
        upper.indexOf("TP") >= 0
        || part.indexOf("减仓") >= 0
        || part.indexOf("清仓") >= 0
        || part.indexOf("组合止盈") >= 0
      ) {
        cls = "strategy-tag strategy-tag-primary";
      }
      html += '<span class="' + cls + '">' + escapeHtml(part) + '</span>';
    }
    return html || '<span class="strategy-tag strategy-tag-empty">未配置策略说明</span>';
  }

  function entryProgressStatus(status) {
    var raw = String(status || "UNKNOWN").toUpperCase();
    if (raw === "COMPLETED") return { text: "已完成", cls: "status-ok" };
    if (raw === "WAITING") return { text: "等待1h阴线", cls: "status-warn" };
    if (raw === "RUNNING") return { text: "执行中", cls: "status-warn" };
    if (raw === "PARTIAL") return { text: "部分完成", cls: "status-warn" };
    if (raw === "SKIPPED") return { text: "已跳过", cls: "status-warn" };
    if (raw === "FAILED") return { text: "失败", cls: "status-bad" };
    return { text: "未开始", cls: "status-warn" };
  }

  function compactLocalTime(value) {
    var raw = String(value || "");
    return raw.length >= 16 ? raw.slice(11, 16) : raw;
  }

  function progressItems(value) {
    return Object.prototype.toString.call(value) === "[object Array]" ? value : [];
  }

  function entryProgressKey(item) {
    var row = item || {};
    return String(row.symbol || "") + "|" + compactLocalTime(row.opened_at_local);
  }

  function buildCommonEntryTimeline(progressRows) {
    var counts = {};
    var samples = {};
    for (var i = 0; i < progressRows.length; i += 1) {
      var openedRows = progressItems(progressRows[i].opened_symbols);
      var seen = {};
      for (var j = 0; j < openedRows.length; j += 1) {
        var item = openedRows[j] || {};
        var key = entryProgressKey(item);
        if (!item.symbol || seen[key]) continue;
        seen[key] = true;
        counts[key] = Number(counts[key] || 0) + 1;
        samples[key] = item;
      }
    }
    var commonKeys = {};
    var grouped = {};
    var expected = progressRows.length;
    Object.keys(counts).forEach(function (key) {
      if (!expected || counts[key] !== expected) return;
      commonKeys[key] = true;
      var item = samples[key] || {};
      var time = compactLocalTime(item.opened_at_local) || "--";
      if (!grouped[time]) grouped[time] = [];
      grouped[time].push(String(item.symbol || ""));
    });
    var groups = Object.keys(grouped).sort().map(function (time) {
      return { time: time, symbols: grouped[time].sort() };
    });
    return { groups: groups, commonKeys: commonKeys };
  }

  function entryProgressWindow(progress) {
    var times = progressItems(progress.opened_symbols).map(function (item) {
      return compactLocalTime((item || {}).opened_at_local);
    }).filter(Boolean).sort();
    if (!times.length) return "--";
    if (times[0] === times[times.length - 1]) return "开仓 " + times[0];
    return "首单 " + times[0] + " · 末单 " + times[times.length - 1];
  }

  function entryProgressDetail(progress, commonKeys) {
    var waitingRows = progressItems(progress.waiting_symbols);
    if (waitingRows.length) {
      var waitingSymbols = waitingRows.map(function (item) {
        return String((item || {}).symbol || "");
      }).filter(Boolean);
      var nextCheck = compactLocalTime(progress.next_check_local);
      return {
        text: "等待 " + (waitingSymbols.join(" · ") || String(progress.waiting_count || 0))
          + (nextCheck ? " · 下次 " + nextCheck : ""),
        cls: "warn"
      };
    }
    var failed = Math.max(0, Number(progress.failed_count || 0));
    if (failed > 0) return { text: "失败 " + failed + "，请查看任务日志", cls: "bad" };
    var differences = [];
    var openedRows = progressItems(progress.opened_symbols);
    for (var i = 0; i < openedRows.length; i += 1) {
      var item = openedRows[i] || {};
      var key = entryProgressKey(item);
      if (!item.symbol || commonKeys[key]) continue;
      differences.push(String(item.symbol) + " " + (compactLocalTime(item.opened_at_local) || "--"));
    }
    if (differences.length) return { text: "差异开仓 " + differences.join(" · "), cls: "warn" };
    return { text: "与共同榜单一致", cls: "ok" };
  }

  function renderEntryProgress() {
    if (!entryProgressBoard) return;
    var rows = summaryRows || [];
    var accountRows = [];
    var todayRows = [];
    var latest = "";
    for (var i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      if (String(row.mode || "full").toLowerCase() !== "full") continue;
      accountRows.push(row);
      var progress = row.entry_progress || null;
      if (progress && progress.is_today !== false) todayRows.push(progress);
      var updated = String((progress && progress.updated_at_local) || "");
      if (updated && (!latest || updated > latest)) latest = updated;
    }
    if (!accountRows.length) {
      entryProgressBoard.innerHTML = '<div class="entry-progress-empty">暂无完整策略账号</div>';
      if (entryProgressUpdated) entryProgressUpdated.textContent = "数据更新时间 --";
      if (entryProgressToggle) entryProgressToggle.hidden = true;
      return;
    }

    var completedAccounts = 0;
    var targetTotal = 0;
    var openedTotal = 0;
    var waitingTotal = 0;
    var failedTotal = 0;
    for (var j = 0; j < todayRows.length; j += 1) {
      var summary = todayRows[j];
      if (String(summary.status || "").toUpperCase() === "COMPLETED") completedAccounts += 1;
      targetTotal += Math.max(0, Number(summary.target_count || 0));
      openedTotal += Math.max(0, Number(summary.opened_count || 0));
      waitingTotal += Math.max(0, Number(summary.waiting_count || 0));
      failedTotal += Math.max(0, Number(summary.failed_count || 0));
    }
    var timeline = buildCommonEntryTimeline(todayRows);
    var hasIssues = todayRows.length !== accountRows.length
      || completedAccounts !== accountRows.length
      || waitingTotal > 0
      || failedTotal > 0
      || !timeline.groups.length;
    var showDetails = hasIssues || entryDetailsExpanded;
    if (entryProgressToggle) {
      entryProgressToggle.hidden = hasIssues;
      entryProgressToggle.textContent = showDetails ? "收起账号明细" : "查看账号明细";
    }
    var html = '<div class="entry-progress-overview">'
      + '<div class="entry-progress-overview-primary"><strong>' + completedAccounts + ' / ' + accountRows.length + ' 账号完成</strong><span>今日榜单执行概览</span></div>'
      + '<div class="entry-progress-stat ok"><strong>' + openedTotal + '</strong><span>/ ' + targetTotal + ' 已开</span></div>'
      + '<div class="entry-progress-stat warn"><strong>' + waitingTotal + '</strong><span>等待</span></div>'
      + '<div class="entry-progress-stat bad"><strong>' + failedTotal + '</strong><span>失败</span></div>'
      + '</div>';
    html += '<div class="entry-progress-timeline"><div class="entry-progress-timeline-label">共同开仓时间线</div><div class="entry-progress-timeline-track">';
    if (timeline.groups.length) {
      for (var k = 0; k < timeline.groups.length; k += 1) {
        var group = timeline.groups[k];
        html += '<div class="entry-progress-timeline-event"><time>' + escapeHtml(group.time) + '</time><span>'
          + escapeHtml(group.symbols.join(" · ")) + '</span></div>';
      }
    } else {
      html += '<div class="entry-progress-detail warn">各账号榜单存在差异，见下方账号明细</div>';
    }
    html += '</div></div>'
      + '<div class="entry-progress-details' + (showDetails ? "" : " is-collapsed") + '">'
      + '<div class="entry-progress-header"><span>账号</span><span>完成度</span><span>状态</span><span>时间</span><span>差异 / 等待</span></div>';

    for (var n = 0; n < accountRows.length; n += 1) {
      var account = accountRows[n] || {};
      var progress = account.entry_progress || null;
      var aid = escapeHtml(String(account.account_id || ""));
      if (!progress || progress.is_today === false) {
        html += '<div class="entry-progress-row">'
          + '<div class="entry-progress-account"><strong>' + aid + '</strong><span>今日尚未执行</span></div>'
          + '<div class="entry-progress-meter-wrap"><div class="entry-progress-counts"><strong>0</strong><span>/ 0</span></div><div class="entry-progress-meter"></div></div>'
          + '<div class="entry-progress-state"><span class="entry-progress-status status-warn">未开始</span><span class="entry-progress-next">--</span></div>'
          + '<div class="entry-progress-window">--</div><div class="entry-progress-detail warn">等待今日榜单</div></div>';
        continue;
      }
      var target = Math.max(0, Number(progress.target_count || 0));
      var opened = Math.max(0, Number(progress.opened_count || 0));
      var waiting = Math.max(0, Number(progress.waiting_count || 0));
      var failed = Math.max(0, Number(progress.failed_count || 0));
      var entryFailed = Math.max(0, Number(progress.entry_failed_count == null ? failed : progress.entry_failed_count));
      var denom = target > 0 ? target : Math.max(1, opened + waiting + failed);
      var openedPct = Math.min(100, opened / denom * 100);
      var waitingPct = Math.min(100 - openedPct, waiting / denom * 100);
      var failedPct = Math.min(100 - openedPct - waitingPct, entryFailed / denom * 100);
      var status = entryProgressStatus(progress.status);
      var nextText = progress.next_check_local ? ("下次 " + compactLocalTime(progress.next_check_local)) : "无等待任务";
      var detail = entryProgressDetail(progress, timeline.commonKeys);
      html += '<div class="entry-progress-row">'
        + '<div class="entry-progress-account"><strong>' + aid + '</strong><span>' + escapeHtml(String(progress.started_at_local || "--")) + '</span></div>'
        + '<div class="entry-progress-meter-wrap"><div class="entry-progress-counts"><strong>' + opened + '</strong><span>/ ' + target + ' 已开</span>'
        + (waiting ? '<span>等待 ' + waiting + '</span>' : '') + (failed ? '<span>失败 ' + failed + '</span>' : '') + '</div>'
        + '<div class="entry-progress-meter"><span class="entry-progress-meter-opened" style="width:' + openedPct.toFixed(2) + '%"></span><span class="entry-progress-meter-waiting" style="width:' + waitingPct.toFixed(2) + '%"></span><span class="entry-progress-meter-failed" style="width:' + failedPct.toFixed(2) + '%"></span></div></div>'
        + '<div class="entry-progress-state"><span class="entry-progress-status ' + status.cls + '">' + status.text + '</span><span class="entry-progress-next">' + escapeHtml(nextText) + '</span></div>'
        + '<div class="entry-progress-window">' + escapeHtml(entryProgressWindow(progress)) + '</div>'
        + '<div class="entry-progress-detail ' + detail.cls + '">' + escapeHtml(detail.text) + '</div></div>';
    }
    html += "</div>";
    entryProgressBoard.innerHTML = html;
    if (entryProgressUpdated) entryProgressUpdated.textContent = "数据更新时间 " + (latest || "--");
  }

  function createObserver() {
    if (curveObserver || !window.IntersectionObserver) return;
    curveObserver = new IntersectionObserver(function (entries) {
      for (var i = 0; i < entries.length; i += 1) {
        var ent = entries[i];
        if (!ent.isIntersecting) continue;
        var aid = ent.target.getAttribute("data-account-id") || "";
        if (aid) fetchCurve(aid);
        curveObserver.unobserve(ent.target);
      }
    }, { rootMargin: "200px 0px" });
  }

  function accountElement(className, aid) {
    var nodes = document.getElementsByClassName(className);
    for (var i = 0; i < nodes.length; i += 1) {
      if (String(nodes[i].getAttribute("data-account-id") || "") === String(aid || "")) {
        return nodes[i];
      }
    }
    return null;
  }

  function drawdownClass(value) {
    if (value <= -(portfolioStopPct * 0.7)) return "status-bad";
    if (value < 0) return "status-warn";
    return "status-ok";
  }

  function cycleStartMs(referenceMs) {
    var shanghaiOffsetMs = 8 * 60 * 60 * 1000;
    var local = new Date(referenceMs + shanghaiOffsetMs);
    var start = Date.UTC(
      local.getUTCFullYear(),
      local.getUTCMonth(),
      local.getUTCDate(),
      portfolioStopHour,
      portfolioStopMinute
    ) - shanghaiOffsetMs;
    if (referenceMs < start) start -= 24 * 60 * 60 * 1000;
    return start;
  }

  function curveMetrics(rawPoints, mode) {
    var points = (rawPoints || []).filter(function (point) {
      return point && Number.isFinite(Number(point.equity));
    });
    if (points.length < 2) return null;
    var startIndex = 0;
    if (mode !== "readonly") {
      var latestMs = new Date(points[points.length - 1].t || "").getTime();
      if (Number.isFinite(latestMs)) {
        var targetMs = cycleStartMs(latestMs);
        var afterIndex = -1;
        for (var i = 0; i < points.length; i += 1) {
          var pointMs = new Date(points[i].t || "").getTime();
          if (!Number.isFinite(pointMs)) continue;
          if (afterIndex < 0 && pointMs >= targetMs) afterIndex = i;
        }
        if (afterIndex < 0) return null;
        startIndex = afterIndex;
      }
    }
    var scoped = points.slice(startIndex);
    if (scoped.length < 2) return null;
    var baseline = Number(scoped[0].equity);
    if (!Number.isFinite(baseline) || baseline === 0) return null;
    var values = scoped.map(function (point) {
      return ((Number(point.equity) - baseline) / baseline) * 100;
    });
    var current = values[values.length - 1];
    var peak = values[0];
    for (var j = 1; j < values.length; j += 1) {
      if (values[j] > peak) peak = values[j];
    }
    return {
      values: values,
      currentReturnPct: current,
      currentDrawdownPct: current - peak,
      stopDistancePct: current + portfolioStopPct
    };
  }

  function chartGeometry(values, width, height, includeStop) {
    var min = 0;
    var max = 0;
    for (var i = 0; i < values.length; i += 1) {
      if (values[i] < min) min = values[i];
      if (values[i] > max) max = values[i];
    }
    if (includeStop) min = Math.min(min, -portfolioStopPct);
    var rawSpan = max - min;
    var padding = Math.max(0.2, rawSpan * 0.12);
    min -= padding;
    max += padding;
    var span = Math.max(0.4, max - min);
    function yFor(value) {
      return height - ((value - min) / span) * height;
    }
    var coords = [];
    for (var j = 0; j < values.length; j += 1) {
      var x = (j / (values.length - 1)) * width;
      coords.push(x.toFixed(2) + "," + yFor(values[j]).toFixed(2));
    }
    return {
      points: coords.join(" "),
      zeroY: yFor(0),
      stopY: yFor(-portfolioStopPct)
    };
  }

  function setAccountMetric(className, aid, text, cls) {
    var el = accountElement(className, aid);
    if (!el) return;
    el.textContent = text;
    el.classList.remove("status-ok", "status-warn", "status-bad", "spark-up", "spark-down", "spark-flat");
    if (cls) el.classList.add(cls);
    el.setAttribute("data-account-id", aid);
  }

  function renderPortfolioStopState(aid, stop, metrics) {
    var meterLabel = accountElement("stop-meter-label", aid);
    var meter = accountElement("stop-meter-fill", aid);
    if (metrics) {
      setAccountMetric(
        "current-drawdown",
        aid,
        fmt(metrics.currentDrawdownPct, 2) + "%",
        drawdownClass(metrics.currentDrawdownPct)
      );
    } else {
      setAccountMetric("current-drawdown", aid, "--", "");
    }
    if (!portfolioStopEnabled) {
      setAccountMetric("stop-distance", aid, "未启用", "status-warn");
      setAccountMetric("risk-state", aid, "组合止损未启用", "status-warn");
      if (meterLabel) {
        meterLabel.textContent = "未启用";
        meterLabel.className = "stop-meter-label status-warn";
      }
      if (meter) {
        meter.style.width = "0%";
        meter.className = "stop-meter-fill status-warn";
      }
      return;
    }
    if (!stop || !stop.status) {
      setAccountMetric("stop-distance", aid, "状态待同步", "status-warn");
      setAccountMetric("risk-state", aid, "等待后端止损状态", "status-warn");
      if (meterLabel) {
        meterLabel.textContent = "待同步";
        meterLabel.className = "stop-meter-label status-warn";
      }
      if (meter) {
        meter.style.width = "0%";
        meter.className = "stop-meter-fill status-warn";
      }
      return;
    }

    var status = String(stop.status || "").toUpperCase();
    var rawDistance = stop.distance_pct;
    var distance = rawDistance === null || rawDistance === undefined ? NaN : Number(rawDistance);
    var hasDistance = Number.isFinite(distance);
    var riskCls = "status-ok";
    var riskText = "正常";
    var stopDistanceText = hasDistance ? fmt(distance, 2) + "%" : "--";
    var meterText = "安全距离";
    var meterPct = hasDistance
      ? Math.max(0, Math.min(100, distance / (portfolioStopPct * 2) * 100))
      : 0;

    if (status === "TRIGGERED_COMPLETE") {
      riskCls = "status-bad";
      riskText = "组合止损已完成";
      stopDistanceText = "已完成";
      meterText = "本周期已止损";
      meterPct = 0;
    } else if (status === "CLOSING") {
      riskCls = "status-bad";
      riskText = "组合止损执行中";
      stopDistanceText = "执行中";
      meterText = stop.state_inconsistent ? "等待持仓对账" : "正在平仓";
      meterPct = 0;
    } else if (status === "THRESHOLD_REACHED") {
      riskCls = "status-bad";
      riskText = "已到止损阈值";
      stopDistanceText = hasDistance ? "已超出 " + fmt(Math.abs(distance), 2) + "%" : "已到阈值";
      meterText = "等待策略执行";
      meterPct = 0;
    } else if (status === "MONITORING" && hasDistance) {
      riskCls = distance <= 1 ? "status-warn" : "status-ok";
      riskText = distance <= 1 ? "接近组合止损" : "正常";
      meterText = distance <= 1 ? "接近阈值" : "安全距离";
    } else {
      riskCls = "status-warn";
      riskText = "等待后端止损状态";
      stopDistanceText = "状态待同步";
      meterText = "待同步";
      meterPct = 0;
    }

    setAccountMetric("stop-distance", aid, stopDistanceText, riskCls);
    setAccountMetric("risk-state", aid, riskText, riskCls);
    if (meterLabel) {
      meterLabel.textContent = meterText;
      meterLabel.className = "stop-meter-label " + riskCls;
    }
    if (meter) {
      meter.style.width = meterPct.toFixed(2) + "%";
      meter.className = "stop-meter-fill " + riskCls;
    }
  }

  function renderCurve(aid) {
    var box = accountElement("spark-box", aid);
    var deltaEl = accountElement("spark-delta", aid);
    if (!box || !deltaEl) return;
    var cached = curveCache[aid];
    var mode = String(accountModes[aid] || "full").toLowerCase();
    var hasSummaryStop = Object.prototype.hasOwnProperty.call(portfolioStopStates, aid);
    var stop = hasSummaryStop
      ? portfolioStopStates[aid]
      : ((cached && cached.portfolioLossCut) || null);
    var rawBackendReturn = stop ? stop.current_return_pct : null;
    var backendReturn = rawBackendReturn === null || rawBackendReturn === undefined
      ? null
      : Number(rawBackendReturn);
    if (!Number.isFinite(backendReturn)) backendReturn = null;
    var metrics = cached ? curveMetrics(cached.points, mode) : null;
    if (!metrics) {
      box.className = "spark-box";
      box.innerHTML = '<div class="spark-empty">暂无曲线</div>';
      if (mode !== "readonly" && backendReturn !== null) {
        var backendCls = backendReturn > 0 ? "spark-up" : (backendReturn < 0 ? "spark-down" : "spark-flat");
        setAccountMetric(
          "spark-delta",
          aid,
          (backendReturn >= 0 ? "+" : "") + fmt(backendReturn, 2) + "%",
          backendCls
        );
        renderPortfolioStopState(aid, stop, null);
      } else {
        setAccountMetric("spark-delta", aid, "--", "");
        if (mode !== "readonly") renderPortfolioStopState(aid, stop, null);
      }
      return;
    }

    var changePct = mode !== "readonly" && backendReturn !== null
      ? backendReturn
      : metrics.currentReturnPct;
    var deltaCls = changePct > 0 ? "spark-up" : (changePct < 0 ? "spark-down" : "spark-flat");
    var width = mode === "readonly" ? 300 : 320;
    var height = mode === "readonly" ? 50 : 126;
    var includeStop = mode !== "readonly" && portfolioStopEnabled;
    var geometry = chartGeometry(metrics.values, width, height, includeStop);
    var area = geometry.points + " " + width + "," + height + " 0," + height;
    box.className = "spark-box " + deltaCls;
    box.innerHTML = ''
      + '<svg class="spark-svg" viewBox="0 0 ' + width + " " + height + '" preserveAspectRatio="none">'
      + '<line class="spark-zero-line" x1="0" x2="' + width + '" y1="' + geometry.zeroY.toFixed(2) + '" y2="' + geometry.zeroY.toFixed(2) + '"></line>'
      + (includeStop ? '<line class="spark-stop-line" x1="0" x2="' + width + '" y1="' + geometry.stopY.toFixed(2) + '" y2="' + geometry.stopY.toFixed(2) + '"></line>' : "")
      + '<polyline class="spark-area" points="' + area + '"></polyline>'
      + '<polyline class="spark-path" points="' + geometry.points + '"></polyline>'
      + "</svg>";

    var signedReturn = (changePct >= 0 ? "+" : "") + fmt(changePct, 2) + "%";
    setAccountMetric("spark-delta", aid, signedReturn, deltaCls);
    if (mode === "readonly") return;
    renderPortfolioStopState(aid, stop, metrics);
  }

  function fetchCurve(aid) {
    if (!aid) return;
    var now = Date.now();
    var cached = curveCache[aid];
    if (cached && cached.ts && (now - cached.ts) < curveTtlMs) {
      renderCurve(aid);
      return;
    }
    if (curveInFlight[aid]) {
      renderCurve(aid);
      return;
    }
    curveInFlight[aid] = true;
    var url = pathPrefix + "/api/account/" + encodeURIComponent(aid) + "/curve"
      + "?window_hours=24&curve_points=160&_=" + now;
    var xhr = new XMLHttpRequest();
    xhr.open("GET", url, true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      curveInFlight[aid] = false;
      if (xhr.status < 200 || xhr.status >= 300) {
        renderCurve(aid);
        return;
      }
      try {
        var payload = JSON.parse(xhr.responseText || "{}");
        var mode = String(accountModes[aid] || "full").toLowerCase();
        var curve = mode === "readonly"
          ? (payload.balance_curve || payload.equity_curve || [])
          : (payload.strategy_equity_curve || payload.equity_curve || []);
        var points = [];
        for (var i = 0; i < curve.length; i += 1) {
          var p = curve[i] || {};
          var n = Number(p.equity);
          if (Number.isFinite(n)) points.push({ t: p.t || "", equity: n });
        }
        if (payload.portfolio_loss_cut) {
          portfolioStopStates[aid] = payload.portfolio_loss_cut;
        }
        curveCache[aid] = {
          points: points.slice(-160),
          portfolioLossCut: payload.portfolio_loss_cut || portfolioStopStates[aid] || null,
          ts: Date.now()
        };
      } catch (e) {}
      renderCurve(aid);
    };
    xhr.send();
  }

  function primeCurveLoad() {
    createObserver();
    var nodes = document.querySelectorAll(".spark-box");
    for (var i = 0; i < nodes.length; i += 1) {
      var node = nodes[i];
      var aid = node.getAttribute("data-account-id") || "";
      renderCurve(aid);
      var cached = curveCache[aid];
      var fresh = !!(cached && cached.ts && (Date.now() - cached.ts) < curveTtlMs);
      if (!aid || fresh) continue;
      if (curveObserver) {
        curveObserver.observe(node);
      } else {
        fetchCurve(aid);
      }
    }
  }

  function renderCards() {
    var rows = summaryRows || [];
    var managedHtml = "";
    var readonlyHtml = "";
    accountModes = {};
    portfolioStopStates = {};
    for (var i = 0; i < rows.length; i += 1) {
      var r = rows[i] || {};
      var mode = String(r.mode || "full").toLowerCase();
      if (mode === "loss_cut_only") continue;  // loss_cut_only 不在上卡片区显示
      var aid = String(r.account_id || "");
      accountModes[aid] = mode;
      portfolioStopStates[aid] = r.portfolio_loss_cut || null;
      var safeAid = escapeHtml(aid);
      var base = pathPrefix + "/account/" + encodeURIComponent(aid) + "/";
      var st = r.last_run_status || "--";

      if (mode === "readonly") {
        var stats = r.trade_stats || {};
        var netPnl = stats.net_realized_pnl;
        var pnlText = netPnl != null ? fmt(netPnl, 4) : "--";
        var winRateText = stats.win_rate_pct != null ? fmt(stats.win_rate_pct, 1) + "%" : "--";
        var tradeCountText = stats.total_trades != null ? fmt(stats.total_trades, 0) : "--";
        var profitFactorText = stats.profit_factor != null ? fmt(stats.profit_factor, 2) : "--";
        var pnlNumber = Number(netPnl);
        var pnlCls = Number.isFinite(pnlNumber) ? (pnlNumber >= 0 ? "status-ok" : "status-bad") : "";
        readonlyHtml += '<article class="readonly-strip">'
          + '<div class="readonly-identity"><div><span class="readonly-id">' + safeAid + '</span><span class="readonly-badge">只读监控</span></div><div class="readonly-source">Binance Futures</div></div>'
          + '<div class="readonly-metric"><span class="metric-label">余额 (USDT)</span><strong class="metric-value">' + fmt(r.wallet_balance_usdt, 4) + '</strong></div>'
          + '<div class="readonly-metric"><span class="metric-label">近30日净盈亏</span><strong class="metric-value pnl-value ' + pnlCls + '">' + pnlText + '</strong></div>'
          + '<div class="readonly-metric"><span class="metric-label">胜率</span><strong class="metric-value">' + winRateText + '</strong></div>'
          + '<div class="readonly-metric readonly-secondary"><span class="metric-label">平仓订单数</span><strong class="metric-value">' + tradeCountText + '</strong></div>'
          + '<div class="readonly-metric readonly-secondary"><span class="metric-label">盈亏比</span><strong class="metric-value">' + profitFactorText + '</strong></div>'
          + '<div class="readonly-chart"><div>'
          + '<div class="spark-box" data-account-id="' + safeAid + '"><div class="spark-empty">加载中...</div></div>'
          + '</div><div class="readonly-chart-meta"><span class="metric-label">1D 权益曲线</span><strong class="metric-value spark-delta" data-account-id="' + safeAid + '">--</strong></div></div>'
          + '<div class="readonly-action"><a class="detail-link" href="' + base + '">余额曲线 / 交易统计</a></div>'
          + "</article>";
      } else {
        var stateCls = statusCls(st);
        var popoverId = "strategy-popover-" + i;
        managedHtml += '<article class="account-card">'
          + '<header class="account-card-head"><span class="aid">' + safeAid + '</span><div class="account-head-actions">'
          + '<div class="strategy-popover-wrap"><button class="strategy-trigger" type="button" aria-expanded="false" aria-controls="' + popoverId + '">策略</button>'
          + '<div id="' + popoverId + '" class="strategy-popover" role="dialog" aria-label="' + safeAid + ' 策略配置">'
          + '<div class="strategy-popover-head"><strong class="strategy-popover-title">策略配置</strong><span class="strategy-popover-subtitle">' + safeAid + '</span></div>'
          + '<div class="strategy-popover-tags">' + formatStrategyNote(r.strategy_note) + '</div></div></div>'
          + '<span class="venue-state ' + stateCls + '">Binance Futures<span class="status-dot"></span></span><a class="detail-link" href="' + base + '">详情</a></div></header>'
          + '<div class="account-card-body">'
          + '<div class="account-primary">'
          + '<div class="metric"><span class="metric-label">策略权益 (USDT)</span><strong class="metric-value">' + fmt(r.wallet_balance_usdt, 4) + '</strong></div>'
          + '<div class="metric"><span class="metric-label">' + String(portfolioStopHour).padStart(2, "0") + ":" + String(portfolioStopMinute).padStart(2, "0") + ' 以来收益</span><strong class="metric-value return-value spark-delta" data-account-id="' + safeAid + '">--</strong></div>'
          + '</div>'
          + '<div class="account-secondary">'
          + '<div class="metric"><span class="metric-label">持仓数量</span><strong class="metric-value">' + fmt(r.open_positions, 0) + '</strong></div>'
          + '<div class="metric"><span class="metric-label">距周期高点回撤</span><strong class="metric-value current-drawdown" data-account-id="' + safeAid + '">--</strong></div>'
          + '<div class="metric"><span class="metric-label">距组合止损</span><strong class="metric-value stop-distance" data-account-id="' + safeAid + '">--</strong></div>'
          + '</div>'
          + '<div class="risk-summary"><span>风险状态</span><strong class="risk-state" data-account-id="' + safeAid + '">计算中</strong></div>'
          + '<div class="spark-block">'
          + '<div class="spark-title"><span class="label">本周期策略权益曲线</span><span class="label">' + (portfolioStopEnabled ? ("止损阈值 -" + fmt(portfolioStopPct, 2) + "%") : "组合止损未启用") + '</span></div>'
          + '<div class="spark-box" data-account-id="' + safeAid + '"><div class="spark-empty">加载中...</div></div>'
          + '<div class="stop-meter"><span>-' + fmt(portfolioStopPct, 2) + '%</span><span class="stop-meter-track"><span class="stop-meter-fill" data-account-id="' + safeAid + '"></span></span><span class="stop-meter-label" data-account-id="' + safeAid + '">计算中</span></div>'
          + "</div></div>"
          + "</article>";
      }
    }
    var html = managedHtml ? '<div class="managed-grid">' + managedHtml + "</div>" : "";
    html += readonlyHtml;
    cards.innerHTML = html || '<div class="task-empty"><strong>暂无账户数据</strong><span>等待账户快照写入</span></div>';
    primeCurveLoad();
  }

  function renderTaskBoardHeader(rows) {
    if (!taskUpdatedAt) return;
    var latest = latestTaskTime(rows || []);
    taskUpdatedAt.textContent = "数据更新时间 " + (latest || "--");
    var filters = ["all", "anomaly", "symbols"];
    for (var i = 0; i < filters.length; i += 1) {
      var el = document.getElementById("task-filter-" + (filters[i] === "all" ? "all" : (filters[i] === "anomaly" ? "anomaly" : "symbols")));
      if (!el) continue;
      if (filters[i] === taskFilter) el.classList.add("active");
      else el.classList.remove("active");
    }
  }

  function toggleTaskFilter(nextFilter) {
    taskFilter = nextFilter || "all";
    renderTaskBoard();
  }

  function renderTaskBoard() {
    var rows = visibleTaskAccounts(summaryRows || []);
    if (!taskBoard) return;
    renderTaskBoardHeader(summaryRows || []);
    var html = "";
    for (var i = 0; i < rows.length; i += 1) {
      var r = rows[i] || {};
      var aid = String(r.account_id || "");
      var safeAid = escapeHtml(aid);
      var st = r.last_run_status || "--";
      var mode = String(r.mode || "full").toLowerCase();
      var modeLabel = mode === "full" ? "完整策略" : (mode === "loss_cut_only" ? "止损保护" : "只读监控");
      var modeBadgeClass = "task-mode-badge" + (mode === "readonly" ? " mode-readonly" : "");
      var accountCls = hasAnomalyTask(r) ? "task-account-card task-account-anomaly" : "task-account-card";
      html += '<article class="' + accountCls + '">'
        + '<div class="task-account-head">'
        + '<div class="task-account-title">'
        + '<span class="task-account-id">' + safeAid + "</span>"
        + '<span class="' + modeBadgeClass + '">' + escapeHtml(modeLabel) + "</span>"
        + (r && r.equity_recovery_take_profit_enabled ? '<span class="task-feature-badge">组合止盈监控</span>' : "")
        + "</div>"
        + '<span class="task-last-run ' + statusCls(st) + '">' + escapeHtml(st) + "</span>"
        + "</div>"
        + '<div class="task-table">'
        + '<div class="task-head">'
        + '<span class="task-col-h">任务</span>'
        + '<span class="task-col-h">状态</span>'
        + '<span class="task-col-h">时间</span>'
        + '<span class="task-col-h">结果</span>'
        + "</div>"
        + taskRowsForAccount(r)
        + "</div>"
        + "</article>";
    }
    if (html) {
      taskBoard.innerHTML = html;
    } else if (taskFilter === "anomaly") {
      taskBoard.innerHTML = '<div class="task-empty"><strong>暂无异常</strong><span>所有系统任务正常运行</span></div>';
    } else {
      taskBoard.innerHTML = '<div class="task-empty"><strong>暂无匹配任务</strong><span>切换筛选条件查看其他任务</span></div>';
    }
  }

  function fetchSummary() {
    var xhr = new XMLHttpRequest();
    xhr.open("GET", summaryApi + "?_=" + Date.now(), true);
    xhr.onreadystatechange = function () {
      if (xhr.readyState !== 4) return;
      if (xhr.status < 200 || xhr.status >= 300) return;
      var payload = {};
      try { payload = JSON.parse(xhr.responseText || "{}"); } catch (e) { return; }
      summaryRows = payload.accounts || [];
      renderCommandBar(payload);
      renderCards();
      renderEntryProgress();
      renderTaskBoard();
    };
    xhr.send();
  }

  fetchSummary();
  fetchHealth();
  function closeStrategyPopovers(exceptWrap) {
    var wraps = cards ? cards.querySelectorAll(".strategy-popover-wrap.is-open") : [];
    for (var i = 0; i < wraps.length; i += 1) {
      if (wraps[i] === exceptWrap) continue;
      wraps[i].classList.remove("is-open");
      var button = wraps[i].querySelector(".strategy-trigger");
      if (button) button.setAttribute("aria-expanded", "false");
    }
  }
  if (cards) {
    cards.addEventListener("click", function (event) {
      var trigger = event.target && event.target.closest
        ? event.target.closest(".strategy-trigger")
        : null;
      if (!trigger) return;
      event.stopPropagation();
      var wrap = trigger.closest(".strategy-popover-wrap");
      if (!wrap) return;
      var shouldOpen = !wrap.classList.contains("is-open");
      closeStrategyPopovers(wrap);
      wrap.classList.toggle("is-open", shouldOpen);
      trigger.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
      if (!shouldOpen && trigger.blur) trigger.blur();
    });
  }
  document.addEventListener("click", function (event) {
    if (event.target && event.target.closest && event.target.closest(".strategy-popover-wrap")) return;
    closeStrategyPopovers(null);
  });
  document.addEventListener("keydown", function (event) {
    if (event.key !== "Escape") return;
    closeStrategyPopovers(null);
    var active = document.activeElement;
    if (active && active.classList && active.classList.contains("strategy-trigger") && active.blur) active.blur();
  });
  window.toggleTaskFilter = toggleTaskFilter;
  var filterAll = document.getElementById("task-filter-all");
  var filterAnomaly = document.getElementById("task-filter-anomaly");
  var filterSymbols = document.getElementById("task-filter-symbols");
  if (filterAll) filterAll.addEventListener("click", function () { toggleTaskFilter("all"); });
  if (filterAnomaly) filterAnomaly.addEventListener("click", function () { toggleTaskFilter("anomaly"); });
  if (filterSymbols) filterSymbols.addEventListener("click", function () { toggleTaskFilter("symbols"); });
  if (entryProgressToggle) {
    entryProgressToggle.addEventListener("click", function () {
      entryDetailsExpanded = !entryDetailsExpanded;
      renderEntryProgress();
    });
  }
  setInterval(fetchSummary, Math.max(15, refreshSec) * 1000);
  setInterval(fetchHealth, Math.max(30, refreshSec * 2) * 1000);
})();
</script>
</body>
</html>
"""
