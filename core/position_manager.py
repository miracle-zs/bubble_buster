import logging
import hashlib
import json
import threading
from collections import deque
from datetime import date, datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

from core.account_snapshot import AccountSnapshot, AccountSnapshotProvider
from core.entry_structure_protection import EntryStructureProtectionState
from core.market_fill_reconciler import MarketFillReconciler
from core.state_store import StateStore
from infra.binance_futures_client import BinanceAPIError, BinanceFuturesClient, OrderStateUnknownError
from infra.notifier import (
    ServerChanNotifier,
    format_markdown_kv_table,
    format_markdown_list_section,
)

LOGGER = logging.getLogger(__name__)


def _serialized_account_mutation(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self._mutation_lock:
            return method(self, *args, **kwargs)

    return wrapped


class PositionManager:
    DAILY_LOSS_CUT_SCOPE_TRACKED = "tracked"
    DAILY_LOSS_CUT_SCOPE_EXCHANGE = "exchange"
    NOON_PROTECTION_LOCK_NAME = "noon_protection_stop_caps_v1"
    MORNING_PROTECTION_LOCK_NAME = "morning_protection_stop_caps_v1"
    HOURLY_EXCHANGE_TP_LOCK_NAME = "hourly_exchange_take_profit_v1"
    ORPHAN_EXIT_ORDER_CLEANUP_LOCK_NAME = "orphan_exit_order_cleanup_v1"
    PORTFOLIO_LOSS_CUT_LOCK_NAME = "portfolio_loss_cut_v1"
    PORTFOLIO_TAKE_PROFIT_LOCK_NAME = "portfolio_take_profit_v2"
    PORTFOLIO_LIMIT_ACTIVE_STATUSES = {"NEW", "PENDING", "PARTIALLY_FILLED"}
    PORTFOLIO_LIMIT_TERMINAL_STATUSES = {
        "FILLED",
        "CANCELED",
        "CANCELLED",
        "EXPIRED",
        "REJECTED",
    }
    NOON_PROTECTION_PRE_ENTRY_HOURS = 2
    # An untracked position has no fill timestamp; assume the normal 08:00 entry
    # and include the two completed hourly candles immediately before it.
    NOON_PROTECTION_UNTRACKED_ENTRY_OFFSET = timedelta(hours=8)

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        notifier: ServerChanNotifier,
        sl_liq_buffer_pct: float,
        trigger_price_type: str,
        daily_loss_cut_scope: str = DAILY_LOSS_CUT_SCOPE_TRACKED,
        account_id: str = "default",
        protection_exempt_symbols: Optional[Set[str]] = None,
        mutation_lock: Optional[Any] = None,
        snapshot_provider: Optional[AccountSnapshotProvider] = None,
        order_state: Optional[Any] = None,
    ):
        self.client = client
        self.store = store
        self.notifier = notifier
        self.sl_liq_buffer_pct = sl_liq_buffer_pct
        self.trigger_price_type = trigger_price_type
        self.daily_loss_cut_scope = self._normalize_daily_loss_cut_scope(daily_loss_cut_scope)
        self.account_id = (account_id or "").strip() or "default"
        self.protection_exempt_symbols = {
            str(symbol or "").strip().upper() for symbol in (protection_exempt_symbols or set()) if str(symbol or "").strip()
        }
        self._noon_protection_caps_cache: Optional[Dict[str, float]] = None
        self._morning_protection_caps_cache: Optional[Dict[str, float]] = None
        self._entry_structure_protection_state = EntryStructureProtectionState(store)
        self._market_fill_reconciler = MarketFillReconciler(client, store)
        self._mutation_lock = mutation_lock or threading.RLock()
        self.snapshot_provider = snapshot_provider
        self.order_state = order_state
        self._active_account_snapshot: Optional[AccountSnapshot] = None

    def _is_protection_exempt(self, symbol: str) -> bool:
        return str(symbol or "").strip().upper() in self.protection_exempt_symbols

    def refresh_hourly_exchange_take_profit_state(
        self,
        now_local: datetime,
        drop_pct: float,
    ) -> Dict[str, object]:
        now_utc = now_local.astimezone(timezone.utc).replace(microsecond=0)
        state = self.store.get_lock_state(self.HOURLY_EXCHANGE_TP_LOCK_NAME) or {}
        raw_symbols = state.get("symbols")
        symbols_state = dict(raw_symbols) if isinstance(raw_symbols, dict) else {}

        summary = {
            "initialized": 0,
            "updated": 0,
            "pruned": 0,
            "errors": 0,
        }
        error_symbols: List[str] = []
        active_symbols = set()
        risks = self._get_all_position_risks()
        for risk in risks:
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if position_amt >= 0:
                continue
            symbol = str(risk.get("symbol") or "").strip()
            if not symbol:
                continue
            if self._is_protection_exempt(symbol):
                continue
            active_symbols.add(symbol)
            entry_price = self._safe_positive_float(risk.get("entryPrice"))
            if not entry_price:
                continue

            existing = symbols_state.get(symbol)
            try:
                if not isinstance(existing, dict):
                    symbols_state[symbol] = self._initialize_hourly_exchange_take_profit_monitor(
                        symbol=symbol,
                        risk=risk,
                        now_utc=now_utc,
                        drop_pct=drop_pct,
                    )
                    summary["initialized"] += 1
                    continue

                refreshed = self._refresh_existing_hourly_exchange_take_profit_monitor(
                    symbol=symbol,
                    risk=risk,
                    existing=existing,
                    now_utc=now_utc,
                    drop_pct=drop_pct,
                )
                symbols_state[symbol] = refreshed
                summary["updated"] += 1
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                error_symbols.append(symbol)
                LOGGER.warning(
                    "Hourly exchange take-profit monitor refresh failed account=%s symbol=%s: %s",
                    self.account_id,
                    symbol,
                    exc,
                )
                continue

        stale_symbols = [symbol for symbol in list(symbols_state.keys()) if symbol not in active_symbols]
        for symbol in stale_symbols:
            symbols_state.pop(symbol, None)
            summary["pruned"] += 1
        if error_symbols:
            summary["error_symbols"] = error_symbols

        self.store.set_lock_state(
            self.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": symbols_state,
                "updated_at_utc": self._utc_now_iso(),
            },
        )
        return summary

    @_serialized_account_mutation
    def run_hourly_exchange_take_profit(
        self,
        now_local: datetime,
        drop_pct: float,
    ) -> Dict[str, object]:
        self.refresh_hourly_exchange_take_profit_state(now_local=now_local, drop_pct=drop_pct)
        state = self.store.get_lock_state(self.HOURLY_EXCHANGE_TP_LOCK_NAME) or {}
        raw_symbols = state.get("symbols")
        symbols_state = raw_symbols if isinstance(raw_symbols, dict) else {}
        summary = {
            "total": 0,
            "closed_take_profit": 0,
            "skipped": 0,
            "errors": 0,
        }

        risks = self._get_all_position_risks()
        for risk in risks:
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if position_amt >= 0:
                continue
            symbol = str(risk.get("symbol") or "").strip()
            if not symbol:
                continue
            if self._is_protection_exempt(symbol):
                continue
            summary["total"] += 1
            monitor = symbols_state.get(symbol)
            if not isinstance(monitor, dict) or not bool(monitor.get("eligible_reached")):
                summary["skipped"] += 1
                continue

            try:
                hour_open = None
                hour_close = None
                hour_open, hour_close = self._get_previous_closed_hour_open_and_close(
                    symbol=symbol,
                    now_local=now_local,
                )
                if hour_open is None or hour_close is None or hour_close <= hour_open:
                    summary["skipped"] += 1
                    continue

                position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
                close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                    position_amt=position_amt,
                    position_side=position_side,
                )
                tracked_position = self._find_open_position_for_exchange_symbol(symbol)
                close_info = self._close_daily_loss_cut(
                    symbol=symbol,
                    qty=abs(position_amt),
                    side=close_side,
                    position_id=int(tracked_position["id"]) if tracked_position is not None else None,
                    cancel_pos=tracked_position,
                    position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                    use_reduce_only=use_reduce_only,
                    close_status="CLOSED_HOURLY_TAKE_PROFIT",
                    close_reason="HOURLY_EXCHANGE_TAKE_PROFIT",
                )
                summary["closed_take_profit"] += 1
                monitor["last_triggered_hour_key"] = now_local.strftime("%Y-%m-%dT%H")
                monitor["last_close_order_id"] = close_info.get("close_order_id")
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                LOGGER.exception(
                    "Hourly exchange take-profit failed symbol=%s hour_open=%s hour_close=%s position_amt=%s: %s",
                    symbol,
                    hour_open,
                    hour_close,
                    position_amt,
                    exc,
                )

        self.store.set_lock_state(
            self.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": symbols_state,
                "updated_at_utc": self._utc_now_iso(),
            },
        )
        return summary

    def _initialize_hourly_exchange_take_profit_monitor(
        self,
        symbol: str,
        risk: Dict[str, Any],
        now_utc: datetime,
        drop_pct: float,
    ) -> Dict[str, object]:
        entry_price = self._safe_positive_float(risk.get("entryPrice"))
        if not entry_price:
            raise RuntimeError(f"missing entry price for {symbol}")
        position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
        opened_at_utc = self._resolve_hourly_monitor_opened_at(
            symbol=symbol,
            current_short_qty=abs(position_amt),
            now_utc=now_utc,
        )
        _high_price, low_price = self._fetch_symbol_extremes_between(
            symbol=symbol,
            start_utc=opened_at_utc,
            end_utc=now_utc,
        )
        lowest_price = low_price if low_price is not None else entry_price
        favorable_drop_pct = max(0.0, (entry_price - lowest_price) / entry_price)
        eligible_reached = favorable_drop_pct >= (float(drop_pct) / 100.0)
        return {
            "symbol": symbol,
            "position_amt": position_amt,
            "entry_price": entry_price,
            "opened_at_utc": opened_at_utc.isoformat(),
            "lowest_price_since_open": float(lowest_price),
            "eligible_reached": eligible_reached,
            "eligible_reached_at_utc": now_utc.isoformat() if eligible_reached else None,
            "last_seen_at_utc": now_utc.isoformat(),
        }

    def _refresh_existing_hourly_exchange_take_profit_monitor(
        self,
        symbol: str,
        risk: Dict[str, Any],
        existing: Dict[str, Any],
        now_utc: datetime,
        drop_pct: float,
    ) -> Dict[str, object]:
        entry_price = self._safe_positive_float(risk.get("entryPrice"))
        if not entry_price:
            raise RuntimeError(f"missing entry price for {symbol}")
        position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
        opened_at_raw = str(existing.get("opened_at_utc") or "").strip()
        existing_opened_at_utc = self._parse_iso_utc(opened_at_raw) if opened_at_raw else None
        opened_at_utc = self._resolve_hourly_monitor_opened_at(
            symbol=symbol,
            current_short_qty=abs(position_amt),
            now_utc=now_utc,
        )
        if existing_opened_at_utc is not None and opened_at_utc > existing_opened_at_utc:
            return self._initialize_hourly_exchange_take_profit_monitor(
                symbol=symbol,
                risk=risk,
                now_utc=now_utc,
                drop_pct=drop_pct,
            )
        _high_price, low_price = self._fetch_symbol_extremes_between(
            symbol=symbol,
            start_utc=opened_at_utc,
            end_utc=now_utc,
        )
        existing_low = self._safe_positive_float(existing.get("lowest_price_since_open"))
        candidates = [price for price in [existing_low, low_price, entry_price] if price is not None]
        lowest_price = min(candidates) if candidates else entry_price

        refreshed = dict(existing)
        refreshed["symbol"] = symbol
        refreshed["position_amt"] = position_amt
        refreshed["entry_price"] = entry_price
        refreshed["opened_at_utc"] = opened_at_utc.isoformat()
        refreshed["lowest_price_since_open"] = float(lowest_price)
        refreshed["last_seen_at_utc"] = now_utc.isoformat()

        favorable_drop_pct = max(0.0, (entry_price - lowest_price) / entry_price)
        if favorable_drop_pct >= (float(drop_pct) / 100.0):
            refreshed["eligible_reached"] = True
            refreshed.setdefault("eligible_reached_at_utc", now_utc.isoformat())
            if not refreshed.get("eligible_reached_at_utc"):
                refreshed["eligible_reached_at_utc"] = now_utc.isoformat()
        else:
            refreshed["eligible_reached"] = bool(existing.get("eligible_reached"))
        return refreshed

    def _find_open_position_for_exchange_symbol(self, symbol: str) -> Optional[Dict[str, Any]]:
        normalized_symbol = str(symbol or "").strip().upper()
        if not normalized_symbol:
            return None
        for pos in self.store.list_open_positions():
            if str(pos.get("symbol") or "").strip().upper() == normalized_symbol:
                return pos
        return None

    def _resolve_hourly_monitor_opened_at(
        self,
        symbol: str,
        current_short_qty: float,
        now_utc: datetime,
    ) -> datetime:
        tracked_position = self._find_open_position_for_exchange_symbol(symbol)
        if tracked_position is not None:
            opened_at_raw = str(tracked_position.get("opened_at_utc") or "").strip()
            if opened_at_raw:
                opened_at_utc = self._parse_iso_utc(opened_at_raw)
                if opened_at_utc <= now_utc + timedelta(minutes=1):
                    return opened_at_utc
                LOGGER.warning(
                    "Ignore future tracked opened_at account=%s symbol=%s opened_at=%s now=%s",
                    self.account_id,
                    symbol,
                    opened_at_utc.isoformat(),
                    now_utc.isoformat(),
                )
        return self._reconstruct_short_opened_at_from_trades(
            symbol=symbol,
            current_short_qty=current_short_qty,
        )

    def _reconstruct_short_opened_at_from_trades(
        self,
        symbol: str,
        current_short_qty: float,
    ) -> datetime:
        return self._reconstruct_position_opened_at_from_trades(
            symbol=symbol,
            current_qty=current_short_qty,
            entry_side="SELL",
        )

    def _reconstruct_position_opened_at_from_trades(
        self,
        symbol: str,
        current_qty: float,
        entry_side: str,
    ) -> datetime:
        trades = self.client.get_user_trades(symbol=symbol, limit=1000)
        target_qty = max(0.0, float(current_qty))
        if target_qty <= 1e-12:
            raise RuntimeError(f"cannot reconstruct opened_at for {symbol}: current_qty is zero")
        normalized_entry_side = str(entry_side or "").strip().upper()
        if normalized_entry_side not in {"BUY", "SELL"}:
            raise RuntimeError(f"cannot reconstruct opened_at for {symbol}: invalid entry_side={entry_side}")

        closing_side = "BUY" if normalized_entry_side == "SELL" else "SELL"
        open_lots: deque[tuple[int, float]] = deque()
        tolerance = max(target_qty * 1e-6, 1e-12)

        ordered_trades: List[tuple[int, float, str]] = []
        for trade in trades or []:
            trade_time = trade.get("time")
            try:
                trade_time_ms = int(trade_time)
            except (TypeError, ValueError):
                continue
            qty = self._safe_float(trade.get("qty"), default=0.0)
            side = str(trade.get("side") or "").strip().upper()
            if qty <= 0 or side not in {"BUY", "SELL"}:
                continue
            ordered_trades.append((trade_time_ms, qty, side))

        for trade_time_ms, qty, side in sorted(ordered_trades, key=lambda item: item[0]):

            if side == normalized_entry_side:
                open_lots.append((trade_time_ms, qty))
                continue

            if side != closing_side or not open_lots:
                continue

            remaining_close = qty
            while remaining_close > tolerance and open_lots:
                lot_time_ms, lot_qty = open_lots[0]
                if lot_qty <= remaining_close + tolerance:
                    remaining_close -= lot_qty
                    open_lots.popleft()
                    continue
                open_lots[0] = (lot_time_ms, lot_qty - remaining_close)
                remaining_close = 0.0

        if not open_lots:
            raise RuntimeError(f"cannot reconstruct opened_at for {symbol}")

        remaining_open_qty = sum(qty for _time_ms, qty in open_lots)
        if remaining_open_qty + tolerance < target_qty:
            raise RuntimeError(
                f"cannot reconstruct opened_at for {symbol}: matched_qty={remaining_open_qty} target_qty={target_qty}"
            )

        opened_at_ms = open_lots[0][0]
        return datetime.fromtimestamp(opened_at_ms / 1000.0, tz=timezone.utc).replace(microsecond=0)

    def _get_previous_closed_hour_open_and_close(
        self,
        symbol: str,
        now_local: datetime,
    ) -> tuple[Optional[float], Optional[float]]:
        local_dt = now_local.astimezone(timezone.utc)
        current_hour_start_utc = local_dt.replace(minute=0, second=0, microsecond=0)
        previous_hour_start_utc = current_hour_start_utc.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        try:
            rows = self.client.get_klines(
                symbol=symbol,
                interval="1h",
                start_time=int(previous_hour_start_utc.timestamp() * 1000),
                end_time=int(current_hour_start_utc.timestamp() * 1000),
                limit=1,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Failed to fetch hourly klines for symbol=%s previous_hour=%s: %s",
                symbol,
                previous_hour_start_utc.isoformat(),
                exc,
            )
            return None, None
        row = rows[0] if rows else None
        if row is None:
            LOGGER.warning(
                "No hourly kline returned for symbol=%s previous_hour=%s",
                symbol,
                previous_hour_start_utc.isoformat(),
            )
            return None, None
        if len(row) < 5:
            LOGGER.warning(
                "Incomplete kline data for symbol=%s previous_hour=%s: row_length=%s",
                symbol,
                previous_hour_start_utc.isoformat(),
                len(row),
            )
            return None, None
        hour_open = self._safe_positive_float(row[1])
        hour_close = self._safe_positive_float(row[4])
        if hour_open is None or hour_close is None:
            LOGGER.warning(
                "Invalid kline prices for symbol=%s previous_hour=%s: open=%s close=%s",
                symbol,
                previous_hour_start_utc.isoformat(),
                row[1] if len(row) > 1 else None,
                row[4] if len(row) > 4 else None,
            )
        return hour_open, hour_close

    @_serialized_account_mutation
    def run_daily_loss_cut(self) -> Dict[str, object]:
        if self.daily_loss_cut_scope == self.DAILY_LOSS_CUT_SCOPE_EXCHANGE:
            return self._run_daily_loss_cut_exchange_positions()
        return self._run_daily_loss_cut_tracked_positions()

    @staticmethod
    def _portfolio_loss_cut_cycle_window(
        now_local: datetime,
        reset_hour: int,
        reset_minute: int,
    ) -> tuple[date, datetime, bool]:
        local_dt = now_local
        if local_dt.tzinfo is None:
            local_dt = local_dt.replace(tzinfo=timezone.utc)
        reset_today = local_dt.replace(
            hour=reset_hour % 24,
            minute=reset_minute % 60,
            second=0,
            microsecond=0,
        )
        if local_dt >= reset_today:
            return local_dt.date(), reset_today, True
        previous_reset = reset_today - timedelta(days=1)
        return previous_reset.date(), previous_reset, False

    @_serialized_account_mutation
    def run_portfolio_loss_cut(
        self,
        current_equity_usdt: float,
        now_local: datetime,
        loss_pct: float = 3.5,
        reset_hour: int = 8,
        reset_minute: int = 0,
    ) -> Dict[str, object]:
        """Monitor and enforce the per-account daily portfolio loss stop.

        The baseline is the first valid wallet snapshot at or after the local
        reset time. Once the equity falls by ``loss_pct`` from that baseline,
        every non-exempt exchange position is market-closed and the account is
        latched until the next reset cycle. The latch is persisted so a service
        restart cannot reopen the account on the same day.
        """
        cycle_date, cycle_start_local, active = self._portfolio_loss_cut_cycle_window(
            now_local=now_local,
            reset_hour=reset_hour,
            reset_minute=reset_minute,
        )
        cycle_key = cycle_date.isoformat()
        if not active:
            return {
                "status": "PRE_RESET",
                "cycle_date": cycle_key,
                "reset_at_local": cycle_start_local.isoformat(timespec="seconds"),
            }

        current_equity = self._safe_float(current_equity_usdt, default=0.0)
        if current_equity <= 0:
            return {"status": "SKIPPED", "reason": "INVALID_EQUITY", "cycle_date": cycle_key}

        normalized_loss_pct = min(100.0, max(0.001, float(loss_pct)))
        state = self.store.get_lock_state(self.PORTFOLIO_LOSS_CUT_LOCK_NAME) or {}
        if str(state.get("cycle_date") or "") != cycle_key:
            cycle_start_utc = cycle_start_local.astimezone(timezone.utc).replace(microsecond=0).isoformat()
            snapshot = self.store.get_wallet_snapshot_first_since(
                start_captured_at_utc=cycle_start_utc,
                end_captured_at_utc=now_local.astimezone(timezone.utc).replace(microsecond=0).isoformat(),
            )
            baseline_equity = self._safe_float(
                snapshot.get("balance_usdt") if snapshot else current_equity,
                default=current_equity,
            )
            if baseline_equity <= 0:
                baseline_equity = current_equity
            threshold_equity = baseline_equity * (1.0 - normalized_loss_pct / 100.0)
            state = {
                "cycle_date": cycle_key,
                "baseline_equity_usdt": baseline_equity,
                "baseline_captured_at_utc": (
                    str(snapshot.get("captured_at_utc") or "").strip() if snapshot else None
                ),
                "threshold_equity_usdt": threshold_equity,
                "loss_pct": normalized_loss_pct,
                "triggered": False,
                "close_complete": False,
                "notification_sent": False,
                "updated_at_utc": self._utc_now_iso(),
            }
            self.store.set_lock_state(self.PORTFOLIO_LOSS_CUT_LOCK_NAME, state)

        baseline_equity = self._safe_float(state.get("baseline_equity_usdt"), default=0.0)
        threshold_equity = self._safe_float(state.get("threshold_equity_usdt"), default=0.0)
        if baseline_equity <= 0 or threshold_equity <= 0:
            return {"status": "SKIPPED", "reason": "INVALID_BASELINE", "cycle_date": cycle_key}

        state["current_equity_usdt"] = current_equity
        state["updated_at_utc"] = self._utc_now_iso()
        already_triggered = bool(state.get("triggered"))
        threshold_eps = max(1e-9, abs(threshold_equity) * 1e-12)
        if not already_triggered and current_equity + threshold_eps > threshold_equity:
            self.store.set_lock_state(self.PORTFOLIO_LOSS_CUT_LOCK_NAME, state)
            return {
                "status": "MONITORING",
                "cycle_date": cycle_key,
                "baseline_equity": round(baseline_equity, 8),
                "current_equity": round(current_equity, 8),
                "threshold_equity": round(threshold_equity, 8),
            }

        if not already_triggered:
            state["triggered"] = True
            state["close_complete"] = False
            state["triggered_at_utc"] = self._utc_now_iso()
            self.store.set_lock_state(self.PORTFOLIO_LOSS_CUT_LOCK_NAME, state)

        if bool(state.get("close_complete")):
            self.store.set_lock_state(self.PORTFOLIO_LOSS_CUT_LOCK_NAME, state)
            return {
                "status": "ALREADY_TRIGGERED",
                "triggered": True,
                "close_complete": True,
                "cycle_date": cycle_key,
                "baseline_equity": round(baseline_equity, 8),
                "current_equity": round(current_equity, 8),
                "threshold_equity": round(threshold_equity, 8),
            }

        close_summary = self._close_all_exchange_positions_for_portfolio_loss_cut()
        close_complete = int(close_summary.get("errors", 0) or 0) == 0
        state["close_complete"] = close_complete
        state["last_close_summary"] = close_summary
        if not bool(state.get("notification_sent")):
            try:
                self.notifier.send(
                    f"【Top10做空】组合止损 -{normalized_loss_pct:.2f}%",
                    self._build_portfolio_loss_cut_notification(
                        close_summary,
                        baseline_equity=baseline_equity,
                        current_equity=current_equity,
                        threshold_equity=threshold_equity,
                        cycle_date=cycle_key,
                    ),
                )
                state["notification_sent"] = True
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Portfolio loss-cut notification failed account=%s: %s", self.account_id, exc)
        self.store.set_lock_state(self.PORTFOLIO_LOSS_CUT_LOCK_NAME, state)

        return {
            "status": "TRIGGERED" if not already_triggered else "TRIGGERED_RETRY",
            "triggered": True,
            "close_complete": close_complete,
            "cycle_date": cycle_key,
            "baseline_equity": round(baseline_equity, 8),
            "current_equity": round(current_equity, 8),
            "threshold_equity": round(threshold_equity, 8),
            **close_summary,
        }

    @_serialized_account_mutation
    def run_portfolio_take_profit(
        self,
        current_equity_usdt: float,
        now_local: datetime,
        profit_pct: float = 9.0,
        reset_hour: int = 8,
        reset_minute: int = 0,
        reduce_ratio: float = 1.0,
        giveback_pct: float = 0.0,
    ) -> Dict[str, object]:
        """Close or reduce the portfolio after a fixed or trailing daily gain.

        Each cycle uses the first valid equity snapshot at or after the local
        reset as a fixed baseline. With ``giveback_pct > 0``, ``profit_pct`` is
        the arming gain and the exit threshold retains ``100-giveback_pct``
        percent of the highest observed profit. A zero giveback preserves the
        legacy fixed-threshold behavior. The trigger is latched once per cycle,
        but it does not block later strategy entries.
        """
        cycle_date, cycle_start_local, _active_after_today_reset = self._portfolio_loss_cut_cycle_window(
            now_local=now_local,
            reset_hour=reset_hour,
            reset_minute=reset_minute,
        )
        cycle_key = cycle_date.isoformat()
        current_equity = self._safe_float(current_equity_usdt, default=0.0)
        if current_equity <= 0:
            return {"status": "SKIPPED", "reason": "INVALID_EQUITY", "cycle_date": cycle_key}

        normalized_profit_pct = min(100.0, max(0.001, float(profit_pct)))
        normalized_reduce_ratio = min(1.0, max(0.05, float(reduce_ratio)))
        normalized_giveback_pct = min(100.0, max(0.0, float(giveback_pct)))
        state = self.store.get_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME) or {}
        if str(state.get("cycle_date") or "") != cycle_key:
            previous_state = state
            previous_plan = previous_state.get("portfolio_limit_plan")
            carried_limit_plan = (
                [dict(item) for item in previous_plan if isinstance(item, dict)]
                if isinstance(previous_plan, list) and not bool(previous_state.get("close_complete"))
                else []
            )
            cycle_start_utc = cycle_start_local.astimezone(timezone.utc).replace(microsecond=0).isoformat()
            current_time_utc = now_local.astimezone(timezone.utc).replace(microsecond=0).isoformat()
            snapshot = self.store.get_wallet_snapshot_first_since(
                start_captured_at_utc=cycle_start_utc,
                end_captured_at_utc=current_time_utc,
            )
            baseline_equity = self._safe_float(
                snapshot.get("balance_usdt") if snapshot else current_equity,
                default=current_equity,
            )
            if baseline_equity <= 0:
                baseline_equity = current_equity
            peak_snapshot = self.store.get_wallet_snapshot_max_since(
                start_captured_at_utc=cycle_start_utc,
                end_captured_at_utc=current_time_utc,
            )
            peak_equity = max(
                baseline_equity,
                current_equity,
                self._safe_float(
                    peak_snapshot.get("balance_usdt") if peak_snapshot else current_equity,
                    default=current_equity,
                ),
            )
            peak_profit_pct = max(0.0, (peak_equity / baseline_equity - 1.0) * 100.0)
            arming_threshold_equity = baseline_equity * (1.0 + normalized_profit_pct / 100.0)
            armed = (
                normalized_giveback_pct > 0.0
                and peak_equity + max(1e-9, abs(arming_threshold_equity) * 1e-12)
                >= arming_threshold_equity
            )
            trailing_profit_pct = peak_profit_pct * (1.0 - normalized_giveback_pct / 100.0)
            trailing_threshold_equity = baseline_equity * (1.0 + trailing_profit_pct / 100.0)
            threshold_equity = trailing_threshold_equity if armed else arming_threshold_equity
            state = {
                "state_version": 3 if carried_limit_plan else 2,
                "cycle_date": cycle_key,
                "cycle_start_at_local": cycle_start_local.isoformat(timespec="seconds"),
                "baseline_equity_usdt": baseline_equity,
                "baseline_captured_at_utc": (
                    str(snapshot.get("captured_at_utc") or "").strip() if snapshot else None
                ),
                "arming_threshold_equity_usdt": arming_threshold_equity,
                "threshold_equity_usdt": threshold_equity,
                "profit_pct": normalized_profit_pct,
                "giveback_pct": normalized_giveback_pct,
                "reduce_ratio": normalized_reduce_ratio,
                "armed": armed,
                "armed_at_utc": current_time_utc if armed else None,
                "peak_equity_usdt": peak_equity,
                "peak_profit_pct": peak_profit_pct,
                "peak_captured_at_utc": (
                    str(peak_snapshot.get("captured_at_utc") or "").strip()
                    if peak_snapshot
                    else current_time_utc
                ),
                "trailing_threshold_equity_usdt": trailing_threshold_equity if armed else None,
                "trailing_threshold_profit_pct": trailing_profit_pct if armed else None,
                # A pending limit plan is carried across the daily baseline
                # reset so the old reduce-only order never becomes orphaned.
                "triggered": bool(carried_limit_plan),
                "close_complete": False,
                "triggered_at_utc": (
                    previous_state.get("triggered_at_utc") if carried_limit_plan else None
                ),
                "trigger_equity_usdt": (
                    previous_state.get("trigger_equity_usdt") if carried_limit_plan else None
                ),
                "trigger_profit_pct": (
                    previous_state.get("trigger_profit_pct") if carried_limit_plan else None
                ),
                "trigger_threshold_equity_usdt": (
                    previous_state.get("trigger_threshold_equity_usdt") if carried_limit_plan else None
                ),
                "portfolio_limit_plan": carried_limit_plan if carried_limit_plan else None,
                "notification_sent": bool(previous_state.get("notification_sent")) if carried_limit_plan else False,
                "updated_at_utc": self._utc_now_iso(),
            }
            self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)

        baseline_equity = self._safe_float(state.get("baseline_equity_usdt"), default=0.0)
        if baseline_equity <= 0:
            return {"status": "SKIPPED", "reason": "INVALID_BASELINE", "cycle_date": cycle_key}

        actual_profit_pct = (current_equity / baseline_equity - 1.0) * 100.0
        state["current_equity_usdt"] = current_equity
        state["current_profit_pct"] = actual_profit_pct
        state["updated_at_utc"] = self._utc_now_iso()
        already_triggered = bool(state.get("triggered"))
        if not already_triggered:
            # Configuration corrections take effect until the trigger. Once an
            # order plan exists, all exit parameters remain immutable so a
            # retry cannot change the intended reduction.
            state["profit_pct"] = normalized_profit_pct
            state["giveback_pct"] = normalized_giveback_pct
            state["reduce_ratio"] = normalized_reduce_ratio
            state["arming_threshold_equity_usdt"] = baseline_equity * (
                1.0 + normalized_profit_pct / 100.0
            )
        active_profit_pct = min(
            100.0,
            max(0.001, self._safe_float(state.get("profit_pct"), default=normalized_profit_pct)),
        )
        active_giveback_pct = min(
            100.0,
            max(0.0, self._safe_float(state.get("giveback_pct"), default=normalized_giveback_pct)),
        )
        active_reduce_ratio = min(
            1.0,
            max(0.05, self._safe_float(state.get("reduce_ratio"), default=normalized_reduce_ratio)),
        )
        arming_threshold_equity = self._safe_float(
            state.get("arming_threshold_equity_usdt"),
            default=baseline_equity * (1.0 + active_profit_pct / 100.0),
        )
        if arming_threshold_equity <= 0:
            return {"status": "SKIPPED", "reason": "INVALID_THRESHOLD", "cycle_date": cycle_key}

        persisted_peak_equity = self._safe_float(
            state.get("peak_equity_usdt"),
            default=current_equity,
        )
        peak_equity = max(
            baseline_equity,
            persisted_peak_equity,
            current_equity if not already_triggered else persisted_peak_equity,
        )
        peak_profit_pct = max(0.0, (peak_equity / baseline_equity - 1.0) * 100.0)
        if not already_triggered:
            previous_peak = self._safe_float(state.get("peak_equity_usdt"), default=0.0)
            if peak_equity > previous_peak + max(1e-9, abs(peak_equity) * 1e-12):
                state["peak_captured_at_utc"] = now_local.astimezone(timezone.utc).replace(
                    microsecond=0
                ).isoformat()
            state["peak_equity_usdt"] = peak_equity
            state["peak_profit_pct"] = peak_profit_pct

        newly_armed = False
        armed = bool(state.get("armed")) and active_giveback_pct > 0.0
        arming_eps = max(1e-9, abs(arming_threshold_equity) * 1e-12)
        if not already_triggered and active_giveback_pct > 0.0 and not armed:
            if peak_equity + arming_eps >= arming_threshold_equity:
                armed = True
                newly_armed = True
                state["armed"] = True
                state["armed_at_utc"] = self._utc_now_iso()

        if active_giveback_pct > 0.0 and armed:
            trailing_profit_pct = peak_profit_pct * (1.0 - active_giveback_pct / 100.0)
            threshold_equity = baseline_equity * (1.0 + trailing_profit_pct / 100.0)
            state["trailing_threshold_profit_pct"] = trailing_profit_pct
            state["trailing_threshold_equity_usdt"] = threshold_equity
        else:
            trailing_profit_pct = None
            threshold_equity = arming_threshold_equity
            state["armed"] = False
            state["trailing_threshold_profit_pct"] = None
            state["trailing_threshold_equity_usdt"] = None
        state["threshold_equity_usdt"] = threshold_equity

        threshold_eps = max(1e-9, abs(threshold_equity) * 1e-12)
        should_trigger = False
        monitoring_status = "MONITORING"
        if not already_triggered:
            if active_giveback_pct <= 0.0:
                should_trigger = current_equity + threshold_eps >= arming_threshold_equity
            elif not armed:
                should_trigger = False
            else:
                should_trigger = current_equity <= threshold_equity + threshold_eps
                monitoring_status = "ARMED" if newly_armed else "TRAILING"

        if not already_triggered and not should_trigger:
            self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)
            return {
                "status": monitoring_status,
                "cycle_date": cycle_key,
                "baseline_equity": round(baseline_equity, 8),
                "current_equity": round(current_equity, 8),
                "threshold_equity": round(threshold_equity, 8),
                "arming_threshold_equity": round(arming_threshold_equity, 8),
                "actual_profit_pct": round(actual_profit_pct, 8),
                "peak_equity": round(peak_equity, 8),
                "peak_profit_pct": round(peak_profit_pct, 8),
                "giveback_pct": active_giveback_pct,
                "armed": armed,
                "reduce_ratio": active_reduce_ratio,
            }

        if not already_triggered:
            state["triggered"] = True
            state["close_complete"] = False
            state["state_version"] = 3
            state["triggered_at_utc"] = self._utc_now_iso()
            state["trigger_equity_usdt"] = current_equity
            state["trigger_profit_pct"] = actual_profit_pct
            state["trigger_threshold_equity_usdt"] = threshold_equity
            self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)

        if bool(state.get("close_complete")):
            self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)
            return {
                "status": "ALREADY_TRIGGERED",
                "triggered": True,
                "close_complete": True,
                "cycle_date": cycle_key,
                "baseline_equity": round(baseline_equity, 8),
                "current_equity": round(current_equity, 8),
                "threshold_equity": round(threshold_equity, 8),
                "arming_threshold_equity": round(arming_threshold_equity, 8),
                "actual_profit_pct": round(actual_profit_pct, 8),
                "peak_equity": round(peak_equity, 8),
                "peak_profit_pct": round(peak_profit_pct, 8),
                "giveback_pct": active_giveback_pct,
                "armed": armed,
                "reduce_ratio": active_reduce_ratio,
            }

        # New portfolio take-profit executions use a persistent, ordinary
        # reduce-only GTC limit order. Keep the legacy market plan available so
        # a service upgrade can finish an already-triggered pre-v3 cycle
        # without silently changing its execution semantics.
        raw_limit_plan = state.get("portfolio_limit_plan")
        legacy_reduction_plan = state.get("reduction_plan")
        if (
            not isinstance(raw_limit_plan, list)
            and isinstance(legacy_reduction_plan, list)
            and active_reduce_ratio < 1.0 - 1e-12
        ):
            reduction_plan = [dict(item) for item in legacy_reduction_plan if isinstance(item, dict)]
            close_summary = self._execute_portfolio_take_profit_reduction_plan(reduction_plan)
            state["reduction_plan"] = reduction_plan
        else:
            if not isinstance(raw_limit_plan, list):
                raw_limit_plan = self._build_portfolio_take_profit_limit_plan(active_reduce_ratio)
                state["portfolio_limit_plan"] = raw_limit_plan
                self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)
            limit_plan = [dict(item) for item in raw_limit_plan if isinstance(item, dict)]
            close_summary = self._execute_portfolio_take_profit_limit_plan(limit_plan)
            state["portfolio_limit_plan"] = limit_plan
        close_summary["reduce_ratio"] = active_reduce_ratio
        close_complete = (
            int(close_summary.get("errors", 0) or 0) == 0
            and int(close_summary.get("pending", 0) or 0) == 0
        )
        state["close_complete"] = close_complete
        state["last_close_summary"] = close_summary
        if not bool(state.get("notification_sent")):
            try:
                notification_label = "组合移动止盈" if active_giveback_pct > 0.0 else "组合止盈"
                self.notifier.send(
                    f"【Top10做空】{notification_label} +{actual_profit_pct:.2f}%",
                    self._build_portfolio_take_profit_notification(
                        close_summary,
                        baseline_equity=baseline_equity,
                        current_equity=current_equity,
                        threshold_equity=threshold_equity,
                        actual_profit_pct=actual_profit_pct,
                        arming_profit_pct=active_profit_pct,
                        giveback_pct=active_giveback_pct,
                        peak_equity=peak_equity,
                        peak_profit_pct=peak_profit_pct,
                        reduce_ratio=active_reduce_ratio,
                        cycle_date=cycle_key,
                    ),
                )
                state["notification_sent"] = True
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Portfolio take-profit notification failed account=%s: %s", self.account_id, exc)
        self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)

        return {
            "status": "TRIGGERED" if not already_triggered else "TRIGGERED_RETRY",
            "triggered": True,
            "close_complete": close_complete,
            "cycle_date": cycle_key,
            "baseline_equity": round(baseline_equity, 8),
            "current_equity": round(current_equity, 8),
            "threshold_equity": round(threshold_equity, 8),
            "arming_threshold_equity": round(arming_threshold_equity, 8),
            "actual_profit_pct": round(actual_profit_pct, 8),
            "peak_equity": round(peak_equity, 8),
            "peak_profit_pct": round(peak_profit_pct, 8),
            "giveback_pct": active_giveback_pct,
            "armed": armed,
            "reduce_ratio": active_reduce_ratio,
            **close_summary,
        }

    def _build_portfolio_take_profit_limit_plan(
        self,
        reduce_ratio: float,
    ) -> List[Dict[str, object]]:
        """Build the immutable per-position plan for a portfolio take-profit.

        The price is captured from ``positionRisk.markPrice`` at trigger time.
        It is deliberately stored in the lock state so retries do not chase a
        later price. A missing mark price is treated as a plan error rather
        than silently falling back to an entry price.
        """
        risks = self._get_all_position_risks()
        plan: List[Dict[str, object]] = []
        for risk in risks:
            symbol = str(risk.get("symbol") or "").strip().upper()
            if not symbol or self._is_protection_exempt(symbol):
                continue
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            initial_qty = abs(position_amt)
            if initial_qty <= 1e-12:
                continue

            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                position_amt=position_amt,
                position_side=position_side,
            )
            trigger_price = self._portfolio_take_profit_reference_price(symbol, risk)
            formatted_price = self.client.format_trigger_price(
                symbol,
                trigger_price,
                round_up=close_side == "SELL",
            )
            normalized_price = self._safe_positive_float(formatted_price)
            if normalized_price is None:
                raise RuntimeError(
                    f"invalid portfolio take-profit limit price symbol={symbol} price={formatted_price}"
                )

            tracked_pos = (
                self._find_open_position_for_exchange_symbol(symbol)
                if position_amt < 0
                else None
            )
            plan.append(
                {
                    "key": f"{symbol}:{position_side}",
                    "symbol": symbol,
                    "position_side": position_side,
                    "initial_qty": initial_qty,
                    "target_remaining_qty": initial_qty * (1.0 - reduce_ratio),
                    "tracked_position_id": (
                        int(tracked_pos["id"]) if tracked_pos is not None else None
                    ),
                    "close_side": close_side,
                    "use_reduce_only": use_reduce_only,
                    # Keep the exchange client's fixed-point text. Converting
                    # this to float makes small prices serialize as scientific
                    # notation (for example, ``8.598e-05``), which Binance
                    # rejects for LIMIT orders.
                    "limit_price": str(formatted_price).strip(),
                    "limit_price_source": "positionRisk.markPrice",
                    "portfolio_order_id": None,
                    "portfolio_client_order_id": None,
                    "portfolio_order_status": None,
                    "portfolio_requested_qty": None,
                    "portfolio_executed_qty": 0.0,
                    "portfolio_fill_recorded_qty": 0.0,
                    "action_complete": False,
                    "protection_complete": False,
                    "retry_count": 0,
                }
            )
        return plan

    def _portfolio_take_profit_reference_price(
        self,
        symbol: str,
        risk: Dict[str, object],
    ) -> float:
        for field in ("markPrice", "lastPrice", "price"):
            price = self._safe_positive_float(risk.get(field))
            if price is not None:
                return price
        try:
            price = self._safe_positive_float(self.client.get_symbol_price(symbol))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"failed to fetch portfolio limit reference price symbol={symbol}: {exc}") from exc
        if price is None:
            raise RuntimeError(f"missing portfolio limit reference price symbol={symbol}")
        return price

    def _canonicalize_portfolio_take_profit_limit_price(
        self,
        item: Dict[str, object],
    ) -> str:
        """Return a Binance-compatible fixed-point price for a persisted plan.

        Plans created before this method was added may contain a float rendered
        in scientific notation. Re-format the immutable stored price using the
        symbol's tick size so retries repair those plans without chasing the
        current market price.
        """
        symbol = str(item.get("symbol") or "").strip().upper()
        raw_price = item.get("limit_price")
        numeric_price = self._safe_positive_float(raw_price)
        if numeric_price is None:
            raise ValueError(f"invalid persisted portfolio limit price symbol={symbol} price={raw_price}")

        close_side = str(item.get("close_side") or "BUY").strip().upper() or "BUY"
        formatted_price = self.client.format_trigger_price(
            symbol,
            numeric_price,
            round_up=close_side == "SELL",
        )
        formatted_text = str(formatted_price or "").strip()
        if self._safe_positive_float(formatted_text) is None:
            raise ValueError(
                f"invalid formatted portfolio limit price symbol={symbol} price={formatted_price}"
            )
        return formatted_text

    def _execute_portfolio_take_profit_limit_plan(
        self,
        limit_plan: List[Dict[str, object]],
    ) -> Dict[str, object]:
        summary: Dict[str, object] = {
            "total": len(limit_plan),
            "closed_take_profit": 0,
            "adjusted_take_profit": 0,
            "pending": 0,
            "errors": 0,
            "limit_orders_placed": 0,
            "closed_symbols": [],
            "adjusted_symbols": [],
            "failed_symbols": [],
        }
        details: Dict[str, List[str]] = {
            "closed_take_profit": [],
            "adjusted_take_profit": [],
            "errors": [],
        }
        closed_symbols: List[str] = []
        adjusted_symbols: List[str] = []
        failed_symbols: List[str] = []

        try:
            risks = self._get_all_position_risks()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Portfolio take-profit failed to query exchange positions: %s", exc)
            summary["errors"] = 1
            summary["pending"] = len(limit_plan)
            details["errors"].append(f"fetch_position_risk_failed: {exc}")
            summary["closed_symbols"] = closed_symbols
            summary["adjusted_symbols"] = adjusted_symbols
            summary["failed_symbols"] = failed_symbols
            summary["details"] = details
            return summary

        risk_map: Dict[tuple[str, str], Dict[str, object]] = {}
        for risk in risks:
            symbol = str(risk.get("symbol") or "").strip().upper()
            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            if symbol:
                risk_map[(symbol, position_side)] = risk

        for item in limit_plan:
            symbol = str(item.get("symbol") or "").strip().upper()
            position_side = str(item.get("position_side") or "BOTH").strip().upper() or "BOTH"
            result: Dict[str, object]
            try:
                result = self._advance_portfolio_take_profit_limit_item(
                    item=item,
                    risk=risk_map.get((symbol, position_side)),
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception(
                    "Portfolio take-profit limit execution failed symbol=%s position_side=%s: %s",
                    symbol,
                    position_side,
                    exc,
                )
                result = {
                    "pending": True,
                    "error": True,
                    "detail": f"{symbol}(limit): {exc}",
                }

            if bool(result.get("pending")):
                summary["pending"] = int(summary["pending"]) + 1
            if bool(result.get("error")):
                summary["errors"] = int(summary["errors"]) + 1
                failed_symbols.append(symbol)
                details["errors"].append(str(result.get("detail") or f"{symbol}(limit)"))
            kind = str(result.get("kind") or "")
            detail = str(result.get("detail") or "")
            if kind == "closed":
                summary["closed_take_profit"] = int(summary["closed_take_profit"]) + 1
                closed_symbols.append(symbol)
                if detail:
                    details["closed_take_profit"].append(detail)
            elif kind == "adjusted":
                summary["adjusted_take_profit"] = int(summary["adjusted_take_profit"]) + 1
                adjusted_symbols.append(symbol)
                if detail:
                    details["adjusted_take_profit"].append(detail)
            if bool(result.get("placed")):
                summary["limit_orders_placed"] = int(summary["limit_orders_placed"]) + 1

        summary["closed_symbols"] = list(dict.fromkeys(closed_symbols))
        summary["adjusted_symbols"] = list(dict.fromkeys(adjusted_symbols))
        summary["failed_symbols"] = list(dict.fromkeys(failed_symbols))
        summary["details"] = details
        return summary

    def _advance_portfolio_take_profit_limit_item(
        self,
        item: Dict[str, object],
        risk: Optional[Dict[str, object]],
    ) -> Dict[str, object]:
        symbol = str(item.get("symbol") or "").strip().upper()
        position_side = str(item.get("position_side") or "BOTH").strip().upper() or "BOTH"
        target_qty = max(0.0, self._safe_float(item.get("target_remaining_qty"), default=0.0))
        current_qty = abs(self._safe_float(risk.get("positionAmt"), default=0.0)) if risk else 0.0
        qty_eps = max(1e-12, target_qty * 1e-10)

        order: Optional[Dict[str, object]] = None
        order_status = str(item.get("portfolio_order_status") or "").strip().upper()
        order_id = item.get("portfolio_order_id")
        client_order_id = item.get("portfolio_client_order_id")
        if order_id or client_order_id:
            try:
                order = self._get_order(
                    symbol=symbol,
                    order_id=order_id,
                    client_order_id=client_order_id,
                )
            except Exception as exc:  # noqa: BLE001
                item["last_error"] = str(exc)
                return {
                    "pending": True,
                    "error": True,
                    "detail": f"{symbol}(order_status): {exc}",
                }
            if order is None:
                order_status = "NOT_FOUND"
            else:
                order_status = str(order.get("status") or "").strip().upper()
                item["portfolio_order_status"] = order_status or "UNKNOWN"
                executed_qty = self._safe_float(order.get("executedQty"), default=0.0)
                if order_status == "FILLED" and executed_qty <= 0:
                    executed_qty = self._safe_float(
                        item.get("portfolio_requested_qty"),
                        default=0.0,
                    )
                if executed_qty > 0:
                    item["portfolio_executed_qty"] = max(
                        self._safe_float(item.get("portfolio_executed_qty"), default=0.0),
                        executed_qty,
                    )
                if order_status in self.PORTFOLIO_LIMIT_TERMINAL_STATUSES and executed_qty > 0:
                    self._record_portfolio_limit_fill(item=item, order=order, executed_qty=executed_qty)

            if order_status in self.PORTFOLIO_LIMIT_ACTIVE_STATUSES:
                if current_qty <= target_qty + qty_eps:
                    self._cancel_order_if_exists(symbol, order_id, client_order_id)
                    item["portfolio_order_status"] = "CANCELED"
                    self._clear_portfolio_take_profit_order(item)
                    return self._complete_portfolio_take_profit_item(
                        item=item,
                        current_qty=current_qty,
                        target_qty=target_qty,
                        portfolio_filled=False,
                    )
                return {
                    "pending": True,
                    "detail": (
                        f"{symbol}(limit_order_id={order_id or '-'}, status={order_status}, "
                        f"remaining={current_qty})"
                    ),
                }

            if order_status == "FILLED":
                filled_qty = self._safe_float(order.get("executedQty"), default=0.0) if order else 0.0
                if filled_qty <= 0:
                    filled_qty = self._safe_float(item.get("portfolio_requested_qty"), default=0.0)
                # A FILLED response can race the positionRisk refresh. Treat
                # the order's executed quantity as progress, but never below
                # the immutable target remaining quantity.
                effective_qty = max(target_qty, current_qty - max(0.0, filled_qty))
                if effective_qty <= target_qty + qty_eps:
                    return self._complete_portfolio_take_profit_item(
                        item=item,
                        current_qty=effective_qty,
                        target_qty=target_qty,
                        portfolio_filled=True,
                    )
                self._clear_portfolio_take_profit_order(item)
            elif order_status in self.PORTFOLIO_LIMIT_TERMINAL_STATUSES or order_status == "NOT_FOUND":
                self._clear_portfolio_take_profit_order(item)
            else:
                return {
                    "pending": True,
                    "error": True,
                    "detail": f"{symbol}(order_status_unknown={order_status or 'EMPTY'})",
                }

        if current_qty <= target_qty + qty_eps:
            return self._complete_portfolio_take_profit_item(
                item=item,
                current_qty=current_qty,
                target_qty=target_qty,
                portfolio_filled=False,
            )

        close_qty = current_qty - target_qty
        try:
            formatted_qty = self.client.format_order_qty(symbol, close_qty)
            order_qty = self._safe_float(formatted_qty, default=0.0)
        except Exception as exc:  # noqa: BLE001
            return {"pending": True, "error": True, "detail": f"{symbol}(quantity): {exc}"}
        if order_qty <= 0:
            item["action_complete"] = True
            item["protection_complete"] = True
            item["skipped_reason"] = "FORMATTED_QTY_ZERO"
            return {
                "kind": "adjusted" if target_qty > 0 else "closed",
                "detail": f"{symbol}(formatted_qty_zero, target_remaining={target_qty})",
            }

        try:
            limit_price = self._canonicalize_portfolio_take_profit_limit_price(item)
        except Exception as exc:  # noqa: BLE001
            item["portfolio_order_status"] = "REJECTED"
            item["last_error"] = str(exc)
            item["retry_count"] = int(self._safe_float(item.get("retry_count"), default=0.0)) + 1
            return {
                "pending": True,
                "error": True,
                "detail": f"{symbol}(limit_price): {exc}",
            }
        item["limit_price"] = limit_price

        client_id = str(item.get("portfolio_client_order_id") or "").strip()
        if not client_id:
            client_id = self._new_client_id("pftlim", symbol)
            item["portfolio_client_order_id"] = client_id
        order_params: Dict[str, object] = {
            "symbol": symbol,
            "side": str(item.get("close_side") or "BUY").strip().upper(),
            "type": "LIMIT",
            "timeInForce": "GTC",
            "price": limit_price,
            "quantity": formatted_qty,
            "newClientOrderId": client_id,
            "newOrderRespType": "RESULT",
        }
        if bool(item.get("use_reduce_only", True)):
            order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            order_params["positionSide"] = position_side

        item["portfolio_requested_qty"] = order_qty
        item["portfolio_order_status"] = "SUBMITTING"
        try:
            created_order = self.client.create_order(**order_params)
        except OrderStateUnknownError as exc:
            item["portfolio_order_status"] = "UNKNOWN"
            item["last_error"] = str(exc)
            item["retry_count"] = int(self._safe_float(item.get("retry_count"), default=0.0)) + 1
            return {
                "pending": True,
                "error": True,
                "detail": f"{symbol}(limit_submit_unknown): {exc}",
            }
        except Exception as exc:  # noqa: BLE001
            self._clear_portfolio_take_profit_order(item)
            item["portfolio_order_status"] = "REJECTED"
            item["last_error"] = str(exc)
            item["retry_count"] = int(self._safe_float(item.get("retry_count"), default=0.0)) + 1
            return {
                "pending": True,
                "error": True,
                "detail": f"{symbol}(limit_submit): {exc}",
            }

        created_order = dict(created_order or {})
        created_status = str(created_order.get("status") or "NEW").strip().upper() or "NEW"
        item["portfolio_order_id"] = created_order.get("orderId")
        item["portfolio_client_order_id"] = (
            created_order.get("clientOrderId") or created_order.get("clientAlgoId") or client_id
        )
        item["portfolio_order_status"] = created_status
        created_executed_qty = self._safe_float(created_order.get("executedQty"), default=0.0)
        if created_status == "FILLED" and created_executed_qty <= 0:
            created_executed_qty = order_qty
        if created_executed_qty > 0:
            item["portfolio_executed_qty"] = max(
                self._safe_float(item.get("portfolio_executed_qty"), default=0.0),
                created_executed_qty,
            )
        if created_status in self.PORTFOLIO_LIMIT_TERMINAL_STATUSES and created_executed_qty > 0:
            self._record_portfolio_limit_fill(
                item=item,
                order=created_order,
                executed_qty=created_executed_qty,
            )
        try:
            self.store.add_order_event(
                symbol=symbol,
                position_id=self._safe_optional_int(item.get("tracked_position_id")),
                event_time_utc=self._utc_now_iso(),
                order_payload=created_order,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Portfolio limit order persistence failed symbol=%s: %s", symbol, exc)

        if created_status in self.PORTFOLIO_LIMIT_ACTIVE_STATUSES:
            if created_status == "PARTIALLY_FILLED" and created_executed_qty > 0:
                effective_qty = max(0.0, current_qty - created_executed_qty)
                if effective_qty <= target_qty + qty_eps:
                    self._cancel_order_if_exists(
                        symbol,
                        item.get("portfolio_order_id"),
                        item.get("portfolio_client_order_id"),
                    )
                    self._clear_portfolio_take_profit_order(item)
                    return self._complete_portfolio_take_profit_item(
                        item=item,
                        current_qty=effective_qty,
                        target_qty=target_qty,
                        portfolio_filled=False,
                    )
            return {
                "pending": True,
                "placed": True,
                "detail": (
                    f"{symbol}(limit_order_id={item.get('portfolio_order_id') or '-'}, "
                    f"price={item.get('limit_price')}, qty={order_qty}, status={created_status})"
                ),
            }

        if created_status == "FILLED":
            effective_qty = max(target_qty, current_qty - max(0.0, created_executed_qty))
            if effective_qty <= target_qty + qty_eps:
                result = self._complete_portfolio_take_profit_item(
                    item=item,
                    current_qty=effective_qty,
                    target_qty=target_qty,
                    portfolio_filled=True,
                )
                result["placed"] = True
                return result

        if created_status in self.PORTFOLIO_LIMIT_TERMINAL_STATUSES:
            self._clear_portfolio_take_profit_order(item)
            return {
                "pending": True,
                "error": True,
                "placed": True,
                "detail": f"{symbol}(limit_submit_terminal={created_status})",
            }
        return {
            "pending": True,
            "error": True,
            "placed": True,
            "detail": f"{symbol}(limit_submit_unknown={created_status})",
        }

    def _complete_portfolio_take_profit_item(
        self,
        item: Dict[str, object],
        current_qty: float,
        target_qty: float,
        portfolio_filled: bool,
    ) -> Dict[str, object]:
        symbol = str(item.get("symbol") or "").strip().upper()
        position_id = self._safe_optional_int(item.get("tracked_position_id"))
        portfolio_order_id = self._safe_optional_int(item.get("portfolio_order_id"))
        tracked_pos = self.store.get_position(position_id) if position_id is not None else None
        if current_qty <= 1e-12:
            if tracked_pos is not None and str(tracked_pos.get("status") or "").upper() == "OPEN":
                if portfolio_filled:
                    self._cancel_exit_orders(tracked_pos)
                    self.store.mark_position_closed(
                        position_id=position_id,
                        status="CLOSED_PORTFOLIO_TAKE_PROFIT",
                        close_reason="PORTFOLIO_EQUITY_TAKE_PROFIT",
                        close_order_id=portfolio_order_id,
                    )
                else:
                    close_result = self._close_if_recorded_exit_filled(tracked_pos)
                    if close_result is None:
                        self._close_external_missing_short(tracked_pos)
            item["action_complete"] = True
            item["protection_complete"] = True
            if portfolio_filled:
                self._clear_portfolio_take_profit_order(item)
            return {
                "kind": "closed",
                "detail": (
                    f"{symbol}(qty=0, limit_order_id={item.get('portfolio_order_id') or '-'}, "
                    f"filled={portfolio_filled})"
                ),
            }

        if current_qty <= target_qty + max(1e-12, target_qty * 1e-10):
            if tracked_pos is not None and str(tracked_pos.get("status") or "").upper() == "OPEN":
                entry_price = self._safe_positive_float(tracked_pos.get("entry_price")) or 0.0
                self.store.set_position_qty(int(position_id), current_qty, entry_price)
            item["action_complete"] = True
            # The original TP/SL intentionally remains in place for a partial
            # reduction. It is only canceled after the exchange position is
            # actually zero.
            item["protection_complete"] = True
            if portfolio_filled:
                self._clear_portfolio_take_profit_order(item)
            return {
                "kind": "adjusted",
                "detail": (
                    f"{symbol}(remaining={current_qty}, target_remaining={target_qty}, "
                    f"limit_order_id={item.get('portfolio_order_id') or '-'})"
                ),
            }
        return {"pending": True}

    def _record_portfolio_limit_fill(
        self,
        item: Dict[str, object],
        order: Dict[str, object],
        executed_qty: float,
    ) -> None:
        recorded_qty = self._safe_float(item.get("portfolio_fill_recorded_qty"), default=0.0)
        if executed_qty <= recorded_qty + max(1e-12, executed_qty * 1e-9):
            return
        self._market_fill_reconciler.record_market_order(
            symbol=str(item.get("symbol") or "").strip().upper(),
            position_id=self._safe_optional_int(item.get("tracked_position_id")),
            order=order,
        )
        item["portfolio_fill_recorded_qty"] = executed_qty

    @staticmethod
    def _clear_portfolio_take_profit_order(item: Dict[str, object]) -> None:
        item["portfolio_order_id"] = None
        item["portfolio_client_order_id"] = None
        item["portfolio_order_status"] = None
        # Fill reconciliation is per exchange order. A replacement order may
        # legitimately execute less than the previous order's quantity.
        item["portfolio_fill_recorded_qty"] = 0.0

    def _build_portfolio_take_profit_reduction_plan(
        self,
        reduce_ratio: float,
    ) -> List[Dict[str, object]]:
        risks = self._get_all_position_risks()
        plan: List[Dict[str, object]] = []
        for risk in risks:
            symbol = str(risk.get("symbol") or "").strip().upper()
            if not symbol or self._is_protection_exempt(symbol):
                continue
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            initial_qty = abs(position_amt)
            if initial_qty <= 1e-12:
                continue
            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            tracked_pos = (
                self._find_open_position_for_exchange_symbol(symbol)
                if position_amt < 0
                else None
            )
            plan.append(
                {
                    "key": f"{symbol}:{position_side}",
                    "symbol": symbol,
                    "position_side": position_side,
                    "initial_qty": initial_qty,
                    "target_remaining_qty": initial_qty * (1.0 - reduce_ratio),
                    "tracked_position_id": int(tracked_pos["id"]) if tracked_pos is not None else None,
                    "action_complete": False,
                    "protection_complete": tracked_pos is None,
                }
            )
        return plan

    def _execute_portfolio_take_profit_reduction_plan(
        self,
        reduction_plan: List[Dict[str, object]],
    ) -> Dict[str, object]:
        summary: Dict[str, object] = {
            "total": len(reduction_plan),
            "closed_take_profit": 0,
            "adjusted_take_profit": 0,
            "pending": 0,
            "errors": 0,
            "closed_symbols": [],
            "adjusted_symbols": [],
            "failed_symbols": [],
        }
        details: Dict[str, List[str]] = {"adjusted_take_profit": [], "errors": []}
        adjusted_symbols: List[str] = []
        failed_symbols: List[str] = []
        risks = self._get_all_position_risks()
        risk_map: Dict[tuple[str, str], Dict[str, Any]] = {}
        for risk in risks:
            symbol = str(risk.get("symbol") or "").strip().upper()
            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            if symbol:
                risk_map[(symbol, position_side)] = risk

        for item in reduction_plan:
            symbol = str(item.get("symbol") or "").strip().upper()
            position_side = str(item.get("position_side") or "BOTH").strip().upper() or "BOTH"
            target_remaining_qty = max(
                0.0,
                self._safe_float(item.get("target_remaining_qty"), default=0.0),
            )
            risk = risk_map.get((symbol, position_side))
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0) if risk else 0.0
            current_qty = abs(position_amt)
            qty_eps = max(1e-12, target_remaining_qty * 1e-10)

            if not bool(item.get("action_complete")):
                if current_qty <= target_remaining_qty + qty_eps:
                    item["action_complete"] = True
                else:
                    close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                        position_amt=position_amt,
                        position_side=position_side,
                    )
                    requested_qty = current_qty - target_remaining_qty
                    try:
                        formatted_qty = self.client.format_order_qty(symbol, requested_qty)
                        order_qty = self._safe_float(formatted_qty, default=0.0)
                        if order_qty <= 0:
                            item["action_complete"] = True
                            item["skipped_reason"] = "FORMATTED_QTY_ZERO"
                        else:
                            order_params: Dict[str, object] = {
                                "symbol": symbol,
                                "side": close_side,
                                "type": "MARKET",
                                "quantity": formatted_qty,
                                "newClientOrderId": self._new_client_id("pft", symbol),
                                "newOrderRespType": "RESULT",
                            }
                            if use_reduce_only:
                                order_params["reduceOnly"] = True
                            if position_side in {"LONG", "SHORT"}:
                                order_params["positionSide"] = position_side
                            close_order = self.client.create_order(**order_params)
                            self._market_fill_reconciler.record_market_order(
                                symbol=symbol,
                                position_id=(
                                    int(item["tracked_position_id"])
                                    if item.get("tracked_position_id") is not None
                                    else None
                                ),
                                order=close_order,
                            )
                            executed_qty = self._safe_float(
                                close_order.get("executedQty"),
                                default=order_qty,
                            )
                            if executed_qty <= 0:
                                executed_qty = order_qty
                            current_qty = max(0.0, current_qty - min(current_qty, executed_qty))
                            item["last_close_order_id"] = close_order.get("orderId")
                            item["executed_reduce_qty"] = self._safe_float(
                                item.get("executed_reduce_qty"),
                                default=0.0,
                            ) + executed_qty
                            item["action_complete"] = current_qty <= target_remaining_qty + qty_eps
                            summary["adjusted_take_profit"] = int(summary["adjusted_take_profit"]) + 1
                            adjusted_symbols.append(symbol)
                            details["adjusted_take_profit"].append(
                                f"{symbol}(qty={executed_qty}, remaining={current_qty}, "
                                f"position_side={position_side}, close_order_id={close_order.get('orderId')})"
                            )
                    except Exception as exc:  # noqa: BLE001
                        summary["errors"] = int(summary["errors"]) + 1
                        failed_symbols.append(symbol)
                        details["errors"].append(f"{symbol}(partial_close): {exc}")
                        LOGGER.exception(
                            "Portfolio take-profit partial close failed symbol=%s position_side=%s: %s",
                            symbol,
                            position_side,
                            exc,
                        )

            tracked_position_id = item.get("tracked_position_id")
            if bool(item.get("action_complete")) and tracked_position_id is not None:
                if current_qty <= 1e-12:
                    item["protection_complete"] = True
                elif not bool(item.get("protection_complete")):
                    tracked_pos = self._find_open_position_for_exchange_symbol(symbol)
                    if tracked_pos is None:
                        item["protection_complete"] = True
                    else:
                        try:
                            self._refresh_tracked_position_after_portfolio_reduction(
                                pos=tracked_pos,
                                remaining_qty=current_qty,
                                entry_price=(
                                    self._safe_positive_float(risk.get("entryPrice"))
                                    if risk is not None
                                    else None
                                ),
                                position_side=position_side,
                                use_reduce_only=(position_side not in {"LONG", "SHORT"}),
                            )
                            item["protection_complete"] = True
                        except Exception as exc:  # noqa: BLE001
                            summary["errors"] = int(summary["errors"]) + 1
                            failed_symbols.append(symbol)
                            details["errors"].append(f"{symbol}(protection_refresh): {exc}")
                            self.store.set_position_error(
                                int(tracked_position_id),
                                f"portfolio_take_profit protection refresh: {exc}",
                            )
                            LOGGER.exception(
                                "Portfolio take-profit protection refresh failed symbol=%s: %s",
                                symbol,
                                exc,
                            )

            if not bool(item.get("action_complete")) or not bool(item.get("protection_complete")):
                summary["pending"] = int(summary["pending"]) + 1

        summary["adjusted_symbols"] = list(dict.fromkeys(adjusted_symbols))
        summary["failed_symbols"] = list(dict.fromkeys(failed_symbols))
        summary["details"] = details
        return summary

    def _refresh_tracked_position_after_portfolio_reduction(
        self,
        pos: Dict[str, object],
        remaining_qty: float,
        entry_price: Optional[float],
        position_side: str,
        use_reduce_only: bool,
    ) -> None:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        stored_entry_price = self._safe_positive_float(pos.get("entry_price")) or 0.0
        self.store.set_position_qty(position_id, remaining_qty, entry_price or stored_entry_price)

        sl_price = self._safe_positive_float(pos.get("sl_price"))
        if sl_price is None:
            raise RuntimeError(f"missing stop price for remaining {symbol} position")
        sl_order = None
        tp_order = None
        try:
            sl_order = self._create_stop_order_with_fallback(
                symbol=symbol,
                side="BUY",
                stop_price=self.client.format_trigger_price(symbol, sl_price, round_up=True),
                qty=remaining_qty,
                client_order_id=self._new_client_id("sl", symbol),
                position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                use_reduce_only=use_reduce_only,
            )
            tp_price = self._safe_positive_float(pos.get("tp_price"))
            if tp_price is not None:
                tp_params: Dict[str, object] = {
                    "symbol": symbol,
                    "side": "BUY",
                    "type": "TAKE_PROFIT_MARKET",
                    "stopPrice": self.client.format_trigger_price(symbol, tp_price, round_up=False),
                    "quantity": self.client.format_order_qty(symbol, remaining_qty),
                    "workingType": self.trigger_price_type,
                    "priceProtect": True,
                    "newClientOrderId": self._new_client_id("tpfix", symbol),
                }
                if use_reduce_only:
                    tp_params["reduceOnly"] = True
                if position_side in {"LONG", "SHORT"}:
                    tp_params["positionSide"] = position_side
                tp_order = self.client.create_order(**tp_params)

            self.store.update_position_orders(
                position_id=position_id,
                tp_order_id=tp_order.get("orderId") if tp_order else None,
                sl_order_id=sl_order.get("orderId"),
                tp_client_order_id=tp_order.get("clientOrderId") if tp_order else None,
                sl_client_order_id=sl_order.get("clientOrderId"),
                tp_price=tp_price,
                sl_price=sl_price,
                liq_price_latest=self._safe_positive_float(pos.get("liq_price_latest")),
            )
        except Exception:
            if tp_order is not None:
                self._cancel_order_if_exists(symbol, tp_order.get("orderId"), tp_order.get("clientOrderId"))
            if sl_order is not None:
                self._cancel_order_if_exists(symbol, sl_order.get("orderId"), sl_order.get("clientOrderId"))
            raise

        self._cancel_order_if_exists(symbol, pos.get("tp_order_id"), pos.get("tp_client_order_id"))
        self._cancel_order_if_exists(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id"))
        self.store.clear_position_error(position_id)

    def _close_all_exchange_positions_for_portfolio_loss_cut(self) -> Dict[str, object]:
        summary: Dict[str, object] = {
            "total": 0,
            "closed_loss_cut": 0,
            "errors": 0,
            "closed_symbols": [],
            "failed_symbols": [],
        }
        details: Dict[str, List[str]] = {"closed_loss_cut": [], "errors": []}
        closed_symbols: List[str] = []
        failed_symbols: List[str] = []
        try:
            risks = self._get_all_position_risks()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Portfolio loss-cut failed to query exchange positions: %s", exc)
            summary["errors"] = 1
            details["errors"].append(f"fetch_position_risk_failed: {exc}")
            summary["closed_symbols"] = closed_symbols
            summary["failed_symbols"] = failed_symbols
            summary["details"] = details
            return summary

        for risk in risks:
            symbol = str(risk.get("symbol") or "").strip().upper()
            if not symbol or self._is_protection_exempt(symbol):
                continue
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if abs(position_amt) <= 1e-12:
                continue
            summary["total"] = int(summary["total"]) + 1
            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                position_amt=position_amt,
                position_side=position_side,
            )
            tracked_pos = self._find_open_position_for_exchange_symbol(symbol)
            tracked_position_id = int(tracked_pos["id"]) if tracked_pos is not None else None
            try:
                close_info = self._close_daily_loss_cut(
                    symbol=symbol,
                    qty=abs(position_amt),
                    side=close_side,
                    position_id=tracked_position_id,
                    cancel_pos=tracked_pos,
                    position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                    use_reduce_only=use_reduce_only,
                    close_status="CLOSED_PORTFOLIO_LOSS_CUT",
                    close_reason="PORTFOLIO_EQUITY_LOSS_CUT",
                    client_id_tag="plc",
                )
                summary["closed_loss_cut"] = int(summary["closed_loss_cut"]) + 1
                details["closed_loss_cut"].append(
                    f"{symbol}(qty={close_info['qty']}, side={close_side}, "
                    f"position_side={position_side}, reduce_only={use_reduce_only}, "
                    f"close_order_id={close_info['close_order_id']})"
                )
                closed_symbols.append(symbol)
            except Exception as exc:  # noqa: BLE001
                summary["errors"] = int(summary["errors"]) + 1
                LOGGER.exception("Portfolio loss-cut failed for exchange position symbol=%s: %s", symbol, exc)
                details["errors"].append(
                    f"{symbol}(qty={abs(position_amt)}, side={close_side}, "
                    f"position_side={position_side}): {exc}"
                )
                failed_symbols.append(symbol)

        summary["closed_symbols"] = closed_symbols
        summary["failed_symbols"] = failed_symbols
        summary["details"] = details
        return summary

    @_serialized_account_mutation
    def run_noon_protection_stop(
        self,
        day_start_utc: datetime,
        noon_time_utc: datetime,
        symbols: Optional[Set[str]] = None,
    ) -> Dict[str, object]:
        day_start = day_start_utc.astimezone(timezone.utc).replace(microsecond=0)
        noon_time = noon_time_utc.astimezone(timezone.utc).replace(microsecond=0)
        target_symbols = {
            str(symbol or "").strip().upper()
            for symbol in symbols
            if str(symbol or "").strip()
        } if symbols is not None else None

        tracked_positions = self.store.list_open_positions()
        tracked_by_symbol: Dict[str, Dict[str, object]] = {}
        for pos in tracked_positions:
            symbol = str(pos.get("symbol") or "").strip()
            if symbol and symbol not in tracked_by_symbol:
                tracked_by_symbol[symbol] = pos

        try:
            risks = self._get_all_position_risks()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Noon protection failed to query exchange positions: %s", exc)
            return {
                "total": 0,
                "updated_sl": 0,
                "closed_immediate": 0,
                "skipped": 0,
                "errors": 1,
            }
        candidate_risks = []
        for risk in risks:
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if abs(position_amt) <= 1e-12:
                continue
            symbol = str(risk.get("symbol") or "").strip()
            if not symbol:
                continue
            if target_symbols is not None and symbol.upper() not in target_symbols:
                continue
            if self._is_protection_exempt(symbol):
                continue
            candidate_risks.append(risk)

        summary = {
            "total": len(candidate_risks),
            "updated_sl": 0,
            "closed_immediate": 0,
            "skipped": 0,
            "errors": 0,
        }
        details: Dict[str, List[str]] = {
            "updated_sl": [],
            "closed_immediate": [],
            "errors": [],
        }
        failed_symbols: List[str] = []
        if noon_time <= day_start:
            summary["errors"] += 1
            details["errors"].append(f"invalid_time_window day_start={day_start.isoformat()} noon={noon_time.isoformat()}")
            return summary

        caps = self._load_noon_protection_caps()
        active_cap_keys = set()
        for risk in candidate_risks:
            symbol = str(risk.get("symbol") or "").strip()
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                position_amt=position_amt,
                position_side=position_side,
            )
            tracked_pos = tracked_by_symbol.get(symbol)
            tracked_position_id: Optional[int] = None
            if tracked_pos is not None:
                try:
                    tracked_position_id = int(tracked_pos["id"])
                except (TypeError, ValueError, KeyError):
                    tracked_position_id = None
            cap_key = self._build_protection_cap_key(
                symbol=symbol,
                position_side=position_side,
                position_amt=position_amt,
                tracked_position_id=tracked_position_id,
            )
            active_cap_keys.add(cap_key)
            try:
                if tracked_pos is not None:
                    opened_at_raw = str(tracked_pos.get("opened_at_utc") or "")
                    opened_at_utc = self._parse_iso_utc(opened_at_raw) if opened_at_raw else day_start
                    start_utc = self._noon_protection_window_start(
                        opened_at_utc=opened_at_utc,
                        day_start_utc=day_start,
                        noon_time_utc=noon_time,
                    )
                else:
                    assumed_opened_at_utc = day_start + self.NOON_PROTECTION_UNTRACKED_ENTRY_OFFSET
                    start_utc = self._noon_protection_window_start(
                        opened_at_utc=assumed_opened_at_utc,
                        day_start_utc=day_start,
                        noon_time_utc=noon_time,
                    )
                if start_utc >= noon_time:
                    summary["skipped"] += 1
                    continue

                highest_price, lowest_price = self._fetch_noon_protection_extremes(
                    symbol=symbol,
                    opened_at_utc=opened_at_utc if tracked_pos is not None else assumed_opened_at_utc,
                    day_start_utc=day_start,
                    noon_time_utc=noon_time,
                )
                noon_ref_price = highest_price if close_side == "BUY" else lowest_price
                if not noon_ref_price:
                    raise RuntimeError(
                        f"no_klines_ref_between start={start_utc.isoformat()} end={noon_time.isoformat()}"
                    )

                old_sl_price = self._safe_positive_float(tracked_pos.get("sl_price")) if tracked_pos is not None else None
                if old_sl_price is None and tracked_position_id is not None:
                    old_sl_price = self._safe_positive_float(caps.get(cap_key))

                round_up = close_side == "BUY"
                noon_sl_price = self.client.normalize_trigger_price(symbol, noon_ref_price, round_up=round_up)
                if old_sl_price:
                    merged_sl_price = min(old_sl_price, noon_sl_price) if close_side == "BUY" else max(old_sl_price, noon_sl_price)
                else:
                    merged_sl_price = noon_sl_price

                rules = self.client.get_symbol_rules().get(symbol)
                min_delta = rules.tick_size if rules else 0.0
                if old_sl_price and abs(merged_sl_price - old_sl_price) <= max(min_delta, 1e-12):
                    summary["skipped"] += 1
                    continue

                qty = abs(position_amt)
                if qty <= 0:
                    raise RuntimeError("position qty is zero")

                sl_stop_price = self.client.format_trigger_price(symbol, merged_sl_price, round_up=round_up)
                try:
                    sl_order = self._create_stop_order_with_fallback(
                        symbol=symbol,
                        side=close_side,
                        stop_price=sl_stop_price,
                        qty=qty,
                        client_order_id=self._new_client_id("nsl", symbol),
                        position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                        use_reduce_only=use_reduce_only,
                    )
                except BinanceAPIError as exc:
                    if not self._is_immediate_trigger_error(exc):
                        raise
                    close_info = self._close_protection_immediate(
                        symbol=symbol,
                        qty=qty,
                        side=close_side,
                        position_id=tracked_position_id,
                        position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                        use_reduce_only=use_reduce_only,
                    )
                    if tracked_pos is not None:
                        self._cancel_exit_orders(tracked_pos)
                    caps.pop(cap_key, None)
                    active_cap_keys.discard(cap_key)
                    if tracked_position_id is not None:
                        self.store.clear_position_error(tracked_position_id)
                    summary["closed_immediate"] += 1
                    details["closed_immediate"].append(
                        (
                            f"{symbol}(cap={cap_key}, stop_price={sl_stop_price}, "
                            f"qty={qty}, close_order_id={close_info.get('close_order_id')})"
                        )
                    )
                    continue

                try:
                    if tracked_position_id is not None:
                        self.store.update_stop_loss(
                            position_id=tracked_position_id,
                            sl_order_id=sl_order.get("orderId"),
                            sl_client_order_id=sl_order.get("clientOrderId"),
                            sl_price=merged_sl_price,
                            liq_price_latest=self._safe_positive_float(risk.get("liquidationPrice")),
                        )
                    self.store.add_order_event(
                        symbol=symbol,
                        position_id=tracked_position_id,
                        event_time_utc=self._utc_now_iso(),
                        order_payload=sl_order,
                    )
                except Exception:
                    self._cancel_order_if_exists(symbol, sl_order.get("orderId"), sl_order.get("clientOrderId"))
                    raise
                if tracked_pos is not None:
                    self._cancel_order_if_exists(symbol, tracked_pos.get("sl_order_id"), tracked_pos.get("sl_client_order_id"))
                caps[cap_key] = merged_sl_price
                if tracked_position_id is not None:
                    self.store.clear_position_error(tracked_position_id)
                summary["updated_sl"] += 1
                details["updated_sl"].append(
                    (
                        f"{symbol}(cap={cap_key}, old_sl={old_sl_price}, "
                        f"window_start={start_utc.isoformat()}, noon_ref={noon_ref_price}, "
                        f"new_sl={merged_sl_price}, side={close_side})"
                    )
                )
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                LOGGER.exception("Noon protection stop failed for symbol=%s cap=%s: %s", symbol, cap_key, exc)
                if tracked_position_id is not None:
                    self.store.set_position_error(tracked_position_id, f"noon_protection: {exc}")
                details["errors"].append(f"{symbol}(cap={cap_key}): {exc}")
                if symbol and symbol not in failed_symbols:
                    failed_symbols.append(symbol)

        if target_symbols is None:
            pruned_caps = {
                cap_key: cap_price
                for cap_key, cap_price in caps.items()
                if cap_key in active_cap_keys
            }
        else:
            # A retry is scoped to failed symbols. Preserve the other symbols'
            # caps because this pass intentionally did not inspect them.
            pruned_caps = caps
        self._persist_noon_protection_caps(
            pruned_caps,
            day_start_utc=day_start_utc,
            noon_time_utc=noon_time_utc,
        )
        self._noon_protection_caps_cache = pruned_caps

        if summary["updated_sl"] > 0 or summary["errors"] > 0:
            self.notifier.send(
                "【Top10做空】12:00保护止损汇总",
                self._build_noon_protection_notification(summary, details),
            )
        summary["failed_symbols"] = failed_symbols
        return summary

    @_serialized_account_mutation
    def run_morning_protection_stop(
        self,
        check_time_utc: datetime,
        min_hold_hours: float,
    ) -> Dict[str, object]:
        check_time = check_time_utc.astimezone(timezone.utc).replace(microsecond=0)
        hour_start = check_time.replace(minute=0, second=0, microsecond=0)

        tracked_positions = self.store.list_open_positions()
        tracked_by_symbol: Dict[str, Dict[str, object]] = {}
        for pos in tracked_positions:
            symbol = str(pos.get("symbol") or "").strip()
            if symbol and symbol not in tracked_by_symbol:
                tracked_by_symbol[symbol] = pos

        try:
            risks = self._get_all_position_risks()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Morning protection failed to query exchange positions: %s", exc)
            return {
                "total": 0,
                "updated_sl": 0,
                "closed_immediate": 0,
                "skipped": 0,
                "errors": 1,
            }

        candidate_risks = []
        for risk in risks:
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if abs(position_amt) <= 1e-12:
                continue
            symbol = str(risk.get("symbol") or "").strip()
            if not symbol:
                continue
            if self._is_protection_exempt(symbol):
                continue
            candidate_risks.append(risk)

        summary = {
            "total": len(candidate_risks),
            "updated_sl": 0,
            "closed_immediate": 0,
            "skipped": 0,
            "errors": 0,
        }
        details: Dict[str, List[str]] = {
            "updated_sl": [],
            "closed_immediate": [],
            "errors": [],
        }
        failed_symbols: List[str] = []
        caps = self._load_morning_protection_caps()
        caps_updated_at_by_key = self._load_morning_protection_updated_at_by_key()
        active_cap_keys = set()
        min_hold_seconds = max(0.0, float(min_hold_hours)) * 3600.0

        for risk in candidate_risks:
            symbol = str(risk.get("symbol") or "").strip()
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                position_amt=position_amt,
                position_side=position_side,
            )
            tracked_pos = tracked_by_symbol.get(symbol)
            tracked_position_id: Optional[int] = None
            if tracked_pos is not None:
                try:
                    tracked_position_id = int(tracked_pos["id"])
                except (TypeError, ValueError, KeyError):
                    tracked_position_id = None
            cap_key = self._build_protection_cap_key(
                symbol=symbol,
                position_side=position_side,
                position_amt=position_amt,
                tracked_position_id=tracked_position_id,
            )
            active_cap_keys.add(cap_key)

            try:
                opened_at_raw = str(tracked_pos.get("opened_at_utc") or "") if tracked_pos is not None else ""
                if opened_at_raw:
                    opened_at_utc = self._parse_iso_utc(opened_at_raw)
                else:
                    opened_at_utc = self._reconstruct_position_opened_at_from_trades(
                        symbol=symbol,
                        current_qty=abs(position_amt),
                        entry_side="SELL" if close_side == "BUY" else "BUY",
                    )

                if (check_time - opened_at_utc).total_seconds() < min_hold_seconds:
                    summary["skipped"] += 1
                    continue

                highest_price, lowest_price = self._fetch_symbol_extremes_between(
                    symbol=symbol,
                    start_utc=hour_start,
                    end_utc=check_time,
                )
                morning_ref_price = highest_price if close_side == "BUY" else lowest_price
                if not morning_ref_price:
                    raise RuntimeError(
                        f"no_klines_ref_between start={hour_start.isoformat()} end={check_time.isoformat()}"
                    )

                old_sl_price = self._safe_positive_float(tracked_pos.get("sl_price")) if tracked_pos is not None else None
                if old_sl_price is None:
                    cap_price = self._safe_positive_float(caps.get(cap_key))
                    if tracked_pos is not None:
                        old_sl_price = cap_price
                    elif cap_price is not None:
                        # Ignore stale exchange cap state from a previous position lifecycle.
                        cap_updated_at = caps_updated_at_by_key.get(cap_key)
                        if cap_updated_at is None or opened_at_utc <= cap_updated_at:
                            old_sl_price = cap_price

                round_up = close_side == "BUY"
                morning_sl_price = self.client.normalize_trigger_price(symbol, morning_ref_price, round_up=round_up)
                if old_sl_price:
                    merged_sl_price = min(old_sl_price, morning_sl_price) if close_side == "BUY" else max(old_sl_price, morning_sl_price)
                else:
                    merged_sl_price = morning_sl_price

                rules = self.client.get_symbol_rules().get(symbol)
                min_delta = rules.tick_size if rules else 0.0
                if old_sl_price and abs(merged_sl_price - old_sl_price) <= max(min_delta, 1e-12):
                    summary["skipped"] += 1
                    continue

                qty = abs(position_amt)
                if qty <= 0:
                    raise RuntimeError("position qty is zero")

                sl_stop_price = self.client.format_trigger_price(symbol, merged_sl_price, round_up=round_up)
                try:
                    sl_order = self._create_stop_order_with_fallback(
                        symbol=symbol,
                        side=close_side,
                        stop_price=sl_stop_price,
                        qty=qty,
                        client_order_id=self._new_client_id("msl", symbol),
                        position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                        use_reduce_only=use_reduce_only,
                    )
                except BinanceAPIError as exc:
                    if not self._is_immediate_trigger_error(exc):
                        raise
                    close_info = self._close_protection_immediate(
                        symbol=symbol,
                        qty=qty,
                        side=close_side,
                        position_id=tracked_position_id,
                        position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                        use_reduce_only=use_reduce_only,
                        close_status="CLOSED_MORNING_PROTECTION",
                        close_reason="MORNING_PROTECTION_IMMEDIATE_TRIGGER",
                        client_id_tag="msi",
                    )
                    if tracked_pos is not None:
                        self._cancel_exit_orders(tracked_pos)
                    caps.pop(cap_key, None)
                    caps_updated_at_by_key.pop(cap_key, None)
                    active_cap_keys.discard(cap_key)
                    if tracked_position_id is not None:
                        self.store.clear_position_error(tracked_position_id)
                    summary["closed_immediate"] += 1
                    details["closed_immediate"].append(
                        (
                            f"{symbol}(cap={cap_key}, stop_price={sl_stop_price}, "
                            f"qty={qty}, close_order_id={close_info.get('close_order_id')})"
                        )
                    )
                    continue

                try:
                    if tracked_position_id is not None:
                        self.store.update_stop_loss(
                            position_id=tracked_position_id,
                            sl_order_id=sl_order.get("orderId"),
                            sl_client_order_id=sl_order.get("clientOrderId"),
                            sl_price=merged_sl_price,
                            liq_price_latest=self._safe_positive_float(risk.get("liquidationPrice")),
                        )
                    self.store.add_order_event(
                        symbol=symbol,
                        position_id=tracked_position_id,
                        event_time_utc=self._utc_now_iso(),
                        order_payload=sl_order,
                    )
                except Exception:
                    self._cancel_order_if_exists(symbol, sl_order.get("orderId"), sl_order.get("clientOrderId"))
                    raise
                if tracked_pos is not None:
                    self._cancel_order_if_exists(symbol, tracked_pos.get("sl_order_id"), tracked_pos.get("sl_client_order_id"))
                caps[cap_key] = merged_sl_price
                caps_updated_at_by_key[cap_key] = self._utc_now_datetime()
                if tracked_position_id is not None:
                    self.store.clear_position_error(tracked_position_id)
                summary["updated_sl"] += 1
                details["updated_sl"].append(
                    (
                        f"{symbol}(cap={cap_key}, old_sl={old_sl_price}, "
                        f"morning_ref={morning_ref_price}, new_sl={merged_sl_price}, side={close_side})"
                    )
                )
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                LOGGER.exception("Morning protection stop failed for symbol=%s cap=%s: %s", symbol, cap_key, exc)
                if tracked_position_id is not None:
                    self.store.set_position_error(tracked_position_id, f"morning_protection: {exc}")
                details["errors"].append(f"{symbol}(cap={cap_key}): {exc}")
                if symbol and symbol not in failed_symbols:
                    failed_symbols.append(symbol)

        pruned_caps = {
            cap_key: cap_price
            for cap_key, cap_price in caps.items()
            if cap_key in active_cap_keys
        }
        pruned_updated_at_by_key = {
            cap_key: updated_at
            for cap_key, updated_at in caps_updated_at_by_key.items()
            if cap_key in active_cap_keys
        }
        self._persist_morning_protection_caps(pruned_caps, pruned_updated_at_by_key)
        self._morning_protection_caps_cache = pruned_caps

        if summary["updated_sl"] > 0 or summary["closed_immediate"] > 0 or summary["errors"] > 0:
            self.notifier.send(
                "【Top10做空】07:55早盘保护止损汇总",
                self._build_noon_protection_notification(summary, details, protection_label="07:55早盘"),
            )
        summary["failed_symbols"] = failed_symbols
        return summary

    def _run_daily_loss_cut_tracked_positions(self) -> Dict[str, object]:
        positions = self.store.list_open_positions()
        summary = {
            "total": len(positions),
            "closed_loss_cut": 0,
            "errors": 0,
        }
        details: Dict[str, List[str]] = {
            "closed_loss_cut": [],
            "errors": [],
        }
        closed_symbols: List[str] = []
        failed_symbols: List[str] = []

        for pos in positions:
            position_id = int(pos["id"])
            symbol = str(pos["symbol"])
            try:
                risk = self._get_symbol_position_risk(symbol)
                if risk is None:
                    self.store.set_position_error(position_id, "position risk not found")
                    continue
                if self._is_protection_exempt(symbol):
                    continue

                position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
                if position_amt >= 0:
                    continue

                unrealized_pnl = self._safe_float(risk.get("unRealizedProfit"), default=0.0)
                if unrealized_pnl >= 0:
                    continue

                close_info = self._close_daily_loss_cut(
                    symbol=symbol,
                    qty=abs(position_amt),
                    side="BUY",
                    position_id=position_id,
                    cancel_pos=pos,
                )
                summary["closed_loss_cut"] += 1
                details["closed_loss_cut"].append(
                    f"{symbol}(id={position_id}, upnl={unrealized_pnl:.6f}, qty={close_info['qty']}, "
                    f"close_order_id={close_info['close_order_id']})"
                )
                if symbol and symbol not in closed_symbols:
                    closed_symbols.append(symbol)
                self.store.clear_position_error(position_id)
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                LOGGER.exception("Daily loss-cut failed for position id=%s symbol=%s: %s", position_id, symbol, exc)
                self.store.set_position_error(position_id, str(exc))
                details["errors"].append(f"{symbol}(id={position_id}): {exc}")
                if symbol and symbol not in failed_symbols:
                    failed_symbols.append(symbol)

        if summary["closed_loss_cut"] > 0 or summary["errors"] > 0:
            self.notifier.send(
                "【Top10做空】11:55浮亏止损汇总",
                self._build_daily_loss_cut_notification(summary, details),
            )

        summary["closed_symbols"] = closed_symbols
        summary["failed_symbols"] = failed_symbols
        return summary

    def _run_daily_loss_cut_exchange_positions(self) -> Dict[str, object]:
        summary = {
            "total": 0,
            "closed_loss_cut": 0,
            "errors": 0,
        }
        details: Dict[str, List[str]] = {
            "closed_loss_cut": [],
            "errors": [],
        }
        closed_symbols: List[str] = []
        failed_symbols: List[str] = []

        try:
            risks = self._get_all_position_risks()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Daily loss-cut failed to query exchange positions: %s", exc)
            summary["errors"] += 1
            details["errors"].append(f"fetch_position_risk_failed: {exc}")
            self.notifier.send(
                "【Top10做空】11:55浮亏止损汇总",
                self._build_daily_loss_cut_notification(summary, details),
            )
            return summary

        for risk in risks:
            symbol = str(risk.get("symbol") or "").strip()
            if not symbol:
                continue
            if self._is_protection_exempt(symbol):
                continue

            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if abs(position_amt) <= 1e-12:
                continue

            summary["total"] += 1
            unrealized_pnl = self._safe_float(risk.get("unRealizedProfit"), default=0.0)
            if unrealized_pnl >= 0:
                continue

            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                position_amt=position_amt,
                position_side=position_side,
            )
            tracked_pos = (
                self._find_open_position_for_exchange_symbol(symbol)
                if close_side == "BUY"
                else None
            )
            tracked_position_id = int(tracked_pos["id"]) if tracked_pos is not None else None
            try:
                close_info = self._close_daily_loss_cut(
                    symbol=symbol,
                    qty=abs(position_amt),
                    side=close_side,
                    position_id=tracked_position_id,
                    cancel_pos=tracked_pos,
                    position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                    use_reduce_only=use_reduce_only,
                )
                summary["closed_loss_cut"] += 1
                details["closed_loss_cut"].append(
                    f"{symbol}(upnl={unrealized_pnl:.6f}, qty={close_info['qty']}, side={close_side}, "
                    f"position_side={position_side}, reduce_only={use_reduce_only}, "
                    f"close_order_id={close_info['close_order_id']})"
                )
                if symbol and symbol not in closed_symbols:
                    closed_symbols.append(symbol)
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                LOGGER.exception("Daily loss-cut failed for exchange position symbol=%s: %s", symbol, exc)
                details["errors"].append(
                    f"{symbol}(upnl={unrealized_pnl:.6f}, side={close_side}, position_side={position_side}, "
                    f"qty={abs(position_amt)}): {exc}"
                )
                if symbol and symbol not in failed_symbols:
                    failed_symbols.append(symbol)

        if summary["closed_loss_cut"] > 0 or summary["errors"] > 0:
            self.notifier.send(
                "【Top10做空】11:55浮亏止损汇总",
                self._build_daily_loss_cut_notification(summary, details),
            )

        summary["closed_symbols"] = closed_symbols
        summary["failed_symbols"] = failed_symbols
        return summary

    @_serialized_account_mutation
    def run_once(self, account_snapshot: Optional[AccountSnapshot] = None) -> Dict[str, int]:
        self._active_account_snapshot = account_snapshot
        try:
            return self._run_once_impl()
        finally:
            self._active_account_snapshot = None

    def _run_once_impl(self) -> Dict[str, int]:
        if self.order_state is not None and not self.order_state.is_certain():
            if not self.order_state.verify_rest(
                force_account_snapshot=self._active_account_snapshot is None,
            ):
                positions = self.store.list_open_positions()
                LOGGER.warning(
                    "Position management paused while user stream state is uncertain account=%s",
                    self.account_id,
                )
                return {
                    "total": len(positions),
                    "closed_tp": 0,
                    "closed_sl": 0,
                    "closed_timeout": 0,
                    "closed_external": 0,
                    "updated_sl": 0,
                    "errors": 1,
                }
        try:
            discovered_fills = self._market_fill_reconciler.reconcile_persisted_missing()
            fill_reconciliation = self._market_fill_reconciler.reconcile_pending()
            if (
                discovered_fills["reconciled"] > 0
                or discovered_fills["queued"] > 0
                or fill_reconciliation["reconciled"] > 0
                or fill_reconciliation["failed"] > 0
            ):
                LOGGER.info(
                    "Market fill reconciliation account=%s discovered=%s pending=%s",
                    self.account_id,
                    discovered_fills,
                    fill_reconciliation,
                )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Market fill reconciliation failed account=%s: %s", self.account_id, exc)
        try:
            self._reconcile_portfolio_take_profit_limit_plan()
        except Exception as exc:  # noqa: BLE001
            # The original per-position stop remains the safety net when the
            # optional portfolio limit status cannot be read.
            LOGGER.warning("Portfolio take-profit limit reconciliation failed account=%s: %s", self.account_id, exc)
        self._noon_protection_caps_cache = self._load_noon_protection_caps()
        self._morning_protection_caps_cache = self._load_morning_protection_caps()
        positions = self.store.list_open_positions()
        summary = {
            "total": len(positions),
            "closed_tp": 0,
            "closed_sl": 0,
            "closed_timeout": 0,
            "closed_external": 0,
            "updated_sl": 0,
            "errors": 0,
        }
        event_details: Dict[str, List[str]] = {
            "closed_tp": [],
            "closed_sl": [],
            "closed_timeout": [],
            "closed_external": [],
            "updated_sl": [],
            "errors": [],
        }

        for pos in positions:
            try:
                result = self._manage_position(pos)
                self.store.clear_position_error(int(pos["id"]))
                if result and result.get("type") in summary:
                    result_type = str(result["type"])
                    summary[result_type] += 1
                    event_details[result_type].append(str(result.get("detail", "")))
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                symbol = str(pos.get("symbol") or "")
                position_id = pos.get("id")
                LOGGER.exception("Failed to manage position id=%s symbol=%s: %s", position_id, symbol, exc)
                self.store.set_position_error(int(pos["id"]), str(exc))
                event_details["errors"].append(f"{symbol}(id={position_id}): {exc}")

        if any(value > 0 for key, value in summary.items() if key != "total"):
            self.notifier.send(
                "【Top10做空】巡检动作汇总",
                self._build_manage_notification(summary, event_details),
            )

        self._noon_protection_caps_cache = None
        self._morning_protection_caps_cache = None
        return summary

    def _reconcile_portfolio_take_profit_limit_plan(self) -> Optional[Dict[str, object]]:
        state = self.store.get_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME) or {}
        raw_plan = state.get("portfolio_limit_plan")
        if not bool(state.get("triggered")) or not isinstance(raw_plan, list):
            return None
        if bool(state.get("close_complete")):
            return None

        limit_plan = [dict(item) for item in raw_plan if isinstance(item, dict)]
        summary = self._execute_portfolio_take_profit_limit_plan(limit_plan)
        state["portfolio_limit_plan"] = limit_plan
        state["close_complete"] = (
            int(summary.get("errors", 0) or 0) == 0
            and int(summary.get("pending", 0) or 0) == 0
        )
        state["last_close_summary"] = summary
        state["updated_at_utc"] = self._utc_now_iso()
        self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)
        return summary

    @_serialized_account_mutation
    def cleanup_portfolio_take_profit_orders_before_entry(self) -> Dict[str, object]:
        """Remove stale portfolio limits immediately before a new entry batch."""
        try:
            self._reconcile_portfolio_take_profit_limit_plan()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Portfolio take-profit pre-entry reconciliation failed account=%s: %s",
                self.account_id,
                exc,
            )

        state = self.store.get_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME) or {}
        raw_plan = state.get("portfolio_limit_plan")
        if not isinstance(raw_plan, list):
            return {"canceled": 0, "failed": 0, "details": []}

        try:
            risks = self._get_all_position_risks()
        except Exception as exc:  # noqa: BLE001
            return {"canceled": 0, "failed": 1, "details": [f"fetch_position_risk_failed: {exc}"]}
        active_by_key = {
            (
                str(risk.get("symbol") or "").strip().upper(),
                str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH",
            ): abs(self._safe_float(risk.get("positionAmt"), default=0.0))
            for risk in risks
        }

        canceled = 0
        failed = 0
        details: List[str] = []
        plan = [dict(item) for item in raw_plan if isinstance(item, dict)]
        for item in plan:
            symbol = str(item.get("symbol") or "").strip().upper()
            position_side = str(item.get("position_side") or "BOTH").strip().upper() or "BOTH"
            current_qty = active_by_key.get((symbol, position_side), 0.0)
            order_id = item.get("portfolio_order_id")
            client_order_id = item.get("portfolio_client_order_id")
            position_id = self._safe_optional_int(item.get("tracked_position_id"))
            tracked_pos = self.store.get_position(position_id) if position_id is not None else None
            tracked_open = tracked_pos is not None and str(tracked_pos.get("status") or "").upper() == "OPEN"
            same_position = tracked_open and current_qty > 1e-12
            should_clear = bool(item.get("action_complete")) or not same_position
            if not should_clear or not (order_id or client_order_id):
                continue
            if self._cancel_order_if_exists(symbol, order_id, client_order_id):
                canceled += 1
                details.append(f"{symbol}(order_id={order_id or '-'}, client_id={client_order_id or '-'})")
                self._clear_portfolio_take_profit_order(item)
            else:
                failed += 1

        state["portfolio_limit_plan"] = plan
        state["updated_at_utc"] = self._utc_now_iso()
        self.store.set_lock_state(self.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)
        return {"canceled": canceled, "failed": failed, "details": details}

    @_serialized_account_mutation
    def cleanup_orphan_exit_orders_once_per_day(self) -> Dict[str, object]:
        day_key = self._local_day_key()
        state = self.store.get_lock_state(self.ORPHAN_EXIT_ORDER_CLEANUP_LOCK_NAME) or {}
        if str(state.get("day_key") or "") == day_key:
            return {"canceled": 0, "details": [], "skipped": True, "day_key": day_key}

        result = self._cleanup_orphan_exit_orders()
        if int(result.get("failed", 0)) == 0:
            self.store.set_lock_state(
                self.ORPHAN_EXIT_ORDER_CLEANUP_LOCK_NAME,
                {
                    "day_key": day_key,
                    "canceled": int(result["canceled"]),
                    "updated_at_utc": self._utc_now_iso(),
                },
            )
        result["day_key"] = day_key
        result["skipped"] = False
        return result

    @staticmethod
    def _local_day_key() -> str:
        return datetime.now().astimezone().date().isoformat()

    def _cleanup_orphan_exit_orders(self) -> Dict[str, object]:
        risks = self._get_all_position_risks()
        active_short_symbols = {
            str(row.get("symbol") or "").strip()
            for row in risks
            if str(row.get("symbol") or "").strip()
            and self._safe_float(row.get("positionAmt"), default=0.0) < 0
        }
        if self.order_state is None:
            # Compatibility adapter for legacy standalone callers. Production
            # runtimes consume the User Stream / periodic verification ledger.
            open_orders = self.client.get_open_orders()
        else:
            open_orders = []
            for local_order in self.store.list_exchange_order_state(active_only=True):
                resolved = self._get_order(
                    symbol=str(local_order.get("symbol") or ""),
                    order_id=local_order.get("order_id"),
                    client_order_id=local_order.get("client_order_id"),
                )
                if resolved is not None:
                    open_orders.append(resolved)
        canceled = 0
        failed = 0
        details: List[str] = []
        failed_details: List[str] = []
        for order in open_orders:
            symbol = str(order.get("symbol") or "").strip()
            if not symbol or symbol in active_short_symbols:
                continue
            if not self._is_orphan_exit_order_candidate(order):
                continue
            order_id = order.get("orderId")
            client_order_id = order.get("clientOrderId")
            detail = f"{symbol}(order_id={order_id or '-'}, client_id={client_order_id or '-'})"
            if self._cancel_order_if_exists(symbol, order_id, client_order_id):
                canceled += 1
                details.append(detail)
            else:
                failed += 1
                failed_details.append(detail)
        return {
            "canceled": canceled,
            "failed": failed,
            "details": details,
            "failed_details": failed_details,
        }

    @classmethod
    def _is_orphan_exit_order_candidate(cls, order: Dict[str, object]) -> bool:
        status = str(order.get("status") or "").strip().upper()
        if status and status not in {"NEW", "PENDING"}:
            return False
        side = str(order.get("side") or "").strip().upper()
        if side != "BUY":
            return False
        position_side = str(order.get("positionSide") or "").strip().upper()
        if position_side == "LONG":
            return False
        order_type = str(order.get("type") or order.get("orderType") or "").strip().upper()
        client_order_id = str(order.get("clientOrderId") or "").strip()
        if order_type == "LIMIT":
            return client_order_id.startswith("t10s-pftlim-")
        if order_type not in {
            "STOP",
            "STOP_MARKET",
            "TAKE_PROFIT",
            "TAKE_PROFIT_MARKET",
            "TRAILING_STOP_MARKET",
        }:
            return False
        if cls._truthy_order_flag(order.get("reduceOnly")) or cls._truthy_order_flag(order.get("closePosition")):
            return True
        return client_order_id.startswith(("t10s-sl-", "t10s-tp-", "t10s-nsl-", "t10s-msl-"))

    @staticmethod
    def _truthy_order_flag(value: object) -> bool:
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes"}

    def _manage_position(self, pos: Dict[str, object]) -> Optional[Dict[str, object]]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])

        risk = self._get_symbol_position_risk(symbol)
        if risk is None:
            close_result = self._close_if_recorded_exit_filled(pos)
            if close_result:
                return close_result
            return self._close_external_missing_short(pos)

        position_amt = float(risk.get("positionAmt", "0") or 0)
        if position_amt >= 0:
            close_result = self._close_if_recorded_exit_filled(pos)
            if close_result:
                return close_result

            return self._close_external_missing_short(pos)

        if self._is_protection_exempt(symbol):
            return None

        tp_status, sl_status = self._get_recorded_exit_statuses(pos)
        close_result = self._close_if_recorded_exit_filled(
            pos,
            tp_status=tp_status,
            sl_status=sl_status,
            statuses_loaded=True,
        )
        if close_result:
            return close_result

        if self._is_expired(str(pos["expire_at_utc"])):
            timeout_info = self._close_timeout(pos, abs(position_amt))
            return {
                "type": "closed_timeout",
                "detail": (
                    f"{symbol}(id={position_id}, qty={timeout_info['qty']}, "
                    f"close_order_id={timeout_info['close_order_id']})"
                ),
            }

        update_info = self._update_dynamic_stop(pos, risk, sl_status=sl_status)
        if update_info:
            if update_info.get("closed_immediate"):
                return {
                    "type": "closed_sl",
                    "detail": (
                        f"{symbol}(id={position_id}, protection_immediate=true, "
                        f"close_order_id={update_info.get('close_order_id')})"
                    ),
                }
            return {
                "type": "updated_sl",
                "detail": (
                    f"{symbol}(id={position_id}, old_sl={update_info['old_sl_price']}, "
                    f"new_sl={update_info['new_sl_price']}, liq={update_info['liq_price']})"
                ),
            }

        repaired_tp = self._repair_take_profit_if_needed(pos, risk, tp_status=tp_status)
        if repaired_tp:
            return {
                "type": "updated_sl",
                "detail": f"{symbol}(id={position_id}, repaired_take_profit={repaired_tp})",
            }

        return None

    def _close_external_missing_short(self, pos: Dict[str, object]) -> Dict[str, object]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        self._cancel_exit_orders(pos)
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_EXTERNAL",
            close_reason="SHORT_POSITION_NOT_FOUND",
        )
        return {
            "type": "closed_external",
            "detail": f"{symbol}(id={position_id}, reason=SHORT_POSITION_NOT_FOUND)",
        }

    def _get_recorded_exit_statuses(
        self,
        pos: Dict[str, object],
    ) -> tuple[Optional[str], Optional[str]]:
        symbol = str(pos["symbol"])
        return (
            self._get_order_status(symbol, pos.get("tp_order_id"), pos.get("tp_client_order_id")),
            self._get_order_status(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id")),
        )

    def _close_if_recorded_exit_filled(
        self,
        pos: Dict[str, object],
        tp_status: Optional[str] = None,
        sl_status: Optional[str] = None,
        statuses_loaded: bool = False,
    ) -> Optional[Dict[str, object]]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        if not statuses_loaded:
            tp_status, sl_status = self._get_recorded_exit_statuses(pos)

        if tp_status == "FILLED":
            close_order_id = self._close_on_trigger(pos, close_status="CLOSED_TP", close_reason="TAKE_PROFIT_FILLED")
            return {
                "type": "closed_tp",
                "detail": f"{symbol}(id={position_id}, order_id={close_order_id})",
            }

        if sl_status == "FILLED":
            close_order_id = self._close_on_trigger(pos, close_status="CLOSED_SL", close_reason="STOP_LOSS_FILLED")
            return {
                "type": "closed_sl",
                "detail": f"{symbol}(id={position_id}, order_id={close_order_id})",
            }

        return None

    def _close_on_trigger(self, pos: Dict[str, object], close_status: str, close_reason: str) -> Optional[int]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        tp_order_id = pos.get("tp_order_id")
        sl_order_id = pos.get("sl_order_id")
        tp_client_order_id = pos.get("tp_client_order_id")
        sl_client_order_id = pos.get("sl_client_order_id")

        if close_status == "CLOSED_TP":
            self._cancel_order_if_exists(symbol, sl_order_id, sl_client_order_id)
            close_order_id = tp_order_id
        else:
            self._cancel_order_if_exists(symbol, tp_order_id, tp_client_order_id)
            close_order_id = sl_order_id

        self._record_close_fill_from_exchange(
            symbol=symbol,
            position_id=position_id,
            order_id=close_order_id,
            client_order_id=tp_client_order_id if close_status == "CLOSED_TP" else sl_client_order_id,
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status=close_status,
            close_reason=close_reason,
            close_order_id=int(close_order_id) if close_order_id else None,
        )
        return int(close_order_id) if close_order_id else None

    def _close_timeout(self, pos: Dict[str, object], qty: float) -> Dict[str, object]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])

        close_order = self.client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=self.client.format_order_qty(symbol, qty),
            reduceOnly=True,
            newClientOrderId=self._new_client_id("to", symbol),
            newOrderRespType="RESULT",
        )

        self._market_fill_reconciler.record_market_order(
            symbol=symbol,
            position_id=position_id,
            order=close_order,
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_TIMEOUT",
            close_reason="MAX_HOLD_EXCEEDED",
            close_order_id=close_order.get("orderId"),
        )
        self._cancel_exit_orders(pos)
        return {"qty": qty, "close_order_id": close_order.get("orderId")}

    def _close_daily_loss_cut(
        self,
        symbol: str,
        qty: float,
        side: str,
        position_id: Optional[int],
        cancel_pos: Optional[Dict[str, object]] = None,
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
        close_status: str = "CLOSED_DAILY_LOSS_CUT",
        close_reason: str = "DAILY_FLOATING_LOSS_CHECK",
        client_id_tag: str = "dl",
    ) -> Dict[str, object]:
        create_order_params: Dict[str, object] = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": self.client.format_order_qty(symbol, qty),
            "newClientOrderId": self._new_client_id(client_id_tag, symbol),
            "newOrderRespType": "RESULT",
        }
        if use_reduce_only:
            create_order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            create_order_params["positionSide"] = position_side

        close_order = self.client.create_order(
            **create_order_params,
        )

        self._market_fill_reconciler.record_market_order(
            symbol=symbol,
            position_id=position_id,
            order=close_order,
        )
        if position_id is not None:
            self.store.mark_position_closed(
                position_id=position_id,
                status=close_status,
                close_reason=close_reason,
                close_order_id=close_order.get("orderId"),
            )
        if cancel_pos is not None:
            self._cancel_exit_orders(cancel_pos)
        return {
            "qty": qty,
            "close_order_id": close_order.get("orderId"),
        }

    def _close_protection_immediate(
        self,
        symbol: str,
        qty: float,
        side: str,
        position_id: Optional[int],
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
        close_status: str = "CLOSED_NOON_PROTECTION",
        close_reason: str = "NOON_PROTECTION_IMMEDIATE_TRIGGER",
        client_id_tag: str = "nsi",
    ) -> Dict[str, object]:
        create_order_params: Dict[str, object] = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": self.client.format_order_qty(symbol, qty),
            "newClientOrderId": self._new_client_id(client_id_tag, symbol),
            "newOrderRespType": "RESULT",
        }
        if use_reduce_only:
            create_order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            create_order_params["positionSide"] = position_side

        close_order = self.client.create_order(**create_order_params)
        self._market_fill_reconciler.record_market_order(
            symbol=symbol,
            position_id=position_id,
            order=close_order,
        )
        if position_id is not None:
            self.store.mark_position_closed(
                position_id=position_id,
                status=close_status,
                close_reason=close_reason,
                close_order_id=close_order.get("orderId"),
            )
        return {
            "qty": qty,
            "close_order_id": close_order.get("orderId"),
        }

    @staticmethod
    def _resolve_close_side_for_exchange_position(
        position_amt: float,
        position_side: str,
    ) -> tuple[str, bool]:
        normalized_side = str(position_side or "").strip().upper()
        if normalized_side == "LONG":
            # Hedge mode LONG leg closes by SELL and must not send reduceOnly.
            return "SELL", False
        if normalized_side == "SHORT":
            # Hedge mode SHORT leg closes by BUY and must not send reduceOnly.
            return "BUY", False
        # One-way mode (BOTH).
        return ("BUY" if position_amt < 0 else "SELL"), True

    @staticmethod
    def _is_immediate_trigger_error(exc: BinanceAPIError) -> bool:
        message = str(getattr(exc, "message", "") or exc).lower()
        return getattr(exc, "code", None) == -2021 or "immediately trigger" in message

    def _update_dynamic_stop(
        self,
        pos: Dict[str, object],
        risk: Dict[str, str],
        sl_status: Optional[str] = None,
    ) -> Optional[Dict[str, object]]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        if self._is_protection_exempt(symbol):
            return None
        raw_position_amt = float(risk.get("positionAmt", "0") or 0)
        position_amt = abs(raw_position_amt)
        if position_amt <= 0:
            return None

        liq_price = self._safe_positive_float(risk.get("liquidationPrice"))
        if not liq_price:
            return None

        old_sl_price = self._safe_positive_float(pos.get("sl_price"))
        new_sl_raw = liq_price * (1 - self.sl_liq_buffer_pct / 100.0)
        new_sl_price = self.client.normalize_trigger_price(symbol, new_sl_raw, round_up=True)

        noon_cap_price = self._get_or_backfill_noon_protection_cap(pos, risk)
        if noon_cap_price:
            new_sl_price = min(new_sl_price, noon_cap_price)
        morning_cap_price = self._get_morning_protection_cap(position_id)
        if morning_cap_price:
            new_sl_price = min(new_sl_price, morning_cap_price)
        entry_structure_protection = self._entry_structure_protection_state.get(position_id)
        if entry_structure_protection is not None:
            new_sl_price = min(new_sl_price, entry_structure_protection.stop_price)
        if old_sl_price:
            new_sl_price = min(new_sl_price, old_sl_price)
        new_sl_stop_price = self.client.format_trigger_price(symbol, new_sl_price, round_up=True)

        rules = self.client.get_symbol_rules().get(symbol)
        min_delta = rules.tick_size if rules else 0.0

        sl_is_live = sl_status in {
            "NEW",
            "PENDING",
            "ACTIVE",
            "PARTIALLY_FILLED",
            "TRIGGERING",
            "TRIGGERED",
        }
        if old_sl_price and abs(new_sl_price - old_sl_price) <= max(min_delta, 1e-12) and sl_is_live:
            return None

        try:
            sl_order = self._create_stop_order_with_fallback(
                symbol=symbol,
                side="BUY",
                stop_price=new_sl_stop_price,
                qty=position_amt,
                client_order_id=self._new_client_id("sl", symbol),
            )
        except BinanceAPIError as exc:
            if not self._is_immediate_trigger_error(exc):
                raise
            position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
            close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
                position_amt=raw_position_amt,
                position_side=position_side,
            )
            close_info = self._close_protection_immediate(
                symbol=symbol,
                qty=position_amt,
                side=close_side,
                position_id=position_id,
                position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                use_reduce_only=use_reduce_only,
                close_status="CLOSED_SL",
                close_reason="PROTECTION_IMMEDIATE_TRIGGER",
                client_id_tag="psi",
            )
            self._cancel_exit_orders(pos)
            return {
                "old_sl_price": old_sl_price,
                "new_sl_price": new_sl_price,
                "liq_price": liq_price,
                "closed_immediate": True,
                "close_order_id": close_info.get("close_order_id"),
            }

        try:
            self.store.update_stop_loss(
                position_id=position_id,
                sl_order_id=sl_order.get("orderId"),
                sl_client_order_id=sl_order.get("clientOrderId"),
                sl_price=new_sl_price,
                liq_price_latest=liq_price,
            )
            self.store.add_order_event(
                symbol=symbol,
                position_id=position_id,
                event_time_utc=self._utc_now_iso(),
                order_payload=sl_order,
            )
        except Exception:
            self._cancel_order_if_exists(symbol, sl_order.get("orderId"), sl_order.get("clientOrderId"))
            raise
        self._cancel_order_if_exists(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id"))
        return {
            "old_sl_price": old_sl_price,
            "new_sl_price": new_sl_price,
            "liq_price": liq_price,
        }

    def _repair_take_profit_if_needed(
        self,
        pos: Dict[str, object],
        risk: Dict[str, str],
        tp_status: Optional[str] = None,
    ) -> Optional[float]:
        tp_price = self._safe_positive_float(pos.get("tp_price"))
        if not tp_price:
            return None
        symbol = str(pos["symbol"])
        if tp_status in {
            "NEW",
            "PENDING",
            "ACTIVE",
            "PARTIALLY_FILLED",
            "TRIGGERING",
            "TRIGGERED",
        }:
            return None

        position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
        if position_amt >= 0:
            return None
        position_side = str(risk.get("positionSide") or "BOTH").strip().upper() or "BOTH"
        close_side, use_reduce_only = self._resolve_close_side_for_exchange_position(
            position_amt=position_amt,
            position_side=position_side,
        )
        order_params: Dict[str, object] = {
            "symbol": symbol,
            "side": close_side,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": self.client.format_trigger_price(symbol, tp_price, round_up=False),
            "quantity": self.client.format_order_qty(symbol, abs(position_amt)),
            "workingType": self.trigger_price_type,
            "priceProtect": True,
            "newClientOrderId": self._new_client_id("tpfix", symbol),
        }
        if use_reduce_only:
            order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            order_params["positionSide"] = position_side
        order = self.client.create_order(**order_params)
        try:
            self.store.update_take_profit(
                position_id=int(pos["id"]),
                tp_order_id=order.get("orderId"),
                tp_client_order_id=order.get("clientOrderId"),
                tp_price=tp_price,
            )
            self.store.add_order_event(
                symbol=symbol,
                position_id=int(pos["id"]),
                event_time_utc=self._utc_now_iso(),
                order_payload=order,
            )
        except Exception:
            self._cancel_order_if_exists(symbol, order.get("orderId"), order.get("clientOrderId"))
            raise
        self._cancel_order_if_exists(symbol, pos.get("tp_order_id"), pos.get("tp_client_order_id"))
        return tp_price

    def _create_stop_order_with_fallback(
        self,
        symbol: str,
        side: str,
        stop_price: str,
        qty: float,
        client_order_id: str,
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
    ) -> Dict[str, object]:
        create_order_params: Dict[str, object] = {
            "symbol": symbol,
            "side": side,
            "type": "STOP_MARKET",
            "stopPrice": stop_price,
            "quantity": self.client.format_order_qty(symbol, qty),
            "workingType": self.trigger_price_type,
            "priceProtect": True,
            "newClientOrderId": client_order_id,
        }
        if use_reduce_only:
            create_order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            create_order_params["positionSide"] = position_side
        return self.client.create_order(**create_order_params)

    def _get_order(
        self,
        symbol: str,
        order_id: object,
        client_order_id: object,
    ) -> Optional[Dict[str, Any]]:
        if not order_id and not client_order_id:
            return None
        if self.order_state is not None:
            row = self.store.get_exchange_order_state(
                symbol=symbol,
                order_id=order_id,
                client_order_id=client_order_id,
            )
            if row is None:
                return None
            payload: Dict[str, Any] = {}
            raw_json = row.get("raw_json")
            if isinstance(raw_json, str) and raw_json.strip():
                try:
                    parsed = json.loads(raw_json)
                    if isinstance(parsed, dict):
                        payload.update(parsed)
                except (TypeError, ValueError):
                    pass
            payload.update(
                {
                    "symbol": row.get("symbol"),
                    "orderId": row.get("order_id"),
                    "clientOrderId": row.get("client_order_id"),
                    "type": row.get("type"),
                    "side": row.get("side"),
                    "positionSide": row.get("position_side"),
                    "status": row.get("status"),
                    "price": row.get("price"),
                    "stopPrice": row.get("stop_price"),
                    "avgPrice": row.get("avg_price"),
                    "origQty": row.get("original_qty"),
                    "executedQty": row.get("executed_qty"),
                    "reduceOnly": bool(row.get("reduce_only")) if row.get("reduce_only") is not None else None,
                    "closePosition": bool(row.get("close_position")) if row.get("close_position") is not None else None,
                }
            )
            return payload
        try:
            parsed_order_id = int(order_id) if order_id else None
            parsed_client_order_id = str(client_order_id) if client_order_id else None
            return self.client.get_order(
                symbol=symbol,
                order_id=parsed_order_id,
                orig_client_order_id=parsed_client_order_id,
            )
        except BinanceAPIError as exc:
            try:
                code = int(exc.code)
            except (TypeError, ValueError):
                code = None
            if code in {-2011, -2013}:
                return None
            raise

    def _get_order_status(
        self,
        symbol: str,
        order_id: object,
        client_order_id: object,
    ) -> Optional[str]:
        order = self._get_order(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
        return str(order.get("status") or "").upper() if order else None

    def _record_close_fill_from_exchange(
        self,
        symbol: str,
        position_id: int,
        order_id: object,
        client_order_id: object,
    ) -> None:
        order = self._get_order(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
        if not order or str(order.get("status") or "").upper() != "FILLED":
            return

        self._market_fill_reconciler.record_market_order(
            symbol=symbol,
            position_id=position_id,
            order=order,
        )

    def _build_close_fill_payload_from_user_trades(
        self,
        symbol: str,
        order: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        order_id = self._safe_optional_int(order.get("actualOrderId")) or self._safe_optional_int(order.get("orderId"))
        if order_id is None:
            return None
        try:
            trades = self.client.get_user_trades(symbol=symbol, order_id=order_id, limit=1000)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("get_user_trades failed for %s/%s: %s", symbol, order_id, exc)
            return None

        buy_trades = [
            trade for trade in trades
            if str(trade.get("side") or "").strip().upper() == "BUY"
            and self._safe_float(trade.get("qty"), default=0.0) > 0
        ]
        if not buy_trades:
            return None

        executed_qty = sum(self._safe_float(trade.get("qty"), default=0.0) for trade in buy_trades)
        quote_qty = sum(self._safe_float(trade.get("quoteQty"), default=0.0) for trade in buy_trades)
        if executed_qty <= 0:
            return None
        avg_price = quote_qty / executed_qty if quote_qty > 0 else None
        if avg_price is None:
            weighted = sum(
                self._safe_float(trade.get("qty"), default=0.0) * self._safe_float(trade.get("price"), default=0.0)
                for trade in buy_trades
            )
            avg_price = weighted / executed_qty if weighted > 0 else None
        if avg_price is None or avg_price <= 0:
            return None

        event_time_ms = max(int(self._safe_float(trade.get("time"), default=0.0)) for trade in buy_trades)
        event_time_utc = datetime.fromtimestamp(event_time_ms / 1000.0, tz=timezone.utc).replace(microsecond=0).isoformat()
        return {
            "orderId": order_id,
            "clientOrderId": order.get("clientOrderId") or order.get("clientAlgoId"),
            "type": order.get("type") or order.get("orderType") or "MARKET",
            "side": "BUY",
            "price": str(avg_price),
            "origQty": str(executed_qty),
            "executedQty": str(executed_qty),
            "cumQuote": str(quote_qty) if quote_qty > 0 else None,
            "avgPrice": str(avg_price),
            "status": "FILLED",
            "reduceOnly": order.get("reduceOnly", True),
            "realizedPnl": str(sum(self._safe_float(trade.get("realizedPnl"), default=0.0) for trade in buy_trades)),
            "commission": str(sum(self._safe_float(trade.get("commission"), default=0.0) for trade in buy_trades)),
            "commissionAsset": str(buy_trades[-1].get("commissionAsset") or "").upper() or None,
            "time": event_time_ms,
            "eventTime": event_time_utc,
            "source": "userTrades",
            "rawOrder": order,
            "trades": buy_trades,
        }

    def _cancel_exit_orders(self, pos: Dict[str, object]) -> None:
        symbol = str(pos["symbol"])
        self._cancel_order_if_exists(symbol, pos.get("tp_order_id"), pos.get("tp_client_order_id"))
        self._cancel_order_if_exists(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id"))

    def _cancel_order_if_exists(self, symbol: str, order_id: object, client_order_id: object) -> bool:
        if not order_id and not client_order_id:
            return True
        try:
            parsed_order_id = int(order_id) if order_id else None
            parsed_client_order_id = str(client_order_id) if client_order_id else None
            canceled = self.client.cancel_order(
                symbol=symbol,
                order_id=parsed_order_id,
                orig_client_order_id=parsed_client_order_id,
            )
            if isinstance(canceled, dict):
                self.store.upsert_exchange_order_state(canceled, source="LOCAL_CANCEL")
            return True
        except BinanceAPIError as exc:
            LOGGER.warning("cancel_order failed for %s/%s/%s: %s", symbol, order_id, client_order_id, exc)
            return False

    @staticmethod
    def _build_manage_notification(summary: Dict[str, int], details: Dict[str, List[str]]) -> str:
        rows = [
            ("open_positions", summary["total"]),
            ("closed_tp", summary["closed_tp"]),
            ("closed_sl", summary["closed_sl"]),
            ("closed_timeout", summary["closed_timeout"]),
            ("closed_external", summary["closed_external"]),
            ("updated_sl", summary["updated_sl"]),
            ("errors", summary["errors"]),
        ]
        lines = [
            "### Top10 做空巡检动作汇总",
            "",
            f"- 巡检时间(UTC): `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
            "",
            "### 摘要",
            "",
            format_markdown_kv_table(rows),
        ]

        for key, title in [
            ("closed_tp", "TP平仓明细"),
            ("closed_sl", "SL平仓明细"),
            ("closed_timeout", "超时平仓明细"),
            ("closed_external", "外部平仓明细"),
            ("updated_sl", "止损更新明细"),
            ("errors", "错误明细"),
        ]:
            values = [item for item in details.get(key, []) if item]
            block = format_markdown_list_section(title, values, max_items=15)
            if block:
                lines.extend(["", block])

        return "\n".join(lines)

    @staticmethod
    def _build_daily_loss_cut_notification(summary: Dict[str, int], details: Dict[str, List[str]]) -> str:
        rows = [
            ("open_positions", summary["total"]),
            ("closed_loss_cut", summary["closed_loss_cut"]),
            ("errors", summary["errors"]),
        ]
        lines = [
            "### Top10 做空 11:55 浮亏止损汇总",
            "",
            f"- 巡检时间(UTC): `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
            "",
            "### 摘要",
            "",
            format_markdown_kv_table(rows),
        ]

        for key, title in [
            ("closed_loss_cut", "浮亏平仓明细"),
            ("errors", "错误明细"),
        ]:
            values = [item for item in details.get(key, []) if item]
            block = format_markdown_list_section(title, values, max_items=20)
            if block:
                lines.extend(["", block])

        return "\n".join(lines)

    @staticmethod
    def _build_portfolio_loss_cut_notification(
        summary: Dict[str, object],
        baseline_equity: float,
        current_equity: float,
        threshold_equity: float,
        cycle_date: str,
    ) -> str:
        rows = [
            ("cycle_date", cycle_date),
            ("baseline_equity_usdt", f"{baseline_equity:.8f}"),
            ("current_equity_usdt", f"{current_equity:.8f}"),
            ("threshold_equity_usdt", f"{threshold_equity:.8f}"),
            ("open_positions", int(summary.get("total", 0) or 0)),
            ("closed_positions", int(summary.get("closed_loss_cut", 0) or 0)),
            ("errors", int(summary.get("errors", 0) or 0)),
        ]
        lines = [
            "### Top10 做空组合止损汇总",
            "",
            f"- 触发周期(本地日期): `{cycle_date}`",
            f"- 触发时间(UTC): `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
            "",
            "### 摘要",
            "",
            format_markdown_kv_table(rows),
        ]
        details = summary.get("details")
        if isinstance(details, dict):
            for key, title in [
                ("closed_loss_cut", "组合止损平仓明细"),
                ("errors", "错误明细"),
            ]:
                values = [item for item in details.get(key, []) if item]
                block = format_markdown_list_section(title, values, max_items=20)
                if block:
                    lines.extend(["", block])
        return "\n".join(lines)

    @staticmethod
    def _build_portfolio_take_profit_notification(
        summary: Dict[str, object],
        baseline_equity: float,
        current_equity: float,
        threshold_equity: float,
        actual_profit_pct: float,
        arming_profit_pct: float,
        giveback_pct: float,
        peak_equity: float,
        peak_profit_pct: float,
        reduce_ratio: float,
        cycle_date: str,
    ) -> str:
        rows = [
            ("cycle_date", cycle_date),
            ("baseline_equity_usdt", f"{baseline_equity:.8f}"),
            ("current_equity_usdt", f"{current_equity:.8f}"),
            ("trigger_threshold_equity_usdt", f"{threshold_equity:.8f}"),
            ("actual_profit_pct", f"{actual_profit_pct:.4f}%"),
            ("arming_profit_pct", f"{arming_profit_pct:.4f}%"),
            ("peak_equity_usdt", f"{peak_equity:.8f}"),
            ("peak_profit_pct", f"{peak_profit_pct:.4f}%"),
            ("peak_profit_giveback_pct", f"{giveback_pct:.2f}%"),
            ("take_profit_reduce_ratio", f"{reduce_ratio * 100.0:.2f}%"),
            ("open_positions", int(summary.get("total", 0) or 0)),
            ("closed_positions", int(summary.get("closed_take_profit", 0) or 0)),
            ("adjusted_positions", int(summary.get("adjusted_take_profit", 0) or 0)),
            ("pending", int(summary.get("pending", 0) or 0)),
            ("errors", int(summary.get("errors", 0) or 0)),
        ]
        lines = [
            "### Top10 做空组合止盈汇总",
            "",
            f"- 触发周期(本地日期): `{cycle_date}`",
            f"- 触发时间(UTC): `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
            "",
            "### 摘要",
            "",
            format_markdown_kv_table(rows),
        ]
        details = summary.get("details")
        if isinstance(details, dict):
            for key, title in [
                ("closed_take_profit", "组合止盈平仓明细"),
                ("adjusted_take_profit", "组合止盈减仓明细"),
                ("errors", "错误明细"),
            ]:
                values = [item for item in details.get(key, []) if item]
                block = format_markdown_list_section(title, values, max_items=20)
                if block:
                    lines.extend(["", block])
        return "\n".join(lines)

    @staticmethod
    def _build_noon_protection_notification(
        summary: Dict[str, int],
        details: Dict[str, List[str]],
        protection_label: str = "12:00",
    ) -> str:
        rows = [
            ("open_positions", summary["total"]),
            ("updated_sl", summary["updated_sl"]),
            ("closed_immediate", summary.get("closed_immediate", 0)),
            ("skipped", summary["skipped"]),
            ("errors", summary["errors"]),
        ]
        lines = [
            f"### Top10 做空 {protection_label} 保护止损汇总",
            "",
            f"- 巡检时间(UTC): `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
            "",
            "### 摘要",
            "",
            format_markdown_kv_table(rows),
        ]

        for key, title in [
            ("updated_sl", "保护止损更新明细"),
            ("closed_immediate", "保护线已触发市价平仓明细"),
            ("errors", "错误明细"),
        ]:
            values = [item for item in details.get(key, []) if item]
            block = format_markdown_list_section(title, values, max_items=20)
            if block:
                lines.extend(["", block])
        return "\n".join(lines)

    def _fetch_symbol_extremes_between(
        self,
        symbol: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> tuple[Optional[float], Optional[float]]:
        if end_utc <= start_utc:
            return None, None
        start_ms = int(start_utc.timestamp() * 1000)
        end_ms = int(end_utc.timestamp() * 1000)
        highs: List[float] = []
        lows: List[float] = []

        cursor_ms = start_ms
        while cursor_ms < end_ms:
            rows = self.client.get_klines(
                symbol=symbol,
                interval="1m",
                start_time=cursor_ms,
                end_time=end_ms,
                limit=1000,
            )
            if not rows:
                break

            for row in rows:
                if len(row) < 4:
                    continue
                high = self._safe_positive_float(row[2])
                low = self._safe_positive_float(row[3])
                if high:
                    highs.append(high)
                if low:
                    lows.append(low)

            if len(rows) < 1000:
                break

            try:
                last_open_ms = int(rows[-1][0])
            except (TypeError, ValueError, IndexError):
                break
            next_cursor_ms = last_open_ms + 60_000
            if next_cursor_ms <= cursor_ms:
                break
            cursor_ms = next_cursor_ms

        return (max(highs) if highs else None, min(lows) if lows else None)

    def _load_noon_protection_caps(self) -> Dict[str, float]:
        state = self.store.get_lock_state(self.NOON_PROTECTION_LOCK_NAME) or {}
        raw_caps = state.get("caps")
        if not isinstance(raw_caps, dict):
            return {}
        parsed: Dict[str, float] = {}
        for key, value in raw_caps.items():
            try:
                cap_price = float(value)
            except (TypeError, ValueError):
                continue
            cap_key = str(key).strip()
            if not cap_key or cap_price <= 0:
                continue
            parsed[cap_key] = cap_price
        return parsed

    def _persist_noon_protection_caps(
        self,
        caps: Dict[str, float],
        day_start_utc: Optional[datetime] = None,
        noon_time_utc: Optional[datetime] = None,
    ) -> None:
        existing = self.store.get_lock_state(self.NOON_PROTECTION_LOCK_NAME) or {}
        payload = {
            "caps": {str(cap_key): float(price) for cap_key, price in caps.items() if price > 0},
            "updated_at_utc": self._utc_now_iso(),
        }
        if day_start_utc is not None:
            payload["day_start_utc"] = day_start_utc.astimezone(timezone.utc).isoformat()
        elif existing.get("day_start_utc"):
            payload["day_start_utc"] = existing.get("day_start_utc")
        if noon_time_utc is not None:
            payload["noon_time_utc"] = noon_time_utc.astimezone(timezone.utc).isoformat()
        elif existing.get("noon_time_utc"):
            payload["noon_time_utc"] = existing.get("noon_time_utc")
        self.store.set_lock_state(self.NOON_PROTECTION_LOCK_NAME, payload)

    def _get_noon_protection_cap(self, position_id: int) -> Optional[float]:
        if self._noon_protection_caps_cache is None:
            self._noon_protection_caps_cache = self._load_noon_protection_caps()
        value = self._noon_protection_caps_cache.get(str(int(position_id)))
        if value is None or value <= 0:
            return None
        return float(value)

    def _get_noon_protection_window(self) -> Optional[tuple[datetime, datetime]]:
        state = self.store.get_lock_state(self.NOON_PROTECTION_LOCK_NAME) or {}
        day_start_raw = str(state.get("day_start_utc") or "").strip()
        noon_raw = str(state.get("noon_time_utc") or "").strip()
        if not day_start_raw or not noon_raw:
            return None
        try:
            day_start = self._parse_iso_utc(day_start_raw)
            noon_time = self._parse_iso_utc(noon_raw)
        except (TypeError, ValueError):
            return None
        if noon_time <= day_start:
            return None
        now_utc = self._utc_now_datetime()
        if now_utc < day_start or now_utc >= day_start + timedelta(days=1):
            return None
        return day_start, noon_time

    def _get_or_backfill_noon_protection_cap(
        self,
        pos: Dict[str, object],
        risk: Dict[str, str],
    ) -> Optional[float]:
        position_id = int(pos["id"])

        existing_cap = self._get_noon_protection_cap(position_id)
        if existing_cap:
            return existing_cap
        window = self._get_noon_protection_window()
        if window is None:
            return None
        day_start, noon_time = window
        opened_at_raw = str(pos.get("opened_at_utc") or "")
        if not opened_at_raw:
            return None
        opened_at = self._parse_iso_utc(opened_at_raw)
        if self._utc_now_datetime() < noon_time or opened_at >= noon_time:
            return None

        symbol = str(pos["symbol"])
        position_amt = float(risk.get("positionAmt", "0") or 0)
        close_side, _ = self._resolve_close_side_for_exchange_position(
            position_amt=position_amt,
            position_side=str(risk.get("positionSide") or "BOTH"),
        )
        highest_price, lowest_price = self._fetch_noon_protection_extremes(
            symbol=symbol,
            opened_at_utc=opened_at,
            day_start_utc=day_start,
            noon_time_utc=noon_time,
        )
        noon_ref_price = highest_price if close_side == "BUY" else lowest_price
        if not noon_ref_price:
            return None

        round_up = close_side == "BUY"
        cap_price = self.client.normalize_trigger_price(symbol, noon_ref_price, round_up=round_up)
        if cap_price <= 0:
            return None

        if self._noon_protection_caps_cache is None:
            self._noon_protection_caps_cache = self._load_noon_protection_caps()
        self._noon_protection_caps_cache[str(position_id)] = cap_price
        self._persist_noon_protection_caps(self._noon_protection_caps_cache)
        return cap_price

    @classmethod
    def _noon_protection_window_start(
        cls,
        opened_at_utc: datetime,
        day_start_utc: datetime,
        noon_time_utc: datetime,
    ) -> datetime:
        """Return the start of the two-completed-hour-plus-to-noon reference window."""
        opened_at = opened_at_utc.astimezone(timezone.utc)
        day_start = day_start_utc.astimezone(timezone.utc)
        noon_time = noon_time_utc.astimezone(timezone.utc)
        if opened_at >= noon_time:
            return noon_time
        if opened_at < day_start:
            # For a position carried into today, use the two complete hours
            # immediately before today's noon instead of the old entry day.
            return noon_time - timedelta(hours=cls.NOON_PROTECTION_PRE_ENTRY_HOURS)
        entry_hour_start = opened_at.replace(minute=0, second=0, microsecond=0)
        return entry_hour_start - timedelta(hours=cls.NOON_PROTECTION_PRE_ENTRY_HOURS)

    def _fetch_noon_protection_extremes(
        self,
        symbol: str,
        opened_at_utc: datetime,
        day_start_utc: datetime,
        noon_time_utc: datetime,
    ) -> tuple[Optional[float], Optional[float]]:
        """Fetch the prior two full hours plus the post-fill part of today's noon window."""
        opened_at = opened_at_utc.astimezone(timezone.utc)
        day_start = day_start_utc.astimezone(timezone.utc)
        noon_time = noon_time_utc.astimezone(timezone.utc)
        if opened_at >= noon_time:
            return None, None

        if opened_at < day_start:
            return self._fetch_symbol_extremes_between(
                symbol=symbol,
                start_utc=noon_time - timedelta(hours=self.NOON_PROTECTION_PRE_ENTRY_HOURS),
                end_utc=noon_time,
            )

        entry_hour_start = opened_at.replace(minute=0, second=0, microsecond=0)
        previous_start = entry_hour_start - timedelta(hours=self.NOON_PROTECTION_PRE_ENTRY_HOURS)
        if opened_at == entry_hour_start:
            return self._fetch_symbol_extremes_between(
                symbol=symbol,
                start_utc=previous_start,
                end_utc=noon_time,
            )

        previous_high, previous_low = self._fetch_symbol_extremes_between(
            symbol=symbol,
            start_utc=previous_start,
            end_utc=entry_hour_start,
        )
        post_entry_high, post_entry_low = self._fetch_symbol_extremes_between(
            symbol=symbol,
            start_utc=opened_at,
            end_utc=noon_time,
        )
        highs = [price for price in (previous_high, post_entry_high) if price is not None]
        lows = [price for price in (previous_low, post_entry_low) if price is not None]
        return (max(highs) if highs else None, min(lows) if lows else None)

    def _load_morning_protection_caps(self) -> Dict[str, float]:
        state = self.store.get_lock_state(self.MORNING_PROTECTION_LOCK_NAME) or {}
        raw_caps = state.get("caps")
        if not isinstance(raw_caps, dict):
            return {}
        parsed: Dict[str, float] = {}
        for key, value in raw_caps.items():
            try:
                cap_price = float(value)
            except (TypeError, ValueError):
                continue
            cap_key = str(key).strip()
            if not cap_key or cap_price <= 0:
                continue
            parsed[cap_key] = cap_price
        return parsed

    def _load_morning_protection_updated_at_by_key(self) -> Dict[str, datetime]:
        state = self.store.get_lock_state(self.MORNING_PROTECTION_LOCK_NAME) or {}
        raw_by_key = state.get("cap_updated_at_utc_by_key")
        parsed: Dict[str, datetime] = {}
        if isinstance(raw_by_key, dict):
            for key, value in raw_by_key.items():
                cap_key = str(key).strip()
                raw_updated_at = str(value or "").strip()
                if not cap_key or not raw_updated_at:
                    continue
                try:
                    parsed[cap_key] = self._parse_iso_utc(raw_updated_at)
                except ValueError:
                    continue
        if parsed:
            return parsed

        raw_updated_at = str(state.get("updated_at_utc") or "").strip()
        if not raw_updated_at:
            return {}
        try:
            fallback_updated_at = self._parse_iso_utc(raw_updated_at)
        except ValueError:
            return {}
        caps = self._load_morning_protection_caps()
        return {cap_key: fallback_updated_at for cap_key in caps}

    def _persist_morning_protection_caps(
        self,
        caps: Dict[str, float],
        updated_at_by_key: Optional[Dict[str, datetime]] = None,
    ) -> None:
        now_iso = self._utc_now_iso()
        serialized_updated_at_by_key: Dict[str, str] = {}
        for cap_key in caps:
            updated_at = None if updated_at_by_key is None else updated_at_by_key.get(cap_key)
            if updated_at is None:
                serialized_updated_at_by_key[str(cap_key)] = now_iso
            else:
                serialized_updated_at_by_key[str(cap_key)] = updated_at.astimezone(timezone.utc).replace(
                    microsecond=0
                ).isoformat()
        payload = {
            "caps": {str(cap_key): float(price) for cap_key, price in caps.items() if price > 0},
            "cap_updated_at_utc_by_key": serialized_updated_at_by_key,
            "updated_at_utc": now_iso,
        }
        self.store.set_lock_state(self.MORNING_PROTECTION_LOCK_NAME, payload)

    def _get_morning_protection_cap(self, position_id: int) -> Optional[float]:
        if self._morning_protection_caps_cache is None:
            self._morning_protection_caps_cache = self._load_morning_protection_caps()
        value = self._morning_protection_caps_cache.get(str(int(position_id)))
        if value is None or value <= 0:
            return None
        return float(value)

    @staticmethod
    def _build_protection_cap_key(
        symbol: str,
        position_side: str,
        position_amt: float,
        tracked_position_id: Optional[int],
    ) -> str:
        if tracked_position_id is not None:
            return str(int(tracked_position_id))
        normalized_side = str(position_side or "").strip().upper() or "BOTH"
        if normalized_side == "BOTH":
            normalized_side = "BOTH_SHORT" if position_amt < 0 else "BOTH_LONG"
        return f"EX:{symbol}:{normalized_side}"

    def _get_all_position_risks(self) -> List[Dict[str, Any]]:
        snapshot = self._active_account_snapshot
        if snapshot is None and self.snapshot_provider is not None:
            snapshot = self.snapshot_provider.capture()
        if snapshot is not None:
            return [dict(row) for row in snapshot.positions]
        return self.client.get_position_risk()

    def _get_symbol_position_risk(self, symbol: str) -> Optional[Dict[str, str]]:
        rows = self._get_all_position_risks()
        fallback = None
        for row in rows:
            if row.get("symbol") != symbol:
                continue
            if fallback is None:
                fallback = row
            position_side = str(row.get("positionSide") or "BOTH").strip().upper()
            position_amt = self._safe_float(row.get("positionAmt"), default=0.0)
            if position_side == "SHORT" or position_amt < 0:
                return row
        return fallback

    @classmethod
    def _normalize_daily_loss_cut_scope(cls, scope: str) -> str:
        normalized = str(scope or "").strip().lower()
        if normalized in {cls.DAILY_LOSS_CUT_SCOPE_TRACKED, cls.DAILY_LOSS_CUT_SCOPE_EXCHANGE}:
            return normalized
        if normalized:
            LOGGER.warning("Invalid daily_loss_cut_scope=%s, fallback to %s", normalized, cls.DAILY_LOSS_CUT_SCOPE_TRACKED)
        return cls.DAILY_LOSS_CUT_SCOPE_TRACKED

    def _is_expired(self, expire_at_utc: str) -> bool:
        expire_time = datetime.fromisoformat(expire_at_utc)
        if expire_time.tzinfo is None:
            expire_time = expire_time.replace(tzinfo=timezone.utc)
        now_utc = self._utc_now_datetime()
        return now_utc >= expire_time

    @staticmethod
    def _safe_positive_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if number <= 0:
            return None
        return number

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_optional_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return int(float(text))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sanitize_client_id_part(value: str, fallback: str, max_len: int) -> str:
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
        cleaned = "".join(ch for ch in str(value) if ch in allowed)
        if not cleaned:
            cleaned = fallback
        return cleaned[: max(1, int(max_len))]

    @staticmethod
    def _new_client_id(tag: str, symbol: str) -> str:
        tag_part = PositionManager._sanitize_client_id_part(tag, fallback="x", max_len=8).lower()
        symbol_part = PositionManager._sanitize_client_id_part(symbol, fallback="sym", max_len=6).upper()
        symbol_hash = hashlib.sha1(str(symbol).encode("utf-8")).hexdigest()[:6]
        nonce = uuid4().hex[:8]
        # Binance Futures requires newClientOrderId to match ^[.A-Z:/a-z0-9_-]{1,36}$.
        return f"t10s-{tag_part}-{symbol_part}-{symbol_hash}-{nonce}"

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _utc_now_datetime() -> datetime:
        return datetime.now(timezone.utc).replace(microsecond=0)

    @staticmethod
    def _parse_iso_utc(text: str) -> datetime:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
