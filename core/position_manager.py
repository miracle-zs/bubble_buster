import logging
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from core.state_store import StateStore
from infra.binance_futures_client import BinanceAPIError, BinanceFuturesClient
from infra.notifier import (
    ServerChanNotifier,
    format_markdown_kv_table,
    format_markdown_list_section,
)

LOGGER = logging.getLogger(__name__)


class PositionManager:
    DAILY_LOSS_CUT_SCOPE_TRACKED = "tracked"
    DAILY_LOSS_CUT_SCOPE_EXCHANGE = "exchange"
    NOON_PROTECTION_LOCK_NAME = "noon_protection_stop_caps_v1"
    HOURLY_EXCHANGE_TP_LOCK_NAME = "hourly_exchange_take_profit_v1"

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        notifier: ServerChanNotifier,
        sl_liq_buffer_pct: float,
        trigger_price_type: str,
        daily_loss_cut_scope: str = DAILY_LOSS_CUT_SCOPE_TRACKED,
        account_id: str = "default",
    ):
        self.client = client
        self.store = store
        self.notifier = notifier
        self.sl_liq_buffer_pct = sl_liq_buffer_pct
        self.trigger_price_type = trigger_price_type
        self.daily_loss_cut_scope = self._normalize_daily_loss_cut_scope(daily_loss_cut_scope)
        self.account_id = (account_id or "").strip() or "default"
        self._noon_protection_caps_cache: Optional[Dict[str, float]] = None

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
        }
        active_symbols = set()
        risks = self.client.get_position_risk()
        for risk in risks:
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if position_amt >= 0:
                continue
            symbol = str(risk.get("symbol") or "").strip()
            if not symbol:
                continue
            active_symbols.add(symbol)
            entry_price = self._safe_positive_float(risk.get("entryPrice"))
            if not entry_price:
                continue

            existing = symbols_state.get(symbol)
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

        stale_symbols = [symbol for symbol in list(symbols_state.keys()) if symbol not in active_symbols]
        for symbol in stale_symbols:
            symbols_state.pop(symbol, None)
            summary["pruned"] += 1

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
        opened_at_utc = self._reconstruct_short_opened_at_from_trades(
            symbol=symbol,
            current_short_qty=abs(position_amt),
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
        opened_at_utc = self._parse_iso_utc(opened_at_raw) if opened_at_raw else self._reconstruct_short_opened_at_from_trades(
            symbol=symbol,
            current_short_qty=abs(position_amt),
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

    def _reconstruct_short_opened_at_from_trades(
        self,
        symbol: str,
        current_short_qty: float,
    ) -> datetime:
        trades = self.client.get_user_trades(symbol=symbol, limit=1000)
        remaining_qty = max(0.0, float(current_short_qty))
        opened_at_ms: Optional[int] = None
        for trade in reversed(trades or []):
            qty = self._safe_float(trade.get("qty"), default=0.0)
            side = str(trade.get("side") or "").strip().upper()
            if qty <= 0 or side not in {"BUY", "SELL"}:
                continue
            if side != "SELL":
                continue
            remaining_qty -= qty
            trade_time = trade.get("time")
            try:
                opened_at_ms = int(trade_time)
            except (TypeError, ValueError):
                continue
            if remaining_qty <= 1e-12:
                break
        if opened_at_ms is None:
            raise RuntimeError(f"cannot reconstruct short opened_at for {symbol}")
        return datetime.fromtimestamp(opened_at_ms / 1000.0, tz=timezone.utc).replace(microsecond=0)

    def run_daily_loss_cut(self) -> Dict[str, object]:
        if self.daily_loss_cut_scope == self.DAILY_LOSS_CUT_SCOPE_EXCHANGE:
            return self._run_daily_loss_cut_exchange_positions()
        return self._run_daily_loss_cut_tracked_positions()

    def run_noon_protection_stop(
        self,
        day_start_utc: datetime,
        noon_time_utc: datetime,
    ) -> Dict[str, object]:
        day_start = day_start_utc.astimezone(timezone.utc).replace(microsecond=0)
        noon_time = noon_time_utc.astimezone(timezone.utc).replace(microsecond=0)

        tracked_positions = self.store.list_open_positions()
        tracked_by_symbol: Dict[str, Dict[str, object]] = {}
        for pos in tracked_positions:
            symbol = str(pos.get("symbol") or "").strip()
            if symbol and symbol not in tracked_by_symbol:
                tracked_by_symbol[symbol] = pos

        try:
            risks = self.client.get_position_risk()
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Noon protection failed to query exchange positions: %s", exc)
            return {
                "total": 0,
                "updated_sl": 0,
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
            candidate_risks.append(risk)

        summary = {
            "total": len(candidate_risks),
            "updated_sl": 0,
            "skipped": 0,
            "errors": 0,
        }
        details: Dict[str, List[str]] = {
            "updated_sl": [],
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
            cap_key = self._build_noon_cap_key(
                symbol=symbol,
                position_side=position_side,
                position_amt=position_amt,
                tracked_position_id=tracked_position_id,
            )
            active_cap_keys.add(cap_key)
            try:
                opened_at_raw = str(tracked_pos.get("opened_at_utc") or "") if tracked_pos is not None else ""
                opened_at_utc = self._parse_iso_utc(opened_at_raw) if opened_at_raw else day_start
                start_utc = opened_at_utc if opened_at_utc > day_start else day_start
                if start_utc >= noon_time:
                    summary["skipped"] += 1
                    old_sl_price = self._safe_positive_float(tracked_pos.get("sl_price")) if tracked_pos is not None else None
                    if old_sl_price:
                        caps[cap_key] = old_sl_price
                    continue

                highest_price, lowest_price = self._fetch_symbol_extremes_between(
                    symbol=symbol,
                    start_utc=start_utc,
                    end_utc=noon_time,
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
                caps[cap_key] = merged_sl_price

                rules = self.client.get_symbol_rules().get(symbol)
                min_delta = rules.tick_size if rules else 0.0
                if old_sl_price and abs(merged_sl_price - old_sl_price) <= max(min_delta, 1e-12):
                    summary["skipped"] += 1
                    continue

                qty = abs(position_amt)
                if qty <= 0:
                    raise RuntimeError("position qty is zero")

                if tracked_pos is not None:
                    self._cancel_order_if_exists(symbol, tracked_pos.get("sl_order_id"), tracked_pos.get("sl_client_order_id"))

                sl_stop_price = self.client.format_trigger_price(symbol, merged_sl_price, round_up=round_up)
                sl_order = self._create_stop_order_with_fallback(
                    symbol=symbol,
                    side=close_side,
                    stop_price=sl_stop_price,
                    qty=qty,
                    client_order_id=self._new_client_id("nsl", symbol),
                    position_side=position_side if position_side in {"LONG", "SHORT"} else None,
                    use_reduce_only=use_reduce_only,
                )

                self.store.add_order_event(
                    symbol=symbol,
                    position_id=tracked_position_id,
                    event_time_utc=self._utc_now_iso(),
                    order_payload=sl_order,
                )
                if tracked_position_id is not None:
                    self.store.update_stop_loss(
                        position_id=tracked_position_id,
                        sl_order_id=sl_order.get("orderId"),
                        sl_client_order_id=sl_order.get("clientOrderId"),
                        sl_price=merged_sl_price,
                        liq_price_latest=self._safe_positive_float(risk.get("liquidationPrice")),
                    )
                    self.store.clear_position_error(tracked_position_id)
                summary["updated_sl"] += 1
                details["updated_sl"].append(
                    (
                        f"{symbol}(cap={cap_key}, old_sl={old_sl_price}, "
                        f"noon_ref={noon_ref_price}, new_sl={merged_sl_price}, side={close_side})"
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

        pruned_caps = {
            cap_key: cap_price
            for cap_key, cap_price in caps.items()
            if cap_key in active_cap_keys
        }
        self._persist_noon_protection_caps(pruned_caps)
        self._noon_protection_caps_cache = pruned_caps

        if summary["updated_sl"] > 0 or summary["errors"] > 0:
            self.notifier.send(
                "【Top10做空】12:00保护止损汇总",
                self._build_noon_protection_notification(summary, details),
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
            risks = self.client.get_position_risk()
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
            try:
                close_info = self._close_daily_loss_cut(
                    symbol=symbol,
                    qty=abs(position_amt),
                    side=close_side,
                    position_id=None,
                    cancel_pos=None,
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

    def run_once(self) -> Dict[str, int]:
        self._noon_protection_caps_cache = self._load_noon_protection_caps()
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
        return summary

    def _manage_position(self, pos: Dict[str, object]) -> Optional[Dict[str, object]]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])

        tp_status = self._get_order_status(
            symbol,
            pos.get("tp_order_id"),
            pos.get("tp_client_order_id"),
        )
        sl_status = self._get_order_status(
            symbol,
            pos.get("sl_order_id"),
            pos.get("sl_client_order_id"),
        )

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

        risk = self._get_symbol_position_risk(symbol)
        if risk is None:
            self.store.set_position_error(position_id, "position risk not found")
            return None

        position_amt = float(risk.get("positionAmt", "0") or 0)
        if position_amt >= 0:
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

        if self._is_expired(str(pos["expire_at_utc"])):
            timeout_info = self._close_timeout(pos, abs(position_amt))
            return {
                "type": "closed_timeout",
                "detail": (
                    f"{symbol}(id={position_id}, qty={timeout_info['qty']}, "
                    f"close_order_id={timeout_info['close_order_id']})"
                ),
            }

        update_info = self._update_dynamic_stop(pos, risk)
        if update_info:
            return {
                "type": "updated_sl",
                "detail": (
                    f"{symbol}(id={position_id}, old_sl={update_info['old_sl_price']}, "
                    f"new_sl={update_info['new_sl_price']}, liq={update_info['liq_price']})"
                ),
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

        self._cancel_exit_orders(pos)

        close_order = self.client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=self.client.format_order_qty(symbol, qty),
            reduceOnly=True,
            newClientOrderId=self._new_client_id("to", symbol),
            newOrderRespType="RESULT",
        )

        self.store.add_order_event(
            symbol=symbol,
            position_id=position_id,
            event_time_utc=self._utc_now_iso(),
            order_payload=close_order,
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_TIMEOUT",
            close_reason="MAX_HOLD_EXCEEDED",
            close_order_id=close_order.get("orderId"),
        )
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
    ) -> Dict[str, object]:
        if cancel_pos is not None:
            self._cancel_exit_orders(cancel_pos)

        create_order_params: Dict[str, object] = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": self.client.format_order_qty(symbol, qty),
            "newClientOrderId": self._new_client_id("dl", symbol),
            "newOrderRespType": "RESULT",
        }
        if use_reduce_only:
            create_order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            create_order_params["positionSide"] = position_side

        close_order = self.client.create_order(
            **create_order_params,
        )

        self.store.add_order_event(
            symbol=symbol,
            position_id=position_id,
            event_time_utc=self._utc_now_iso(),
            order_payload=close_order,
        )
        if position_id is not None:
            self.store.mark_position_closed(
                position_id=position_id,
                status="CLOSED_DAILY_LOSS_CUT",
                close_reason="DAILY_FLOATING_LOSS_CHECK",
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

    def _update_dynamic_stop(self, pos: Dict[str, object], risk: Dict[str, str]) -> Optional[Dict[str, object]]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        position_amt = abs(float(risk.get("positionAmt", "0") or 0))
        if position_amt <= 0:
            return None

        liq_price = self._safe_positive_float(risk.get("liquidationPrice"))
        if not liq_price:
            return None

        old_sl_price = self._safe_positive_float(pos.get("sl_price"))
        new_sl_raw = liq_price * (1 - self.sl_liq_buffer_pct / 100.0)
        new_sl_price = self.client.normalize_trigger_price(symbol, new_sl_raw, round_up=True)

        noon_cap_price = self._get_noon_protection_cap(position_id)
        if noon_cap_price:
            new_sl_price = min(new_sl_price, noon_cap_price)
        new_sl_stop_price = self.client.format_trigger_price(symbol, new_sl_price, round_up=True)

        rules = self.client.get_symbol_rules().get(symbol)
        min_delta = rules.tick_size if rules else 0.0

        if old_sl_price and abs(new_sl_price - old_sl_price) <= max(min_delta, 1e-12):
            return None

        self._cancel_order_if_exists(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id"))
        sl_order = self._create_stop_order_with_fallback(
            symbol=symbol,
            side="BUY",
            stop_price=new_sl_stop_price,
            qty=position_amt,
            client_order_id=self._new_client_id("sl", symbol),
        )

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
        return {
            "old_sl_price": old_sl_price,
            "new_sl_price": new_sl_price,
            "liq_price": liq_price,
        }

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
            "newClientOrderId": client_order_id,
        }
        if use_reduce_only:
            create_order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            create_order_params["positionSide"] = position_side
        return self.client.create_order(**create_order_params)

    def _get_order_status(
        self,
        symbol: str,
        order_id: object,
        client_order_id: object,
    ) -> Optional[str]:
        if not order_id and not client_order_id:
            return None
        try:
            parsed_order_id = int(order_id) if order_id else None
            parsed_client_order_id = str(client_order_id) if client_order_id else None
            order = self.client.get_order(
                symbol=symbol,
                order_id=parsed_order_id,
                orig_client_order_id=parsed_client_order_id,
            )
            return order.get("status")
        except BinanceAPIError as exc:
            # Order may already be gone due to auto-cancel, ignore and continue with position state.
            LOGGER.debug("get_order failed for %s/%s/%s: %s", symbol, order_id, client_order_id, exc)
            return None

    def _cancel_exit_orders(self, pos: Dict[str, object]) -> None:
        symbol = str(pos["symbol"])
        self._cancel_order_if_exists(symbol, pos.get("tp_order_id"), pos.get("tp_client_order_id"))
        self._cancel_order_if_exists(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id"))

    def _cancel_order_if_exists(self, symbol: str, order_id: object, client_order_id: object) -> None:
        if not order_id and not client_order_id:
            return
        try:
            parsed_order_id = int(order_id) if order_id else None
            parsed_client_order_id = str(client_order_id) if client_order_id else None
            self.client.cancel_order(
                symbol=symbol,
                order_id=parsed_order_id,
                orig_client_order_id=parsed_client_order_id,
            )
        except BinanceAPIError as exc:
            LOGGER.debug("cancel_order ignored for %s/%s/%s: %s", symbol, order_id, client_order_id, exc)

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
    def _build_noon_protection_notification(summary: Dict[str, int], details: Dict[str, List[str]]) -> str:
        rows = [
            ("open_positions", summary["total"]),
            ("updated_sl", summary["updated_sl"]),
            ("skipped", summary["skipped"]),
            ("errors", summary["errors"]),
        ]
        lines = [
            "### Top10 做空 12:00 保护止损汇总",
            "",
            f"- 巡检时间(UTC): `{datetime.now(timezone.utc).replace(microsecond=0).isoformat()}`",
            "",
            "### 摘要",
            "",
            format_markdown_kv_table(rows),
        ]

        for key, title in [
            ("updated_sl", "保护止损更新明细"),
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
        rows = self.client.get_klines(
            symbol=symbol,
            interval="1m",
            start_time=start_ms,
            end_time=end_ms,
            limit=1000,
        )
        highs: List[float] = []
        lows: List[float] = []
        for row in rows or []:
            if len(row) < 4:
                continue
            high = self._safe_positive_float(row[2])
            low = self._safe_positive_float(row[3])
            if high:
                highs.append(high)
            if low:
                lows.append(low)
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

    def _persist_noon_protection_caps(self, caps: Dict[str, float]) -> None:
        payload = {
            "caps": {str(cap_key): float(price) for cap_key, price in caps.items() if price > 0},
            "updated_at_utc": self._utc_now_iso(),
        }
        self.store.set_lock_state(self.NOON_PROTECTION_LOCK_NAME, payload)

    def _get_noon_protection_cap(self, position_id: int) -> Optional[float]:
        if self._noon_protection_caps_cache is None:
            self._noon_protection_caps_cache = self._load_noon_protection_caps()
        value = self._noon_protection_caps_cache.get(str(int(position_id)))
        if value is None or value <= 0:
            return None
        return float(value)

    @staticmethod
    def _build_noon_cap_key(
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

    def _get_symbol_position_risk(self, symbol: str) -> Optional[Dict[str, str]]:
        rows = self.client.get_position_risk(symbol=symbol)
        for row in rows:
            if row.get("symbol") == symbol:
                return row
        return None

    @classmethod
    def _normalize_daily_loss_cut_scope(cls, scope: str) -> str:
        normalized = str(scope or "").strip().lower()
        if normalized in {cls.DAILY_LOSS_CUT_SCOPE_TRACKED, cls.DAILY_LOSS_CUT_SCOPE_EXCHANGE}:
            return normalized
        if normalized:
            LOGGER.warning("Invalid daily_loss_cut_scope=%s, fallback to %s", normalized, cls.DAILY_LOSS_CUT_SCOPE_TRACKED)
        return cls.DAILY_LOSS_CUT_SCOPE_TRACKED

    @staticmethod
    def _is_expired(expire_at_utc: str) -> bool:
        expire_time = datetime.fromisoformat(expire_at_utc)
        now_utc = datetime.now(timezone.utc)
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
    def _parse_iso_utc(text: str) -> datetime:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
