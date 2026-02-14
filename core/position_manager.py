import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
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
    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        notifier: ServerChanNotifier,
        sl_liq_buffer_pct: float,
        trigger_price_type: str,
    ):
        self.client = client
        self.store = store
        self.notifier = notifier
        self.sl_liq_buffer_pct = sl_liq_buffer_pct
        self.trigger_price_type = trigger_price_type

    def run_daily_loss_cut(self) -> Dict[str, int]:
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

        for pos in positions:
            position_id = int(pos["id"])
            symbol = str(pos["symbol"])
            try:
                risk = self._get_symbol_position_risk(symbol)
                if risk is None:
                    self.store.set_position_error(position_id, "position risk not found")
                    continue

                position_amt = float(risk.get("positionAmt", "0") or 0)
                if position_amt >= 0:
                    continue

                unrealized_pnl = float(risk.get("unRealizedProfit", "0") or 0)
                if unrealized_pnl >= 0:
                    continue

                close_info = self._close_daily_loss_cut(pos, abs(position_amt), unrealized_pnl)
                summary["closed_loss_cut"] += 1
                details["closed_loss_cut"].append(
                    f"{symbol}(id={position_id}, upnl={unrealized_pnl:.6f}, qty={close_info['qty']}, "
                    f"close_order_id={close_info['close_order_id']})"
                )
                self.store.clear_position_error(position_id)
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                LOGGER.exception("Daily loss-cut failed for position id=%s symbol=%s: %s", position_id, symbol, exc)
                self.store.set_position_error(position_id, str(exc))
                details["errors"].append(f"{symbol}(id={position_id}): {exc}")

        if summary["closed_loss_cut"] > 0 or summary["errors"] > 0:
            self.notifier.send(
                "【Top10做空】11:55浮亏止损汇总",
                self._build_daily_loss_cut_notification(summary, details),
            )

        return summary

    def run_once(self) -> Dict[str, int]:
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

    def _close_daily_loss_cut(self, pos: Dict[str, object], qty: float, unrealized_pnl: float) -> Dict[str, object]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])

        self._cancel_exit_orders(pos)

        close_order = self.client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=self.client.format_order_qty(symbol, qty),
            reduceOnly=True,
            newClientOrderId=self._new_client_id("dl", symbol),
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
            status="CLOSED_DAILY_LOSS_CUT",
            close_reason="DAILY_FLOATING_LOSS_CHECK",
            close_order_id=close_order.get("orderId"),
        )
        return {
            "qty": qty,
            "close_order_id": close_order.get("orderId"),
            "unrealized_pnl": unrealized_pnl,
        }

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
        new_sl_stop_price = self.client.format_trigger_price(symbol, new_sl_price, round_up=True)

        rules = self.client.get_symbol_rules().get(symbol)
        min_delta = rules.tick_size if rules else 0.0

        if old_sl_price and abs(new_sl_price - old_sl_price) <= max(min_delta, 1e-12):
            return None

        self._cancel_order_if_exists(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id"))
        sl_order = self._create_stop_order_with_fallback(
            symbol=symbol,
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
        stop_price: str,
        qty: float,
        client_order_id: str,
    ) -> Dict[str, object]:
        try:
            return self.client.create_order(
                symbol=symbol,
                side="BUY",
                type="STOP_MARKET",
                stopPrice=stop_price,
                closePosition=True,
                workingType=self.trigger_price_type,
                newClientOrderId=client_order_id,
            )
        except BinanceAPIError as exc:
            try:
                code = int(exc.code)
            except (TypeError, ValueError):
                code = None
            if code != -4120:
                raise

            LOGGER.warning("Fallback to reduceOnly stop for %s due to -4120", symbol)
            return self.client.create_order(
                symbol=symbol,
                side="BUY",
                type="STOP_MARKET",
                stopPrice=stop_price,
                quantity=self.client.format_order_qty(symbol, qty),
                reduceOnly=True,
                workingType=self.trigger_price_type,
                newClientOrderId=client_order_id,
            )

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

    def _get_symbol_position_risk(self, symbol: str) -> Optional[Dict[str, str]]:
        rows = self.client.get_position_risk(symbol=symbol)
        for row in rows:
            if row.get("symbol") == symbol:
                return row
        return None

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
    def _new_client_id(tag: str, symbol: str) -> str:
        return f"t10s-{tag}-{symbol}-{uuid4().hex[:8]}"[:36]

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
