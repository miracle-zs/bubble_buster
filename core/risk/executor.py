"""Unified executor for risk exits, stop-loss orders and protective market closures."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from core.market_fill_reconciler import MarketFillReconciler
from core.risk.models import ExitIntent
from core.state_store import StateStore
from infra.binance_futures_client import BinanceAPIError, BinanceFuturesClient

LOGGER = logging.getLogger(__name__)


def is_immediate_trigger_error(exc: BinanceAPIError) -> bool:
    """Return True if Binance error indicates order would trigger immediately (code -2021)."""
    message = str(getattr(exc, "message", "") or exc).lower()
    return getattr(exc, "code", None) == -2021 or "immediately trigger" in message


@dataclass
class StopLossExecutionResult:
    """Outcome of attempting to execute a stop-loss update intent."""
    status: str  # "UPDATED", "CLOSED_IMMEDIATE", "FAILED"
    sl_order: Optional[Dict[str, Any]] = None
    close_info: Optional[Dict[str, Any]] = None
    error: Optional[Exception] = None


class CentralExitExecutor:
    """Handles order placement, cancellation and persistence for risk and protection intents."""

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        market_fill_reconciler: Optional[MarketFillReconciler] = None,
        trigger_price_type: str = "CONTRACT_PRICE",
        new_client_id_fn: Optional[Callable[[str, str], str]] = None,
        now_iso_fn: Optional[Callable[[], str]] = None,
    ) -> None:
        self.client = client
        self.store = store
        self.reconciler = market_fill_reconciler or MarketFillReconciler(client, store)
        self.trigger_price_type = trigger_price_type
        self.new_client_id_fn = new_client_id_fn or (lambda tag, sym: f"{tag}-{sym}")
        self.now_iso_fn = now_iso_fn or (lambda: "")

    def create_stop_order_with_fallback(
        self,
        symbol: str,
        side: str,
        stop_price: str,
        qty: float,
        client_order_id: str,
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
    ) -> Dict[str, Any]:
        """Place a STOP_MARKET conditional order with price protection."""
        create_order_params: Dict[str, Any] = {
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

    def close_protection_immediate(
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
    ) -> Dict[str, Any]:
        """Execute immediate market close when a stop-loss would immediately trigger."""
        create_order_params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": self.client.format_order_qty(symbol, qty),
            "newClientOrderId": self.new_client_id_fn(client_id_tag, symbol),
            "newOrderRespType": "RESULT",
        }
        if use_reduce_only:
            create_order_params["reduceOnly"] = True
        if position_side in {"LONG", "SHORT"}:
            create_order_params["positionSide"] = position_side

        close_order = self.client.create_order(**create_order_params)
        with self.store.unit_of_work():
            self.reconciler.record_market_order(
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

    def close_position(
        self,
        symbol: str,
        qty: float,
        side: str,
        position_id: Optional[int] = None,
        cancel_pos: Optional[Dict[str, Any]] = None,
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
        close_status: str = "CLOSED_MARKET",
        close_reason: str = "MANUAL_OR_RISK",
        client_id_tag: str = "close",
    ) -> Dict[str, Any]:
        """Execute a market close order, record fill, mark position closed, and cancel exit orders."""
        res = self.close_protection_immediate(
            symbol=symbol,
            qty=qty,
            side=side,
            position_id=position_id,
            position_side=position_side,
            use_reduce_only=use_reduce_only,
            close_status=close_status,
            close_reason=close_reason,
            client_id_tag=client_id_tag,
        )
        if cancel_pos is not None:
            self.cancel_exit_orders(cancel_pos)
        return res

    def cancel_order_if_exists(
        self,
        symbol: str,
        order_id: Optional[object] = None,
        client_order_id: Optional[object] = None,
    ) -> bool:
        """Safely cancel an order if id is present, recording local state."""
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

    def cancel_exit_orders(self, tracked_pos: Dict[str, Any]) -> None:
        """Cancel both TP and SL orders associated with a tracked position."""
        symbol = str(tracked_pos.get("symbol") or "")
        if not symbol:
            return
        self.cancel_order_if_exists(symbol, tracked_pos.get("tp_order_id"), tracked_pos.get("tp_client_order_id"))
        self.cancel_order_if_exists(symbol, tracked_pos.get("sl_order_id"), tracked_pos.get("sl_client_order_id"))

    def execute_stop_loss_intent(
        self,
        intent: ExitIntent,
        tracked_pos: Optional[Dict[str, Any]] = None,
        liquidation_price: Optional[float] = None,
        client_id_prefix: str = "nsl",
        immediate_close_status: str = "CLOSED_NOON_PROTECTION",
        immediate_close_reason: str = "NOON_PROTECTION_IMMEDIATE_TRIGGER",
        immediate_client_id_tag: str = "nsi",
    ) -> StopLossExecutionResult:
        """Execute an UPDATE_STOP_LOSS intent, handling immediate triggers and rollback."""
        symbol = intent.symbol
        qty = intent.qty or 0.0
        if qty <= 0:
            return StopLossExecutionResult(status="FAILED", error=ValueError("position qty is zero"))

        if intent.target_price is None or intent.target_price <= 0:
            return StopLossExecutionResult(status="FAILED", error=ValueError("invalid target_price"))

        round_up = intent.close_side == "BUY"
        sl_stop_price = self.client.format_trigger_price(symbol, intent.target_price, round_up=round_up)
        client_order_id = self.new_client_id_fn(client_id_prefix, symbol)

        try:
            sl_order = self.create_stop_order_with_fallback(
                symbol=symbol,
                side=intent.close_side,
                stop_price=sl_stop_price,
                qty=qty,
                client_order_id=client_order_id,
                position_side=intent.position_side,
                use_reduce_only=intent.use_reduce_only,
            )
        except BinanceAPIError as exc:
            if not is_immediate_trigger_error(exc):
                return StopLossExecutionResult(status="FAILED", error=exc)

            # Order would immediately trigger -> fallback to immediate market close
            close_info = self.close_protection_immediate(
                symbol=symbol,
                qty=qty,
                side=intent.close_side,
                position_id=intent.position_id,
                position_side=intent.position_side,
                use_reduce_only=intent.use_reduce_only,
                close_status=immediate_close_status,
                close_reason=immediate_close_reason,
                client_id_tag=immediate_client_id_tag,
            )
            if tracked_pos is not None:
                self.cancel_exit_orders(tracked_pos)

            if intent.position_id is not None:
                self.store.clear_position_error(intent.position_id)

            return StopLossExecutionResult(
                status="CLOSED_IMMEDIATE",
                close_info=close_info,
                error=None,
            )

        # Place succeeded: update state and cancel old stop order
        try:
            with self.store.unit_of_work():
                if intent.position_id is not None:
                    self.store.update_stop_loss(
                        position_id=intent.position_id,
                        sl_order_id=sl_order.get("orderId"),
                        sl_client_order_id=sl_order.get("clientOrderId"),
                        sl_price=intent.target_price,
                        liq_price_latest=liquidation_price,
                    )
                self.store.add_order_event(
                    symbol=symbol,
                    position_id=intent.position_id,
                    event_time_utc=self.now_iso_fn(),
                    order_payload=sl_order,
                )
        except Exception as exc:
            # Rollback: cancel the newly created order if DB persistence failed
            self.cancel_order_if_exists(symbol, sl_order.get("orderId"), sl_order.get("clientOrderId"))
            return StopLossExecutionResult(status="FAILED", error=exc)

        # Cancel the previous SL order on exchange
        if tracked_pos is not None:
            self.cancel_order_if_exists(symbol, tracked_pos.get("sl_order_id"), tracked_pos.get("sl_client_order_id"))

        if intent.position_id is not None:
            self.store.clear_position_error(intent.position_id)

        return StopLossExecutionResult(
            status="UPDATED",
            sl_order=sl_order,
            error=None,
        )
