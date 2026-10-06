"""Unified executor for risk exits, stop-loss orders and protective market closures."""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from core.market_fill_reconciler import MarketFillReconciler
from core.risk.models import ExitIntent
from core.state_store import StateStore
from infra.binance_futures_client import (
    BinanceAPIError,
    BinanceFuturesClient,
    OrderStateUnknownError,
)

LOGGER = logging.getLogger(__name__)

INSUFFICIENT_MARGIN_ERROR_CODES = {-2019, -2027, -2028}
COOLING_OFF_ERROR_CODES = {-4192}


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
        self._execution_engine: Optional[Any] = None

    @property
    def account_id(self) -> str:
        val = getattr(self.store, "account_id", None)
        if isinstance(val, str) and val:
            return val
        return "default"

    @property
    def execution_engine(self) -> Any:
        if self._execution_engine is None:
            from core.execution.engine import ExecutionEngine
            self._execution_engine = ExecutionEngine(
                client=self.client,
                store=self.store,
                reconciler=self.reconciler,
                now_iso_fn=self.now_iso_fn,
            )
        return self._execution_engine

    def submit_intent(
        self,
        intent: Any,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
        time_in_force: str = "GTC",
        close_position: bool = False,
    ) -> Any:
        return self.execution_engine.submit_intent(
            intent=intent,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            position_side=position_side,
            time_in_force=time_in_force,
            close_position=close_position,
        )

    def create_stop_order_with_fallback(
        self,
        symbol: str,
        side: str,
        stop_price: str,
        qty: float,
        client_order_id: str,
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
        episode_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a STOP_MARKET conditional order with price protection via execution engine."""
        from core.execution.models import OrderIntent

        if episode_id is None:
            active_ep = self.store.get_active_position_episode(symbol)
            if active_ep:
                episode_id = active_ep.get("episode_id")

        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            account_id=self.account_id,
            client_intent_key=f"stop_{symbol}_{client_order_id}",
            symbol=symbol,
            side=side,
            order_type="STOP_MARKET",
            target_qty=qty,
            intent_scope="PROTECTION",
            episode_id=episode_id,
            reason="STOP_LOSS",
        )
        attempt = self.execution_engine.submit_intent(
            intent=intent,
            client_order_id=client_order_id,
            reduce_only=use_reduce_only,
            position_side=position_side,
            stop_price=stop_price,
            working_type=self.trigger_price_type,
            price_protect=True,
            raise_on_error=True,
        )
        return (
            attempt.exchange_response
            if isinstance(attempt.exchange_response, dict)
            else {
                "orderId": attempt.exchange_order_id,
                "status": attempt.status,
                "clientOrderId": attempt.client_order_id,
            }
        )

    def create_limit_order(
        self,
        symbol: str,
        side: str,
        price: str,
        qty: float,
        client_order_id: str,
        time_in_force: str = "GTC",
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
        episode_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a LIMIT order via execution engine."""
        from core.execution.models import OrderIntent
        try:
            target_price = float(price)
        except (ValueError, TypeError):
            target_price = None

        if episode_id is None:
            active_ep = self.store.get_active_position_episode(symbol)
            if active_ep:
                episode_id = active_ep.get("episode_id")

        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            account_id=self.account_id,
            client_intent_key=f"limit_{symbol}_{client_order_id}",
            symbol=symbol,
            side=side,
            order_type="LIMIT",
            target_qty=qty,
            target_price=target_price,
            intent_scope="POSITION",
            episode_id=episode_id,
            reason="LIMIT_ORDER",
        )
        attempt = self.execution_engine.submit_intent(
            intent=intent,
            client_order_id=client_order_id,
            reduce_only=use_reduce_only,
            position_side=position_side,
            time_in_force=time_in_force,
            price=price,
            raise_on_error=True,
        )
        return (
            attempt.exchange_response
            if isinstance(attempt.exchange_response, dict)
            else {
                "orderId": attempt.exchange_order_id,
                "status": attempt.status,
                "clientOrderId": attempt.client_order_id,
            }
        )

    def create_take_profit_order_with_fallback(
        self,
        symbol: str,
        side: str,
        stop_price: str,
        qty: float,
        client_order_id: str,
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
        close_position: bool = False,
        episode_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a TAKE_PROFIT_MARKET conditional order with closePosition/reduceOnly fallback via execution engine."""
        from core.execution.models import OrderIntent

        if episode_id is None:
            active_ep = self.store.get_active_position_episode(symbol)
            if active_ep:
                episode_id = active_ep.get("episode_id")

        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            account_id=self.account_id,
            client_intent_key=f"tp_{symbol}_{client_order_id}",
            symbol=symbol,
            side=side,
            order_type="TAKE_PROFIT_MARKET",
            target_qty=qty,
            intent_scope="PROTECTION",
            episode_id=episode_id,
            reason="TAKE_PROFIT",
        )
        if close_position:
            try:
                attempt = self.execution_engine.submit_intent(
                    intent=intent,
                    client_order_id=client_order_id,
                    close_position=True,
                    position_side=position_side,
                    stop_price=stop_price,
                    working_type=self.trigger_price_type,
                    price_protect=True,
                    raise_on_error=True,
                )
                return (
                    attempt.exchange_response
                    if isinstance(attempt.exchange_response, dict)
                    else {
                        "orderId": attempt.exchange_order_id,
                        "status": attempt.status,
                        "clientOrderId": attempt.client_order_id,
                    }
                )
            except BinanceAPIError as exc:
                try:
                    code = int(getattr(exc, "code", 0) or 0)
                except (TypeError, ValueError):
                    code = 0
                if code not in {-4120, -4130}:
                    raise
                LOGGER.warning(
                    "Fallback to reduceOnly TAKE_PROFIT_MARKET for %s due to code=%s",
                    symbol,
                    code,
                )
                attempt = self.execution_engine.submit_intent(
                    intent=intent,
                    client_order_id=client_order_id,
                    close_position=False,
                    reduce_only=True,
                    position_side=position_side,
                    stop_price=stop_price,
                    working_type=self.trigger_price_type,
                    price_protect=True,
                    raise_on_error=True,
                )
                return (
                    attempt.exchange_response
                    if isinstance(attempt.exchange_response, dict)
                    else {
                        "orderId": attempt.exchange_order_id,
                        "status": attempt.status,
                        "clientOrderId": attempt.client_order_id,
                    }
                )
        else:
            attempt = self.execution_engine.submit_intent(
                intent=intent,
                client_order_id=client_order_id,
                close_position=False,
                reduce_only=use_reduce_only,
                position_side=position_side,
                stop_price=stop_price,
                working_type=self.trigger_price_type,
                price_protect=True,
                raise_on_error=True,
            )
            return (
                attempt.exchange_response
                if isinstance(attempt.exchange_response, dict)
                else {
                    "orderId": attempt.exchange_order_id,
                    "status": attempt.status,
                    "clientOrderId": attempt.client_order_id,
                }
            )

    def create_conditional_order_with_fallback(
        self,
        symbol: str,
        order_type: str,
        side: str,
        stop_price: str,
        qty: float,
        client_order_id: str,
        position_side: Optional[str] = None,
        episode_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place conditional order (STOP_MARKET or TAKE_PROFIT_MARKET) with closePosition fallback via execution engine."""
        from core.execution.models import OrderIntent

        if episode_id is None:
            active_ep = self.store.get_active_position_episode(symbol)
            if active_ep:
                episode_id = active_ep.get("episode_id")

        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            account_id=self.account_id,
            client_intent_key=f"cond_{symbol}_{client_order_id}",
            symbol=symbol,
            side=side,
            order_type=order_type,
            target_qty=qty,
            intent_scope="PROTECTION",
            episode_id=episode_id,
            reason=order_type,
        )
        try:
            attempt = self.execution_engine.submit_intent(
                intent=intent,
                client_order_id=client_order_id,
                close_position=True,
                position_side=position_side,
                stop_price=stop_price,
                working_type=self.trigger_price_type,
                price_protect=True,
                raise_on_error=True,
            )
            return (
                attempt.exchange_response
                if isinstance(attempt.exchange_response, dict)
                else {
                    "orderId": attempt.exchange_order_id,
                    "status": attempt.status,
                    "clientOrderId": attempt.client_order_id,
                }
            )
        except BinanceAPIError as exc:
            try:
                code = int(getattr(exc, "code", 0) or 0)
            except (TypeError, ValueError):
                code = 0
            if code not in {-4120, -4130}:
                raise
            LOGGER.warning(
                "Fallback to reduceOnly conditional order for %s/%s due to code=%s",
                symbol,
                order_type,
                code,
            )
            attempt = self.execution_engine.submit_intent(
                intent=intent,
                client_order_id=client_order_id,
                close_position=False,
                reduce_only=True,
                position_side=position_side,
                stop_price=stop_price,
                working_type=self.trigger_price_type,
                price_protect=True,
                raise_on_error=True,
            )
            return (
                attempt.exchange_response
                if isinstance(attempt.exchange_response, dict)
                else {
                    "orderId": attempt.exchange_order_id,
                    "status": attempt.status,
                    "clientOrderId": attempt.client_order_id,
                }
            )

    def close_market_order(
        self,
        symbol: str,
        qty: Optional[float] = None,
        side: str = "BUY",
        quantity: Optional[str] = None,
        client_order_id: Optional[str] = None,
        client_id_tag: str = "close",
        position_id: Optional[int] = None,
        position_side: Optional[str] = None,
        use_reduce_only: bool = True,
        close_status: Optional[str] = None,
        close_reason: Optional[str] = None,
        episode_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Execute a market order via execution engine, record fill in reconciler, and optionally update position status."""
        cid = client_order_id or self.new_client_id_fn(client_id_tag, symbol)
        target_qty: Optional[float] = None
        if quantity is not None:
            try:
                target_qty = float(quantity)
            except (ValueError, TypeError):
                target_qty = float(qty) if isinstance(qty, (int, float)) else None
        elif isinstance(qty, (int, float)):
            target_qty = float(qty)
        elif isinstance(qty, str):
            try:
                target_qty = float(qty)
            except (ValueError, TypeError):
                target_qty = None

        if episode_id is None and position_id is not None:
            pos = self.store.get_position(position_id)
            if pos and pos.get("episode_id"):
                episode_id = pos["episode_id"]
        if episode_id is None:
            active_ep = self.store.get_active_position_episode(symbol)
            if active_ep:
                episode_id = active_ep.get("episode_id")

        from core.execution.models import OrderIntent
        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            account_id=self.account_id,
            client_intent_key=f"close_{symbol}_{cid}",
            symbol=symbol,
            side=side,
            order_type="MARKET",
            target_qty=target_qty,
            intent_scope="POSITION",
            position_id=position_id,
            episode_id=episode_id,
            reason=close_reason or "MARKET_CLOSE",
        )

        attempt = self.execution_engine.submit_intent(
            intent=intent,
            client_order_id=cid,
            reduce_only=use_reduce_only,
            position_side=position_side,
            quantity=str(quantity) if quantity is not None else None,
            raise_on_error=True,
        )

        order_dict = (
            attempt.exchange_response
            if isinstance(attempt.exchange_response, dict)
            else {
                "orderId": attempt.exchange_order_id,
                "status": attempt.status,
                "clientOrderId": attempt.client_order_id,
                "executedQty": attempt.executed_qty,
                "avgPrice": attempt.avg_price,
            }
        )

        if position_id is not None and close_status is not None:
            with self.store.unit_of_work():
                self.store.mark_position_closed(
                    position_id=position_id,
                    status=close_status,
                    close_reason=close_reason or "MARKET_CLOSE",
                    close_order_id=order_dict.get("orderId") if isinstance(order_dict, dict) else attempt.exchange_order_id,
                )
        return order_dict

    def create_market_order(
        self,
        symbol: str,
        side: str,
        qty: Optional[float] = None,
        quantity: Optional[str] = None,
        client_order_id: Optional[str] = None,
        position_side: Optional[str] = None,
        use_reduce_only: bool = False,
        episode_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place a raw MARKET order via execution engine."""
        cid = client_order_id or self.new_client_id_fn("mkt", symbol)
        target_qty: Optional[float] = None
        if quantity is not None:
            try:
                target_qty = float(quantity)
            except (ValueError, TypeError):
                target_qty = float(qty) if isinstance(qty, (int, float)) else None
        elif isinstance(qty, (int, float)):
            target_qty = float(qty)
        elif isinstance(qty, str):
            try:
                target_qty = float(qty)
            except (ValueError, TypeError):
                target_qty = None

        if episode_id is None and use_reduce_only:
            active_ep = self.store.get_active_position_episode(symbol)
            if active_ep:
                episode_id = active_ep.get("episode_id")

        from core.execution.models import OrderIntent
        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            account_id=self.account_id,
            client_intent_key=f"mkt_{symbol}_{cid}",
            symbol=symbol,
            side=side,
            order_type="MARKET",
            target_qty=target_qty,
            intent_scope="ENTRY" if not use_reduce_only else "EXIT",
            episode_id=episode_id,
            reason="MARKET_ORDER",
        )

        attempt = self.execution_engine.submit_intent(
            intent=intent,
            client_order_id=cid,
            reduce_only=use_reduce_only,
            position_side=position_side,
            quantity=str(quantity) if quantity is not None else None,
            raise_on_error=True,
        )

        return (
            attempt.exchange_response
            if isinstance(attempt.exchange_response, dict)
            else {
                "orderId": attempt.exchange_order_id,
                "status": attempt.status,
                "clientOrderId": attempt.client_order_id,
                "executedQty": attempt.executed_qty,
            }
        )

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[object] = None,
        client_order_id: Optional[object] = None,
    ) -> Dict[str, Any]:
        """Cancel an order via execution engine with full intent audit."""
        return self.execution_engine.cancel_order(
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
            reason="LOCAL_CANCEL",
        )

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
        close_order = self.close_market_order(
            symbol=symbol,
            qty=qty,
            side=side,
            client_id_tag=client_id_tag,
            position_id=position_id,
            position_side=position_side,
            use_reduce_only=use_reduce_only,
            close_status=close_status,
            close_reason=close_reason,
        )
        return {
            "qty": qty,
            "close_order_id": close_order.get("orderId"),
            "order": close_order,
        }

    def create_order_with_cooling_off_retry(
        self,
        submit_order: Callable[[], Dict[str, Any]],
        symbol: str,
        side: str,
        context: str,
        max_retries: int = 0,
        delay_sec: float = 0.0,
        account_id: str = "",
    ) -> Dict[str, Any]:
        """Execute an order submission with cooling-off error (-4192) backoff retry."""
        for attempt in range(max_retries + 1):
            try:
                return submit_order()
            except BinanceAPIError as exc:
                code = getattr(exc, "code", None)
                if code not in COOLING_OFF_ERROR_CODES:
                    raise
                if max_retries <= 0 or delay_sec <= 0 or attempt >= max_retries:
                    raise
                LOGGER.warning(
                    "Cooling-off retry scheduled: account=%s symbol=%s side=%s context=%s wait_sec=%s retry=%s/%s",
                    account_id,
                    symbol,
                    str(side or "").upper() or "-",
                    context,
                    delay_sec,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay_sec)

        raise RuntimeError(f"Cooling-off retry exhausted unexpectedly for {symbol}")

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
        """Safely cancel an order if id is present via execution engine."""
        if not order_id and not client_order_id:
            return True
        try:
            parsed_order_id = int(order_id) if order_id else None
            parsed_client_order_id = str(client_order_id) if client_order_id else None
            res = self.cancel_order(
                symbol=symbol,
                order_id=parsed_order_id,
                client_order_id=parsed_client_order_id,
            )
            if isinstance(res, dict):
                self.store.upsert_exchange_order_state(res, source="LOCAL_CANCEL")
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


OrderExecutor = CentralExitExecutor
