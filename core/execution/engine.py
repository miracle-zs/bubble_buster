"""ExecutionEngine: first-principles execution protocol coordinator.

Implements the canonical lifecycle:
1. Intent & attempt persistence in short DB transaction.
2. Exchange submission OUTSIDE DB transactions.
3. Explicit state progression: PREPARED -> SUBMITTING -> ACKNOWLEDGED / FILLED / UNKNOWN / REJECTED.
4. Idempotent unknown order recovery by client_order_id before retry.
5. Fill reconciliation and position episode synchronization.
"""

from __future__ import annotations

import hashlib
import logging
import math
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from infra.binance_futures_client import (
    BinanceAPIError,
    BinanceFuturesClient,
    OrderStateUnknownError,
)
from core.execution.ledger import TradingLedger
from core.execution.models import (
    AttemptStatus,
    EpisodeStatus,
    ExecutionFill,
    IntentStatus,
    OrderAttempt,
    OrderIntent,
    PositionEpisode,
    utc_now_iso,
)
from core.market_fill_reconciler import MarketFillReconciler
from core.state_store import StateStore

LOGGER = logging.getLogger(__name__)


def sanitize_client_order_id(cid: str, max_len: int = 36) -> str:
    """Sanitize client_order_id to strictly match Binance Futures requirement: ^[.A-Z:/a-z0-9_-]{1,36}$."""
    raw = str(cid or "").strip()
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-:/")
    cleaned = "".join(ch for ch in raw if ch in allowed)

    if not cleaned or len(cleaned) != len(raw):
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:6]
        prefix_limit = max(1, max_len - 7)
        cleaned = f"{cleaned[:prefix_limit]}-{digest}" if cleaned else f"bb-{digest}"

    if len(cleaned) > max_len:
        digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:6]
        prefix_limit = max(1, max_len - 7)
        cleaned = f"{cleaned[:prefix_limit]}-{digest}"

    return cleaned


class ExecutionEngine:
    """Manages order intents and attempts with robust failure recovery."""

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        reconciler: Optional[MarketFillReconciler] = None,
        now_iso_fn: Optional[Callable[[], str]] = None,
        ledger: Optional[TradingLedger] = None,
        account_id: Optional[str] = None,
    ) -> None:
        self.client = client
        self.store = store
        self.account_id = account_id or getattr(self.store, "account_id", "default")
        self.reconciler = reconciler or MarketFillReconciler(client=client, store=store)
        self.now_iso_fn = now_iso_fn or utc_now_iso
        self.ledger = ledger or TradingLedger(store=store, now_iso_fn=self.now_iso_fn)

    def _dict_to_attempt(self, d: Dict[str, Any]) -> OrderAttempt:
        return OrderAttempt(
            attempt_id=str(d["attempt_id"]),
            intent_id=str(d["intent_id"]),
            account_id=str(d.get("account_id") or getattr(self.store, "account_id", "default")),
            symbol=str(d["symbol"]),
            client_order_id=str(d["client_order_id"]),
            exchange_order_id=d.get("exchange_order_id"),
            attempt_number=int(d.get("attempt_number") or 1),
            status=str(d.get("status") or AttemptStatus.PREPARED.value),
            submitted_qty=float(d["submitted_qty"]) if d.get("submitted_qty") is not None else None,
            executed_qty=float(d.get("executed_qty") or 0.0),
            cumulative_quote_qty=float(d["cumulative_quote_qty"]) if d.get("cumulative_quote_qty") is not None else None,
            avg_price=float(d["avg_price"]) if d.get("avg_price") is not None else None,
            error_message=d.get("error_message"),
            parent_attempt_id=d.get("parent_attempt_id"),
            created_at_utc=str(d.get("created_at_utc") or self.now_iso_fn()),
            updated_at_utc=str(d.get("updated_at_utc") or self.now_iso_fn()),
        )

    def submit_intent(
        self,
        intent: OrderIntent,
        client_order_id: Optional[str] = None,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
        time_in_force: str = "GTC",
        close_position: bool = False,
        stop_price: Optional[str] = None,
        working_type: Optional[str] = None,
        price_protect: Optional[bool] = None,
        price: Optional[str] = None,
        quantity: Optional[str] = None,
        raise_on_error: bool = False,
    ) -> OrderAttempt:
        """Submit an OrderIntent through the execution state machine."""
        # 1. Short DB transaction: persist or verify intent
        with self.store.unit_of_work():
            existing_intent = self.store.get_order_intent_by_key(intent.client_intent_key)
            if isinstance(existing_intent, dict):
                intent.intent_id = existing_intent.get("intent_id") or intent.intent_id
                intent.status = existing_intent.get("status") or intent.status
                if not intent.position_id:
                    intent.position_id = existing_intent.get("position_id")
                if not intent.episode_id:
                    intent.episode_id = existing_intent.get("episode_id")
            else:
                self.store.save_order_intent(
                    intent_id=intent.intent_id,
                    client_intent_key=intent.client_intent_key,
                    symbol=intent.symbol,
                    side=intent.side,
                    order_type=intent.order_type,
                    target_qty=intent.target_qty,
                    target_price=intent.target_price,
                    intent_scope=intent.intent_scope,
                    position_id=intent.position_id,
                    episode_id=intent.episode_id,
                    status=IntentStatus.PENDING.value,
                    reason=intent.reason,
                )

        # Idempotency checks:
        # A. If intent is already COMPLETED: return the existing completed attempt
        if intent.status == IntentStatus.COMPLETED.value:
            latest_att = self.store.get_latest_order_attempt_for_intent(intent.intent_id)
            if latest_att:
                LOGGER.info(
                    "Intent %s already COMPLETED, returning existing attempt %s (idempotent)",
                    intent.intent_id,
                    latest_att.get("attempt_id"),
                )
                return self._dict_to_attempt(latest_att)

        # B. Check existing attempts for this intent
        prev_attempts = self.store.list_order_attempts_for_intent(intent.intent_id)
        if prev_attempts:
            for prev_dict in reversed(prev_attempts):
                att_status = prev_dict.get("status")
                if att_status == AttemptStatus.UNKNOWN.value:
                    att_obj = self._dict_to_attempt(prev_dict)
                    recovered = self.recover_unknown_attempt(att_obj)
                    if recovered.status == AttemptStatus.FILLED.value:
                        return recovered
                    if recovered.status in {
                        AttemptStatus.ACKNOWLEDGED.value,
                        AttemptStatus.PARTIALLY_FILLED.value,
                    }:
                        return recovered
                elif att_status in {
                    AttemptStatus.ACKNOWLEDGED.value,
                    AttemptStatus.PARTIALLY_FILLED.value,
                    AttemptStatus.SUBMITTING.value,
                }:
                    LOGGER.info(
                        "Intent %s already has in-flight attempt %s with status %s, returning it",
                        intent.intent_id,
                        prev_dict.get("attempt_id"),
                        att_status,
                    )
                    return self._dict_to_attempt(prev_dict)
                elif att_status == AttemptStatus.FILLED.value:
                    return self._dict_to_attempt(prev_dict)

        attempt_number = len(prev_attempts) + 1
        parent_attempt_id = prev_attempts[-1].get("attempt_id") if prev_attempts else None

        if client_order_id:
            cid = f"{client_order_id}_{attempt_number}" if attempt_number > 1 else client_order_id
        else:
            cid = f"bb_{intent.symbol.lower()}_{uuid.uuid4().hex[:10]}_{attempt_number}"
        cid = sanitize_client_order_id(cid)

        acct_id = getattr(self.store, "account_id", None) or intent.account_id or "default"
        attempt = OrderAttempt(
            attempt_id=str(uuid.uuid4()),
            intent_id=intent.intent_id,
            account_id=acct_id,
            symbol=intent.symbol,
            client_order_id=cid,
            attempt_number=attempt_number,
            status=AttemptStatus.SUBMITTING.value,
            submitted_qty=intent.target_qty,
            parent_attempt_id=parent_attempt_id,
            created_at_utc=self.now_iso_fn(),
            updated_at_utc=self.now_iso_fn(),
        )

        # Persist attempt state before network call
        with self.store.unit_of_work():
            self.store.save_order_attempt(
                attempt_id=attempt.attempt_id,
                intent_id=attempt.intent_id,
                symbol=attempt.symbol,
                client_order_id=attempt.client_order_id,
                attempt_number=attempt.attempt_number,
                status=attempt.status,
                submitted_qty=attempt.submitted_qty,
                parent_attempt_id=attempt.parent_attempt_id,
            )

        # 2. Network execution OUTSIDE database transaction
        order_resp: Optional[Any] = None
        submission_error: Optional[Exception] = None
        confirmed_flat = False

        if intent.intent_scope in {"EXIT", "PROTECTION"}:
            reduce_only = True

        try:
            order_params: Dict[str, Any] = {
                "symbol": intent.symbol,
                "side": intent.side,
                "type": intent.order_type,
                "newClientOrderId": attempt.client_order_id,
                "newOrderRespType": "RESULT",
            }
            if quantity is not None and not close_position:
                order_params["quantity"] = str(quantity)
            elif intent.target_qty is not None and not close_position:
                order_params["quantity"] = self.client.format_order_qty(intent.symbol, intent.target_qty)

            # Price parameter is only valid for limit order types.
            # In Binance Futures: MARKET, STOP_MARKET, TAKE_PROFIT_MARKET forbid 'price'.
            market_types = {"MARKET", "STOP_MARKET", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"}
            if intent.order_type not in market_types:
                if price is not None:
                    order_params["price"] = str(price)
                elif intent.target_price is not None:
                    order_params["price"] = str(intent.target_price)

            stop_types = {"STOP", "STOP_MARKET", "STOP_LOSS", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"}
            if stop_price is not None:
                order_params["stopPrice"] = str(stop_price)
            elif intent.target_price is not None and intent.order_type in stop_types:
                formatted = None
                if hasattr(self.client, "format_trigger_price"):
                    try:
                        res = self.client.format_trigger_price(intent.symbol, intent.target_price)
                        if isinstance(res, (str, int, float)):
                            formatted = str(res)
                    except Exception:
                        pass
                order_params["stopPrice"] = formatted if formatted is not None else str(intent.target_price)

            if working_type is not None:
                order_params["workingType"] = working_type
            if price_protect is not None:
                order_params["priceProtect"] = price_protect
            if reduce_only and not close_position:
                order_params["reduceOnly"] = True
            if close_position:
                order_params["closePosition"] = True
            if position_side in {"LONG", "SHORT"}:
                order_params["positionSide"] = position_side
            if intent.order_type == "LIMIT":
                order_params["timeInForce"] = time_in_force

            order_resp = self.client.create_order(**order_params)

        except OrderStateUnknownError as exc:
            LOGGER.warning(
                "Order state unknown account=%s symbol=%s cid=%s: %s",
                self.store.account_id,
                intent.symbol,
                attempt.client_order_id,
                exc,
            )
            submission_error = exc
            attempt.status = AttemptStatus.UNKNOWN.value
            attempt.error_message = str(exc)

        except BinanceAPIError as exc:
            err_code = getattr(exc, "code", None)
            err_msg = str(getattr(exc, "message", "") or exc)
            submission_error = exc
            attempt.status = AttemptStatus.REJECTED.value
            attempt.error_message = str(exc)
            if (str(err_code) == "-2022" or "ReduceOnly Order is rejected" in err_msg) and intent.intent_scope in {"EXIT", "PROTECTION"}:
                # Rejection can mean conflicting exit orders, not a flat position.
                # REST verification is outside the persistence transaction.
                confirmed_flat = self._confirm_exchange_flat(intent, position_side)
            if confirmed_flat:
                LOGGER.info(
                    "Reduce-only order rejected account=%s symbol=%s cid=%s; REST confirmed position flat",
                    self.store.account_id,
                    intent.symbol,
                    attempt.client_order_id,
                )
                submission_error = None
            else:
                LOGGER.warning(
                    "Order rejected account=%s symbol=%s cid=%s code=%s: %s",
                    self.store.account_id,
                    intent.symbol,
                    attempt.client_order_id,
                    err_code,
                    exc,
                )

        except Exception as exc:
            LOGGER.error(
                "Unexpected order error account=%s symbol=%s cid=%s: %s",
                self.store.account_id,
                intent.symbol,
                attempt.client_order_id,
                exc,
            )
            submission_error = exc
            attempt.status = AttemptStatus.UNKNOWN.value
            attempt.error_message = str(exc)

        # 3. Post-execution handling
        if order_resp is not None:
            attempt.exchange_response = order_resp
            if isinstance(order_resp, dict):
                exchange_status = str(order_resp.get("status") or "").upper()
                order_id = str(order_resp.get("orderId") or "")
                executed_qty = float(order_resp.get("executedQty") or 0.0)
                avg_price = float(order_resp.get("avgPrice") or 0.0) if order_resp.get("avgPrice") else None
                cum_quote = float(order_resp.get("cumQuote") or 0.0) if order_resp.get("cumQuote") else None
            else:
                order_id = str(getattr(order_resp, "orderId", None) or "")
                exchange_status = str(getattr(order_resp, "status", None) or "FILLED").upper()
                executed_qty = float(getattr(order_resp, "executedQty", 0.0) or 0.0)
                avg_price = None
                cum_quote = None

            attempt.exchange_order_id = order_id
            attempt.executed_qty = executed_qty
            attempt.avg_price = avg_price
            attempt.cumulative_quote_qty = cum_quote

            if exchange_status == "FILLED":
                attempt.status = AttemptStatus.FILLED.value
            elif exchange_status in {"PARTIALLY_FILLED"}:
                attempt.status = AttemptStatus.PARTIALLY_FILLED.value
            elif exchange_status in {"CANCELED", "EXPIRED"}:
                attempt.status = AttemptStatus.CANCELED.value
            else:
                attempt.status = AttemptStatus.ACKNOWLEDGED.value

            # Reconcile fills (network call happens outside DB tx inside reconciler)
            if self.reconciler and intent.order_type == "MARKET":
                self.reconciler.record_market_order(
                    symbol=intent.symbol,
                    position_id=intent.position_id,
                    order=order_resp,
                )

        # 4. Short DB transaction: update attempt, intent status, fills, and episode
        with self.store.unit_of_work():
            self.store.update_order_attempt_status(
                attempt_id=attempt.attempt_id,
                status=attempt.status,
                executed_qty=attempt.executed_qty,
                avg_price=attempt.avg_price,
                cumulative_quote_qty=attempt.cumulative_quote_qty,
                exchange_order_id=attempt.exchange_order_id,
                error_message=attempt.error_message,
            )

            intent_final_status = IntentStatus.PENDING.value
            if attempt.status == AttemptStatus.FILLED.value or confirmed_flat:
                intent_final_status = IntentStatus.COMPLETED.value
            elif attempt.status == AttemptStatus.REJECTED.value:
                intent_final_status = IntentStatus.FAILED.value
            elif attempt.status in {AttemptStatus.ACKNOWLEDGED.value, AttemptStatus.PARTIALLY_FILLED.value}:
                intent_final_status = IntentStatus.SUBMITTED.value

            self.store.update_order_intent_status(
                intent_id=intent.intent_id,
                status=intent_final_status,
                reason=attempt.error_message,
            )

            # Record execution fills in normalized ledger
            if attempt.executed_qty > 0:
                self._record_execution_fills(attempt=attempt, intent=intent, order_resp=order_resp)

            # Synchronize PositionEpisode
            if attempt.executed_qty > 0 or confirmed_flat:
                self._sync_position_episode(intent=intent, attempt=attempt, confirmed_flat=confirmed_flat)

        if submission_error is not None and attempt.status == AttemptStatus.UNKNOWN.value:
            # Attempt active recovery
            attempt = self.recover_unknown_attempt(attempt)

        if submission_error is not None and raise_on_error and attempt.status != AttemptStatus.FILLED.value:
            raise submission_error

        return attempt

    def _confirm_exchange_flat(self, intent: OrderIntent, position_side: Optional[str]) -> bool:
        """Require an explicit zero row for the target side; missing data is uncertain."""
        target_side = position_side or ("SHORT" if intent.side == "BUY" else "LONG")
        try:
            rows = self.client.get_position_risk()
            if not isinstance(rows, list):
                return False
            amounts = []
            for row in rows:
                if not isinstance(row, dict) or row.get("symbol") != intent.symbol:
                    continue
                if str(row.get("positionSide") or "BOTH").upper() not in {"BOTH", target_side}:
                    continue
                amount = float(row["positionAmt"])
                if not math.isfinite(amount):
                    return False
                amounts.append(amount)
            return bool(amounts) and all(amount == 0.0 for amount in amounts)
        except Exception as exc:
            LOGGER.warning("Flat-position verification failed account=%s symbol=%s: %s", self.account_id, intent.symbol, exc)
            return False

    def _record_execution_fills(
        self,
        attempt: OrderAttempt,
        intent: OrderIntent,
        order_resp: Optional[Any] = None,
    ) -> None:
        """Record normalized atomic fills in execution_fills ledger."""
        if attempt.executed_qty <= 0:
            return

        fills_data: List[Dict[str, Any]] = []
        if isinstance(order_resp, dict) and isinstance(order_resp.get("fills"), list) and order_resp["fills"]:
            fills_data = order_resp["fills"]

        if fills_data:
            for idx, fill in enumerate(fills_data):
                trade_id = str(fill.get("id") or fill.get("tradeId") or f"{attempt.exchange_order_id}_{idx+1}")
                self.store.save_execution_fill(
                    fill_id=str(uuid.uuid4()),
                    attempt_id=attempt.attempt_id,
                    intent_id=intent.intent_id,
                    symbol=intent.symbol,
                    exchange_trade_id=trade_id,
                    side=intent.side,
                    price=float(fill.get("price") or attempt.avg_price or intent.target_price or 0.0),
                    qty=float(fill.get("qty") or fill.get("quantity") or 0.0),
                    commission=float(fill.get("commission") or 0.0),
                    commission_asset=str(fill.get("commissionAsset") or "USDT"),
                    trade_time_utc=self.now_iso_fn(),
                    exchange_order_id=attempt.exchange_order_id,
                )
        else:
            synth_trade_id = f"tr_{attempt.exchange_order_id or attempt.client_order_id}"
            fill_price = attempt.avg_price or intent.target_price or 0.0
            self.store.save_execution_fill(
                fill_id=str(uuid.uuid4()),
                attempt_id=attempt.attempt_id,
                intent_id=intent.intent_id,
                symbol=intent.symbol,
                exchange_trade_id=synth_trade_id,
                side=intent.side,
                price=fill_price,
                qty=attempt.executed_qty,
                commission=0.0,
                commission_asset="USDT",
                trade_time_utc=self.now_iso_fn(),
                exchange_order_id=attempt.exchange_order_id,
            )

    def _sync_position_episode(
        self, intent: OrderIntent, attempt: OrderAttempt,
        executed_delta: Optional[float] = None, confirmed_flat: bool = False,
    ) -> None:
        """Update or create PositionEpisode when an attempt executes quantity."""
        delta = attempt.executed_qty if executed_delta is None else executed_delta
        pos = self.store.get_position(int(intent.position_id)) if intent.position_id else None
        episode_id = intent.episode_id
        if not episode_id and pos:
            episode_id = pos.get("episode_id")
        if not episode_id and not pos:
            ep = self.store.get_active_position_episode(intent.symbol)
            if ep:
                episode_id = ep.get("episode_id")

        if not episode_id and intent.intent_scope == "ENTRY":
            episode_id = str(uuid.uuid4())
            intent.episode_id = episode_id
            self.store.update_order_intent_episode_id(intent.intent_id, episode_id)

        if episode_id:
            episode = self.store.get_position_episode(episode_id)
            if intent.intent_scope == "ENTRY":
                if isinstance(episode, dict):
                    prev_qty = float(episode.get("current_qty") or 0.0)
                    new_qty = prev_qty + delta
                    target_qty = (episode.get("target_qty") or 0.0) + delta
                    self.store.save_position_episode(
                        episode_id=episode_id,
                        symbol=intent.symbol,
                        position_side=episode.get("position_side", "SHORT"),
                        status=EpisodeStatus.OPEN.value,
                        target_qty=target_qty,
                        current_qty=new_qty,
                    )
                else:
                    self.store.save_position_episode(
                        episode_id=episode_id,
                        symbol=intent.symbol,
                        position_side="SHORT" if intent.side == "SELL" else "LONG",
                        status=EpisodeStatus.OPEN.value,
                        target_qty=intent.target_qty or attempt.executed_qty,
                        current_qty=delta,
                    )
            else:
                # EXIT, REBALANCE, PROTECTION
                if isinstance(episode, dict):
                    prev_qty = float(episode.get("current_qty") or 0.0)
                    if confirmed_flat:
                        new_qty = 0.0
                    else:
                        new_qty = max(0.0, prev_qty - delta)
                    ep_status = (
                        EpisodeStatus.CLOSED.value if new_qty <= 0 else
                        EpisodeStatus.OPEN.value if attempt.status == AttemptStatus.FILLED.value else
                        EpisodeStatus.CLOSING.value
                    )
                    self.store.save_position_episode(
                        episode_id=episode_id,
                        symbol=intent.symbol,
                        position_side=episode.get("position_side", "SHORT"),
                        status=ep_status,
                        current_qty=new_qty,
                        realized_pnl=float(episode.get("realized_pnl") or 0.0),
                        closed_at_utc=self.now_iso_fn() if ep_status == EpisodeStatus.CLOSED.value else None,
                    )

        if intent.intent_scope in {"EXIT", "PROTECTION"} and pos:
            if str(pos.get("status") or "").startswith("CLOSED"):
                return
            remaining_qty = 0.0 if confirmed_flat else max(0.0, float(pos["qty"]) - delta)
            if remaining_qty > 0:
                if delta > 0:
                    self.store.set_position_qty(
                        position_id=int(intent.position_id), qty=remaining_qty,
                        entry_price=float(pos["entry_price"]),
                    )
                return
            if not confirmed_flat and delta <= 0:
                return
            close_status = "CLOSED_MARKET"
            close_reason = intent.reason or "EXIT"
            if confirmed_flat:
                close_status = "CLOSED_EXTERNAL"
                close_reason = "EXCHANGE_ALREADY_FLAT"
            elif intent.reason == "HOLD_EXPIRY":
                close_status = "CLOSED_HOLD_EXPIRY"
            elif intent.reason == "PORTFOLIO_LOSS_CUT":
                close_status = "CLOSED_LOSS_CUT"
            elif "TP" in (intent.reason or ""):
                close_status = "CLOSED_TP"
            elif "PROTECTION" in (intent.reason or ""):
                close_status = "CLOSED_NOON_PROTECTION" if "NOON" in (intent.reason or "") else "CLOSED_MORNING_PROTECTION"
            parsed_order_id = int(attempt.exchange_order_id) if attempt.exchange_order_id and str(attempt.exchange_order_id).isdigit() else None
            self.store.mark_position_closed(
                position_id=int(intent.position_id),
                status=close_status,
                close_reason=close_reason,
                close_order_id=parsed_order_id,
            )

    def recover_unknown_attempt(self, attempt: Any) -> OrderAttempt:
        """Query Binance by client_order_id to resolve an UNKNOWN order attempt."""
        if isinstance(attempt, str):
            att_row = self.store.get_order_attempt(attempt)
            if not att_row:
                raise ValueError(f"Attempt not found: {attempt}")
            attempt = self._dict_to_attempt(att_row)

        LOGGER.info(
            "Attempting unknown order recovery account=%s symbol=%s cid=%s",
            self.store.account_id,
            attempt.symbol,
            attempt.client_order_id,
        )
        try:
            order_info = self.client.get_order(
                symbol=attempt.symbol,
                orig_client_order_id=attempt.client_order_id,
            )
            if not isinstance(order_info, dict):
                return attempt

            status_str = str(order_info.get("status") or "").upper()
            executed_qty = float(order_info.get("executedQty") or 0.0)
            avg_price = float(order_info.get("avgPrice") or 0.0) if order_info.get("avgPrice") else None
            order_id = str(order_info.get("orderId") or "")

            attempt.exchange_order_id = order_id
            attempt.executed_qty = executed_qty
            attempt.avg_price = avg_price

            if status_str == "FILLED":
                attempt.status = AttemptStatus.FILLED.value
            elif status_str == "PARTIALLY_FILLED":
                attempt.status = AttemptStatus.PARTIALLY_FILLED.value
            elif status_str in {"CANCELED", "EXPIRED"}:
                attempt.status = AttemptStatus.CANCELED.value
            elif status_str in {"NEW"}:
                attempt.status = AttemptStatus.ACKNOWLEDGED.value
            else:
                attempt.status = AttemptStatus.UNKNOWN.value

            with self.store.unit_of_work():
                persisted_attempt = self.store.get_order_attempt(attempt.attempt_id)
                previous_qty = float(persisted_attempt.get("executed_qty") or 0.0) if persisted_attempt else 0.0
                # Exchange responses contain cumulative fills, not increments.
                executed_delta = max(0.0, attempt.executed_qty - previous_qty)
                self.store.update_order_attempt_status(
                    attempt_id=attempt.attempt_id,
                    status=attempt.status,
                    executed_qty=attempt.executed_qty,
                    avg_price=attempt.avg_price,
                    exchange_order_id=attempt.exchange_order_id,
                )
                if attempt.status == AttemptStatus.FILLED.value:
                    self.store.update_order_intent_status(
                        intent_id=attempt.intent_id,
                        status=IntentStatus.COMPLETED.value,
                    )
                # Record execution fills and sync episode upon recovery
                if attempt.executed_qty > 0:
                    intent_row = self.store.get_order_intent(attempt.intent_id)
                    if intent_row:
                        intent_obj = OrderIntent(
                            intent_id=intent_row["intent_id"],
                            account_id=intent_row.get("account_id", "default"),
                            client_intent_key=intent_row["client_intent_key"],
                            symbol=intent_row["symbol"],
                            side=intent_row["side"],
                            order_type=intent_row["order_type"],
                            intent_scope=intent_row.get("intent_scope", "EXIT"),
                            position_id=intent_row.get("position_id"),
                            reason=intent_row.get("reason"),
                            episode_id=intent_row.get("episode_id"),
                            target_qty=intent_row.get("target_qty"),
                            target_price=intent_row.get("target_price"),
                        )
                        self._record_execution_fills(attempt=attempt, intent=intent_obj, order_resp=order_info)
                        self._sync_position_episode(intent=intent_obj, attempt=attempt, executed_delta=executed_delta)

            LOGGER.info(
                "Successfully recovered unknown order account=%s symbol=%s cid=%s status=%s",
                self.store.account_id,
                attempt.symbol,
                attempt.client_order_id,
                attempt.status,
            )
        except BinanceAPIError as exc:
            try:
                code = int(getattr(exc, "code", 0) or 0)
            except (TypeError, ValueError):
                code = 0
            if code == -2013:  # Order does not exist
                LOGGER.info(
                    "Order definitely does not exist on exchange account=%s symbol=%s cid=%s",
                    self.store.account_id,
                    attempt.symbol,
                    attempt.client_order_id,
                )
                attempt.status = AttemptStatus.REJECTED.value
                attempt.error_message = "Order does not exist on exchange (-2013)"
                with self.store.unit_of_work():
                    self.store.update_order_attempt_status(
                        attempt_id=attempt.attempt_id,
                        status=attempt.status,
                        error_message=attempt.error_message,
                    )
                    self.store.update_order_intent_status(
                        intent_id=attempt.intent_id,
                        status=IntentStatus.FAILED.value,
                        reason=attempt.error_message,
                    )
            else:
                LOGGER.warning(
                    "Error querying order for recovery account=%s symbol=%s cid=%s: %s",
                    self.store.account_id,
                    attempt.symbol,
                    attempt.client_order_id,
                    exc,
                )
        except Exception as exc:
            LOGGER.warning(
                "Failed to recover unknown order account=%s symbol=%s cid=%s: %s",
                self.store.account_id,
                attempt.symbol,
                attempt.client_order_id,
                exc,
            )

        return attempt

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[object] = None,
        client_order_id: Optional[object] = None,
        reason: str = "LOCAL_CANCEL",
    ) -> Dict[str, Any]:
        """Cancel an order via execution engine with full audit trail as OrderIntent."""
        parsed_order_id = int(order_id) if order_id else None
        parsed_client_order_id = str(client_order_id) if client_order_id else None

        target_id_str = str(parsed_order_id or parsed_client_order_id or uuid.uuid4().hex[:8])
        intent = OrderIntent(
            intent_id=str(uuid.uuid4()),
            account_id=getattr(self.store, "account_id", "default"),
            client_intent_key=f"cancel_{symbol}_{target_id_str}",
            symbol=symbol,
            side="CANCEL",
            order_type="CANCEL",
            intent_scope="CANCEL",
            status=IntentStatus.PENDING.value,
            reason=reason,
        )
        with self.store.unit_of_work():
            existing = self.store.get_order_intent_by_key(intent.client_intent_key)
            if isinstance(existing, dict):
                intent.intent_id = existing.get("intent_id") or intent.intent_id
                intent.status = existing.get("status") or intent.status
            else:
                self.store.save_order_intent(
                    intent_id=intent.intent_id,
                    client_intent_key=intent.client_intent_key,
                    symbol=intent.symbol,
                    side=intent.side,
                    order_type=intent.order_type,
                    target_qty=None,
                    target_price=None,
                    intent_scope=intent.intent_scope,
                    status=intent.status,
                    reason=intent.reason,
                )

        canceled_resp: Dict[str, Any] = {}
        error: Optional[Exception] = None
        try:
            res = self.client.cancel_order(
                symbol=symbol,
                order_id=parsed_order_id,
                orig_client_order_id=parsed_client_order_id,
            )
            if isinstance(res, dict):
                canceled_resp = res
        except BinanceAPIError as exc:
            error = exc
            try:
                code = int(getattr(exc, "code", 0) or 0)
            except (TypeError, ValueError):
                code = 0
            if code in {-2011, -2013}:
                # Order does not exist or already canceled/filled
                canceled_resp = {
                    "symbol": symbol,
                    "orderId": parsed_order_id,
                    "clientOrderId": parsed_client_order_id,
                    "status": "CANCELED",
                    "note": str(exc),
                }
            else:
                raise
        except Exception as exc:
            error = exc
            raise
        finally:
            with self.store.unit_of_work():
                final_intent_status = (
                    IntentStatus.COMPLETED.value
                    if not error or canceled_resp
                    else IntentStatus.FAILED.value
                )
                self.store.update_order_intent_status(
                    intent_id=intent.intent_id,
                    status=final_intent_status,
                    reason=str(error) if error else None,
                )
                if parsed_client_order_id:
                    att = self.store.get_order_attempt_by_client_id(parsed_client_order_id)
                    if att:
                        self.store.update_order_attempt_status(
                            attempt_id=att["attempt_id"],
                            status=AttemptStatus.CANCELED.value,
                        )
                if canceled_resp:
                    self.store.upsert_exchange_order_state(canceled_resp, source=reason)

        return canceled_resp
