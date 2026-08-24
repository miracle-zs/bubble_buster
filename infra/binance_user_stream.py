"""USD-M User Stream backed local account/order state."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from core.account_snapshot import AccountSnapshotProvider
from core.state_store import StateStore
from infra.binance_futures_client import BinanceAPIError, BinanceFuturesClient


LOGGER = logging.getLogger(__name__)


def _event_time_iso(payload: Dict[str, Any]) -> str:
    raw = payload.get("E") or payload.get("T") or payload.get("updateTime") or payload.get("time")
    try:
        event_ms = int(raw)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return datetime.fromtimestamp(event_ms / 1000.0, tz=timezone.utc).replace(microsecond=0).isoformat()


class BinanceUserStreamState:
    """Own stream certainty, REST verification and the persisted local ledger."""

    LOCK_NAME = "user_stream_state_v1"

    def __init__(
        self,
        *,
        client: BinanceFuturesClient,
        store: StateStore,
        snapshot_provider: AccountSnapshotProvider,
        account_id: str,
        websocket_base_url: Optional[str] = None,
        rest_verify_interval_sec: float = 300.0,
        reconnect_delay_sec: float = 5.0,
    ) -> None:
        self.client = client
        self.store = store
        self.snapshot_provider = snapshot_provider
        self.account_id = str(account_id or "default").strip() or "default"
        self.websocket_base_url = websocket_base_url or self._derive_websocket_base_url(client.base_url)
        self.rest_verify_interval_sec = max(60.0, float(rest_verify_interval_sec))
        self.reconnect_delay_sec = max(1.0, float(reconnect_delay_sec))
        self._lock = threading.RLock()
        self._verify_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._stream_thread: Optional[threading.Thread] = None
        self._verify_thread: Optional[threading.Thread] = None
        self._websocket_app: Any = None
        self._listen_key: Optional[str] = None
        self._certain = False
        self._connected = False
        self._last_verified_monotonic = 0.0
        self._last_event_monotonic = 0.0
        self._last_error: Optional[str] = "STARTUP_UNVERIFIED"
        self._persist_state()

    @staticmethod
    def _derive_websocket_base_url(rest_base_url: str) -> str:
        host = (urlparse(rest_base_url).hostname or "").lower()
        if "testnet" in host or "demo" in host or "binancefuture" in host:
            return "wss://fstream.binancefuture.com/ws"
        # The current production private-stream namespace is separate from
        # public market streams: /private/ws/<listenKey>.
        return "wss://fstream.binance.com/private/ws"

    def start(self) -> None:
        if self._stream_thread is not None and self._stream_thread.is_alive():
            return
        self._stop_event.clear()
        self._stream_thread = threading.Thread(
            target=self._stream_loop,
            name=f"binance-user-stream-{self.account_id}",
            daemon=True,
        )
        self._verify_thread = threading.Thread(
            target=self._verify_loop,
            name=f"binance-user-verify-{self.account_id}",
            daemon=True,
        )
        self._stream_thread.start()
        self._verify_thread.start()

    def stop(self, timeout_sec: float = 10.0) -> None:
        self._stop_event.set()
        app = self._websocket_app
        if app is not None:
            try:
                app.close()
            except Exception:  # noqa: BLE001
                pass
        for thread in (self._stream_thread, self._verify_thread):
            if thread is not None and thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=max(0.0, float(timeout_sec)))
        self._stream_thread = None
        self._verify_thread = None

    def is_certain(self) -> bool:
        with self._lock:
            return bool(self._certain)

    def entry_allowed(self) -> bool:
        """New exposure is allowed only after stream/REST state is reconciled."""
        return self.is_certain()

    def mark_uncertain(self, reason: str) -> None:
        with self._lock:
            self._certain = False
            self._connected = False
            self._last_error = str(reason or "STREAM_UNCERTAIN")[:500]
        self._persist_state()

    def verify_rest(
        self,
        *,
        full_order_scan: bool = True,
        force_account_snapshot: bool = True,
    ) -> bool:
        """Validate account/order state without any per-position risk reads.

        Startup and reconnects request the complete open-order set.  The
        five-minute periodic pass skips risk/order endpoints for a genuinely
        empty local and exchange account.
        """
        if not self._verify_lock.acquire(blocking=False):
            return self.is_certain()
        try:
            with self.client.background_requests():
                snapshot = self.snapshot_provider.capture(force=force_account_snapshot)
                has_exchange_positions = any(
                    abs(float(position.get("positionAmt") or 0.0)) > 1e-12
                    for position in snapshot.positions
                )
                if has_exchange_positions:
                    position_risks = self.client.get_position_risk()
                    self.snapshot_provider.merge_position_risks(position_risks)
                needs_order_scan = (
                    full_order_scan
                    or has_exchange_positions
                    or bool(self.store.list_open_positions())
                    or bool(self.store.list_exchange_order_state(active_only=True))
                )
                if needs_order_scan:
                    orders = self.client.get_open_orders()
                    self.store.reconcile_open_order_state(orders)
                # Only orders absent from the all-open snapshot need an
                # individual read, and only during this startup/reconnect/5m
                # fallback. Normal minute patrols never query per position.
                for position in self.store.list_open_positions():
                    symbol = str(position.get("symbol") or "").strip().upper()
                    for order_id, client_order_id in (
                        (position.get("tp_order_id"), position.get("tp_client_order_id")),
                        (position.get("sl_order_id"), position.get("sl_client_order_id")),
                    ):
                        local_status = self.store.get_exchange_order_status(
                            symbol=symbol,
                            order_id=order_id,
                            client_order_id=client_order_id,
                        )
                        if local_status != "MISSING":
                            continue
                        try:
                            resolved = self.client.get_order(
                                symbol=symbol,
                                order_id=int(order_id) if order_id not in (None, "") else None,
                                orig_client_order_id=(
                                    str(client_order_id) if client_order_id not in (None, "") else None
                                ),
                            )
                        except BinanceAPIError as exc:
                            try:
                                error_code = int(exc.code)
                            except (TypeError, ValueError):
                                error_code = None
                            if error_code in {-2011, -2013}:
                                # Definitive absence is a known local state,
                                # unlike a timeout/429/transport failure.
                                continue
                            raise
                        if isinstance(resolved, dict):
                            self.store.upsert_exchange_order_state(
                                resolved,
                                source="REST_VERIFY_ORDER",
                            )
            with self._lock:
                self._certain = True
                self._last_verified_monotonic = time.monotonic()
                self._last_error = None
            self._persist_state()
            return True
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("User stream REST verification failed account=%s: %s", self.account_id, exc)
            with self._lock:
                self._certain = False
                self._last_error = str(exc)[:500]
            self._persist_state()
            return False
        finally:
            self._verify_lock.release()

    def handle_event(self, payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            return
        event_type = str(payload.get("e") or "").strip()
        event_time = _event_time_iso(payload)
        with self._lock:
            self._last_event_monotonic = time.monotonic()
            # Stream events are deltas, so they cannot establish a complete
            # baseline after startup/reconnect. Only successful REST
            # verification may transition uncertain -> certain.
            if self._certain:
                self._last_error = None

        if event_type == "ORDER_TRADE_UPDATE":
            raw_order = payload.get("o")
            if isinstance(raw_order, dict):
                normalized = self._normalize_order_update(raw_order)
                previous = self.store.get_exchange_order_state(
                    symbol=str(normalized.get("symbol") or ""),
                    order_id=normalized.get("orderId"),
                    client_order_id=normalized.get("clientOrderId"),
                )
                self.store.upsert_exchange_order_state(
                    normalized,
                    source="ORDER_TRADE_UPDATE",
                    event_time_utc=event_time,
                )
                actual_order_id = normalized.get("orderId")
                actual_status = str(normalized.get("status") or "").strip().upper()
                if actual_order_id not in (None, "") and actual_status in {
                    "FILLED",
                    "CANCELED",
                    "EXPIRED",
                    "REJECTED",
                }:
                    self.store.update_parent_algo_order_status(
                        actual_order_id=actual_order_id,
                        status=actual_status,
                        event_time_utc=event_time,
                    )
                if (
                    str(normalized.get("status") or "").upper() == "FILLED"
                    and str((previous or {}).get("status") or "").upper() != "FILLED"
                ):
                    position_id = self._open_position_id(str(normalized.get("symbol") or ""))
                    self.store.add_order_event(
                        symbol=str(normalized.get("symbol") or ""),
                        position_id=position_id,
                        event_time_utc=event_time,
                        order_payload=normalized,
                    )
        elif event_type == "ALGO_UPDATE":
            raw_order = payload.get("o") or payload.get("a")
            if isinstance(raw_order, dict):
                normalized = self._normalize_order_update(raw_order)
                actual_order_id = (
                    raw_order.get("actualOrderId")
                    or raw_order.get("actualOrderID")
                    or raw_order.get("aoid")
                )
                if (
                    str(normalized.get("status") or "").upper() == "FINISHED"
                    and actual_order_id not in (None, "")
                ):
                    actual = self.store.get_exchange_order_state(
                        symbol=str(normalized.get("symbol") or ""),
                        order_id=actual_order_id,
                    )
                    actual_status = str((actual or {}).get("status") or "").upper()
                    if actual_status in {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}:
                        normalized["status"] = actual_status
                self.store.upsert_exchange_order_state(
                    normalized,
                    source="ALGO_UPDATE",
                    event_time_utc=event_time,
                )
        elif event_type == "ACCOUNT_UPDATE":
            account = payload.get("a")
            if isinstance(account, dict):
                balances = account.get("B") if isinstance(account.get("B"), list) else []
                positions = account.get("P") if isinstance(account.get("P"), list) else []
                self.snapshot_provider.apply_stream_update(
                    balances=balances,
                    positions=positions,
                    captured_at_utc=event_time,
                )
        elif event_type == "CONDITIONAL_ORDER_TRIGGER_REJECT":
            raw_order = payload.get("or")
            if isinstance(raw_order, dict):
                rejected = self._normalize_order_update(raw_order)
                rejected["status"] = "REJECTED"
                self.store.upsert_exchange_order_state(
                    rejected,
                    source="CONDITIONAL_ORDER_TRIGGER_REJECT",
                    event_time_utc=event_time,
                )
            self.mark_uncertain("CONDITIONAL_ORDER_TRIGGER_REJECT")
            return
        elif event_type == "listenKeyExpired":
            self.mark_uncertain("LISTEN_KEY_EXPIRED")
            return
        self._persist_state()

    def _open_position_id(self, symbol: str) -> Optional[int]:
        normalized = str(symbol or "").strip().upper()
        for position in self.store.list_open_positions():
            if str(position.get("symbol") or "").strip().upper() == normalized:
                return int(position["id"])
        return None

    def _normalize_order_update(self, order: Dict[str, Any]) -> Dict[str, Any]:
        raw_status = order.get("status") or order.get("algoStatus") or order.get("X")
        if order.get("algoStatus") is not None:
            raw_status = self.client._map_algo_status(
                order.get("algoStatus"),
                actual_order_id=(
                    order.get("actualOrderId")
                    or order.get("actualOrderID")
                    or order.get("aoid")
                ),
                actual_order_status=(
                    order.get("actualOrderStatus")
                    or order.get("actualStatus")
                    or order.get("orderStatus")
                ),
            )
        return {
            **order,
            "symbol": order.get("symbol") or order.get("s"),
            "clientOrderId": (
                order.get("clientOrderId")
                or order.get("clientAlgoId")
                or order.get("caid")
                or order.get("c")
            ),
            "orderId": order.get("orderId") or order.get("algoId") or order.get("aid") or order.get("i"),
            "type": order.get("type") or order.get("orderType") or order.get("ot") or order.get("o"),
            "side": order.get("side") or order.get("S"),
            "positionSide": order.get("positionSide") or order.get("ps"),
            "status": raw_status,
            "executionType": order.get("executionType") or order.get("x"),
            "price": order.get("price") or order.get("p"),
            "stopPrice": order.get("stopPrice") or order.get("triggerPrice") or order.get("sp"),
            "avgPrice": order.get("avgPrice") or order.get("ap"),
            "origQty": order.get("origQty") or order.get("quantity") or order.get("q"),
            "executedQty": order.get("executedQty") or order.get("z"),
            "reduceOnly": order.get("reduceOnly") if "reduceOnly" in order else order.get("R"),
            "closePosition": order.get("closePosition") if "closePosition" in order else order.get("cp"),
            "realizedPnl": order.get("realizedPnl") or order.get("rp"),
            "commission": order.get("commission") or order.get("n"),
            "commissionAsset": order.get("commissionAsset") or order.get("N"),
        }

    def _verify_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                was_certain = self._certain
                last_error = self._last_error
                due = (
                    not self._certain
                    or time.monotonic() - self._last_verified_monotonic >= self.rest_verify_interval_sec
                )
            if due:
                self.verify_rest(
                    full_order_scan=not was_certain,
                    force_account_snapshot=(
                        not was_certain and last_error != "STARTUP_UNVERIFIED"
                    ),
                )
            self._stop_event.wait(5.0)

    def _stream_loop(self) -> None:
        try:
            import websocket  # type: ignore[import-not-found]
        except ImportError:
            self.mark_uncertain("WEBSOCKET_CLIENT_NOT_INSTALLED")
            LOGGER.error("websocket-client is required for account=%s user stream", self.account_id)
            return

        while not self._stop_event.is_set():
            try:
                with self.client.background_requests():
                    listen_key = self.client.start_user_data_stream()
                with self._lock:
                    self._listen_key = listen_key
                url = f"{self.websocket_base_url.rstrip('/')}/{listen_key}"

                def on_open(_app: Any) -> None:
                    with self._lock:
                        self._connected = True
                    self._persist_state()
                    threading.Thread(
                        target=self._keepalive_loop,
                        args=(listen_key,),
                        name=f"binance-user-keepalive-{self.account_id}",
                        daemon=True,
                    ).start()

                def on_message(_app: Any, message: str) -> None:
                    try:
                        decoded = json.loads(message)
                    except (TypeError, ValueError):
                        return
                    if isinstance(decoded, dict):
                        try:
                            self.handle_event(decoded)
                        except Exception as exc:  # noqa: BLE001
                            LOGGER.warning(
                                "User stream event apply failed account=%s error_type=%s",
                                self.account_id,
                                type(exc).__name__,
                            )
                            self.mark_uncertain(
                                f"EVENT_APPLY_FAILED:{type(exc).__name__}"
                            )
                            _app.close()

                def on_error(_app: Any, error: Any) -> None:
                    LOGGER.warning(
                        "User stream error account=%s error_type=%s",
                        self.account_id,
                        type(error).__name__,
                    )

                def on_close(_app: Any, _status: Any, _message: Any) -> None:
                    self.mark_uncertain("WEBSOCKET_DISCONNECTED")

                app = websocket.WebSocketApp(
                    url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                self._websocket_app = app
                app.run_forever(ping_interval=180, ping_timeout=30)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "User stream loop failed account=%s error_type=%s",
                    self.account_id,
                    type(exc).__name__,
                )
                self.mark_uncertain(f"STREAM_LOOP_FAILED:{type(exc).__name__}")
            finally:
                self._websocket_app = None
                with self._lock:
                    self._connected = False
            if not self._stop_event.is_set():
                # Restore a certain local snapshot before allowing new entries;
                # websocket reconnection continues independently.
                self.verify_rest()
                self._stop_event.wait(self.reconnect_delay_sec)

    def _keepalive_loop(self, listen_key: str) -> None:
        while not self._stop_event.wait(30 * 60):
            with self._lock:
                if self._listen_key != listen_key or not self._connected:
                    return
            try:
                with self.client.background_requests():
                    self.client.keepalive_user_data_stream()
            except Exception as exc:  # noqa: BLE001
                self.mark_uncertain(f"KEEPALIVE_FAILED: {exc}")
                app = self._websocket_app
                if app is not None:
                    try:
                        app.close()
                    except Exception:  # noqa: BLE001
                        pass
                return

    def _persist_state(self) -> None:
        with self._lock:
            state = {
                "certain": bool(self._certain),
                "connected": bool(self._connected),
                "last_error": self._last_error,
                "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            }
        try:
            self.store.set_lock_state(self.LOCK_NAME, state)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug("Failed to persist user stream state account=%s: %s", self.account_id, exc)
