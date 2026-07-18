import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from core.state_store import StateStore
from infra.binance_futures_client import BinanceFuturesClient


LOGGER = logging.getLogger(__name__)


class MarketFillReconciler:
    LOCK_NAME = "pending_market_fill_reconciliation_v1"
    RETRY_DELAYS_SEC = (30, 60, 300, 900, 3600, 21600)

    def __init__(self, client: BinanceFuturesClient, store: StateStore):
        self.client = client
        self.store = store

    def record_market_order(
        self,
        symbol: str,
        position_id: Optional[int],
        order: Dict[str, Any],
    ) -> bool:
        """Persist a market order and enrich its fill when userTrades is ready."""
        normalized_symbol = str(symbol or "").strip().upper()
        order_id = self._safe_optional_int(order.get("actualOrderId")) or self._safe_optional_int(order.get("orderId"))
        client_order_id = str(order.get("clientOrderId") or order.get("clientAlgoId") or "").strip() or None
        order_event_id: Optional[int] = None
        try:
            found_order_event_id = self.store.find_order_event_id(
                symbol=normalized_symbol,
                position_id=position_id,
                order_id=order_id,
                client_order_id=client_order_id,
            )
            order_event_id = found_order_event_id if isinstance(found_order_event_id, int) else None
            if order_event_id is None:
                order_event_id = self.store.add_order_event(
                    symbol=normalized_symbol,
                    position_id=position_id,
                    event_time_utc=self._event_time_utc(order),
                    order_payload=order,
                )
            else:
                self.store.update_order_event(
                    order_event_id=order_event_id,
                    symbol=normalized_symbol,
                    position_id=position_id,
                    event_time_utc=self._event_time_utc(order),
                    order_payload=order,
                )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Initial market order persistence failed account=%s symbol=%s order_id=%s: %s",
                self.store.account_id,
                normalized_symbol,
                order_id,
                exc,
            )
        payload = self._build_fill_payload(normalized_symbol, order)
        if payload is not None:
            try:
                if order_event_id is None:
                    order_event_id = self.store.add_order_event(
                        symbol=normalized_symbol,
                        position_id=position_id,
                        event_time_utc=self._event_time_utc(payload),
                        order_payload=payload,
                    )
                else:
                    self.store.update_order_event(
                        order_event_id=order_event_id,
                        symbol=normalized_symbol,
                        position_id=position_id,
                        event_time_utc=self._event_time_utc(payload),
                        order_payload=payload,
                    )
                return True
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "Market fill enrichment persistence failed account=%s symbol=%s order_id=%s: %s",
                    self.store.account_id,
                    normalized_symbol,
                    order.get("orderId"),
                    exc,
                )

        try:
            self._queue_pending(
                symbol=normalized_symbol,
                position_id=position_id,
                order=order,
                order_event_id=order_event_id,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Market fill retry persistence failed account=%s symbol=%s order_id=%s: %s",
                self.store.account_id,
                normalized_symbol,
                order.get("orderId"),
                exc,
            )
        return False

    def reconcile_pending(
        self,
        now_utc: Optional[datetime] = None,
        max_items: int = 10,
    ) -> Dict[str, int]:
        now = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
        state = self._load_state()
        raw_items = state.get("items")
        items = dict(raw_items) if isinstance(raw_items, dict) else {}
        summary = {"pending": len(items), "reconciled": 0, "deferred": 0, "failed": 0}
        processed = 0

        for key, raw_item in list(items.items()):
            if processed >= max(1, int(max_items)):
                summary["deferred"] += 1
                continue
            if not isinstance(raw_item, dict):
                items.pop(key, None)
                continue

            next_retry = self._parse_iso_utc(raw_item.get("next_retry_at_utc"))
            if next_retry is not None and next_retry > now:
                summary["deferred"] += 1
                continue

            processed += 1
            symbol = str(raw_item.get("symbol") or "").strip().upper()
            order = raw_item.get("order")
            if not symbol or not isinstance(order, dict):
                items.pop(key, None)
                summary["failed"] += 1
                continue

            payload = self._build_fill_payload(symbol, order)
            if payload is not None:
                position_id = self._safe_optional_int(raw_item.get("position_id"))
                order_event_id = self._safe_optional_int(raw_item.get("order_event_id"))
                if order_event_id is not None:
                    self.store.update_order_event(
                        order_event_id=order_event_id,
                        symbol=symbol,
                        position_id=position_id,
                        event_time_utc=self._event_time_utc(payload),
                        order_payload=payload,
                    )
                else:
                    self.store.add_order_event(
                        symbol=symbol,
                        position_id=position_id,
                        event_time_utc=self._event_time_utc(payload),
                        order_payload=payload,
                    )
                items.pop(key, None)
                summary["reconciled"] += 1
                continue

            attempts = max(0, self._safe_optional_int(raw_item.get("attempts")) or 0) + 1
            raw_item["attempts"] = attempts
            raw_item["last_attempt_at_utc"] = now.isoformat()
            raw_item["next_retry_at_utc"] = (
                now + timedelta(seconds=self._retry_delay_sec(attempts))
            ).isoformat()
            items[key] = raw_item
            summary["failed"] += 1

        self._save_state(items)
        summary["pending"] = len(items)
        return summary

    def reconcile_persisted_missing(self, max_items: int = 10) -> Dict[str, int]:
        item_limit = max(1, int(max_items))
        rows = self.store.list_market_order_events_missing_realized_fill(limit=item_limit * 20)
        state = self._load_state()
        raw_pending = state.get("items")
        pending = raw_pending if isinstance(raw_pending, dict) else {}
        summary = {"found": len(rows), "reconciled": 0, "queued": 0, "deferred": 0}

        processed = 0
        for row in rows:
            try:
                raw_json = json.loads(str(row.get("raw_json") or "{}"))
            except (TypeError, ValueError):
                raw_json = {}
            order = raw_json if isinstance(raw_json, dict) else {}
            order.setdefault("orderId", row.get("order_id"))
            order.setdefault("clientOrderId", row.get("client_order_id"))
            order.setdefault("type", row.get("type") or "MARKET")
            order.setdefault("side", row.get("side") or "BUY")
            order.setdefault("status", row.get("status") or "FILLED")
            position_id = self._safe_optional_int(row.get("position_id"))
            key = self._pending_key(position_id=position_id, order=order)
            if key in pending:
                summary["deferred"] += 1
                continue
            if processed >= item_limit:
                summary["deferred"] += 1
                continue
            processed += 1

            symbol = str(row.get("symbol") or "").strip().upper()
            payload = self._build_fill_payload(symbol, order)
            if payload is not None:
                self.store.update_order_event(
                    order_event_id=int(row["order_event_id"]),
                    symbol=symbol,
                    position_id=position_id,
                    event_time_utc=self._event_time_utc(payload),
                    order_payload=payload,
                )
                summary["reconciled"] += 1
                continue

            self._queue_pending(
                symbol=symbol,
                position_id=position_id,
                order=order,
                order_event_id=int(row["order_event_id"]),
            )
            summary["queued"] += 1
        return summary

    def _build_fill_payload(
        self,
        symbol: str,
        order: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        order_id = self._safe_optional_int(order.get("actualOrderId")) or self._safe_optional_int(
            order.get("orderId")
        )
        if order_id is None:
            return None
        try:
            trades = self.client.get_user_trades(symbol=symbol, order_id=order_id, limit=1000)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Market fill lookup failed account=%s symbol=%s order_id=%s: %s",
                self.store.account_id,
                symbol,
                order_id,
                exc,
            )
            return None

        close_side = str(order.get("side") or "").strip().upper()
        matched_trades = []
        for trade in trades or []:
            if not isinstance(trade, dict):
                continue
            trade_order_id = self._safe_optional_int(trade.get("orderId"))
            if trade_order_id is not None and trade_order_id != order_id:
                continue
            trade_side = str(trade.get("side") or "").strip().upper()
            if close_side and trade_side != close_side:
                continue
            if self._safe_float(trade.get("qty")) <= 0:
                continue
            matched_trades.append(trade)
        if not matched_trades:
            return None

        executed_qty = sum(self._safe_float(trade.get("qty")) for trade in matched_trades)
        expected_qty = self._safe_float(order.get("executedQty"))
        if expected_qty > 0:
            tolerance = max(1e-12, expected_qty * 1e-9)
            if executed_qty + tolerance < expected_qty:
                return None
        quote_qty = sum(self._safe_float(trade.get("quoteQty")) for trade in matched_trades)
        if executed_qty <= 0:
            return None
        if quote_qty <= 0:
            quote_qty = sum(
                self._safe_float(trade.get("qty")) * self._safe_float(trade.get("price"))
                for trade in matched_trades
            )
        avg_price = quote_qty / executed_qty if quote_qty > 0 else 0.0
        if avg_price <= 0:
            return None

        event_time_ms = max(self._safe_optional_int(trade.get("time")) or 0 for trade in matched_trades)
        payload = dict(order)
        payload.update(
            {
                "orderId": order_id,
                "clientOrderId": order.get("clientOrderId") or order.get("clientAlgoId"),
                "type": order.get("type") or order.get("orderType") or "MARKET",
                "side": close_side or str(matched_trades[-1].get("side") or "").strip().upper(),
                "price": str(avg_price),
                "origQty": str(executed_qty),
                "executedQty": str(executed_qty),
                "cumQuote": str(quote_qty),
                "avgPrice": str(avg_price),
                "status": "FILLED",
                "realizedPnl": str(sum(self._safe_float(trade.get("realizedPnl")) for trade in matched_trades)),
                "commission": str(sum(self._safe_float(trade.get("commission")) for trade in matched_trades)),
                "commissionAsset": str(matched_trades[-1].get("commissionAsset") or "").strip().upper() or None,
                "time": event_time_ms or order.get("updateTime") or order.get("time"),
                "source": "userTrades",
                "rawOrder": order,
                "trades": matched_trades,
            }
        )
        return payload

    def _queue_pending(
        self,
        symbol: str,
        position_id: Optional[int],
        order: Dict[str, Any],
        order_event_id: Optional[int],
    ) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        state = self._load_state()
        raw_items = state.get("items")
        items = dict(raw_items) if isinstance(raw_items, dict) else {}
        key = self._pending_key(position_id=position_id, order=order)
        existing = items.get(key)
        attempts = 1
        first_seen = now.isoformat()
        if isinstance(existing, dict):
            attempts = max(1, self._safe_optional_int(existing.get("attempts")) or 1)
            first_seen = str(existing.get("first_seen_at_utc") or first_seen)
        items[key] = {
            "symbol": symbol,
            "position_id": position_id,
            "order_event_id": int(order_event_id) if order_event_id is not None else None,
            "order": order,
            "attempts": attempts,
            "first_seen_at_utc": first_seen,
            "last_attempt_at_utc": now.isoformat(),
            "next_retry_at_utc": (now + timedelta(seconds=self._retry_delay_sec(attempts))).isoformat(),
        }
        self._save_state(items)

    def _load_state(self) -> Dict[str, Any]:
        state = self.store.get_lock_state(self.LOCK_NAME) or {}
        return state if isinstance(state, dict) else {}

    def _save_state(self, items: Dict[str, Any]) -> None:
        self.store.set_lock_state(
            self.LOCK_NAME,
            {
                "items": items,
                "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            },
        )

    @classmethod
    def _retry_delay_sec(cls, attempts: int) -> int:
        index = min(max(1, int(attempts)) - 1, len(cls.RETRY_DELAYS_SEC) - 1)
        return cls.RETRY_DELAYS_SEC[index]

    @classmethod
    def _pending_key(cls, position_id: Optional[int], order: Dict[str, Any]) -> str:
        order_id = cls._safe_optional_int(order.get("actualOrderId")) or cls._safe_optional_int(order.get("orderId"))
        client_order_id = str(order.get("clientOrderId") or order.get("clientAlgoId") or "").strip()
        return f"{position_id if position_id is not None else 'EX'}:{order_id or client_order_id or 'UNKNOWN'}"

    @staticmethod
    def _event_time_utc(payload: Dict[str, Any]) -> str:
        event_time_raw = payload.get("eventTime")
        if isinstance(event_time_raw, str):
            try:
                return datetime.fromisoformat(event_time_raw).astimezone(timezone.utc).replace(microsecond=0).isoformat()
            except ValueError:
                pass
        event_time_ms = MarketFillReconciler._safe_optional_int(payload.get("time"))
        if event_time_ms and event_time_ms > 0:
            return datetime.fromtimestamp(event_time_ms / 1000.0, tz=timezone.utc).replace(microsecond=0).isoformat()
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _parse_iso_utc(value: Any) -> Optional[datetime]:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _safe_optional_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return None

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0
