"""TradingLedger: atomic transactional ledger for execution protocol and position episodes.

Fulfills Phase D requirements:
- Short DB transactions without network calls.
- Merges order facts, execution fills, and position episode attribution.
- Guaranteed single source of truth for execution state.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from core.execution.models import (
    AttemptStatus,
    EpisodeStatus,
    ExecutionFill,
    IntentScope,
    IntentStatus,
    OrderAttempt,
    OrderIntent,
    PositionEpisode,
    utc_now_iso,
)
from core.state_store import StateStore

LOGGER = logging.getLogger(__name__)


class TradingLedger:
    """Transactional ledger maintaining order intents, attempts, fills, and episodes."""

    def __init__(
        self,
        store: StateStore,
        now_iso_fn: Optional[Callable[[], str]] = None,
    ) -> None:
        self.store = store
        self.now_iso_fn = now_iso_fn or utc_now_iso

    # -------------------------------------------------------------------------
    # Episodes
    # -------------------------------------------------------------------------
    def open_episode(
        self,
        episode_id: str,
        symbol: str,
        position_side: str = "SHORT",
        target_qty: Optional[float] = None,
        current_qty: float = 0.0,
    ) -> PositionEpisode:
        """Create and persist a new PositionEpisode in a short transaction."""
        now_iso = self.now_iso_fn()
        with self.store.unit_of_work():
            self.store.save_position_episode(
                episode_id=episode_id,
                symbol=symbol,
                position_side=position_side,
                status=EpisodeStatus.OPEN.value,
                target_qty=target_qty,
                current_qty=current_qty,
            )
        return PositionEpisode(
            episode_id=episode_id,
            account_id=self.store.account_id,
            symbol=symbol,
            position_side=position_side,
            status=EpisodeStatus.OPEN.value,
            opened_at_utc=now_iso,
            target_qty=target_qty,
            current_qty=current_qty,
            realized_pnl=0.0,
            created_at_utc=now_iso,
            updated_at_utc=now_iso,
        )

    def close_episode(
        self,
        episode_id: str,
        closed_at_utc: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> None:
        """Close an existing PositionEpisode in a short transaction."""
        closed_at = closed_at_utc or self.now_iso_fn()
        with self.store.unit_of_work():
            ep = self.store.get_position_episode(episode_id)
            if ep:
                self.store.save_position_episode(
                    episode_id=episode_id,
                    symbol=ep["symbol"],
                    position_side=ep["position_side"],
                    status=EpisodeStatus.CLOSED.value,
                    target_qty=ep.get("target_qty"),
                    current_qty=0.0,
                    realized_pnl=float(ep.get("realized_pnl") or 0.0),
                    closed_at_utc=closed_at,
                )

    def get_episode(self, episode_id: str) -> Optional[PositionEpisode]:
        """Fetch PositionEpisode by ID."""
        row = self.store.get_position_episode(episode_id)
        if not row:
            return None
        return PositionEpisode(
            episode_id=str(row["episode_id"]),
            account_id=str(row["account_id"]),
            symbol=str(row["symbol"]),
            position_side=str(row.get("position_side") or "SHORT"),
            status=str(row.get("status") or EpisodeStatus.OPEN.value),
            opened_at_utc=str(row["opened_at_utc"]),
            closed_at_utc=row.get("closed_at_utc"),
            target_qty=float(row["target_qty"]) if row.get("target_qty") is not None else None,
            current_qty=float(row.get("current_qty") or 0.0),
            realized_pnl=float(row.get("realized_pnl") or 0.0),
            created_at_utc=str(row.get("created_at_utc") or ""),
            updated_at_utc=str(row.get("updated_at_utc") or ""),
        )

    def list_open_episodes(self, symbol: Optional[str] = None) -> List[PositionEpisode]:
        """List all currently active or closing PositionEpisodes."""
        rows = self.store.list_open_position_episodes(symbol=symbol)
        episodes: List[PositionEpisode] = []
        for row in rows:
            episodes.append(
                PositionEpisode(
                    episode_id=str(row["episode_id"]),
                    account_id=str(row["account_id"]),
                    symbol=str(row["symbol"]),
                    position_side=str(row.get("position_side") or "SHORT"),
                    status=str(row.get("status") or EpisodeStatus.OPEN.value),
                    opened_at_utc=str(row["opened_at_utc"]),
                    closed_at_utc=row.get("closed_at_utc"),
                    target_qty=float(row["target_qty"]) if row.get("target_qty") is not None else None,
                    current_qty=float(row.get("current_qty") or 0.0),
                    realized_pnl=float(row.get("realized_pnl") or 0.0),
                    created_at_utc=str(row.get("created_at_utc") or ""),
                    updated_at_utc=str(row.get("updated_at_utc") or ""),
                )
            )
        return episodes

    # -------------------------------------------------------------------------
    # Intents
    # -------------------------------------------------------------------------
    def record_intent(self, intent: OrderIntent) -> OrderIntent:
        """Idempotently persist or retrieve an OrderIntent in a short transaction."""
        with self.store.unit_of_work():
            existing = self.store.get_order_intent_by_key(intent.client_intent_key)
            if isinstance(existing, dict):
                intent.intent_id = existing.get("intent_id") or intent.intent_id
                intent.status = existing.get("status") or intent.status
                if not intent.position_id:
                    intent.position_id = existing.get("position_id")
                if not intent.episode_id:
                    intent.episode_id = existing.get("episode_id")
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
                    status=intent.status,
                    reason=intent.reason,
                )
        return intent

    def get_intent(self, intent_id: str) -> Optional[OrderIntent]:
        row = self.store.get_order_intent(intent_id)
        if not row:
            return None
        return self._dict_to_intent(row)

    def get_intent_by_key(self, client_intent_key: str) -> Optional[OrderIntent]:
        row = self.store.get_order_intent_by_key(client_intent_key)
        if not row:
            return None
        return self._dict_to_intent(row)

    def list_active_intents(self, symbol: Optional[str] = None) -> List[OrderIntent]:
        rows = self.store.list_active_order_intents(symbol=symbol)
        return [self._dict_to_intent(r) for r in rows]

    def update_intent_status(
        self, intent_id: str, status: str, reason: Optional[str] = None
    ) -> None:
        with self.store.unit_of_work():
            self.store.update_order_intent_status(intent_id, status=status, reason=reason)

    def _dict_to_intent(self, row: Dict[str, Any]) -> OrderIntent:
        return OrderIntent(
            intent_id=str(row["intent_id"]),
            account_id=str(row.get("account_id") or self.store.account_id),
            client_intent_key=str(row["client_intent_key"]),
            symbol=str(row["symbol"]),
            side=str(row["side"]),
            order_type=str(row["order_type"]),
            target_qty=float(row["target_qty"]) if row.get("target_qty") is not None else None,
            target_price=float(row["target_price"]) if row.get("target_price") is not None else None,
            intent_scope=str(row.get("intent_scope") or "EXIT"),
            position_id=int(row["position_id"]) if row.get("position_id") is not None else None,
            episode_id=str(row["episode_id"]) if row.get("episode_id") is not None else None,
            status=str(row.get("status") or IntentStatus.PENDING.value),
            reason=row.get("reason"),
            created_at_utc=str(row.get("created_at_utc") or ""),
            updated_at_utc=str(row.get("updated_at_utc") or ""),
        )

    # -------------------------------------------------------------------------
    # Attempts
    # -------------------------------------------------------------------------
    def record_attempt(self, attempt: OrderAttempt) -> OrderAttempt:
        """Persist or update an OrderAttempt in a short transaction."""
        with self.store.unit_of_work():
            self.store.save_order_attempt(
                attempt_id=attempt.attempt_id,
                intent_id=attempt.intent_id,
                symbol=attempt.symbol,
                client_order_id=attempt.client_order_id,
                exchange_order_id=attempt.exchange_order_id,
                attempt_number=attempt.attempt_number,
                status=attempt.status,
                submitted_qty=attempt.submitted_qty,
                executed_qty=attempt.executed_qty,
                cumulative_quote_qty=attempt.cumulative_quote_qty,
                avg_price=attempt.avg_price,
                error_message=attempt.error_message,
                parent_attempt_id=attempt.parent_attempt_id,
            )
        return attempt

    def get_attempt(self, attempt_id: str) -> Optional[OrderAttempt]:
        row = self.store.get_order_attempt(attempt_id)
        if not row:
            return None
        return self._dict_to_attempt(row)

    def get_latest_attempt_for_intent(self, intent_id: str) -> Optional[OrderAttempt]:
        row = self.store.get_latest_order_attempt_for_intent(intent_id)
        if not row:
            return None
        return self._dict_to_attempt(row)

    def update_attempt_status(
        self,
        attempt_id: str,
        status: str,
        exchange_order_id: Optional[str] = None,
        error_message: Optional[str] = None,
        executed_qty: Optional[float] = None,
        cumulative_quote_qty: Optional[float] = None,
        avg_price: Optional[float] = None,
    ) -> None:
        with self.store.unit_of_work():
            self.store.update_order_attempt_status(
                attempt_id=attempt_id,
                status=status,
                exchange_order_id=exchange_order_id,
                error_message=error_message,
                executed_qty=executed_qty,
                cumulative_quote_qty=cumulative_quote_qty,
                avg_price=avg_price,
            )

    def _dict_to_attempt(self, d: Dict[str, Any]) -> OrderAttempt:
        return OrderAttempt(
            attempt_id=str(d["attempt_id"]),
            intent_id=str(d["intent_id"]),
            account_id=str(d.get("account_id") or self.store.account_id),
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
            created_at_utc=str(d.get("created_at_utc") or ""),
            updated_at_utc=str(d.get("updated_at_utc") or ""),
        )

    # -------------------------------------------------------------------------
    # Fills & Episode Attribution
    # -------------------------------------------------------------------------
    def record_fill(
        self,
        fill: ExecutionFill,
        episode_id: Optional[str] = None,
        intent_scope: Optional[str] = None,
    ) -> None:
        """Atomically record execution fill and update PositionEpisode attribution."""
        with self.store.unit_of_work():
            # 1. Record fill
            self.store.save_execution_fill(
                fill_id=fill.fill_id,
                attempt_id=fill.attempt_id,
                intent_id=fill.intent_id,
                symbol=fill.symbol,
                exchange_trade_id=fill.exchange_trade_id,
                side=fill.side,
                price=fill.price,
                qty=fill.qty,
                commission=fill.commission,
                commission_asset=fill.commission_asset,
                trade_time_utc=fill.trade_time_utc,
                exchange_order_id=fill.exchange_order_id,
            )

            # 2. Update episode attribution if episode_id provided
            target_episode_id = episode_id
            target_scope = intent_scope
            if not target_episode_id or not target_scope:
                intent_dict = self.store.get_order_intent(fill.intent_id)
                if intent_dict:
                    target_episode_id = target_episode_id or intent_dict.get("episode_id")
                    target_scope = target_scope or intent_dict.get("intent_scope")

            if target_episode_id:
                ep = self.store.get_position_episode(target_episode_id)
                if ep:
                    cur_qty = float(ep.get("current_qty") or 0.0)
                    scope_val = str(target_scope or "").upper()
                    if scope_val in ("ENTRY", "REBALANCE"):
                        new_qty = cur_qty + fill.qty
                        new_status = EpisodeStatus.OPEN.value
                        closed_at = None
                    else:  # EXIT, REDUCE, TAKE_PROFIT, LOSS_CUT
                        new_qty = max(0.0, cur_qty - fill.qty)
                        if new_qty <= 1e-6:
                            new_qty = 0.0
                            new_status = EpisodeStatus.CLOSED.value
                            closed_at = self.now_iso_fn()
                        else:
                            new_status = ep.get("status") or EpisodeStatus.OPEN.value
                            closed_at = None

                    self.store.save_position_episode(
                        episode_id=target_episode_id,
                        symbol=ep["symbol"],
                        position_side=ep["position_side"],
                        status=new_status,
                        target_qty=ep.get("target_qty"),
                        current_qty=new_qty,
                        realized_pnl=float(ep.get("realized_pnl") or 0.0),
                        closed_at_utc=closed_at,
                    )
