"""Ports and protocols for infrastructure adapters.

Defines structural interfaces (Ports) required by infrastructure adapters,
enabling infrastructure components to decouple entirely from core domain and storage implementations.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class UserStreamStoreProtocol(Protocol):
    """Port defining persistence operations needed by BinanceUserStreamState."""

    account_id: str

    def list_open_positions(self) -> List[Dict[str, Any]]: ...

    def list_exchange_order_state(self, active_only: bool = False) -> List[Dict[str, Any]]: ...

    def reconcile_open_order_state(self, orders: List[Dict[str, Any]]) -> None: ...

    def get_exchange_order_status(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Optional[str]: ...

    def upsert_exchange_order_state(
        self,
        order_payload: Dict[str, Any],
        source: str = "STREAM",
    ) -> None: ...

    def get_exchange_order_state(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        client_order_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]: ...

    def update_parent_algo_order_status(
        self,
        symbol: str,
        client_algo_id: str,
        actual_order_id: int,
        status: str,
    ) -> bool: ...

    def add_order_event(
        self,
        symbol: str,
        position_id: Optional[int],
        event_time_utc: str,
        order_payload: Dict[str, Any],
    ) -> int: ...

    def save_cursor_state(self, cursor_key: str, state_payload: Dict[str, Any]) -> None: ...


@runtime_checkable
class UserStreamSnapshotProtocol(Protocol):
    """Port defining account snapshot operations needed by BinanceUserStreamState."""

    def capture(self, force: bool = False) -> Any: ...

    def merge_position_risks(self, position_risks: List[Dict[str, Any]]) -> None: ...

    def apply_stream_update(self, payload: Dict[str, Any]) -> None: ...


@runtime_checkable
class TradeStatsStoreProtocol(Protocol):
    """Port defining persistence operations needed by TradeStatsFetcher."""

    account_id: str

    def get_cursor_state(self, cursor_key: str) -> Optional[Dict[str, Any]]: ...

    def save_cursor_state(self, cursor_key: str, state_payload: Dict[str, Any]) -> None: ...

    def batch_insert_binance_user_trades(
        self,
        trades: List[Dict[str, Any]],
        account_id: Optional[str] = None,
    ) -> int: ...

    def get_latest_binance_user_trade_time(
        self,
        symbol: str,
        account_id: Optional[str] = None,
    ) -> Optional[int]: ...
