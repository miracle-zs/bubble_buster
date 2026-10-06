"""Ports and interfaces for external infrastructure adapters.

Following hexagonal architecture / Clean Architecture:
Core domain and application services define structural interfaces (ports)
for external dependencies such as exchange clients and notification services.
Infrastructure adapters implement these interfaces without core depending on concrete infra classes.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class ExchangeClientPort(Protocol):
    """Port for exchange execution and market query capabilities."""

    def create_order(self, **params: Any) -> Dict[str, Any]: ...

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def get_open_orders(
        self,
        symbol: Optional[str] = None,
    ) -> List[Dict[str, Any]]: ...

    def get_symbol_rules(self, refresh: bool = False) -> Dict[str, Any]: ...

    def normalize_order_qty(self, symbol: str, notional: float, price: float) -> float: ...

    def format_order_qty(self, symbol: str, qty: float) -> str: ...

    def normalize_trigger_price(
        self,
        symbol: str,
        price: float,
        round_up: bool = False,
    ) -> float: ...

    def format_trigger_price(
        self,
        symbol: str,
        price: float,
        round_up: bool = False,
    ) -> str: ...


@runtime_checkable
class NotifierPort(Protocol):
    """Port for notification delivery."""

    def send(self, title: str, content: str) -> None: ...
