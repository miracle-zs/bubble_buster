"""Lightweight Unit of Work (UoW) implementation for SQLite transaction boundaries.

Ensures atomic transaction management across trading aggregate operations
(positions, order events, fills, exchange states). Supports ambient context
propagation via ContextVar, nested transactions via SQLite SAVEPOINTs, and
post-transaction callbacks.
"""

from __future__ import annotations

import logging
import sqlite3
import uuid
from contextvars import ContextVar, Token
from typing import Any, Callable, List, Optional

LOGGER = logging.getLogger(__name__)

_current_uow: ContextVar[Optional["UnitOfWork"]] = ContextVar("bb_current_uow", default=None)


def get_current_uow() -> Optional["UnitOfWork"]:
    """Retrieve the currently active Unit of Work in the ambient context, if any."""
    return _current_uow.get()


class UnitOfWork:
    """Manages an atomic SQLite transaction boundary for StateStore operations."""

    def __init__(self, store: Any) -> None:
        self.store = store
        self.conn: Optional[sqlite3.Connection] = None
        self.is_nested: bool = False
        self.savepoint_name: Optional[str] = None
        self._token: Optional[Token[Optional[UnitOfWork]]] = None
        self._committed: bool = False
        self._rolled_back: bool = False
        self._after_commit_callbacks: List[Callable[[], None]] = []
        self._after_rollback_callbacks: List[Callable[[], None]] = []

    @property
    def is_active(self) -> bool:
        """Check if this Unit of Work is currently active and has an open connection."""
        return self.conn is not None and not (self._committed or self._rolled_back)

    def add_after_commit(self, fn: Callable[[], None]) -> None:
        """Register a callback to be invoked after transaction is committed successfully."""
        self._after_commit_callbacks.append(fn)

    def add_after_rollback(self, fn: Callable[[], None]) -> None:
        """Register a callback to be invoked if transaction is rolled back."""
        self._after_rollback_callbacks.append(fn)

    def __enter__(self) -> "UnitOfWork":
        parent = get_current_uow()
        # If there is already an active UoW on the same database, join via SQLite SAVEPOINT
        if (
            parent is not None
            and parent.conn is not None
            and getattr(parent.store, "db_path", None) == getattr(self.store, "db_path", None)
        ):
            self.conn = parent.conn
            self.is_nested = True
            self.savepoint_name = f"sp_{uuid.uuid4().hex[:8]}"
            self.conn.execute(f"SAVEPOINT {self.savepoint_name}")
        else:
            self.conn = self.store._connect()
            self.is_nested = False
            self.savepoint_name = None

        self._token = _current_uow.set(self)
        return self

    def __exit__(self, exc_type: Optional[type], exc_val: Optional[BaseException], exc_tb: Any) -> None:
        try:
            if exc_type is not None:
                self._handle_exception_exit()
            else:
                self._handle_normal_exit()
        finally:
            if self._token is not None:
                _current_uow.reset(self._token)
                self._token = None
            if not self.is_nested and self.conn is not None:
                try:
                    self.conn.close()
                except Exception as close_exc:
                    LOGGER.warning("Error closing UnitOfWork connection: %s", close_exc)
                finally:
                    self.conn = None

    def _handle_exception_exit(self) -> None:
        if self.conn is None:
            return
        if self.is_nested and self.savepoint_name:
            try:
                self.conn.execute(f"ROLLBACK TO {self.savepoint_name}")
                self.conn.execute(f"RELEASE SAVEPOINT {self.savepoint_name}")
            except Exception as sp_exc:
                LOGGER.warning("Error rolling back savepoint %s: %s", self.savepoint_name, sp_exc)
        else:
            if not self._rolled_back and not self._committed:
                try:
                    self.conn.rollback()
                except Exception as rb_exc:
                    LOGGER.warning("Error rolling back UnitOfWork transaction: %s", rb_exc)
                self._rolled_back = True

        for cb in self._after_rollback_callbacks:
            try:
                cb()
            except Exception as cb_exc:
                LOGGER.warning("UnitOfWork after_rollback callback failed: %s", cb_exc)

    def _handle_normal_exit(self) -> None:
        if self.conn is None:
            return
        if self.is_nested and self.savepoint_name:
            self.conn.execute(f"RELEASE SAVEPOINT {self.savepoint_name}")
        else:
            if not self._committed and not self._rolled_back:
                try:
                    self.conn.commit()
                    self._committed = True
                except Exception:
                    self.conn.rollback()
                    self._rolled_back = True
                    for cb in self._after_rollback_callbacks:
                        try:
                            cb()
                        except Exception as cb_exc:
                            LOGGER.warning("UnitOfWork after_rollback callback failed: %s", cb_exc)
                    raise

        for cb in self._after_commit_callbacks:
            try:
                cb()
            except Exception as cb_exc:
                LOGGER.warning("UnitOfWork after_commit callback failed: %s", cb_exc)

    def commit(self) -> None:
        """Manually commit current transaction or release savepoint."""
        if self.conn is None:
            raise RuntimeError("Cannot commit an inactive UnitOfWork")
        if self.is_nested and self.savepoint_name:
            self.conn.execute(f"RELEASE SAVEPOINT {self.savepoint_name}")
            self.savepoint_name = f"sp_{uuid.uuid4().hex[:8]}"
            self.conn.execute(f"SAVEPOINT {self.savepoint_name}")
        else:
            self.conn.commit()
            self._committed = True

    def rollback(self) -> None:
        """Manually rollback current transaction or rollback to savepoint."""
        if self.conn is None:
            raise RuntimeError("Cannot rollback an inactive UnitOfWork")
        if self.is_nested and self.savepoint_name:
            self.conn.execute(f"ROLLBACK TO {self.savepoint_name}")
        else:
            self.conn.rollback()
            self._rolled_back = True
