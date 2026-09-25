"""Strongly-typed dual-access domain and storage models.

This module provides data models for trading aggregate roots, position states,
runs, order events, and execution records. All records inherit from `DualAccessRecord`
(a dictionary subclass), ensuring:
1. 100% backward compatibility for dict access: `record["symbol"]`, `record.get("id")`, `dict(record)`.
2. Clean typed attribute access: `record.symbol`, `record.id`, `record.entry_price`.
3. Native JSON serialization without custom encoders.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar

T = TypeVar("T", bound="DualAccessRecord")


class DualAccessRecord(dict):
    """Dictionary subclass providing transparent attribute and key-based access."""

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError:
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def __setattr__(self, name: str, value: Any) -> None:
        self[name] = value

    def __delattr__(self, name: str) -> None:
        try:
            del self[name]
        except KeyError:
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

    def to_dict(self) -> Dict[str, Any]:
        """Return a standard Python dictionary copy."""
        return dict(self)

    def copy(self) -> "DualAccessRecord":
        return type(self)(dict(self))

    @classmethod
    def from_row(cls: Type[T], row: Any) -> Optional[T]:
        """Instantiate a typed record from an sqlite3.Row, dict, or mapping."""
        if row is None:
            return None
        return cls(dict(row))

    def __repr__(self) -> str:
        fields = ", ".join(f"{k}={v!r}" for k, v in self.items())
        return f"{self.__class__.__name__}({fields})"


class PositionRecord(DualAccessRecord):
    """Domain model representing a position in the trading lifecycle.

    Supports both dictionary access (`pos["symbol"]`, `pos.get("id")`)
    and typed attribute access (`pos.symbol`, `pos.id`).
    """

    POSITIONAL_FIELDS: Tuple[str, ...] = (
        "id",
        "run_id",
        "symbol",
        "side",
        "qty",
        "entry_price",
        "liq_price_open",
        "liq_price_latest",
        "tp_price",
        "sl_price",
        "tp_order_id",
        "sl_order_id",
        "opened_at_utc",
        "expire_at_utc",
        "closed_at_utc",
        "status",
        "close_reason",
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if len(args) == 1 and isinstance(args[0], (dict, DualAccessRecord)):
            data = dict(args[0])
            data.update(kwargs)
            super().__init__(data)
        elif len(args) > 1:
            data = {}
            for key, val in zip(self.POSITIONAL_FIELDS, args):
                data[key] = val
            data.update(kwargs)
            super().__init__(data)
        else:
            super().__init__(**kwargs)

    @property
    def is_open(self) -> bool:
        return self.get("status") == "OPEN"

    @property
    def is_active(self) -> bool:
        return self.get("status") in {"PENDING_ENTRY", "PENDING_EXIT_SETUP", "OPEN"}

    @property
    def is_closed(self) -> bool:
        status = self.get("status") or ""
        return status.startswith("CLOSED") or status in {"TIMEOUT", "ENTRY_FAILED"}


# Backward-compatible alias for existing code referencing PositionState
PositionState = PositionRecord


class RunRecord(DualAccessRecord):
    """Domain model representing an execution run cycle."""

    POSITIONAL_FIELDS: Tuple[str, ...] = (
        "run_id",
        "account_id",
        "trade_day_utc",
        "started_at_utc",
        "completed_at_utc",
        "status",
        "reason",
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if len(args) == 1 and isinstance(args[0], (dict, DualAccessRecord)):
            data = dict(args[0])
            data.update(kwargs)
            super().__init__(data)
        elif len(args) > 1:
            data = {}
            for key, val in zip(self.POSITIONAL_FIELDS, args):
                data[key] = val
            data.update(kwargs)
            super().__init__(data)
        else:
            super().__init__(**kwargs)

        if "reason" not in self and "message" in self:
            self["reason"] = self["message"]
        elif "message" not in self and "reason" in self:
            self["message"] = self["reason"]
        if "account_id" not in self or not self.get("account_id"):
            self["account_id"] = "default"

    @property
    def reason(self) -> Optional[str]:
        return self.get("reason") or self.get("message")

    @property
    def message(self) -> Optional[str]:
        return self.get("message") or self.get("reason")

    @property
    def is_running(self) -> bool:
        return self.get("status") == "RUNNING"

    @property
    def is_success(self) -> bool:
        return self.get("status") == "SUCCESS"

    @property
    def is_failed(self) -> bool:
        return self.get("status") == "FAILED"


# Backward-compatible alias for existing code referencing RunState
RunState = RunRecord


class OrderEventRecord(DualAccessRecord):
    """Domain model representing an audit event of an exchange order."""

    POSITIONAL_FIELDS: Tuple[str, ...] = (
        "id",
        "account_id",
        "position_id",
        "symbol",
        "order_id",
        "client_order_id",
        "type",
        "side",
        "price",
        "qty",
        "status",
        "event_time_utc",
        "raw_json",
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if len(args) == 1 and isinstance(args[0], (dict, DualAccessRecord)):
            data = dict(args[0])
            data.update(kwargs)
            super().__init__(data)
        elif len(args) > 1:
            data = {}
            for key, val in zip(self.POSITIONAL_FIELDS, args):
                data[key] = val
            data.update(kwargs)
            super().__init__(data)
        else:
            super().__init__(**kwargs)

    def parsed_payload(self) -> Dict[str, Any]:
        """Safely decode raw_json payload into a dictionary."""
        raw = self.get("raw_json")
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return {}


class FillRecord(DualAccessRecord):
    """Domain model representing a trade execution fill."""

    POSITIONAL_FIELDS: Tuple[str, ...] = (
        "id",
        "order_event_id",
        "position_id",
        "symbol",
        "order_id",
        "client_order_id",
        "side",
        "reduce_only",
        "status",
        "executed_qty",
        "quote_qty",
        "avg_price",
        "realized_pnl",
        "commission",
        "commission_asset",
        "event_time_utc",
        "raw_json",
        "created_at_utc",
    )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if len(args) == 1 and isinstance(args[0], (dict, DualAccessRecord)):
            data = dict(args[0])
            data.update(kwargs)
            super().__init__(data)
        elif len(args) > 1:
            data = {}
            for key, val in zip(self.POSITIONAL_FIELDS, args):
                data[key] = val
            data.update(kwargs)
            super().__init__(data)
        else:
            super().__init__(**kwargs)

    def parsed_payload(self) -> Dict[str, Any]:
        """Safely decode raw_json payload into a dictionary."""
        raw = self.get("raw_json")
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return {}


class TaskExecutionRecord(DualAccessRecord):
    """Domain model representing a structured scheduled task execution audit."""

    def parsed_payload(self) -> Dict[str, Any]:
        raw = self.get("payload_json")
        if not raw:
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw)
        except Exception:
            return {}


class WalletSnapshotRecord(DualAccessRecord):
    """Domain model representing an account wallet snapshot."""
    pass
