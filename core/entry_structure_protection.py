from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from core.state_store import StateStore


ENTRY_STRUCTURE_PROTECTION_LOCK_NAME = "entry_structure_protection_v1"


@dataclass(frozen=True)
class EntryStructureProtection:
    stop_price: float
    bearish_close_time_utc: datetime
    window_start_utc: datetime
    window_end_utc: datetime

    def to_payload(self) -> Dict[str, object]:
        return {
            "stop_price": float(self.stop_price),
            "bearish_close_time_utc": self._iso_utc(self.bearish_close_time_utc),
            "window_start_utc": self._iso_utc(self.window_start_utc),
            "window_end_utc": self._iso_utc(self.window_end_utc),
        }

    @classmethod
    def from_payload(cls, payload: object) -> Optional["EntryStructureProtection"]:
        if not isinstance(payload, dict):
            return None
        try:
            stop_price = float(payload["stop_price"])
            bearish_close = cls._parse_utc(payload["bearish_close_time_utc"])
            window_start = cls._parse_utc(payload["window_start_utc"])
            window_end = cls._parse_utc(payload["window_end_utc"])
        except (KeyError, TypeError, ValueError):
            return None
        if stop_price <= 0 or not (window_start < window_end):
            return None
        return cls(
            stop_price=stop_price,
            bearish_close_time_utc=bearish_close,
            window_start_utc=window_start,
            window_end_utc=window_end,
        )

    @staticmethod
    def _iso_utc(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _parse_utc(value: object) -> datetime:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


class EntryStructureProtectionState:
    def __init__(self, store: StateStore):
        self.store = store

    def get(self, position_id: int) -> Optional[EntryStructureProtection]:
        data = self.store.get_entry_structure_protection(position_id)
        if isinstance(data, dict):
            return EntryStructureProtection.from_payload(data)
        return None

    def put(self, position_id: int, protection: EntryStructureProtection) -> None:
        self.store.set_entry_structure_protection(
            position_id=position_id,
            stop_price=protection.stop_price,
            bearish_close_time_utc=protection._iso_utc(protection.bearish_close_time_utc),
            window_start_utc=protection._iso_utc(protection.window_start_utc),
            window_end_utc=protection._iso_utc(protection.window_end_utc),
        )
