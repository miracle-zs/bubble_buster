import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from core.entry_structure_protection import (
    ENTRY_STRUCTURE_PROTECTION_LOCK_NAME,
    EntryStructureProtection,
    EntryStructureProtectionState,
)
from core.state_store import StateStore


class EntryStructureProtectionStateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=db_path, schema_path=schema_path, account_id="acc01")
        self.store.init_schema()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_round_trips_protection_by_position_lifecycle(self) -> None:
        protection = EntryStructureProtection(
            stop_price=0.02590,
            bearish_close_time_utc=datetime(2026, 7, 17, 4, 0, tzinfo=timezone.utc),
            window_start_utc=datetime(2026, 7, 17, 2, 0, tzinfo=timezone.utc),
            window_end_utc=datetime(2026, 7, 17, 4, 0, 5, 804000, tzinfo=timezone.utc),
        )
        state = EntryStructureProtectionState(self.store)

        state.put(position_id=5618, protection=protection)

        self.assertEqual(state.get(position_id=5618), protection)
        self.assertIsNone(state.get(position_id=9999))
        raw = self.store.get_lock_state(ENTRY_STRUCTURE_PROTECTION_LOCK_NAME)
        self.assertEqual(raw["positions"]["5618"]["stop_price"], 0.02590)


if __name__ == "__main__":
    unittest.main()
