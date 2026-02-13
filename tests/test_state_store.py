import tempfile
import unittest
from pathlib import Path

from core.state_store import StateStore


class StateStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=self.db_path, schema_path=schema_path)
        self.store.init_schema()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_create_run_idempotent_by_trade_day(self) -> None:
        run1, created1 = self.store.create_run("2026-02-13")
        run2, created2 = self.store.create_run("2026-02-13")
        self.assertTrue(created1)
        self.assertFalse(created2)
        self.assertEqual(run1, run2)

    def test_insert_and_close_position(self) -> None:
        run_id, _ = self.store.create_run("2026-02-13")
        position_id = self.store.insert_position(
            run_id=run_id,
            symbol="BTCUSDT",
            side="SHORT",
            qty=0.01,
            entry_price=50000.0,
            liq_price_open=60000.0,
            tp_price=40000.0,
            sl_price=59000.0,
            tp_order_id=1,
            sl_order_id=2,
            tp_client_order_id="tp1",
            sl_client_order_id="sl1",
            opened_at_utc="2026-02-13T00:00:00+00:00",
            expire_at_utc="2026-02-14T23:30:00+00:00",
        )

        symbols = self.store.list_open_symbols()
        self.assertIn("BTCUSDT", symbols)

        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_TP",
            close_reason="TAKE_PROFIT_FILLED",
            close_order_id=1,
        )
        symbols_after = self.store.list_open_symbols()
        self.assertNotIn("BTCUSDT", symbols_after)

    def test_wallet_snapshot_roundtrip(self) -> None:
        snapshot_id = self.store.add_wallet_snapshot(
            captured_at_utc="2026-02-13T00:00:00+00:00",
            balance_usdt=123.456,
            source="API",
            error=None,
        )
        self.assertGreater(snapshot_id, 0)
        latest = self.store.get_latest_wallet_snapshot()
        self.assertIsNotNone(latest)
        self.assertAlmostEqual(float(latest["balance_usdt"]), 123.456)
        self.assertEqual(latest["source"], "API")


if __name__ == "__main__":
    unittest.main()
