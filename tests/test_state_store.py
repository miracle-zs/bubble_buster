import tempfile
import unittest
import sqlite3
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

    def test_rebalance_cycle_and_action_roundtrip(self) -> None:
        run_id, _ = self.store.create_run("2026-02-13")
        cycle_id = self.store.create_rebalance_cycle(
            run_id=run_id,
            reason_tag="pre",
            mode="equal_risk",
            reduce_only=True,
            target_count=15,
        )
        self.assertGreater(cycle_id, 0)

        action_id = self.store.add_rebalance_action(
            cycle_id=cycle_id,
            run_id=run_id,
            position_id=None,
            symbol="BTCUSDT",
            action_side="BUY",
            reduce_only=True,
            ref_price=100000.0,
            current_notional_usdt=120.0,
            target_notional_usdt=80.0,
            deviation_notional_usdt=-40.0,
            deadband_notional_usdt=8.0,
            max_adjust_notional_usdt=48.0,
            requested_adjust_notional_usdt=40.0,
            qty=0.0004,
            est_notional_usdt=40.0,
            status="PLANNED",
        )
        self.assertGreater(action_id, 0)
        self.store.update_rebalance_action_result(
            action_id=action_id,
            status="ADJUSTED",
            order_id=999001,
            client_order_id="rbpre-btc-1",
        )
        self.store.finalize_rebalance_cycle(
            cycle_id=cycle_id,
            summary={
                "open_positions": 12,
                "virtual_slots": 3,
                "equity_usdt": 1000.0,
                "target_gross_notional": 1800.0,
                "target_notional_per_position": 120.0,
                "planned": 8,
                "adjusted": 7,
                "errors": 1,
                "reduced_notional": 220.0,
                "added_notional": 140.0,
            },
        )

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cycle_row = conn.execute("SELECT * FROM rebalance_cycles WHERE id = ?", (cycle_id,)).fetchone()
            action_row = conn.execute("SELECT * FROM rebalance_actions WHERE id = ?", (action_id,)).fetchone()

        self.assertIsNotNone(cycle_row)
        self.assertIsNotNone(action_row)
        self.assertEqual(int(cycle_row["open_positions"]), 12)
        self.assertAlmostEqual(float(cycle_row["target_gross_notional_usdt"]), 1800.0)
        self.assertEqual(action_row["status"], "ADJUSTED")
        self.assertEqual(int(action_row["order_id"]), 999001)

    def test_add_order_event_creates_fill_row(self) -> None:
        run_id, _ = self.store.create_run("2026-02-13")
        position_id = self.store.insert_position(
            run_id=run_id,
            symbol="ETHUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=3000.0,
            liq_price_open=4000.0,
            tp_price=2500.0,
            sl_price=3900.0,
            tp_order_id=11,
            sl_order_id=12,
            tp_client_order_id="tp-1",
            sl_client_order_id="sl-1",
            opened_at_utc="2026-02-13T00:00:00+00:00",
            expire_at_utc="2026-02-14T00:00:00+00:00",
        )

        event_id = self.store.add_order_event(
            symbol="ETHUSDT",
            position_id=position_id,
            event_time_utc="2026-02-13T01:00:00+00:00",
            order_payload={
                "orderId": 7001,
                "clientOrderId": "close-eth-1",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "1",
                "executedQty": "1",
                "avgPrice": "2850",
                "cumQuote": "2850",
                "status": "FILLED",
                "reduceOnly": True,
                "realizedPnl": "12.5",
                "commission": "0.9",
                "commissionAsset": "USDT",
            },
        )
        self.assertGreater(event_id, 0)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            fill_row = conn.execute("SELECT * FROM fills WHERE order_event_id = ?", (event_id,)).fetchone()

        self.assertIsNotNone(fill_row)
        self.assertEqual(fill_row["symbol"], "ETHUSDT")
        self.assertAlmostEqual(float(fill_row["executed_qty"]), 1.0)
        self.assertAlmostEqual(float(fill_row["avg_price"]), 2850.0)
        self.assertAlmostEqual(float(fill_row["realized_pnl"]), 12.5)


if __name__ == "__main__":
    unittest.main()
