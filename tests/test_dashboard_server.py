import tempfile
import unittest
import sqlite3
from datetime import datetime, timezone
from typing import Dict
from pathlib import Path

from dashboard_server import DashboardDataProvider
from state_store import StateStore


class DashboardServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        self.db_path = str(root / "state.db")
        self.log_file = str(root / "strategy.log")

        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=self.db_path, schema_path=schema_path)
        self.store.init_schema()

        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write("line-a\nline-b\nline-c\n")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_snapshot_contains_runs_positions_events(self) -> None:
        run_id, _ = self.store.create_run("2026-02-13")
        self.store.finalize_run(run_id, "SUCCESS", "done")

        now = datetime.now(timezone.utc).replace(microsecond=0)
        position_id = self.store.insert_position(
            run_id=run_id,
            symbol="BTCUSDT",
            side="SHORT",
            qty=0.01,
            entry_price=50000.0,
            liq_price_open=60000.0,
            tp_price=40000.0,
            sl_price=59000.0,
            tp_order_id=1001,
            sl_order_id=1002,
            tp_client_order_id="tp-x",
            sl_client_order_id="sl-x",
            opened_at_utc=now.isoformat(),
            expire_at_utc=now.isoformat(),
            status="OPEN",
        )

        self.store.add_order_event(
            symbol="BTCUSDT",
            position_id=position_id,
            event_time_utc=now.isoformat(),
            order_payload={
                "orderId": 1001,
                "clientOrderId": "tp-x",
                "type": "TAKE_PROFIT_MARKET",
                "side": "BUY",
                "price": "0",
                "origQty": "0.01",
                "status": "NEW",
            },
        )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )

        snapshot = provider.snapshot(log_lines=2)

        self.assertEqual(snapshot["summary"]["open_positions"], 1)
        self.assertEqual(snapshot["summary"]["open_symbols"], 1)
        self.assertEqual(snapshot["summary"]["last_run_status"], "SUCCESS")
        self.assertEqual(len(snapshot["runs"]), 1)
        self.assertEqual(len(snapshot["open_positions"]), 1)
        self.assertEqual(len(snapshot["events"]), 1)
        self.assertEqual(snapshot["log_tail"], ["line-b", "line-c"])
        self.assertIn("next_entry_local", snapshot)
        self.assertGreaterEqual(snapshot["seconds_to_next_entry"], 0)
        self.assertIn("equity_curve", snapshot)
        self.assertIn("drawdown_stats", snapshot)
        self.assertIn("wallet", snapshot)

    def test_snapshot_without_db_file(self) -> None:
        missing_db = str(Path(self.temp_dir.name) / "missing.db")
        provider = DashboardDataProvider(
            db_path=missing_db,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        snapshot = provider.snapshot(log_lines=1)
        self.assertEqual(snapshot["summary"]["open_positions"], 0)
        self.assertEqual(snapshot["log_tail"], ["line-c"])

    def test_equity_curve_and_wallet_cache(self) -> None:
        run_id, _ = self.store.create_run("2026-02-14")
        now = datetime.now(timezone.utc).replace(microsecond=0)
        position_id = self.store.insert_position(
            run_id=run_id,
            symbol="TESTUSDT",
            side="SHORT",
            qty=2.0,
            entry_price=100.0,
            liq_price_open=140.0,
            tp_price=80.0,
            sl_price=120.0,
            tp_order_id=2001,
            sl_order_id=2002,
            tp_client_order_id="tp-t",
            sl_client_order_id="sl-t",
            opened_at_utc=now.isoformat(),
            expire_at_utc=now.isoformat(),
            status="OPEN",
        )
        close_payload: Dict[str, object] = {
            "orderId": 2001,
            "clientOrderId": "tp-close",
            "type": "MARKET",
            "side": "BUY",
            "price": "0",
            "origQty": "2",
            "executedQty": "2",
            "avgPrice": "80",
            "status": "FILLED",
        }
        self.store.add_order_event(
            symbol="TESTUSDT",
            position_id=position_id,
            event_time_utc=now.isoformat(),
            order_payload=close_payload,
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_TP",
            close_reason="TAKE_PROFIT_FILLED",
            close_order_id=2001,
        )

        calls = {"n": 0}

        def _mock_balance_fetcher() -> float:
            calls["n"] += 1
            return 120.0

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            balance_fetcher=_mock_balance_fetcher,
            balance_cache_ttl_sec=60,
        )

        first = provider.snapshot(log_lines=0)
        second = provider.snapshot(log_lines=0)

        self.assertEqual(calls["n"], 1)
        self.assertEqual(first["wallet"]["source"], "DB")
        self.assertEqual(second["wallet"]["source"], "DB")
        self.assertEqual(first["wallet"]["live_source"], "API")
        self.assertEqual(second["wallet"]["live_source"], "CACHE")

        stats = first["drawdown_stats"]
        self.assertAlmostEqual(stats["total_realized_pnl"], 40.0)
        self.assertAlmostEqual(stats["wallet_balance_usdt"], 120.0)
        self.assertEqual(stats["closed_trades_priced"], 1)
        self.assertAlmostEqual(stats["win_rate_pct"], 100.0)

        curve = first["equity_curve"]
        self.assertEqual(len(curve), 1)
        self.assertAlmostEqual(curve[0]["pnl"], 40.0)
        self.assertAlmostEqual(curve[0]["cum_pnl"], 40.0)
        self.assertAlmostEqual(curve[0]["equity"], 120.0)

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM wallet_snapshots").fetchone()
        self.assertEqual(int(row[0]), 1)

    def test_wallet_fetch_error_is_throttled(self) -> None:
        calls = {"n": 0}

        def _always_fail() -> float:
            calls["n"] += 1
            raise RuntimeError("network down")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            balance_fetcher=_always_fail,
            balance_cache_ttl_sec=60,
        )

        first = provider.snapshot(log_lines=0)
        second = provider.snapshot(log_lines=0)

        self.assertEqual(calls["n"], 1)
        self.assertEqual(first["wallet"]["source"], "ERROR")
        self.assertEqual(second["wallet"]["source"], "COOLDOWN")


if __name__ == "__main__":
    unittest.main()
