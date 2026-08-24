import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from core.balance_sampler import WalletSnapshotSampler
from core.state_store import StateStore


class BalanceSamplerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=self.db_path, schema_path=schema_path)
        self.store.init_schema()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_run_once_persists_snapshot(self) -> None:
        class ClientStub:
            def get_balance(self):
                return [{"asset": "USDT", "balance": "88.1234"}]
            def get_position_risk(self):
                return [{"symbol": "BTCUSDT", "unRealizedProfit": "1.25"}]

        sampler = WalletSnapshotSampler(client=ClientStub(), store=self.store, asset="USDT")
        result = sampler.run_once()

        self.assertIn("snapshot_id", result)
        self.assertAlmostEqual(float(result["wallet_balance"]), 88.1234)
        self.assertAlmostEqual(float(result["unrealized_pnl"]), 1.25)
        self.assertAlmostEqual(float(result["equity"]), 89.3734)
        self.assertAlmostEqual(float(result["balance"]), 89.3734)
        latest = self.store.get_latest_wallet_snapshot()
        self.assertIsNotNone(latest)
        self.assertAlmostEqual(float(latest["balance_usdt"]), 89.3734)

    def test_run_once_raises_when_asset_missing(self) -> None:
        class ClientStub:
            def get_balance(self):
                return [{"asset": "BTC", "balance": "1"}]
            def get_position_risk(self):
                return []

        sampler = WalletSnapshotSampler(client=ClientStub(), store=self.store, asset="USDT")
        with self.assertRaises(ValueError):
            sampler.run_once()

    def test_position_risk_failure_is_persisted_as_invalid_snapshot(self) -> None:
        class ClientStub:
            def get_balance(self):
                return [{"asset": "USDT", "balance": "88.1234"}]

            def get_position_risk(self):
                raise RuntimeError("position risk unavailable")

        sampler = WalletSnapshotSampler(client=ClientStub(), store=self.store, asset="USDT")
        result = sampler.run_once()

        self.assertIn("snapshot_id", result)
        self.assertIsNone(self.store.get_latest_wallet_snapshot())
        with sqlite3.connect(self.db_path) as conn:
            error = conn.execute(
                "SELECT error FROM wallet_snapshots WHERE id = ?",
                (int(result["snapshot_id"]),),
            ).fetchone()[0]
        self.assertIn("position risk unavailable", str(error))

    def test_cashflow_sync_uses_one_unfiltered_request_and_local_filtering(self) -> None:
        class ClientStub:
            def __init__(self):
                self.calls = []

            def get_balance(self):
                return [{"asset": "USDT", "balance": "88.1234"}]

            def get_position_risk(self):
                return []

            def get_income_history(self, **kwargs):
                self.calls.append(kwargs)
                return [
                    {
                        "time": 1000,
                        "incomeType": "TRANSFER",
                        "asset": "USDT",
                        "income": "1.0",
                        "tranId": "transfer-1",
                    },
                    {
                        "time": 2000,
                        "incomeType": "OTHER",
                        "asset": "USDT",
                        "income": "9.0",
                        "tranId": "other-1",
                    },
                ]

        client = ClientStub()
        sampler = WalletSnapshotSampler(
            client=client,
            store=self.store,
            asset="USDT",
            sync_cashflows=True,
            cashflow_income_types=["TRANSFER", "WELCOME_BONUS"],
        )
        sampler._resolve_cashflow_start_ms = lambda income_type=None: 0

        inserted = sampler._sync_cashflows()

        self.assertEqual(inserted, 1)
        self.assertEqual(len(client.calls), 1)
        self.assertIsNone(client.calls[0]["income_type"])
        cursor = self.store.get_lock_state("cashflow_income_cursor_v2")
        self.assertIsNotNone(cursor)
        self.assertEqual(cursor["last_row_count"], 2)

    def test_cashflow_full_page_continues_same_window_on_next_minute(self) -> None:
        class ClientStub:
            def __init__(self):
                self.calls = []

            def get_income_history(self, **kwargs):
                self.calls.append(kwargs)
                page = int(kwargs.get("page") or 1)
                if page == 1:
                    return [
                        {
                            "time": 1500,
                            "incomeType": "TRANSFER",
                            "asset": "USDT",
                            "income": "1",
                            "tranId": f"transfer-{index}",
                        }
                        for index in range(1000)
                    ]
                return [
                    {
                        "time": 1500,
                        "incomeType": "TRANSFER",
                        "asset": "USDT",
                        "income": "1",
                        "tranId": "transfer-1000",
                    }
                ]

        client = ClientStub()
        sampler = WalletSnapshotSampler(
            client=client,
            store=self.store,
            asset="USDT",
            sync_cashflows=True,
            cashflow_income_types=["TRANSFER"],
        )
        sampler._resolve_cashflow_start_ms = lambda income_type=None: 1000

        first_inserted = sampler.sync_cashflows_once(
            now_utc=datetime.fromtimestamp(5, tz=timezone.utc)
        )
        second_inserted = sampler.sync_cashflows_once(
            now_utc=datetime.fromtimestamp(65, tz=timezone.utc)
        )

        self.assertEqual(first_inserted, 1000)
        self.assertEqual(second_inserted, 1)
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(client.calls[0]["page"], 1)
        self.assertEqual(client.calls[1]["page"], 2)
        self.assertEqual(client.calls[1]["start_time"], client.calls[0]["start_time"])
        self.assertEqual(client.calls[1]["end_time"], client.calls[0]["end_time"])
        cursor = self.store.get_lock_state("cashflow_income_cursor_v2")
        self.assertFalse(cursor["draining_full_page"])
        self.assertEqual(cursor["cursor_ms"], 5000)


if __name__ == "__main__":
    unittest.main()
