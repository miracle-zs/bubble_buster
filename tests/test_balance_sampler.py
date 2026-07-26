import sqlite3
import tempfile
import unittest
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

    def test_cashflow_pagination_advances_past_unpersisted_rows(self) -> None:
        class ClientStub:
            def __init__(self):
                self.calls = []

            def get_balance(self):
                return [{"asset": "USDT", "balance": "88.1234"}]

            def get_position_risk(self):
                return []

            def get_income_history(self, **kwargs):
                self.calls.append(kwargs)
                if kwargs.get("income_type") == "WELCOME_BONUS":
                    return []
                if len([call for call in self.calls if call.get("income_type") == "TRANSFER"]) == 1:
                    rows = [
                        {
                            "time": 1000,
                            "incomeType": "TRANSFER",
                            "asset": "USDT",
                            "income": "1.0",
                            "tranId": "transfer-1",
                        }
                    ]
                    rows.extend(
                        {
                            "time": 2000,
                            "incomeType": "OTHER",
                            "asset": "USDT",
                            "income": "0.0",
                        }
                        for _ in range(999)
                    )
                    return rows
                return [
                    {
                        "time": 3000,
                        "incomeType": "TRANSFER",
                        "asset": "USDT",
                        "income": "2.0",
                        "tranId": "transfer-2",
                    }
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

        self.assertEqual(inserted, 2)
        transfer_calls = [call for call in client.calls if call.get("income_type") == "TRANSFER"]
        self.assertEqual(len(transfer_calls), 2)
        self.assertGreater(int(transfer_calls[1]["start_time"]), 2000)


if __name__ == "__main__":
    unittest.main()
