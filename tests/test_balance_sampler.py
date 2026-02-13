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

        sampler = WalletSnapshotSampler(client=ClientStub(), store=self.store, asset="USDT")
        result = sampler.run_once()

        self.assertIn("snapshot_id", result)
        self.assertAlmostEqual(float(result["balance"]), 88.1234)
        latest = self.store.get_latest_wallet_snapshot()
        self.assertIsNotNone(latest)
        self.assertAlmostEqual(float(latest["balance_usdt"]), 88.1234)

    def test_run_once_raises_when_asset_missing(self) -> None:
        class ClientStub:
            def get_balance(self):
                return [{"asset": "BTC", "balance": "1"}]

        sampler = WalletSnapshotSampler(client=ClientStub(), store=self.store, asset="USDT")
        with self.assertRaises(ValueError):
            sampler.run_once()


if __name__ == "__main__":
    unittest.main()
