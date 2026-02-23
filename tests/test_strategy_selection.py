import importlib.util
import tempfile
import unittest
from pathlib import Path

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from core.state_store import StateStore
from core.strategy_top10_short import RankEntry, Top10ShortStrategy


class StrategySelectionTest(unittest.TestCase):
    def test_select_candidates_backfills_to_target(self) -> None:
        ranked = [
            RankEntry(symbol=f"S{i}USDT", pct_change=float(20 - i), last_price=1.0, quote_volume=1000.0)
            for i in range(12)
        ]
        open_symbols = {"S2USDT", "S7USDT"}

        candidates, skipped = Top10ShortStrategy._select_entry_candidates(
            ranked=ranked,
            open_symbols=open_symbols,
            target_count=10,
        )

        self.assertEqual(len(candidates), 10)
        self.assertEqual([item.symbol for item in candidates], [
            "S0USDT",
            "S1USDT",
            "S3USDT",
            "S4USDT",
            "S5USDT",
            "S6USDT",
            "S8USDT",
            "S9USDT",
            "S10USDT",
            "S11USDT",
        ])
        self.assertEqual(skipped, ["S2USDT", "S7USDT"])

    def test_select_candidates_returns_partial_when_not_enough(self) -> None:
        ranked = [
            RankEntry(symbol="AUSDT", pct_change=5.0, last_price=1.0, quote_volume=1000.0),
            RankEntry(symbol="BUSDT", pct_change=4.0, last_price=1.0, quote_volume=1000.0),
            RankEntry(symbol="CUSDT", pct_change=3.0, last_price=1.0, quote_volume=1000.0),
        ]
        open_symbols = {"AUSDT", "BUSDT"}

        candidates, skipped = Top10ShortStrategy._select_entry_candidates(
            ranked=ranked,
            open_symbols=open_symbols,
            target_count=10,
        )

        self.assertEqual([item.symbol for item in candidates], ["CUSDT"])
        self.assertEqual(skipped, ["AUSDT", "BUSDT"])

    def test_entry_does_not_see_other_account_open_positions(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "state.db")
            schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
            base_store = StateStore(db_path=db_path, schema_path=schema_path)
            base_store.init_schema()

            run_id, _ = base_store.create_run("2026-02-13", account_id="acc02")
            base_store.insert_position(
                run_id=run_id,
                symbol="BTCUSDT",
                side="SHORT",
                qty=1.0,
                entry_price=100.0,
                liq_price_open=200.0,
                tp_price=90.0,
                sl_price=120.0,
                tp_order_id=None,
                sl_order_id=None,
                tp_client_order_id=None,
                sl_client_order_id=None,
                opened_at_utc="2026-02-13T00:00:00+00:00",
                expire_at_utc="2026-02-14T00:00:00+00:00",
                status="OPEN",
            )

            acc01_store = base_store.scoped("acc01")
            open_symbols = acc01_store.list_open_symbols()
            self.assertNotIn("BTCUSDT", open_symbols)

            ranked = [RankEntry(symbol="BTCUSDT", pct_change=10.0, last_price=1.0, quote_volume=1000.0)]
            candidates, skipped = Top10ShortStrategy._select_entry_candidates(
                ranked=ranked,
                open_symbols=open_symbols,
                target_count=1,
            )
            self.assertEqual([item.symbol for item in candidates], ["BTCUSDT"])
            self.assertEqual(skipped, [])


if __name__ == "__main__":
    unittest.main()
