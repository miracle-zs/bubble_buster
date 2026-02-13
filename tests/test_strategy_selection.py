import importlib.util
import unittest

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from strategy_top10_short import RankEntry, Top10ShortStrategy


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


if __name__ == "__main__":
    unittest.main()
