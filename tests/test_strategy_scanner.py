"""Unit tests for MarketRankScanner."""

import unittest

from core.strategy.models import RankEntry
from core.strategy.scanner import MarketRankScanner


class MarketRankScannerTest(unittest.TestCase):
    def test_build_ranked_entries_handles_valid_and_invalid(self) -> None:
        raw_items = [
            {"symbol": "BTCUSDT", "change": "12.5", "current_price": "60000", "volume": "50000000"},
            {"symbol": "ETHUSDT", "change": "8.0", "current_price": "3000", "volume": "30000000"},
            {"symbol": "CORRUPT", "change": "invalid"},  # should be safely skipped
        ]
        entries = MarketRankScanner.build_ranked_entries(raw_items)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0].symbol, "BTCUSDT")
        self.assertEqual(entries[0].pct_change, 12.5)
        self.assertEqual(entries[0].last_price, 60000.0)
        self.assertEqual(entries[0].quote_volume, 50000000.0)

    def test_filter_and_sort_ranked_entries(self) -> None:
        entries = [
            RankEntry(symbol="A", pct_change=5.0, last_price=1.0, quote_volume=100.0),
            RankEntry(symbol="B", pct_change=25.0, last_price=2.0, quote_volume=50.0),
            RankEntry(symbol="C", pct_change=15.0, last_price=3.0, quote_volume=200.0),
        ]
        # Without volume filter
        res1 = MarketRankScanner.filter_and_sort_ranked_entries(entries, volume_threshold=0.0)
        self.assertEqual([e.symbol for e in res1], ["B", "C", "A"])

        # With volume filter >= 100
        res2 = MarketRankScanner.filter_and_sort_ranked_entries(entries, volume_threshold=100.0)
        self.assertEqual([e.symbol for e in res2], ["C", "A"])

    def test_select_entry_candidates(self) -> None:
        entries = [
            RankEntry(symbol="A", pct_change=20.0, last_price=1.0, quote_volume=100.0),
            RankEntry(symbol="B", pct_change=15.0, last_price=1.0, quote_volume=100.0),
            RankEntry(symbol="C", pct_change=10.0, last_price=1.0, quote_volume=100.0),
            RankEntry(symbol="D", pct_change=5.0, last_price=1.0, quote_volume=100.0),
        ]
        open_symbols = {"B", "D"}
        candidates, skipped = MarketRankScanner.select_entry_candidates(
            ranked=entries,
            open_symbols=open_symbols,
            target_count=1,
        )
        self.assertEqual([c.symbol for c in candidates], ["A"])
        self.assertEqual(skipped, [])

        candidates, skipped = MarketRankScanner.select_entry_candidates(
            ranked=entries,
            open_symbols=open_symbols,
            target_count=2,
        )
        self.assertEqual([c.symbol for c in candidates], ["A", "C"])
        self.assertEqual(skipped, ["B"])


if __name__ == "__main__":
    unittest.main()
