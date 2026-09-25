"""Market rank scanner and candidate filtering pure logic."""

from __future__ import annotations

from typing import Any, Dict, List, Set, Tuple

from core.strategy.models import RankEntry


class MarketRankScanner:
    """Pure, stateless scanner for parsing, filtering, and selecting top gainer candidates."""

    @staticmethod
    def build_ranked_entries(top_gainers: List[Dict[str, Any]]) -> List[RankEntry]:
        """Convert raw exchange 24h ticker gainer payloads into structured RankEntry objects."""
        ranked: List[RankEntry] = []
        for item in top_gainers:
            try:
                ranked.append(
                    RankEntry(
                        symbol=str(item["symbol"]),
                        pct_change=float(item["change"]),
                        last_price=float(item["current_price"]),
                        quote_volume=float(item["volume"]),
                    )
                )
            except Exception:  # noqa: BLE001
                continue
        return ranked

    @staticmethod
    def filter_and_sort_ranked_entries(
        ranked: List[RankEntry],
        volume_threshold: float = 0.0,
    ) -> List[RankEntry]:
        """Sort ranked entries by pct_change descending, and filter by minimum quote volume."""
        sorted_entries = sorted(ranked, key=lambda item: item.pct_change, reverse=True)
        if volume_threshold > 0:
            return [item for item in sorted_entries if item.quote_volume >= volume_threshold]
        return sorted_entries

    @staticmethod
    def select_entry_candidates(
        ranked: List[RankEntry],
        open_symbols: Set[str],
        target_count: int,
    ) -> Tuple[List[RankEntry], List[str]]:
        """Select up to target_count candidates that are not already in open_symbols.

        Returns (candidates, skipped_symbols).
        """
        candidates: List[RankEntry] = []
        skipped_symbols: List[str] = []
        target = max(0, int(target_count))
        if target == 0:
            return candidates, skipped_symbols
        for entry in ranked:
            if entry.symbol in open_symbols:
                skipped_symbols.append(entry.symbol)
                continue
            candidates.append(entry)
            if len(candidates) >= target:
                break
        return candidates, skipped_symbols
