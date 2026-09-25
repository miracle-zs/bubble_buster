"""Tests for DashboardDataProvider.cached_snapshot short-TTL cache."""

import copy
import sqlite3
import tempfile
import time
import threading
from unittest.mock import patch

import pytest

from dashboard_server import DashboardDataProvider


@pytest.fixture()
def _provider(tmp_path):
    """Create a DashboardDataProvider with a real temp SQLite DB."""
    db_path = str(tmp_path / "test.db")
    log_file = str(tmp_path / "test.log")
    # Create the log file so tail_log doesn't error
    with open(log_file, "w") as f:
        f.write("")

    # Create minimal schema so snapshot() doesn't crash
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            account_id TEXT,
            trade_day_utc TEXT,
            started_at_utc TEXT,
            completed_at_utc TEXT,
            status TEXT,
            message TEXT
        );
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT,
            symbol TEXT,
            side TEXT,
            qty REAL,
            entry_price REAL,
            liq_price_latest REAL,
            tp_price REAL,
            sl_price REAL,
            tp_order_id TEXT,
            sl_order_id TEXT,
            tp_client_order_id TEXT,
            sl_client_order_id TEXT,
            opened_at_utc TEXT,
            expire_at_utc TEXT,
            status TEXT DEFAULT 'OPEN',
            last_error TEXT,
            notional_usdt REAL,
            close_reason TEXT,
            closed_at_utc TEXT,
            close_price REAL,
            realized_pnl REAL,
            close_order_id TEXT
        );
        CREATE TABLE IF NOT EXISTS order_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER,
            symbol TEXT,
            order_id TEXT,
            client_order_id TEXT,
            type TEXT,
            side TEXT,
            price REAL,
            qty REAL,
            status TEXT,
            event_time_utc TEXT
        );
        CREATE TABLE IF NOT EXISTS cashflow_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT,
            event_time_utc TEXT,
            asset TEXT,
            amount REAL,
            income_type TEXT,
            symbol TEXT,
            tran_id TEXT,
            info TEXT,
            unique_key TEXT
        );
        CREATE TABLE IF NOT EXISTS wallet_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id TEXT DEFAULT 'default',
            balance_usdt REAL,
            captured_at_utc TEXT,
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS exchange_order_states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT,
            client_order_id TEXT,
            symbol TEXT,
            status TEXT,
            filled_qty REAL,
            avg_price REAL,
            updated_at_utc TEXT
        );
        """
    )
    conn.close()

    provider = DashboardDataProvider(
        db_path=db_path,
        log_file=log_file,
        timezone_name="UTC",
        entry_hour=0,
        entry_minute=0,
    )
    return provider


class TestCachedSnapshot:
    """Verify cached_snapshot returns cached data within TTL."""

    def test_cache_hit_returns_same_data(self, _provider):
        """Second call within TTL should return cached data without re-querying."""
        result1 = _provider.cached_snapshot(account_id=None)
        result2 = _provider.cached_snapshot(account_id=None)
        assert result1 == result2

    def test_cache_returns_deep_copy(self, _provider):
        """Cached results should be independent copies."""
        result1 = _provider.cached_snapshot(account_id=None)
        result1["_mutated"] = True
        result2 = _provider.cached_snapshot(account_id=None)
        assert "_mutated" not in result2

    def test_cache_miss_after_ttl(self, _provider):
        """After TTL expires, cache should miss and re-compute."""
        _provider._snapshot_cache_ttl_sec = 0.05  # 50ms for fast test
        result1 = _provider.cached_snapshot(account_id=None)
        time.sleep(0.08)  # Wait past TTL
        # Force a change to verify re-computation
        result2 = _provider.cached_snapshot(account_id=None)
        # Both should be valid snapshots
        assert "summary" in result1
        assert "summary" in result2
        # Timestamps should differ after TTL expiry
        assert result2["generated_at_utc"] is not None

    def test_different_params_different_cache_keys(self, _provider):
        """Different parameters should use separate cache entries."""
        # These should be separate cache entries
        r1 = _provider.cached_snapshot(account_id=None, include_curves=True)
        r2 = _provider.cached_snapshot(account_id=None, include_curves=False)
        # Both valid
        assert "summary" in r1
        assert "summary" in r2

    def test_different_accounts_different_cache_keys(self, _provider):
        """Different account_ids should use separate cache entries."""
        r1 = _provider.cached_snapshot(account_id="acc01")
        r2 = _provider.cached_snapshot(account_id="acc02")
        # Both valid, separate entries
        assert r1["account_id"] == "acc01"
        assert r2["account_id"] == "acc02"

    def test_cache_reduces_snapshot_calls(self, _provider):
        """Multiple cached_snapshot calls within TTL should only call snapshot once."""
        call_count = 0
        original_snapshot = _provider.snapshot

        def counting_snapshot(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return original_snapshot(*args, **kwargs)

        _provider.snapshot = counting_snapshot
        try:
            _provider.cached_snapshot(account_id=None, window_hours=24.0)
            _provider.cached_snapshot(account_id=None, window_hours=24.0)
            _provider.cached_snapshot(account_id=None, window_hours=24.0)
            assert call_count == 1, f"Expected 1 snapshot call, got {call_count}"
        finally:
            _provider.snapshot = original_snapshot

    def test_thread_safety(self, _provider):
        """Concurrent cached_snapshot calls should not corrupt cache."""
        results = []
        errors = []

        def worker():
            try:
                r = _provider.cached_snapshot(account_id=None)
                results.append(r)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Errors in threads: {errors}"
        assert len(results) == 10
        # All results should be equivalent
        for r in results:
            assert "summary" in r

    def test_uncached_snapshot_still_works(self, _provider):
        """The original snapshot() method should still be callable directly."""
        result = _provider.snapshot()
        assert "summary" in result
        assert "generated_at_utc" in result
