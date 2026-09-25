"""Unit tests for core/task_status.py pure formatting functions."""

import pytest
from core.task_status import (
    append_summary_part,
    format_symbol_field,
    format_task_status,
    safe_float,
    safe_int,
    status_from_error_count,
    task_status_template,
)


class TestTaskStatus:
    def test_safe_conversions(self):
        assert safe_int("10") == 10
        assert safe_int(12.3) == 12
        assert safe_int("bad", default=5) == 5

        assert safe_float("12.34") == 12.34
        assert safe_float(None, default=1.0) == 1.0
        assert safe_float("bad", default=2.0) == 2.0

    def test_status_from_error_count(self):
        assert status_from_error_count(0, 10) == "SUCCESS"
        assert status_from_error_count(2, 5) == "PARTIAL"
        assert status_from_error_count(5, 0) == "FAILED"

    def test_format_symbol_field(self):
        assert format_symbol_field("BTCUSDT, ETHUSDT , BTCUSDT") == "BTCUSDT,ETHUSDT"
        assert format_symbol_field(["SOLUSDT", "BTCUSDT", "SOLUSDT"]) == "SOLUSDT,BTCUSDT"
        assert format_symbol_field([]) == "-"
        assert format_symbol_field(None) == "-"

    def test_append_summary_part(self):
        parts = ["total=1"]
        append_summary_part(parts, "errors", 2)
        assert parts == ["total=1", "errors=2"]

        # Blank or '-' are ignored
        append_summary_part(parts, "failed_symbols", "-")
        append_summary_part(parts, "skipped_symbols", "")
        assert parts == ["total=1", "errors=2"]

    def test_task_status_template(self):
        tmpl = task_status_template()
        assert set(tmpl.keys()) == {
            "entry",
            "daily_loss_cut",
            "noon_protection",
            "manage",
            "equity_recovery_take_profit",
        }
        for v in tmpl.values():
            assert v["status"] == "UNKNOWN"
            assert v["summary"] == "--"

    def test_format_entry(self):
        payload = {
            "status": "SUCCESS",
            "opened": 10,
            "failed": 0,
            "skipped": 2,
            "skipped_symbols": ["XRPUSDT"],
        }
        res = format_task_status("entry", payload, time_local="2026-03-01 07:30:00")
        assert res["status"] == "SUCCESS"
        assert res["time_local"] == "2026-03-01 07:30:00"
        assert "opened=10" in res["summary"]
        assert "failed=0" in res["summary"]
        assert "skipped=2" in res["summary"]
        assert "skipped_symbols=XRPUSDT" in res["summary"]

    def test_format_daily_loss_cut(self):
        payload = {
            "total": 3,
            "closed_loss_cut": 2,
            "errors": 1,
            "failed_symbols": ["SOLUSDT"],
        }
        res = format_task_status("daily_loss_cut", payload, time_local="2026-03-01 08:00:00")
        assert res["status"] == "PARTIAL"
        assert "total=3" in res["summary"]
        assert "closed=2" in res["summary"]
        assert "errors=1" in res["summary"]
        assert "failed_symbols=SOLUSDT" in res["summary"]

    def test_format_noon_protection(self):
        payload = {
            "total": 5,
            "updated_sl": 5,
            "skipped": 0,
            "errors": 0,
        }
        res = format_task_status("noon_protection", payload, time_local="2026-03-01 12:00:00")
        assert res["status"] == "SUCCESS"
        assert "total=5" in res["summary"]
        assert "updated=5" in res["summary"]

    def test_format_manage(self):
        payload = {
            "summary": {
                "total": 10,
                "closed_tp": 2,
                "closed_sl": 1,
                "closed_timeout": 0,
                "updated_sl": 3,
                "errors": 0,
            }
        }
        res = format_task_status("manage", payload, time_local="2026-03-01 12:01:00")
        assert res["status"] == "SUCCESS"
        assert "total=10 tp=2 sl=1 timeout=0 updated=3 errors=0" in res["summary"]

        # Manage skipped
        res_skipped = format_task_status("manage", {"skipped": True, "reason": "COOLDOWN"})
        assert res_skipped["status"] == "SKIPPED"
        assert res_skipped["summary"] == "reason=COOLDOWN"

        # Manage failed
        res_failed = format_task_status("manage", {"error": "Connection timeout"})
        assert res_failed["status"] == "FAILED"
        assert "error=Connection timeout" in res_failed["summary"]

    def test_format_equity_recovery(self):
        # Portfolio take-profit style
        payload = {
            "status": "TRIGGERED",
            "threshold_equity": 1000.0,
            "current_equity": 1050.0,
            "actual_profit_pct": 5.0,
            "closed_take_profit": 2,
            "errors": 0,
        }
        res = format_task_status("equity_recovery_take_profit", payload, time_local="2026-03-01 13:00:00")
        assert res["status"] == "SUCCESS"
        assert "equity=1050.00/1000.00" in res["summary"]
        assert "profit=5.00%" in res["summary"]
