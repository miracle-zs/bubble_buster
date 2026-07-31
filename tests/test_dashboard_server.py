import tempfile
import unittest
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Dict
from pathlib import Path

from dashboard_server import (
    DashboardDataProvider,
    _safe_query_int,
    render_account_dashboard_html,
    render_accounts_overview_html,
)
from core.state_store import StateStore


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

    def _backdate_runs(self, timestamp: str = "2026-01-01T00:00:00+00:00") -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("UPDATE runs SET started_at_utc = ?", (timestamp,))

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
        self.assertIn("cashflow_events", snapshot)
        self.assertIn("unpriced_closed_details", snapshot)
        self.assertIn("net_cashflow_usdt", snapshot["summary"])

    def test_snapshot_without_trade_stats_still_returns_curve_stats(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.store.scoped("acc01").add_wallet_snapshot(
            (now - timedelta(minutes=1)).isoformat(),
            1000.0,
            source="API",
        )
        self.store.scoped("acc01").add_wallet_snapshot(now.isoformat(), 990.0, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )

        snapshot = provider.snapshot(
            log_lines=0,
            window_hours=24,
            account_id="acc01",
            include_details=False,
            include_log=False,
            include_curves=True,
            include_balance_curve=True,
            include_trade_stats=False,
        )

        self.assertEqual(len(snapshot["strategy_equity_curve"]), 2)
        self.assertEqual(snapshot["drawdown_stats_strategy"]["realized_fill_count"], 0)

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

    def test_connect_ctx_closes_connection(self) -> None:
        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        conn = None
        with provider._connect_ctx() as active:
            conn = active
            active.execute("SELECT 1").fetchone()
        self.assertIsNotNone(conn)
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1").fetchone()  # type: ignore[union-attr]

    def test_connections_enable_wal_and_busy_timeout(self) -> None:
        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        conn = provider._connect()
        try:
            busy_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]
            journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        finally:
            conn.close()

        self.assertGreaterEqual(int(busy_timeout), 30000)
        self.assertEqual(str(journal_mode).lower(), "wal")

    def test_window_picker_refreshes_curve_without_core_request(self) -> None:
        html = render_account_dashboard_html(
            refresh_sec=5,
            account_id="acc01",
            echarts_src="/static/vendor/echarts.min.js",
        )

        handler = html.split('el.windowRow.addEventListener("click"', 1)[1].split(
            "setInterval(function ()",
            1,
        )[0]
        self.assertIn("refreshCurveFast();", handler)
        self.assertNotIn("refreshCore", handler)

    def test_accounts_summary_returns_grouped_metrics(self) -> None:
        run1, _ = self.store.create_run("2026-02-13", account_id="acc01")
        run2, _ = self.store.create_run("2026-02-13", account_id="acc02")
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        self.store.insert_position(
            run_id=run1,
            symbol="AUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=150.0,
            tp_price=90.0,
            sl_price=120.0,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc=now,
            expire_at_utc=now,
            status="OPEN",
        )
        self.store.insert_position(
            run_id=run2,
            symbol="BUSDT",
            side="SHORT",
            qty=2.0,
            entry_price=200.0,
            liq_price_open=250.0,
            tp_price=180.0,
            sl_price=220.0,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc=now,
            expire_at_utc=now,
            status="OPEN",
        )
        self.store.scoped("acc01").add_wallet_snapshot(now, 1000.0, source="API")
        self.store.scoped("acc02").add_wallet_snapshot(now, 2000.0, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        payload = provider.accounts_summary()
        account_ids = [row["account_id"] for row in payload["accounts"]]
        self.assertIn("acc01", account_ids)
        self.assertIn("acc02", account_ids)

    def test_accounts_summary_uses_configured_accounts_without_wallet_account_scan(self) -> None:
        run1, _ = self.store.create_run("2026-02-13", account_id="acc01")
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        self.store.insert_position(
            run_id=run1,
            symbol="AUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=150.0,
            tp_price=90.0,
            sl_price=120.0,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc=now,
            expire_at_utc=now,
            status="OPEN",
        )
        self.store.scoped("acc01").add_wallet_snapshot(now, 1000.0, source="API")
        self.store.scoped("acc02").add_wallet_snapshot(now, 2000.0, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            overview_account_ids=["acc01", "acc02"],
        )
        seen_sql = []
        original_query_rows = provider._query_rows

        def recording_query_rows(conn, sql, params=()):
            seen_sql.append(" ".join(str(sql).split()))
            return original_query_rows(conn, sql, params)

        provider._query_rows = recording_query_rows  # type: ignore[method-assign]

        payload = provider.accounts_summary()
        rows = {row["account_id"]: row for row in payload["accounts"]}

        self.assertEqual(set(rows), {"acc01", "acc02"})
        self.assertAlmostEqual(float(rows["acc01"]["wallet_balance_usdt"]), 1000.0)
        self.assertAlmostEqual(float(rows["acc02"]["wallet_balance_usdt"]), 2000.0)
        combined_sql = "\n".join(seen_sql)
        self.assertNotIn("SELECT DISTINCT account_id FROM wallet_snapshots", combined_sql)
        self.assertNotIn("MAX(id) AS max_id FROM wallet_snapshots GROUP BY account_id", combined_sql)

    def test_accounts_summary_includes_task_status_from_service_logs(self) -> None:
        self.store.create_run("2026-02-13", account_id="acc01")
        self.store.create_run("2026-02-13", account_id="acc02")
        self.store.create_run("2026-02-13", account_id="acc03")
        self._backdate_runs()
        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write(
                "\n".join(
                    [
                        "2026-02-27 07:40:15,865 - INFO - core.runtime_service - service entry result: {'acc01': {'status': 'SUCCESS', 'opened': 10, 'failed': 0, 'skipped': 0}, 'acc02': {'status': 'FAILED', 'opened': 2, 'failed': 8, 'skipped': 0}}",
                        "2026-02-27 11:55:01,180 - INFO - core.runtime_service - service daily loss-cut result: {'acc01': {'total': 16, 'closed_loss_cut': 5, 'errors': 0}, 'acc02': {'total': 10, 'closed_loss_cut': 4, 'errors': 1}}",
                        "2026-02-27 12:00:08,090 - INFO - core.runtime_service - service noon protection result: {'acc01': {'total': 11, 'updated_sl': 11, 'skipped': 0, 'errors': 0}, 'acc02': {'total': 6, 'updated_sl': 1, 'skipped': 0, 'errors': 5}}",
                        "2026-02-27 12:01:08,090 - INFO - core.runtime_service - service manage summary: {'acc01': {'account_id': 'acc01', 'summary': {'total': 3, 'closed_tp': 0, 'closed_sl': 0, 'closed_timeout': 0, 'closed_external': 0, 'updated_sl': 0, 'errors': 0}}, 'acc02': {'account_id': 'acc02', 'error': 'cooling-off'}}",
                        "2026-02-27 12:02:08,090 - INFO - core.runtime_service - service equity recovery take-profit account=acc01 result: {'status': 'TRIGGERED', 'adjusted': 4, 'errors': 0, 'reduced_notional': 320.5}",
                    ]
                )
                + "\n"
            )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        payload = provider.accounts_summary()
        rows = {row["account_id"]: row for row in payload["accounts"]}

        self.assertEqual(rows["acc01"]["tasks"]["entry"]["status"], "SUCCESS")
        self.assertIn("opened=10", rows["acc01"]["tasks"]["entry"]["summary"])
        self.assertEqual(rows["acc02"]["tasks"]["entry"]["status"], "FAILED")
        self.assertEqual(rows["acc02"]["tasks"]["daily_loss_cut"]["status"], "PARTIAL")
        self.assertEqual(rows["acc02"]["tasks"]["noon_protection"]["status"], "PARTIAL")
        self.assertEqual(rows["acc02"]["tasks"]["manage"]["status"], "FAILED")
        self.assertEqual(rows["acc01"]["tasks"]["equity_recovery_take_profit"]["status"], "SUCCESS")
        self.assertEqual(rows["acc03"]["tasks"]["entry"]["status"], "RUNNING")

    def test_accounts_summary_falls_back_to_persisted_entry_run_when_log_is_missing(self) -> None:
        run_id, _ = self.store.create_run("2026-07-18", account_id="acc01")
        self.store.finalize_run(
            run_id,
            "SUCCESS",
            "run_id=x, opened=4, failed=1, entry_failed=1, exit_setup_failed=0, skipped_existing=2",
        )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )

        payload = provider.accounts_summary()
        row = next(item for item in payload["accounts"] if item["account_id"] == "acc01")

        self.assertEqual(row["tasks"]["entry"]["status"], "SUCCESS")
        self.assertIn("opened=4", row["tasks"]["entry"]["summary"])
        self.assertIn("failed=1", row["tasks"]["entry"]["summary"])
        self.assertIn("skipped=2", row["tasks"]["entry"]["summary"])
        self.assertIsNotNone(row["tasks"]["entry"]["time_local"])

    def test_accounts_summary_reports_persisted_bearish_entry_wait_as_running(self) -> None:
        run_id, _ = self.store.create_run("2026-07-18", account_id="acc01")
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.store.insert_position(
            run_id=run_id,
            symbol="BTCUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=150.0,
            tp_price=None,
            sl_price=120.0,
            tp_order_id=None,
            sl_order_id=1001,
            tp_client_order_id=None,
            sl_client_order_id="sl-btc",
            opened_at_utc=now.isoformat(),
            expire_at_utc=(now + timedelta(days=2)).isoformat(),
            status="OPEN",
        )
        self.store.scoped("acc01").set_lock_state(
            "bearish_hour_entry_wait_v1",
            {
                "run_id": run_id,
                "deadline_utc": (now + timedelta(hours=8)).isoformat(),
                "updated_at_utc": now.isoformat(),
                "pending": {
                    "1": {
                        "symbol": "ETHUSDT",
                        "signal_time_utc": now.isoformat(),
                        "hour_open_utc": now.replace(minute=0, second=0).isoformat(),
                    },
                    "2": {
                        "symbol": "SOLUSDT",
                        "signal_time_utc": now.isoformat(),
                        "hour_open_utc": now.replace(minute=0, second=0).isoformat(),
                    },
                },
            },
        )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )

        payload = provider.accounts_summary()
        row = next(item for item in payload["accounts"] if item["account_id"] == "acc01")

        self.assertEqual(row["tasks"]["entry"]["status"], "RUNNING")
        self.assertIn("waiting=2", row["tasks"]["entry"]["summary"])
        progress = row["entry_progress"]
        self.assertEqual(progress["status"], "WAITING")
        self.assertEqual(progress["target_count"], 3)
        self.assertEqual(progress["opened_count"], 1)
        self.assertEqual(progress["waiting_count"], 2)
        self.assertEqual([item["symbol"] for item in progress["opened_symbols"]], ["BTCUSDT"])
        self.assertEqual(
            [item["symbol"] for item in progress["waiting_symbols"]],
            ["ETHUSDT", "SOLUSDT"],
        )
        self.assertIsNotNone(progress["next_check_local"])
        self.assertIsNotNone(progress["deadline_local"])

    def test_accounts_summary_keeps_completed_entry_progress_after_wait_state_is_cleared(self) -> None:
        run_id, _ = self.store.create_run("2026-07-18", account_id="acc01")
        now = datetime.now(timezone.utc).replace(microsecond=0)
        for symbol in ("BTCUSDT", "ETHUSDT"):
            self.store.insert_position(
                run_id=run_id,
                symbol=symbol,
                side="SHORT",
                qty=1.0,
                entry_price=100.0,
                liq_price_open=150.0,
                tp_price=None,
                sl_price=120.0,
                tp_order_id=None,
                sl_order_id=1001,
                tp_client_order_id=None,
                sl_client_order_id=f"sl-{symbol}",
                opened_at_utc=now.isoformat(),
                expire_at_utc=(now + timedelta(days=2)).isoformat(),
                status="OPEN",
            )
        self.store.finalize_run(
            run_id,
            "SUCCESS",
            "run_id=x, opened=2, failed=2, entry_failed=1, exit_setup_failed=1, skipped_existing=1",
        )
        self.store.scoped("acc01").set_lock_state("bearish_hour_entry_wait_v1", {})

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )

        row = provider.accounts_summary()["accounts"][0]
        progress = row["entry_progress"]
        self.assertEqual(progress["status"], "PARTIAL")
        self.assertEqual(progress["target_count"], 4)
        self.assertEqual(progress["opened_count"], 2)
        self.assertEqual(progress["waiting_count"], 0)
        self.assertEqual(progress["failed_count"], 2)
        self.assertEqual(progress["entry_failed_count"], 1)
        self.assertEqual(progress["exit_setup_failed_count"], 1)
        self.assertEqual(progress["skipped_count"], 1)
        self.assertEqual(
            [item["symbol"] for item in progress["opened_symbols"]],
            ["BTCUSDT", "ETHUSDT"],
        )

    def test_entry_progress_uses_entry_cycle_instead_of_calendar_day(self) -> None:
        run_id, _ = self.store.create_run("2026-07-24", account_id="acc01")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE runs
                SET started_at_utc = ?, completed_at_utc = ?, status = ?, message = ?
                WHERE run_id = ?
                """,
                (
                    "2026-07-24T23:40:00+00:00",
                    "2026-07-25T07:00:00+00:00",
                    "SUCCESS",
                    "opened=10, failed=0, entry_failed=0, exit_setup_failed=0, skipped_existing=0",
                    run_id,
                ),
            )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="Asia/Shanghai",
            entry_hour=7,
            entry_minute=40,
        )
        with provider._connect_ctx() as conn:
            before_next_entry = provider._entry_progresses_from_db(
                conn,
                ["acc01"],
                now_local=datetime(2026, 7, 26, 1, 30, tzinfo=provider.local_tz),
            )
            after_next_entry = provider._entry_progresses_from_db(
                conn,
                ["acc01"],
                now_local=datetime(2026, 7, 26, 7, 40, tzinfo=provider.local_tz),
            )

        self.assertTrue(before_next_entry["acc01"]["is_today"])
        self.assertFalse(after_next_entry["acc01"]["is_today"])

    def test_accounts_summary_finds_morning_entry_result_in_large_current_log(self) -> None:
        self.store.create_run("2026-06-29", account_id="acc01")
        self._backdate_runs()
        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write(
                "2026-06-29 07:40:14,932 - INFO - core.runtime_service - "
                "service entry result: {'acc01': {'status': 'SUCCESS', 'opened': 9, "
                "'failed': 1, 'skipped': 0, 'entry_failed_symbols': ['OUSDT']}}\n"
            )
            for i in range(25001):
                f.write(
                    f"2026-06-29 15:00:{i % 60:02d},000 - INFO - core.runtime_service - "
                    f"service wallet snapshot account=acc01: {{'snapshot_id': {i}}}\n"
                )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        payload = provider.accounts_summary()
        row = payload["accounts"][0]

        self.assertEqual(row["tasks"]["entry"]["status"], "SUCCESS")
        self.assertEqual(row["tasks"]["entry"]["time_local"], "2026-06-29 07:40:14")
        self.assertIn("opened=9", row["tasks"]["entry"]["summary"])
        self.assertIn("failed_symbols=OUSDT", row["tasks"]["entry"]["summary"])

    def test_accounts_summary_keeps_full_symbol_lists_for_task_details(self) -> None:
        self.store.create_run("2026-02-13", account_id="acc01")
        self._backdate_runs()
        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write(
                "\n".join(
                    [
                        "2026-02-27 07:40:15,865 - INFO - core.runtime_service - service entry result: {'acc01': {'status': 'PARTIAL', 'opened': 5, 'failed': 4, 'skipped': 1, 'entry_failed_symbols': ['OPNUSDT', 'METUSDT', 'BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'DOGEUSDT'], 'skipped_symbols': ['XRPUSDT']}}",
                        "2026-02-27 11:55:01,180 - INFO - core.runtime_service - service daily loss-cut result: {'acc01': {'total': 7, 'closed_loss_cut': 3, 'errors': 1, 'closed_symbols': ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'DOGEUSDT'], 'failed_symbols': ['OPNUSDT']}}",
                    ]
                )
                + "\n"
            )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        payload = provider.accounts_summary()
        rows = {row["account_id"]: row for row in payload["accounts"]}

        self.assertIn(
            "failed_symbols=OPNUSDT,METUSDT,BTCUSDT,ETHUSDT,SOLUSDT,DOGEUSDT",
            rows["acc01"]["tasks"]["entry"]["summary"],
        )
        self.assertIn("skipped_symbols=XRPUSDT", rows["acc01"]["tasks"]["entry"]["summary"])
        self.assertIn(
            "closed_symbols=BTCUSDT,ETHUSDT,SOLUSDT,DOGEUSDT",
            rows["acc01"]["tasks"]["daily_loss_cut"]["summary"],
        )
        self.assertIn("failed_symbols=OPNUSDT", rows["acc01"]["tasks"]["daily_loss_cut"]["summary"])

    def test_account_snapshot_filters_by_account_id(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        run1, _ = self.store.create_run("2026-02-13", account_id="acc01")
        run2, _ = self.store.create_run("2026-02-13", account_id="acc02")
        self.store.insert_position(
            run_id=run1,
            symbol="ACC01USDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=150.0,
            tp_price=90.0,
            sl_price=120.0,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc=now,
            expire_at_utc=now,
            status="OPEN",
        )
        self.store.insert_position(
            run_id=run2,
            symbol="ACC02USDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=150.0,
            tp_price=90.0,
            sl_price=120.0,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc=now,
            expire_at_utc=now,
            status="OPEN",
        )
        self.store.scoped("acc01").add_wallet_snapshot(now, 111.0, source="API")
        self.store.scoped("acc02").add_wallet_snapshot(now, 222.0, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        payload = provider.snapshot(log_lines=0, account_id="acc01")
        symbols = {row["symbol"] for row in payload["open_positions"]}
        self.assertEqual(symbols, {"ACC01USDT"})
        self.assertEqual(payload["account_id"], "acc01")

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
            "realizedPnl": "40",
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

        strategy_stats = first["drawdown_stats_strategy"]
        balance_stats = first["drawdown_stats_balance"]
        self.assertAlmostEqual(strategy_stats["total_realized_pnl"], 0.0)
        self.assertEqual(strategy_stats["closed_trades_priced"], 1)
        self.assertAlmostEqual(strategy_stats["win_rate_pct"], 100.0)
        self.assertAlmostEqual(strategy_stats["trade_realized_pnl"], 40.0)
        self.assertAlmostEqual(strategy_stats["gross_profit"], 40.0)
        self.assertAlmostEqual(strategy_stats["gross_loss_abs"], 0.0)
        self.assertAlmostEqual(strategy_stats["avg_win"], 40.0)
        self.assertAlmostEqual(strategy_stats["avg_loss_abs"], 0.0)
        self.assertIsNone(strategy_stats["profit_factor"])
        self.assertAlmostEqual(strategy_stats["net_cashflow_usdt"], 0.0)
        self.assertAlmostEqual(balance_stats["wallet_balance_usdt"], 120.0)
        self.assertEqual(balance_stats["closed_trades_priced"], 0)

        curve = first["balance_curve"]
        self.assertEqual(len(curve), 1)
        self.assertAlmostEqual(curve[0]["pnl"], 0.0)
        self.assertAlmostEqual(curve[0]["cum_pnl"], 0.0)
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

    def test_equity_curve_prefers_wallet_snapshots(self) -> None:
        self.store.add_wallet_snapshot("2026-02-13T00:00:00+00:00", 100.0, source="API")
        self.store.add_wallet_snapshot("2026-02-13T00:01:00+00:00", 95.0, source="API")
        self.store.add_wallet_snapshot("2026-02-13T00:02:00+00:00", 110.0, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            balance_fetcher=lambda: 110.0,
            balance_cache_ttl_sec=60,
        )
        snapshot = provider.snapshot(log_lines=0)
        curve = snapshot["balance_curve"]
        stats = snapshot["drawdown_stats_balance"]

        self.assertEqual(len(curve), 4)  # 3 seeded + 1 live API snapshot persisted
        self.assertAlmostEqual(curve[0]["equity"], 100.0)
        self.assertAlmostEqual(curve[1]["equity"], 95.0)
        self.assertAlmostEqual(curve[-1]["equity"], 110.0)
        self.assertAlmostEqual(stats["max_drawdown"], 5.0)
        self.assertAlmostEqual(stats["max_drawdown_pct"], 5.0)

    def test_invalid_wallet_snapshot_does_not_poison_dashboard_equity(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.store.add_wallet_snapshot(
            (now - timedelta(minutes=1)).isoformat(),
            1000.0,
            source="API",
        )
        self.store.add_wallet_snapshot(
            now.isoformat(),
            1200.0,
            source="API",
            error="position_risk: network down",
        )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        snapshot = provider.snapshot(
            log_lines=0,
            window_hours=24,
            include_details=False,
            include_log=False,
            include_curves=True,
            include_balance_curve=True,
            include_trade_stats=False,
        )

        self.assertEqual(snapshot["wallet"]["balance_usdt"], 1000.0)
        self.assertEqual(snapshot["balance_curve"][-1]["equity"], 1000.0)

    def test_strategy_equity_ignores_cashflow(self) -> None:
        self.store.add_wallet_snapshot("2026-02-13T00:00:00+00:00", 100.0, source="API")
        self.store.add_wallet_snapshot("2026-02-13T00:01:00+00:00", 130.0, source="API")
        self.store.add_wallet_snapshot("2026-02-13T00:02:00+00:00", 125.0, source="API")
        self.store.add_cashflow_event(
            event_time_utc="2026-02-13T00:00:30+00:00",
            asset="USDT",
            amount=30.0,
            income_type="TRANSFER",
            tran_id="t-1",
        )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        snapshot = provider.snapshot(log_lines=0)
        strategy_curve = snapshot["strategy_equity_curve"]
        balance_curve = snapshot["balance_curve"]
        strategy_stats = snapshot["drawdown_stats_strategy"]

        self.assertEqual([round(x["equity"], 8) for x in balance_curve[:3]], [100.0, 130.0, 125.0])
        self.assertEqual([round(x["equity"], 8) for x in strategy_curve[:3]], [100.0, 100.0, 95.0])
        self.assertAlmostEqual(strategy_stats["net_cashflow_usdt"], 30.0)
        self.assertAlmostEqual(strategy_stats["total_realized_pnl"], -5.0)

    def test_strategy_equity_starts_equal_to_balance_when_prior_cashflow_exists(self) -> None:
        self.store.add_cashflow_event(
            event_time_utc="2026-02-12T23:59:00+00:00",
            asset="USDT",
            amount=50.0,
            income_type="TRANSFER",
            tran_id="t-prior",
        )
        self.store.add_wallet_snapshot("2026-02-13T00:00:00+00:00", 100.0, source="API")
        self.store.add_wallet_snapshot("2026-02-13T00:01:00+00:00", 120.0, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        snapshot = provider.snapshot(log_lines=0)
        strategy_curve = snapshot["strategy_equity_curve"]
        balance_curve = snapshot["balance_curve"]
        strategy_stats = snapshot["drawdown_stats_strategy"]

        self.assertEqual(round(strategy_curve[0]["equity"], 8), round(balance_curve[0]["equity"], 8))
        self.assertEqual(round(strategy_curve[1]["equity"], 8), round(balance_curve[1]["equity"], 8))
        self.assertAlmostEqual(strategy_stats["net_cashflow_usdt"], 0.0)

    def test_curve_window_uses_resample_instead_of_tail_limit(self) -> None:
        base = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=179)
        for i in range(180):
            ts = (base + timedelta(minutes=i)).isoformat()
            equity = 1000.0 + (35.0 if i % 2 == 0 else -22.0) + i * 0.1
            self.store.add_wallet_snapshot(ts, equity, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        snapshot = provider.snapshot(log_lines=0, window_hours=12, curve_points=100)
        balance_curve = snapshot["balance_curve"]
        strategy_curve = snapshot["strategy_equity_curve"]

        self.assertLessEqual(len(balance_curve), 100)
        self.assertLessEqual(len(strategy_curve), 100)
        self.assertEqual(balance_curve[0]["t"], base.isoformat())
        self.assertEqual(balance_curve[-1]["t"], (base + timedelta(minutes=179)).isoformat())
        self.assertEqual(strategy_curve[0]["t"], base.isoformat())
        self.assertEqual(strategy_curve[-1]["t"], (base + timedelta(minutes=179)).isoformat())

    def test_trade_outcome_stats_require_exchange_realized_pnl(self) -> None:
        run_id, _ = self.store.create_run("2026-02-15")
        now = datetime.now(timezone.utc).replace(microsecond=0)
        position_id = self.store.insert_position(
            run_id=run_id,
            symbol="AZTECUSDT",
            side="SHORT",
            qty=10,
            entry_price=1.0,
            liq_price_open=2.0,
            tp_price=0.8,
            sl_price=1.2,
            tp_order_id=7001,
            sl_order_id=7002,
            tp_client_order_id="tp-az",
            sl_client_order_id="sl-az",
            opened_at_utc=now.isoformat(),
            expire_at_utc=now.isoformat(),
            status="OPEN",
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_EXTERNAL",
            close_reason="SHORT_POSITION_NOT_FOUND",
            close_order_id=None,
        )

        calls = {"ids": []}

        def _mock_close_price_fetcher(symbol: str, order_id: int) -> float | None:
            calls["ids"].append(order_id)
            if symbol == "AZTECUSDT" and order_id == 7001:
                return 0.8
            return None

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            close_price_fetcher=_mock_close_price_fetcher,
        )
        snapshot = provider.snapshot(log_lines=0)
        strategy_stats = snapshot["drawdown_stats_strategy"]
        unpriced = snapshot["unpriced_closed_details"]

        self.assertEqual(strategy_stats["closed_trades_priced"], 0)
        self.assertAlmostEqual(strategy_stats["trade_realized_pnl"], 0.0)
        self.assertEqual(len(unpriced), 1)
        self.assertEqual(unpriced[0]["detected_reason"], "MISSING_EXCHANGE_REALIZED_PNL")
        self.assertEqual(calls["ids"], [])

    def test_trade_outcome_stats_include_profit_factor_and_avg_ratio(self) -> None:
        run_id, _ = self.store.create_run("2026-02-16")
        now = datetime.now(timezone.utc).replace(microsecond=0)

        win_id = self.store.insert_position(
            run_id=run_id,
            symbol="WINUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=130.0,
            tp_price=80.0,
            sl_price=120.0,
            tp_order_id=8101,
            sl_order_id=8102,
            tp_client_order_id="tp-win",
            sl_client_order_id="sl-win",
            opened_at_utc=now.isoformat(),
            expire_at_utc=now.isoformat(),
            status="OPEN",
        )
        self.store.add_order_event(
            symbol="WINUSDT",
            position_id=win_id,
            event_time_utc=now.isoformat(),
            order_payload={
                "orderId": 8101,
                "clientOrderId": "tp-win",
                "type": "MARKET",
                "side": "BUY",
                "price": "90",
                "origQty": "1",
                "executedQty": "1",
                "realizedPnl": "10",
                "status": "FILLED",
            },
        )
        self.store.mark_position_closed(
            position_id=win_id,
            status="CLOSED_TP",
            close_reason="TAKE_PROFIT_FILLED",
            close_order_id=8101,
        )

        loss_id = self.store.insert_position(
            run_id=run_id,
            symbol="LOSSUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=130.0,
            tp_price=80.0,
            sl_price=120.0,
            tp_order_id=8201,
            sl_order_id=8202,
            tp_client_order_id="tp-loss",
            sl_client_order_id="sl-loss",
            opened_at_utc=now.isoformat(),
            expire_at_utc=now.isoformat(),
            status="OPEN",
        )
        self.store.add_order_event(
            symbol="LOSSUSDT",
            position_id=loss_id,
            event_time_utc=now.isoformat(),
            order_payload={
                "orderId": 8202,
                "clientOrderId": "sl-loss",
                "type": "MARKET",
                "side": "BUY",
                "price": "110",
                "origQty": "1",
                "executedQty": "1",
                "realizedPnl": "-10",
                "status": "FILLED",
            },
        )
        self.store.mark_position_closed(
            position_id=loss_id,
            status="CLOSED_SL",
            close_reason="STOP_LOSS_FILLED",
            close_order_id=8202,
        )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        snapshot = provider.snapshot(log_lines=0)
        stats = snapshot["drawdown_stats_strategy"]

        self.assertEqual(stats["closed_trades_priced"], 2)
        self.assertAlmostEqual(stats["gross_profit"], 10.0)
        self.assertAlmostEqual(stats["gross_loss_abs"], 10.0)
        self.assertAlmostEqual(stats["avg_win"], 10.0)
        self.assertAlmostEqual(stats["avg_loss_abs"], 10.0)
        self.assertAlmostEqual(stats["profit_factor"], 1.0)
        self.assertAlmostEqual(stats["avg_win_loss_ratio"], 1.0)

    def test_trade_outcome_stats_use_exchange_realized_pnl_for_partial_exits(self) -> None:
        run_id, _ = self.store.create_run("2026-02-17")
        now = datetime.now(timezone.utc).replace(microsecond=0)
        position_id = self.store.insert_position(
            run_id=run_id,
            symbol="PARTIALUSDT",
            side="SHORT",
            qty=10.0,
            entry_price=100.0,
            liq_price_open=130.0,
            tp_price=80.0,
            sl_price=120.0,
            tp_order_id=8301,
            sl_order_id=8302,
            tp_client_order_id="tp-partial",
            sl_client_order_id="sl-partial",
            opened_at_utc=now.isoformat(),
            expire_at_utc=now.isoformat(),
            status="OPEN",
        )
        self.store.add_order_event(
            symbol="PARTIALUSDT",
            position_id=position_id,
            event_time_utc=now.isoformat(),
            order_payload={
                "orderId": 8301,
                "clientOrderId": "tp-partial-1",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "4",
                "executedQty": "4",
                "avgPrice": "90",
                "realizedPnl": "4.0",
                "commission": "0.04",
                "commissionAsset": "USDT",
                "status": "FILLED",
            },
        )
        self.store.add_order_event(
            symbol="PARTIALUSDT",
            position_id=position_id,
            event_time_utc=(now + timedelta(seconds=1)).isoformat(),
            order_payload={
                "orderId": 8302,
                "clientOrderId": "tp-partial-2",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "6",
                "executedQty": "6",
                "avgPrice": "105",
                "realizedPnl": "-3.0",
                "commission": "0.06",
                "commissionAsset": "USDT",
                "status": "FILLED",
            },
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_TP",
            close_reason="TAKE_PROFIT_FILLED",
            close_order_id=8302,
        )

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )

        stats = provider.snapshot(log_lines=0)["drawdown_stats_strategy"]

        self.assertEqual(stats["closed_trades_priced"], 2)
        self.assertAlmostEqual(stats["trade_realized_pnl"], 1.0)
        self.assertAlmostEqual(stats["gross_profit"], 4.0)
        self.assertAlmostEqual(stats["gross_loss_abs"], 3.0)
        self.assertAlmostEqual(stats["trading_fees_usdt"], 0.1)
        self.assertAlmostEqual(stats["net_trade_pnl"], 0.9)

    def test_strategy_stats_include_all_time_account_pnl_adjusted_for_later_cashflows(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        baseline = now - timedelta(days=2)
        self.store.add_cashflow_event(
            event_time_utc=(baseline - timedelta(minutes=1)).isoformat(),
            asset="USDT",
            amount=1000.0,
            income_type="TRANSFER",
            tran_id="before-baseline",
        )
        self.store.add_wallet_snapshot(baseline.isoformat(), 1000.0, source="API")
        self.store.add_cashflow_event(
            event_time_utc=(baseline + timedelta(minutes=1)).isoformat(),
            asset="USDT",
            amount=50.0,
            income_type="TRANSFER",
            tran_id="after-baseline",
        )
        self.store.add_wallet_snapshot(now.isoformat(), 371.0, source="API")

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )

        stats = provider.snapshot(log_lines=0, window_hours=24)["drawdown_stats_strategy"]

        self.assertAlmostEqual(stats["all_time_account_pnl"], -679.0)
        self.assertAlmostEqual(stats["all_time_account_cashflow_usdt"], 50.0)
        self.assertAlmostEqual(stats["all_time_account_baseline_usdt"], 1000.0)

    def test_cashflow_stats_and_details_deduplicate_same_transfer_id(self) -> None:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.store.add_wallet_snapshot((now - timedelta(hours=2)).isoformat(), 1000.0, source="API")
        self.store.add_wallet_snapshot(now.isoformat(), 900.0, source="API")
        with sqlite3.connect(self.db_path) as conn:
            for row_id in (1, 2):
                conn.execute(
                    """
                    INSERT INTO cashflow_events (
                        account_id, unique_key, event_time_utc, asset, amount, income_type,
                        symbol, tran_id, info, raw_json, created_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "default",
                        f"duplicate-{row_id}",
                        (now - timedelta(hours=1)).isoformat(),
                        "USDT",
                        50.0,
                        "TRANSFER",
                        None,
                        "same-transfer",
                        "TRANSFER",
                        None,
                        now.isoformat(),
                    ),
                )
            conn.commit()

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
        )
        snapshot = provider.snapshot(log_lines=0, window_hours=24)

        self.assertAlmostEqual(snapshot["drawdown_stats_strategy"]["all_time_account_cashflow_usdt"], 50.0)
        self.assertEqual(len(snapshot["cashflow_events"]), 1)

    def test_render_account_dashboard_html_escapes_account_id(self) -> None:
        html = render_account_dashboard_html(
            refresh_sec=5,
            account_id='acc01";alert(1);//',
        )
        self.assertIn("encodeURIComponent(accountId)", html)
        self.assertNotIn('/api/account/acc01";alert(1);//', html)
        self.assertIn("All-time Equity Change", html)
        self.assertIn("Window Cashflow", html)
        self.assertNotIn("Recorded Exchange Realized PnL", html)
        self.assertNotIn("Gross Profit", html)
        self.assertNotIn("Profit Factor", html)
        self.assertNotIn('id="winRate"', html)

    def test_render_overview_uses_readable_task_layout(self) -> None:
        html = render_accounts_overview_html(
            refresh_sec=5,
            portfolio_loss_cut_enabled=True,
            portfolio_loss_cut_pct=3.5,
        )
        self.assertIn('class="command-bar"', html)
        self.assertIn("组合止损 -3.5% 已启用", html)
        self.assertIn('class="managed-grid"', html)
        self.assertIn('class="readonly-strip"', html)
        self.assertIn('class="strategy-popover"', html)
        self.assertIn('class="strategy-trigger"', html)
        self.assertIn("策略配置", html)
        self.assertIn("formatStrategyNote(r.strategy_note)", html)
        self.assertIn("strategy-tag-primary", html)
        self.assertIn("strategy-tag-protection", html)
        self.assertIn("strategy-tag-off", html)
        self.assertIn("距周期高点回撤", html)
        self.assertIn('if (value < 0) return "status-warn";', html)
        self.assertIn("stop-meter-label", html)
        self.assertIn("已超出 ", html)
        self.assertNotIn("spark-grid-line", html)
        self.assertIn("closeStrategyPopovers", html)
        self.assertIn('event.key !== "Escape"', html)
        self.assertIn("近30日盈亏", html)
        self.assertIn("盈亏比", html)
        self.assertIn("mode === \"readonly\"", html)
        self.assertIn("? (payload.balance_curve || payload.equity_curve || [])", html)
        self.assertIn("entry-progress-details", html)
        self.assertIn('id="entry-progress-toggle"', html)
        self.assertIn("暂无异常", html)
        self.assertIn('id="entry-progress-board"', html)
        self.assertIn('id="entry-progress-updated"', html)
        self.assertIn("今日开单进度", html)
        self.assertIn("renderEntryProgress", html)
        self.assertIn("buildCommonEntryTimeline", html)
        self.assertIn("entry-progress-overview", html)
        self.assertIn("entry-progress-timeline", html)
        self.assertIn("entry-progress-meter", html)
        self.assertIn("与共同榜单一致", html)
        self.assertIn("等待1h阴线", html)
        self.assertNotIn("entry-progress-symbols", html)
        self.assertNotIn("entry-progress-deadline", html)
        self.assertIn('id="task-board"', html)
        self.assertIn('id="task-updated-at"', html)
        self.assertIn('id="task-filter-all"', html)
        self.assertIn('id="task-filter-anomaly"', html)
        self.assertIn('id="task-filter-symbols"', html)
        self.assertIn("renderTaskBoard", html)
        self.assertIn("renderTaskBoardHeader", html)
        self.assertIn("toggleTaskFilter", html)
        self.assertIn("formatTaskResultLines", html)
        self.assertIn("sortTaskAccounts", html)
        self.assertIn("return (rows || []).slice();", html)
        self.assertIn(".task-table", html)
        self.assertIn(".task-row", html)
        self.assertIn(".task-mode-badge", html)
        self.assertIn(".task-result-lines", html)
        self.assertIn(".task-filter-chip", html)
        self.assertIn(".task-result", html)
        self.assertIn("组合止盈监控", html)
        self.assertIn("巡检内触发", html)
        self.assertIn('fullText += (fullText ? "\\n" : "")', html)
        self.assertIn('taskUpdatedAt.textContent = "数据更新时间 " + (latest || "--")', html)
        self.assertIn('var timeText = timeRaw || "--";', html)
        self.assertIn("grid-template-columns: minmax(170px,1.1fr) 120px 180px minmax(260px,1.6fr);", html)
        self.assertIn("grid-template-columns: minmax(120px,1fr) 92px 160px minmax(180px,1.2fr);", html)
        self.assertNotIn("var aAnomaly = hasAnomalyTask(a) ? 0 : 1;", html)
        self.assertNotIn("rows.sort(function (a, b)", html)
        self.assertNotIn("toggleSymbolDetail", html)
        self.assertNotIn(".task-symbol-toggle", html)
        self.assertNotIn('title="', html)

    def test_render_overview_throttles_refresh_and_uses_lightweight_curve_points(self) -> None:
        html = render_accounts_overview_html(refresh_sec=5)
        self.assertIn('<span id="refresh">15</span>', html)
        self.assertIn("setInterval(fetchSummary, Math.max(15, refreshSec) * 1000)", html)
        self.assertIn('/curve"', html)
        self.assertIn("?window_hours=24&curve_points=160", html)

    def test_safe_query_int_handles_invalid_values(self) -> None:
        self.assertEqual(_safe_query_int("abc", default=80, min_value=0, max_value=300), 80)
        self.assertEqual(_safe_query_int(None, default=80, min_value=0, max_value=300), 80)
        self.assertEqual(_safe_query_int("9999", default=80, min_value=0, max_value=300), 300)
        self.assertEqual(_safe_query_int("-1", default=80, min_value=0, max_value=300), 0)

    def test_live_wallet_snapshot_uses_configured_account_id(self) -> None:
        calls = {"n": 0}

        def _mock_balance_fetcher() -> float:
            calls["n"] += 1
            return 321.0

        provider = DashboardDataProvider(
            db_path=self.db_path,
            log_file=self.log_file,
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            balance_fetcher=_mock_balance_fetcher,
            live_wallet_account_id="acc01",
        )
        snapshot = provider.snapshot(log_lines=0)
        self.assertEqual(snapshot["wallet"]["source"], "DB")
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT account_id, balance_usdt FROM wallet_snapshots ORDER BY id DESC LIMIT 1"
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(str(row[0]), "acc01")
        self.assertAlmostEqual(float(row[1]), 321.0)


if __name__ == "__main__":
    unittest.main()
