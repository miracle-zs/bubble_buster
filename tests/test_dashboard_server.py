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

    def test_accounts_summary_includes_task_status_from_service_logs(self) -> None:
        self.store.create_run("2026-02-13", account_id="acc01")
        self.store.create_run("2026-02-13", account_id="acc02")
        self.store.create_run("2026-02-13", account_id="acc03")
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
        self.assertEqual(rows["acc03"]["tasks"]["entry"]["status"], "UNKNOWN")

    def test_accounts_summary_keeps_full_symbol_lists_for_task_details(self) -> None:
        self.store.create_run("2026-02-13", account_id="acc01")
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

    def test_close_price_fetcher_falls_back_to_tp_sl_order_ids(self) -> None:
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

        self.assertEqual(strategy_stats["closed_trades_priced"], 1)
        self.assertAlmostEqual(strategy_stats["trade_realized_pnl"], 2.0)
        self.assertEqual(unpriced, [])
        self.assertIn(7001, calls["ids"])

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

    def test_render_account_dashboard_html_escapes_account_id(self) -> None:
        html = render_account_dashboard_html(
            refresh_sec=5,
            account_id='acc01";alert(1);//',
        )
        self.assertIn("encodeURIComponent(accountId)", html)
        self.assertNotIn('/api/account/acc01";alert(1);//', html)

    def test_render_overview_uses_readable_task_layout(self) -> None:
        html = render_accounts_overview_html(refresh_sec=5)
        self.assertIn('id="task-board"', html)
        self.assertIn('id="task-updated-at"', html)
        self.assertIn('id="task-filter-all"', html)
        self.assertIn('id="task-filter-anomaly"', html)
        self.assertIn('id="task-filter-symbols"', html)
        self.assertIn("renderTaskBoard", html)
        self.assertIn("renderTaskBoardHeader", html)
        self.assertIn("toggleTaskFilter", html)
        self.assertIn("toggleSymbolDetail", html)
        self.assertIn("formatTaskResultLines", html)
        self.assertIn("sortTaskAccounts", html)
        self.assertIn(".task-table", html)
        self.assertIn(".task-row", html)
        self.assertIn(".task-mode-badge", html)
        self.assertIn(".task-result-lines", html)
        self.assertIn(".task-symbol-toggle", html)
        self.assertIn(".task-filter-chip", html)
        self.assertIn(".task-result", html)
        self.assertIn("组合止盈监控", html)
        self.assertIn("巡检内触发", html)
        self.assertIn('fullText += (fullText ? "\\n" : "")', html)
        self.assertIn('onclick="toggleSymbolDetail(', html)

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
