import importlib.util
import tempfile
import unittest
from pathlib import Path

if importlib.util.find_spec("fastapi") is None or importlib.util.find_spec("httpx") is None:
    raise unittest.SkipTest("fastapi/httpx is not installed")

from fastapi.testclient import TestClient

from core.state_store import StateStore
from dashboard_fastapi import create_app, create_dashboard_context


class DashboardFastAPITest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)

        self.config_path = self.root / "config.ini"
        self.config_path.write_text(
            """
[runtime]
timezone = UTC
entry_hour = 7
entry_minute = 40
dashboard_refresh_sec = 9
run_service_with_dashboard = false
db_path = data/state.db
log_dir = logs

[accounts]
enabled = acc01,acc02,55
mode.acc01 = full
mode.acc02 = full
mode.55 = loss_cut_only
strategy_note.acc01 = TP 9% / 减仓50% / 浮亏砍仓ON
strategy_note.acc02 = TP 9% / 清仓100% / 浮亏砍仓ON
""".strip()
            + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_create_dashboard_context(self) -> None:
        ctx = create_dashboard_context(str(self.config_path))
        self.assertEqual(ctx.timezone_name, "UTC")
        self.assertEqual(ctx.refresh_sec, 9)
        self.assertTrue(ctx.db_path.endswith("data/state.db"))
        self.assertTrue(ctx.log_file.endswith("logs/strategy.log"))
        self.assertTrue(Path(ctx.db_path).exists())

    def test_app_health_and_dashboard_api(self) -> None:
        app = create_app(config_path=str(self.config_path))
        with TestClient(app) as client:
            overview = client.get("/")
            self.assertEqual(overview.status_code, 200)
            self.assertIn("账户总览", overview.text)

            compact = client.get("/account/acc01/")
            self.assertEqual(compact.status_code, 200)
            self.assertIn("acc01", compact.text)
            self.assertIn('var accountId = "acc01";', compact.text)
            self.assertIn("encodeURIComponent(accountId)", compact.text)

            health = client.get("/healthz")
            self.assertEqual(health.status_code, 200)
            h = health.json()
            self.assertTrue(h["ok"])
            self.assertFalse(h["service_enabled"])
            self.assertFalse(h["service_running"])

            data = client.get("/api/dashboard")
            self.assertEqual(data.status_code, 200)
            payload = data.json()
            self.assertIn("service", payload)
            self.assertFalse(payload["service"]["enabled"])
            self.assertFalse(payload["service"]["running"])
            self.assertIn("equity_curve", payload)
            self.assertIn("drawdown_stats", payload)
            self.assertIn("wallet", payload)
            self.assertNotIn("config_path", payload)
            self.assertNotIn("db_path", payload)
            self.assertNotIn("config_path", payload["runtime_settings"])

    def test_accounts_summary_and_account_snapshot_api(self) -> None:
        app = create_app(config_path=str(self.config_path))
        with TestClient(app) as client:
            db_path = str(self.root / "data" / "state.db")
            schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
            store = StateStore(db_path=db_path, schema_path=schema_path)
            store.init_schema()

            now = "2026-02-13T00:00:00+00:00"
            run1, _ = store.create_run("2026-02-13", account_id="acc01")
            run2, _ = store.create_run("2026-02-13", account_id="acc02")
            run3, _ = store.create_run("2026-02-13", account_id="55")
            store.insert_position(
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
            store.insert_position(
                run_id=run2,
                symbol="BUSDT",
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
            store.insert_position(
                run_id=run3,
                symbol="CUSDT",
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
            store.scoped("acc01").add_wallet_snapshot(now, 111.0, source="API")
            store.scoped("acc02").add_wallet_snapshot(now, 222.0, source="API")
            store.scoped("55").add_wallet_snapshot(now, 333.0, source="API")

            summary = client.get("/api/accounts/summary")
            self.assertEqual(summary.status_code, 200)
            rows = summary.json()["accounts"]
            ids = [row["account_id"] for row in rows]
            self.assertIn("acc01", ids)
            self.assertIn("acc02", ids)
            self.assertIn("55", ids)
            notes = {row["account_id"]: row.get("strategy_note") for row in rows}
            self.assertEqual(notes.get("acc01"), "TP 9% / 减仓50% / 浮亏砍仓ON")
            self.assertEqual(notes.get("acc02"), "TP 9% / 清仓100% / 浮亏砍仓ON")
            task_keys = set((rows[0].get("tasks") or {}).keys())
            self.assertEqual(
                task_keys,
                {"entry", "daily_loss_cut", "noon_protection", "manage", "equity_recovery_take_profit"},
            )
            modes = {row["account_id"]: row.get("mode") for row in rows}
            self.assertEqual(modes.get("55"), "loss_cut_only")

            snap = client.get("/api/account/acc01/snapshot")
            self.assertEqual(snap.status_code, 200)
            payload = snap.json()
            self.assertEqual(payload["account_id"], "acc01")
            self.assertIn("service", payload)
            self.assertNotIn("config_path", payload)
            self.assertNotIn("db_path", payload)
            symbols = {row["symbol"] for row in payload["open_positions"]}
            self.assertEqual(symbols, {"AUSDT"})

            core = client.get("/api/account/acc01/core")
            self.assertEqual(core.status_code, 200)
            core_payload = core.json()
            self.assertNotIn("config_path", core_payload)
            self.assertNotIn("db_path", core_payload)
            self.assertNotIn("strategy_equity_curve", core_payload)
            self.assertNotIn("balance_curve", core_payload)
            self.assertNotIn("equity_curve", core_payload)
            self.assertNotIn("drawdown_stats_strategy", core_payload)
            self.assertNotIn("drawdown_stats_balance", core_payload)
            self.assertNotIn("drawdown_stats", core_payload)
            self.assertEqual(core_payload.get("open_positions"), [])
            self.assertEqual(core_payload.get("log_tail"), [])

            curve = client.get("/api/account/acc01/curve")
            self.assertEqual(curve.status_code, 200)
            curve_payload = curve.json()
            self.assertNotIn("config_path", curve_payload)
            self.assertNotIn("db_path", curve_payload)
            self.assertIn("strategy_equity_curve", curve_payload)
            self.assertTrue(isinstance(curve_payload.get("strategy_equity_curve"), list))
            self.assertIn("balance_curve", curve_payload)
            self.assertTrue(isinstance(curve_payload.get("balance_curve"), list))
            self.assertGreater(len(curve_payload.get("balance_curve") or []), 0)

            details = client.get("/api/account/acc01/details")
            self.assertEqual(details.status_code, 200)
            detail_payload = details.json()
            self.assertNotIn("config_path", detail_payload)
            self.assertNotIn("db_path", detail_payload)
            self.assertIn("open_positions", detail_payload)
            self.assertIn("log_tail", detail_payload)
            self.assertEqual(detail_payload.get("strategy_equity_curve"), [])


if __name__ == "__main__":
    unittest.main()
