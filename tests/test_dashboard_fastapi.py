import importlib.util
import threading
import tempfile
import unittest
from pathlib import Path

if importlib.util.find_spec("fastapi") is None or importlib.util.find_spec("httpx") is None:
    raise unittest.SkipTest("fastapi/httpx is not installed")

from fastapi.testclient import TestClient

from core.state_store import StateStore
from dashboard_fastapi import TradeStatsBackgroundRefresher, create_app, create_dashboard_context


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
portfolio_loss_cut_enabled = true
portfolio_loss_cut_pct = 3.5
portfolio_loss_cut_hour = 8
portfolio_loss_cut_minute = 0
portfolio_take_profit_enabled = false
portfolio_take_profit_pct = 9.0
portfolio_take_profit_hour = 8
portfolio_take_profit_minute = 0
dashboard_refresh_sec = 9
run_service_with_dashboard = false
db_path = data/state.db
log_dir = logs

[accounts]
enabled = acc01,acc02,55
equity_comparison_accounts = acc01,acc02,55
mode.acc01 = full
mode.acc02 = full
mode.55 = loss_cut_only
strategy_note.acc01 = TP 9% / 减仓50% / 浮亏砍仓ON
strategy_note.acc02 = TP 9% / 清仓100% / 浮亏砍仓ON

[account.acc01.runtime]
portfolio_take_profit_enabled = true
""".strip()
            + "\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_create_dashboard_context(self) -> None:
        ctx = create_dashboard_context(str(self.config_path))
        self.assertEqual(ctx.timezone_name, "UTC")
        self.assertEqual(ctx.refresh_sec, 15)
        self.assertTrue(ctx.portfolio_loss_cut_enabled)
        self.assertEqual(ctx.portfolio_loss_cut_pct, 3.5)
        self.assertEqual(ctx.portfolio_loss_cut_hour, 8)
        self.assertEqual(ctx.portfolio_loss_cut_minute, 0)
        self.assertTrue(ctx.provider.account_equity_recovery_enabled["acc01"])
        self.assertFalse(ctx.provider.account_equity_recovery_enabled["acc02"])
        self.assertEqual(ctx.provider.equity_comparison_account_ids, {"acc01", "acc02", "55"})
        self.assertTrue(ctx.db_path.endswith("data/state.db"))
        self.assertTrue(ctx.log_file.endswith("logs/strategy.log"))
        self.assertTrue(Path(ctx.db_path).exists())

    def test_trade_stats_background_refresher_refreshes_without_request(self) -> None:
        refreshed = threading.Event()
        calls = []

        class Fetcher:
            def refresh_stats(self, *, account_id: str, lookback_days: int):
                calls.append((account_id, lookback_days))
                refreshed.set()
                return None

        refresher = TradeStatsBackgroundRefresher(
            {"readonly01": Fetcher()},
            refresh_interval_sec=300,
        )
        refresher.start()
        try:
            self.assertTrue(refreshed.wait(timeout=1.0))
        finally:
            refresher.stop()

        self.assertEqual(calls, [("readonly01", 30)])

    def test_accounts_summary_fast_details_and_static_cache_headers(self) -> None:
        app = create_app(config_path=str(self.config_path))
        with TestClient(app) as client:
            fast = client.get("/api/accounts/summary/fast")
            self.assertEqual(fast.status_code, 200)
            self.assertEqual(fast.headers.get("cache-control"), "no-store")
            fast_rows = fast.json()["accounts"]
            self.assertEqual({row["account_id"] for row in fast_rows}, {"55", "acc01", "acc02"})
            self.assertNotIn("tasks", fast_rows[0])
            self.assertNotIn("entry_progress", fast_rows[0])

            details = client.get("/api/accounts/summary/details")
            self.assertEqual(details.status_code, 200)
            details_payload = details.json()
            self.assertTrue(details_payload["ready"])
            details_by_account = {
                row["account_id"]: row for row in details_payload["accounts"]
            }
            self.assertIn("tasks", details_by_account["acc01"])
            self.assertIn("entry_progress", details_by_account["acc01"])

            static = client.get("/static/vendor/echarts.min.js")
            self.assertEqual(static.status_code, 200)
            self.assertIn("max-age=", static.headers.get("cache-control", ""))

            ops_css = client.get("/static/ops.css", headers={"Accept-Encoding": "gzip"})
            self.assertEqual(ops_css.status_code, 200)
            self.assertEqual(ops_css.headers.get("x-content-type-options"), "nosniff")
            self.assertEqual(ops_css.headers.get("x-frame-options"), "SAMEORIGIN")
            self.assertEqual(ops_css.headers.get("referrer-policy"), "same-origin")
            self.assertIn("Obsidian Desk", ops_css.text)

            ops_js = client.get("/static/ops.js", headers={"Accept-Encoding": "gzip"})
            self.assertEqual(ops_js.status_code, 200)
            self.assertIn("window.BBOps", ops_js.text.replace("global.BBOps", "window.BBOps"))
            self.assertIn("readQueryParams", ops_js.text)
            self.assertIn("updateQueryParams", ops_js.text)
            self.assertIn("splitStrategyParts", ops_js.text)
            self.assertIn("formatStrategyHighlights", ops_js.text)
            self.assertIn("evaluatePortfolioStopState", ops_js.text)

            favicon_ico = client.get("/favicon.ico")
            self.assertEqual(favicon_ico.status_code, 200)
            self.assertIn("image/svg+xml", favicon_ico.headers.get("content-type", ""))
            favicon_svg = client.get("/static/favicon.svg")
            self.assertEqual(favicon_svg.status_code, 200)

    def test_app_health_and_dashboard_api(self) -> None:
        app = create_app(config_path=str(self.config_path))
        with TestClient(app) as client:
            from web.template_loader import static_asset_version

            ops_css_v = static_asset_version("ops.css")
            ops_js_v = static_asset_version("ops.js")
            overview_css_v = static_asset_version("overview.css")
            overview_js_v = static_asset_version("overview.js")
            dashboard_css_v = static_asset_version("dashboard.css")
            dashboard_js_v = static_asset_version("dashboard.js")
            comparison_css_v = static_asset_version("comparison.css")
            comparison_render_js_v = static_asset_version("comparison_render.js")
            comparison_js_v = static_asset_version("comparison.js")
            favicon_v = static_asset_version("favicon.svg")
            echarts_v = static_asset_version("vendor/echarts.min.js")

            versioned_static = client.get(f"/static/ops.css?v={ops_css_v}")
            self.assertEqual(versioned_static.status_code, 200)
            self.assertEqual(
                versioned_static.headers.get("cache-control"),
                "public, max-age=31536000, immutable",
            )

            overview = client.get("/")
            self.assertEqual(overview.status_code, 200)
            self.assertEqual(overview.headers.get("x-content-type-options"), "nosniff")
            self.assertEqual(overview.headers.get("cache-control"), "no-cache")
            self.assertIn("script-src 'self'", overview.headers.get("content-security-policy", ""))
            self.assertIn("style-src-elem 'self'", overview.headers.get("content-security-policy", ""))
            self.assertIn(f"static/ops.css?v={ops_css_v}", overview.text)
            self.assertIn(f"static/overview.css?v={overview_css_v}", overview.text)
            self.assertIn(f"static/ops.js?v={ops_js_v}", overview.text)
            self.assertIn(f"static/overview.js?v={overview_js_v}", overview.text)
            self.assertIn(f"static/favicon.svg?v={favicon_v}", overview.text)
            self.assertNotIn("<style>", overview.text)
            self.assertIn('id="page-config" type="application/json"', overview.text)
            self.assertIn('id="overview-refresh-btn"', overview.text)
            self.assertIn('id="overview-anomaly-btn"', overview.text)
            self.assertIn('id="fleet-incident-banner"', overview.text)
            self.assertIn('class="ops-topbar"', overview.text)
            self.assertIn('"id": "acc01"', overview.text)
            self.assertIn("账户总览", overview.text)
            self.assertIn("组合止损 -3.5% 已启用", overview.text)
            self.assertIn('href="accounts/comparison/"', overview.text)

            comparison_page = client.get("/accounts/comparison/")
            self.assertEqual(comparison_page.status_code, 200)
            self.assertEqual(comparison_page.headers.get("cache-control"), "no-cache")
            self.assertIn("script-src 'self'", comparison_page.headers.get("content-security-policy", ""))
            self.assertIn(f"../../static/ops.css?v={ops_css_v}", comparison_page.text)
            self.assertIn(f"../../static/comparison.css?v={comparison_css_v}", comparison_page.text)
            self.assertIn(f"../../static/ops.js?v={ops_js_v}", comparison_page.text)
            self.assertIn(f"../../static/comparison_render.js?v={comparison_render_js_v}", comparison_page.text)
            self.assertIn(f"../../static/comparison.js?v={comparison_js_v}", comparison_page.text)
            self.assertIn(f"../../static/favicon.svg?v={favicon_v}", comparison_page.text)
            self.assertNotIn("<style>", comparison_page.text)
            self.assertIn('class="ops-topbar"', comparison_page.text)
            self.assertIn("<h1>多账户权益走势同轴对比</h1>", comparison_page.text)
            self.assertIn(f"../../static/vendor/echarts.min.js?v={echarts_v}", comparison_page.text)
            self.assertIn('id="comparison-metrics-table"', comparison_page.text)
            self.assertIn('id="page-config" type="application/json"', comparison_page.text)
            self.assertIn('"id": "acc01"', comparison_page.text)
            self.assertIn('aria-sort="descending"', comparison_page.text)
            self.assertNotIn("aria-sort-dir", comparison_page.text)
            self.assertIn("策略事件不叠加到主图", comparison_page.text)

            missing_account = client.get("/account/nonexistent_acc/")
            self.assertEqual(missing_account.status_code, 404)
            self.assertNotIn("nonexistent_acc", missing_account.text)

            missing_api = client.get("/api/account/nonexistent_acc/core")
            self.assertEqual(missing_api.status_code, 404)
            self.assertNotIn("nonexistent_acc", missing_api.text)

            raw_static_tpl = client.get("/static/account-comparison.html")
            self.assertEqual(raw_static_tpl.status_code, 404)

            fav_ico = client.get("/favicon.ico")
            self.assertEqual(fav_ico.headers.get("content-security-policy"), "default-src 'none'; sandbox")
            fav_svg = client.get("/static/favicon.svg")
            self.assertEqual(fav_svg.headers.get("content-security-policy"), "default-src 'none'; sandbox")

            method_not_allowed = client.post("/")
            self.assertEqual(method_not_allowed.status_code, 405)
            self.assertEqual(method_not_allowed.headers.get("x-content-type-options"), "nosniff")

            comparison_js = client.get("/static/comparison.js")
            self.assertEqual(comparison_js.status_code, 200)
            self.assertIn('pathPrefix + "/api/accounts/equity-comparison?"', comparison_js.text)

            compact = client.get("/account/acc01/")
            self.assertEqual(compact.status_code, 200)
            self.assertEqual(compact.headers.get("cache-control"), "no-cache")
            self.assertIn("script-src 'self'", compact.headers.get("content-security-policy", ""))
            self.assertIn(f"../../static/ops.css?v={ops_css_v}", compact.text)
            self.assertIn(f"../../static/dashboard.css?v={dashboard_css_v}", compact.text)
            self.assertIn(f"../../static/ops.js?v={ops_js_v}", compact.text)
            self.assertIn(f"../../static/dashboard.js?v={dashboard_js_v}", compact.text)
            self.assertIn(f"../../static/favicon.svg?v={favicon_v}", compact.text)
            self.assertIn(f'rel="preload" as="script" href="../../static/vendor/echarts.min.js?v={echarts_v}"', compact.text)
            self.assertIn(f'"echartsSrc": "/static/vendor/echarts.min.js?v={echarts_v}"', compact.text)
            self.assertNotIn("<style>", compact.text)
            self.assertIn('id="page-config" type="application/json"', compact.text)
            self.assertIn('id="detailRefreshBtn"', compact.text)
            self.assertIn('id="chartSparseHint"', compact.text)
            self.assertIn('id="readonlyStatsPanel"', compact.text)
            self.assertIn('class="ops-topbar"', compact.text)
            self.assertIn('"id": "acc01"', compact.text)
            self.assertIn('"accountId": "acc01"', compact.text)
            self.assertIn('"accountMode": "full"', compact.text)
            self.assertIn("TP 9% / 减仓50% / 浮亏砍仓ON", compact.text)
            self.assertIn('"portfolioStopEnabled": true', compact.text)
            self.assertIn('class="account-command-bar"', compact.text)
            self.assertIn("账户周期风险", compact.text)
            self.assertIn("当前剩余金额 / 当前剩余数量", compact.text)
            self.assertIn("入场价 / 标记价", compact.text)
            self.assertIn("未实现盈亏", compact.text)
            self.assertIn("止盈 / 止损", compact.text)
            self.assertIn(
                "当前剩余名义金额优先基于实时标记价计算，缺失时按入场价预估；保留当前剩余数量用于对账（非原始开仓数量）",
                compact.text,
            )

            health = client.get("/healthz")
            self.assertEqual(health.status_code, 200)
            h = health.json()
            self.assertTrue(h["ok"])
            self.assertFalse(h["service_enabled"])
            self.assertFalse(h["service_running"])

            data = client.get("/api/account/acc01/snapshot")
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

            runtime_res = client.get("/api/runtime/settings")
            self.assertEqual(runtime_res.status_code, 200)
            runtime_payload = runtime_res.json()
            self.assertNotIn("config_path", runtime_payload)
            self.assertFalse(runtime_payload["mutable"])

            original_config = self.config_path.read_text(encoding="utf-8")
            blocked = client.post("/api/runtime/settings/entry-catchup?enabled=false&persist=true")
            self.assertEqual(blocked.status_code, 405)
            self.assertEqual(blocked.json()["detail"], "Dashboard is read-only")
            self.assertEqual(self.config_path.read_text(encoding="utf-8"), original_config)

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
            later = "2026-08-10T03:00:00+00:00"
            launch_window_start = "2026-08-10T02:00:00+00:00"
            store.scoped("acc01").add_wallet_snapshot(launch_window_start, 111.0, source="API")
            store.scoped("acc02").add_wallet_snapshot(launch_window_start, 222.0, source="API")
            store.scoped("55").add_wallet_snapshot(launch_window_start, 333.0, source="API")
            store.scoped("acc01").add_wallet_snapshot(later, 121.0, source="API")
            store.scoped("acc02").add_wallet_snapshot(later, 200.0, source="API")
            store.scoped("55").add_wallet_snapshot(later, 333.0, source="API")

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
            self.assertEqual(curve.headers.get("cache-control"), "no-store")
            curve_payload = curve.json()
            self.assertNotIn("config_path", curve_payload)
            self.assertNotIn("db_path", curve_payload)
            self.assertIn("strategy_equity_curve", curve_payload)
            self.assertTrue(isinstance(curve_payload.get("strategy_equity_curve"), list))
            self.assertIn("balance_curve", curve_payload)
            self.assertTrue(isinstance(curve_payload.get("balance_curve"), list))
            self.assertGreater(len(curve_payload.get("balance_curve") or []), 0)

            batch_curves = client.get("/api/accounts/curves?window_hours=24&curve_points=160")
            self.assertEqual(batch_curves.status_code, 200)
            self.assertEqual(batch_curves.headers.get("cache-control"), "no-store")
            batch_payload = batch_curves.json()["curves"]
            self.assertEqual(set(batch_payload), {"acc01", "acc02"})
            self.assertGreater(len(batch_payload["acc01"].get("balance_curve") or []), 0)

            comparison = client.get("/api/accounts/equity-comparison?all_time=true")
            self.assertEqual(comparison.status_code, 200)
            comparison_payload = comparison.json()
            self.assertEqual(
                comparison_payload["comparison_start_at_utc"],
                "2026-08-10T01:32:10+00:00",
            )
            self.assertEqual(
                comparison_payload["baseline"],
                "first_valid_snapshot_at_or_after_portfolio_take_profit_launch",
            )
            comparison_rows = {row["account_id"]: row for row in comparison_payload["series"]}
            self.assertEqual(set(comparison_rows), {"acc01", "acc02", "55"})
            self.assertAlmostEqual(comparison_rows["acc01"]["change_pct"], 9.009009, places=4)
            self.assertAlmostEqual(comparison_rows["acc02"]["change_pct"], -9.90991, places=4)
            self.assertEqual(comparison_rows["55"]["change_pct"], 0.0)
            self.assertEqual(comparison_rows["55"]["mode"], "loss_cut_only")
            self.assertEqual(comparison_rows["acc01"]["strategy_note"], "TP 9% / 减仓50% / 浮亏砍仓ON")
            self.assertEqual(len(comparison_rows["acc01"]["points"]), 2)

            details = client.get("/api/account/acc01/details")
            self.assertEqual(details.status_code, 200)
            detail_payload = details.json()
            self.assertNotIn("config_path", detail_payload)
            self.assertNotIn("db_path", detail_payload)
            self.assertIn("open_positions", detail_payload)
            self.assertIn("log_tail", detail_payload)
            self.assertEqual(detail_payload.get("strategy_equity_curve"), [])

    def test_db_error_returns_503(self) -> None:
        app = create_app(config_path=str(self.config_path))
        with TestClient(app) as client:
            app.state.ctx.provider.cached_accounts_summary_fast = lambda: {
                "generated_at_utc": "2026-08-15T00:00:00+00:00",
                "accounts": [],
                "db_error": "database is locked",
            }
            resp = client.get("/api/accounts/summary/fast")
            self.assertEqual(resp.status_code, 503)
            self.assertEqual(resp.json()["db_error"], "database is locked")


if __name__ == "__main__":
    unittest.main()
