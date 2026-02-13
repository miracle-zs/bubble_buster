import importlib.util
import tempfile
import unittest
from pathlib import Path

if importlib.util.find_spec("fastapi") is None or importlib.util.find_spec("httpx") is None:
    raise unittest.SkipTest("fastapi/httpx is not installed")

from fastapi.testclient import TestClient

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


if __name__ == "__main__":
    unittest.main()
