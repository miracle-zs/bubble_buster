import unittest
from datetime import datetime
from threading import Event, Thread
from zoneinfo import ZoneInfo

from core.runtime_service import ServiceRuntimeConfig, StrategyRuntimeService


class RuntimeServiceTest(unittest.TestCase):
    def _create_service(self, **overrides):
        cfg = ServiceRuntimeConfig(
            timezone_name=overrides.get("timezone_name", "UTC"),
            entry_hour=overrides.get("entry_hour", 7),
            entry_minute=overrides.get("entry_minute", 40),
            entry_misfire_grace_min=overrides.get("entry_misfire_grace_min", 120),
            entry_catchup_enabled=overrides.get("entry_catchup_enabled", True),
            daily_loss_cut_enabled=overrides.get("daily_loss_cut_enabled", True),
            daily_loss_cut_hour=overrides.get("daily_loss_cut_hour", 11),
            daily_loss_cut_minute=overrides.get("daily_loss_cut_minute", 55),
            manager_interval_sec=overrides.get("manager_interval_sec", 60),
            manager_max_catch_up_runs=overrides.get("manager_max_catch_up_runs", 3),
            loop_sleep_sec=overrides.get("loop_sleep_sec", 1.0),
            run_manage_on_startup=overrides.get("run_manage_on_startup", False),
        )

        class StrategyStub:
            def __init__(self):
                self.calls = 0

            def run_entry(self):
                self.calls += 1
                return {"status": "SUCCESS"}

        class ManagerStub:
            def __init__(self):
                self.calls = 0
                self.daily_loss_calls = 0

            def run_once(self):
                self.calls += 1
                return {"total": 0}

            def run_daily_loss_cut(self):
                self.daily_loss_calls += 1
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        class WalletSamplerStub:
            def __init__(self):
                self.calls = 0

            def run_once(self):
                self.calls += 1
                return {"snapshot_id": self.calls}

        strategy = StrategyStub()
        manager = ManagerStub()
        sampler = WalletSamplerStub() if overrides.get("with_sampler", False) else None
        service = StrategyRuntimeService(
            strategy=strategy,
            manager=manager,
            cfg=cfg,
            balance_sampler=sampler,
            now_monotonic=overrides.get("start_monotonic", 0.0),
        )
        return service, strategy, manager, sampler

    def test_entry_runs_once_within_grace(self):
        service, strategy, _, _ = self._create_service(
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=120,
        )

        now_local = datetime(2026, 2, 13, 8, 0, tzinfo=ZoneInfo("UTC"))
        service.run_cycle(now_local=now_local, now_monotonic=100.0)
        service.run_cycle(now_local=now_local, now_monotonic=120.0)

        self.assertEqual(strategy.calls, 1)

    def test_entry_skips_when_missed_beyond_grace(self):
        service, strategy, _, _ = self._create_service(
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=30,
        )

        now_local = datetime(2026, 2, 13, 12, 0, tzinfo=ZoneInfo("UTC"))
        service.run_cycle(now_local=now_local, now_monotonic=100.0)
        self.assertEqual(strategy.calls, 0)

        # Same day should stay skipped.
        service.run_cycle(now_local=now_local, now_monotonic=160.0)
        self.assertEqual(strategy.calls, 0)

    def test_entry_skips_when_catchup_disabled(self):
        service, strategy, _, _ = self._create_service(
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=120,
            entry_catchup_enabled=False,
        )

        missed_local = datetime(2026, 2, 13, 7, 45, tzinfo=ZoneInfo("UTC"))
        service.run_cycle(now_local=missed_local, now_monotonic=100.0)
        self.assertEqual(strategy.calls, 0)

        # Same day should remain skipped even if called again.
        service.run_cycle(now_local=missed_local, now_monotonic=110.0)
        self.assertEqual(strategy.calls, 0)

    def test_manage_interval_and_catch_up_limit(self):
        service, _, manager, _ = self._create_service(
            run_manage_on_startup=True,
            manager_interval_sec=60,
            manager_max_catch_up_runs=2,
            entry_hour=23,
            entry_minute=59,
        )

        now_local = datetime(2026, 2, 13, 1, 0, tzinfo=ZoneInfo("UTC"))

        # First run triggers startup manage.
        service.run_cycle(now_local=now_local, now_monotonic=10.0)
        self.assertEqual(manager.calls, 1)

        # Not due yet.
        service.run_cycle(now_local=now_local, now_monotonic=30.0)
        self.assertEqual(manager.calls, 1)

        # Due once.
        service.run_cycle(now_local=now_local, now_monotonic=70.0)
        self.assertEqual(manager.calls, 2)

        # Far behind: catch-up is capped at 2 runs in one cycle.
        service.run_cycle(now_local=now_local, now_monotonic=400.0)
        self.assertEqual(manager.calls, 4)

    def test_run_forever_can_stop_via_event(self):
        service, _, manager, _ = self._create_service(
            run_manage_on_startup=True,
            manager_interval_sec=1,
            loop_sleep_sec=0.2,
            entry_hour=23,
            entry_minute=59,
        )
        stop_event = Event()

        th = Thread(target=service.run_forever, kwargs={"stop_event": stop_event}, daemon=True)
        th.start()
        stop_event.set()
        th.join(timeout=2)

        self.assertFalse(th.is_alive())
        self.assertGreaterEqual(manager.calls, 0)

    def test_wallet_sampler_runs_with_manage_cycle(self):
        service, _, manager, sampler = self._create_service(
            run_manage_on_startup=True,
            manager_interval_sec=60,
            manager_max_catch_up_runs=2,
            with_sampler=True,
            entry_hour=23,
            entry_minute=59,
        )
        self.assertIsNotNone(sampler)
        now_local = datetime(2026, 2, 13, 1, 0, tzinfo=ZoneInfo("UTC"))

        service.run_cycle(now_local=now_local, now_monotonic=10.0)
        service.run_cycle(now_local=now_local, now_monotonic=70.0)
        service.run_cycle(now_local=now_local, now_monotonic=400.0)

        self.assertEqual(manager.calls, 4)
        self.assertEqual(sampler.calls, 4)  # type: ignore[union-attr]

    def test_daily_loss_cut_runs_once_per_day_after_schedule(self):
        service, _, manager, _ = self._create_service(
            run_manage_on_startup=False,
            manager_interval_sec=3600,
            daily_loss_cut_enabled=True,
            daily_loss_cut_hour=11,
            daily_loss_cut_minute=55,
            entry_hour=23,
            entry_minute=59,
        )

        before = datetime(2026, 2, 13, 11, 54, tzinfo=ZoneInfo("UTC"))
        due = datetime(2026, 2, 13, 11, 55, tzinfo=ZoneInfo("UTC"))
        service.run_cycle(now_local=before, now_monotonic=1.0)
        self.assertEqual(manager.daily_loss_calls, 0)
        service.run_cycle(now_local=due, now_monotonic=2.0)
        self.assertEqual(manager.daily_loss_calls, 1)
        service.run_cycle(now_local=due, now_monotonic=3.0)
        self.assertEqual(manager.daily_loss_calls, 1)


if __name__ == "__main__":
    unittest.main()
