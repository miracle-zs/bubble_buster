import unittest
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from unittest.mock import MagicMock
import time
from unittest.mock import patch
from zoneinfo import ZoneInfo

from core.runtime_service import ServiceRuntimeConfig, StrategyRuntimeService


def _wait_until(predicate, timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


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
            orphan_exit_order_cleanup_enabled=overrides.get("orphan_exit_order_cleanup_enabled", True),
            orphan_exit_order_cleanup_hour=overrides.get("orphan_exit_order_cleanup_hour", 3),
            orphan_exit_order_cleanup_minute=overrides.get("orphan_exit_order_cleanup_minute", 30),
            readonly_wallet_snapshot_interval_sec=overrides.get("readonly_wallet_snapshot_interval_sec", 60.0),
        )

        class StrategyStub:
            def __init__(self):
                self.calls = 0
                self.equity_recovery_calls = 0

            def run_entry(self):
                self.calls += 1
                return {"status": "SUCCESS"}

            def run_equity_recovery_take_profit(self):
                self.equity_recovery_calls += 1
                return {"status": "SKIPPED", "reason": "THRESHOLD_NOT_REACHED"}

        class ManagerStub:
            def __init__(self):
                self.calls = 0
                self.daily_loss_calls = 0
                self.orphan_cleanup_calls = 0

            def run_once(self):
                self.calls += 1
                return {"total": 0}

            def run_daily_loss_cut(self):
                self.daily_loss_calls += 1
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

            def cleanup_orphan_exit_orders_once_per_day(self):
                self.orphan_cleanup_calls += 1
                return {"canceled": 0, "details": [], "day_key": "2026-02-13"}

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

    def test_readonly_wallet_snapshot_is_throttled(self):
        service, _, _, sampler = self._create_service(
            with_sampler=True,
            readonly_wallet_snapshot_interval_sec=60.0,
        )
        assert sampler is not None
        service.account_runtimes = {
            "readonly01": {
                "mode": "readonly",
                "balance_sampler": sampler,
            }
        }

        now_local = datetime(2026, 2, 13, 0, 0, tzinfo=ZoneInfo("UTC"))
        for mono in [0.0, 1.0, 2.0, 30.0, 59.0]:
            service.run_cycle(now_local=now_local, now_monotonic=mono)
        self.assertEqual(sampler.calls, 1)

        service.run_cycle(now_local=now_local, now_monotonic=60.0)
        self.assertEqual(sampler.calls, 2)

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
        self.assertEqual(manager.orphan_cleanup_calls, 0)

        # Not due yet.
        service.run_cycle(now_local=now_local, now_monotonic=30.0)
        self.assertEqual(manager.calls, 1)

        # Due once.
        service.run_cycle(now_local=now_local, now_monotonic=70.0)
        self.assertEqual(manager.calls, 2)

        # Far behind: catch-up is capped at 2 runs in one cycle.
        service.run_cycle(now_local=now_local, now_monotonic=400.0)
        self.assertEqual(manager.calls, 4)
        self.assertEqual(manager.orphan_cleanup_calls, 0)

    def test_orphan_exit_order_cleanup_runs_once_at_fixed_time(self):
        service, _, manager, _ = self._create_service(
            run_manage_on_startup=True,
            manager_interval_sec=60,
            orphan_exit_order_cleanup_enabled=True,
            orphan_exit_order_cleanup_hour=3,
            orphan_exit_order_cleanup_minute=30,
            entry_hour=23,
            entry_minute=59,
        )

        before = datetime(2026, 2, 13, 3, 29, tzinfo=ZoneInfo("UTC"))
        due = datetime(2026, 2, 13, 3, 30, tzinfo=ZoneInfo("UTC"))
        next_day = datetime(2026, 2, 14, 3, 30, tzinfo=ZoneInfo("UTC"))

        service.run_cycle(now_local=before, now_monotonic=10.0)
        self.assertEqual(manager.orphan_cleanup_calls, 0)

        service.run_cycle(now_local=due, now_monotonic=70.0)
        self.assertEqual(manager.orphan_cleanup_calls, 1)

        service.run_cycle(now_local=due, now_monotonic=130.0)
        self.assertEqual(manager.orphan_cleanup_calls, 1)

        service.run_cycle(now_local=next_day, now_monotonic=190.0)
        self.assertEqual(manager.orphan_cleanup_calls, 2)

    def test_orphan_exit_order_cleanup_catches_up_after_service_restart(self):
        service, _, manager, _ = self._create_service(
            run_manage_on_startup=False,
            manager_interval_sec=3600,
            orphan_exit_order_cleanup_enabled=True,
            orphan_exit_order_cleanup_hour=3,
            orphan_exit_order_cleanup_minute=30,
            entry_hour=23,
            entry_minute=59,
        )

        missed = datetime(2026, 2, 13, 9, 0, tzinfo=ZoneInfo("UTC"))
        next_day_due = datetime(2026, 2, 14, 3, 30, tzinfo=ZoneInfo("UTC"))

        service.run_cycle(now_local=missed, now_monotonic=10.0)
        self.assertEqual(manager.orphan_cleanup_calls, 1)

        service.run_cycle(now_local=missed, now_monotonic=70.0)
        self.assertEqual(manager.orphan_cleanup_calls, 1)

        service.run_cycle(now_local=next_day_due, now_monotonic=130.0)
        self.assertEqual(manager.orphan_cleanup_calls, 2)

    def test_orphan_exit_order_cleanup_retries_same_day_after_cancel_failure(self):
        service, _, manager, _ = self._create_service(
            run_manage_on_startup=False,
            manager_interval_sec=3600,
            orphan_exit_order_cleanup_enabled=True,
            orphan_exit_order_cleanup_hour=3,
            orphan_exit_order_cleanup_minute=30,
            entry_hour=23,
            entry_minute=59,
        )
        manager.cleanup_orphan_exit_orders_once_per_day = MagicMock(
            side_effect=[
                {"canceled": 0, "failed": 1, "details": []},
                {"canceled": 1, "failed": 0, "details": ["BASUSDT"]},
            ]
        )
        due = datetime(2026, 2, 13, 3, 30, tzinfo=ZoneInfo("UTC"))

        service.run_cycle(now_local=due, now_monotonic=10.0)
        service.run_cycle(now_local=due, now_monotonic=20.0)
        service.run_cycle(now_local=due, now_monotonic=30.0)

        self.assertEqual(manager.cleanup_orphan_exit_orders_once_per_day.call_count, 2)

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

    def test_equity_recovery_hook_runs_with_manage_cycle(self):
        service, strategy, manager, _ = self._create_service(
            run_manage_on_startup=True,
            manager_interval_sec=60,
            manager_max_catch_up_runs=2,
            entry_hour=23,
            entry_minute=59,
        )
        now_local = datetime(2026, 2, 13, 1, 0, tzinfo=ZoneInfo("UTC"))

        service.run_cycle(now_local=now_local, now_monotonic=10.0)
        service.run_cycle(now_local=now_local, now_monotonic=70.0)
        service.run_cycle(now_local=now_local, now_monotonic=400.0)

        self.assertEqual(manager.calls, 4)
        self.assertEqual(strategy.equity_recovery_calls, 4)

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

    def test_daily_loss_cut_skips_if_strict_window_missed(self):
        service, _, manager, _ = self._create_service(
            run_manage_on_startup=False,
            manager_interval_sec=3600,
            daily_loss_cut_enabled=True,
            daily_loss_cut_hour=11,
            daily_loss_cut_minute=55,
            entry_hour=23,
            entry_minute=59,
        )

        missed = datetime(2026, 2, 13, 22, 25, tzinfo=ZoneInfo("UTC"))
        service.run_cycle(now_local=missed, now_monotonic=1.0)
        self.assertEqual(manager.daily_loss_calls, 0)

        # Same day remains skipped.
        service.run_cycle(now_local=missed, now_monotonic=2.0)
        self.assertEqual(manager.daily_loss_calls, 0)

    def test_daily_loss_cut_skips_accounts_with_daily_loss_cut_disabled(self):
        class StrategyStub:
            def run_entry(self):
                return {"status": "SKIPPED"}

        class ManagerStub:
            def __init__(self):
                self.calls = 0

            def run_once(self):
                return {"total": 0}

            def run_daily_loss_cut(self):
                self.calls += 1
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        m_enabled = ManagerStub()
        m_disabled = ManagerStub()
        strategy = StrategyStub()
        cfg = ServiceRuntimeConfig(
            timezone_name="UTC",
            entry_hour=23,
            entry_minute=59,
            entry_misfire_grace_min=120,
            entry_catchup_enabled=True,
            daily_loss_cut_enabled=True,
            daily_loss_cut_hour=11,
            daily_loss_cut_minute=55,
            manager_interval_sec=3600,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=False,
        )
        service = StrategyRuntimeService(
            strategy=strategy,
            manager=m_enabled,
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": strategy,
                    "manager": m_enabled,
                    "balance_sampler": None,
                    "daily_loss_cut_enabled": True,
                },
                "acc55": {
                    "mode": "loss_cut_only",
                    "strategy": strategy,
                    "manager": m_disabled,
                    "balance_sampler": None,
                    "daily_loss_cut_enabled": False,
                },
            },
            max_account_workers=2,
        )
        due = datetime(2026, 2, 13, 11, 55, tzinfo=ZoneInfo("UTC"))
        service.run_cycle(now_local=due, now_monotonic=2.0)
        self.assertEqual(m_enabled.calls, 1)
        self.assertEqual(m_disabled.calls, 0)

    def test_entry_runs_only_accounts_due_at_current_time(self):
        class StrategyStub:
            def __init__(self, name: str) -> None:
                self.name = name
                self.entry_calls = 0

            def run_entry(self, shared_top_gainers=None):
                self.entry_calls += 1
                return {"status": "SUCCESS", "shared": shared_top_gainers}

        class ManagerStub:
            def run_once(self):
                return {"total": 0}

            def run_daily_loss_cut(self):
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        acc01 = StrategyStub("acc01")
        acc02 = StrategyStub("acc02")
        manager = ManagerStub()
        cfg = ServiceRuntimeConfig(
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=120,
            entry_catchup_enabled=True,
            daily_loss_cut_enabled=False,
            daily_loss_cut_hour=23,
            daily_loss_cut_minute=59,
            manager_interval_sec=3600,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=False,
            max_account_workers=2,
        )
        service = StrategyRuntimeService(
            strategy=acc01,
            manager=manager,
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": acc01,
                    "manager": manager,
                    "balance_sampler": None,
                    "entry_hour": 7,
                    "entry_minute": 40,
                },
                "acc02": {
                    "mode": "full",
                    "strategy": acc02,
                    "manager": manager,
                    "balance_sampler": None,
                    "entry_hour": 7,
                    "entry_minute": 45,
                },
            },
            max_account_workers=2,
        )

        service.run_cycle(
            now_local=datetime(2026, 2, 13, 7, 40, tzinfo=ZoneInfo("UTC")),
            now_monotonic=10.0,
        )
        self.assertEqual(acc01.entry_calls, 1)
        self.assertEqual(acc02.entry_calls, 0)

        service.run_cycle(
            now_local=datetime(2026, 2, 13, 7, 45, tzinfo=ZoneInfo("UTC")),
            now_monotonic=20.0,
        )
        self.assertTrue(_wait_until(lambda: acc02.entry_calls == 1))
        self.assertEqual(acc01.entry_calls, 1)
        self.assertEqual(acc02.entry_calls, 1)

    def test_entry_reuses_cached_ranking_for_staggered_accounts(self):
        class StrategyStub:
            def __init__(self, name: str) -> None:
                self.name = name
                self.shared_payloads = []

            def run_entry(self, shared_top_gainers=None):
                self.shared_payloads.append(shared_top_gainers)
                return {"status": "SUCCESS"}

        class ManagerStub:
            def run_once(self):
                return {"total": 0}

            def run_daily_loss_cut(self):
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        ranking = [{"symbol": "AAAUSDT", "pctChange": "10.0"}]
        build_calls = []
        acc01 = StrategyStub("acc01")
        acc03 = StrategyStub("acc03")
        acc02 = StrategyStub("acc02")
        manager = ManagerStub()
        cfg = ServiceRuntimeConfig(
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=120,
            entry_catchup_enabled=True,
            daily_loss_cut_enabled=False,
            daily_loss_cut_hour=23,
            daily_loss_cut_minute=59,
            manager_interval_sec=3600,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=False,
            max_account_workers=3,
        )
        service = StrategyRuntimeService(
            strategy=acc01,
            manager=manager,
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": acc01,
                    "manager": manager,
                    "balance_sampler": None,
                    "entry_hour": 7,
                    "entry_minute": 40,
                },
                "acc03": {
                    "mode": "full",
                    "strategy": acc03,
                    "manager": manager,
                    "balance_sampler": None,
                    "entry_hour": 7,
                    "entry_minute": 40,
                },
                "acc02": {
                    "mode": "full",
                    "strategy": acc02,
                    "manager": manager,
                    "balance_sampler": None,
                    "entry_hour": 7,
                    "entry_minute": 45,
                },
            },
            max_account_workers=3,
        )

        def fake_build(account_ids):
            build_calls.append(tuple(sorted(account_ids)))
            return ranking

        service._build_shared_top_gainers = fake_build  # type: ignore[method-assign]

        service.run_cycle(
            now_local=datetime(2026, 2, 13, 7, 40, tzinfo=ZoneInfo("UTC")),
            now_monotonic=10.0,
        )
        service.run_cycle(
            now_local=datetime(2026, 2, 13, 7, 45, tzinfo=ZoneInfo("UTC")),
            now_monotonic=20.0,
        )

        self.assertTrue(_wait_until(lambda: len(acc02.shared_payloads) == 1))
        self.assertEqual(build_calls, [("acc01", "acc03")])
        self.assertIs(acc01.shared_payloads[0], ranking)
        self.assertIs(acc03.shared_payloads[0], ranking)
        self.assertIs(acc02.shared_payloads[0], ranking)

    def test_entry_failure_remains_due_within_grace_window(self):
        class StrategyStub:
            def __init__(self, results):
                self.results = list(results)
                self.entry_calls = 0

            def run_entry(self, shared_top_gainers=None):
                self.entry_calls += 1
                result = self.results.pop(0)
                if isinstance(result, Exception):
                    raise result
                return result

        class ManagerStub:
            def run_once(self):
                return {"total": 0}

            def run_daily_loss_cut(self):
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        acc01 = StrategyStub([{"status": "SUCCESS"}])
        acc03 = StrategyStub([RuntimeError("database is locked"), {"status": "SUCCESS"}])
        manager = ManagerStub()
        cfg = ServiceRuntimeConfig(
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=20,
            entry_catchup_enabled=True,
            daily_loss_cut_enabled=False,
            daily_loss_cut_hour=23,
            daily_loss_cut_minute=59,
            manager_interval_sec=3600,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=False,
            max_account_workers=2,
        )
        service = StrategyRuntimeService(
            strategy=acc01,
            manager=manager,
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": acc01,
                    "manager": manager,
                    "balance_sampler": None,
                },
                "acc03": {
                    "mode": "full",
                    "strategy": acc03,
                    "manager": manager,
                    "balance_sampler": None,
                },
            },
            max_account_workers=2,
        )
        service._build_shared_top_gainers = lambda _account_ids: None  # type: ignore[method-assign]

        service.run_cycle(
            now_local=datetime(2026, 2, 13, 7, 40, tzinfo=ZoneInfo("UTC")),
            now_monotonic=10.0,
        )
        self.assertTrue(_wait_until(lambda: acc03.entry_calls == 1))
        service.run_cycle(
            now_local=datetime(2026, 2, 13, 7, 41, tzinfo=ZoneInfo("UTC")),
            now_monotonic=70.0,
        )
        self.assertTrue(_wait_until(lambda: acc03.entry_calls == 2))

        self.assertEqual(acc01.entry_calls, 1)
        self.assertEqual(acc03.entry_calls, 2)

    def test_entry_success_and_skip_are_marked_done_for_the_day(self):
        class StrategyStub:
            def __init__(self, status: str):
                self.status = status
                self.entry_calls = 0

            def run_entry(self, shared_top_gainers=None):
                self.entry_calls += 1
                return {"status": self.status}

        class ManagerStub:
            def run_once(self):
                return {"total": 0}

            def run_daily_loss_cut(self):
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        acc01 = StrategyStub("SUCCESS")
        acc02 = StrategyStub("SKIPPED")
        manager = ManagerStub()
        cfg = ServiceRuntimeConfig(
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=20,
            entry_catchup_enabled=True,
            daily_loss_cut_enabled=False,
            daily_loss_cut_hour=23,
            daily_loss_cut_minute=59,
            manager_interval_sec=3600,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=False,
            max_account_workers=2,
        )
        service = StrategyRuntimeService(
            strategy=acc01,
            manager=manager,
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": acc01,
                    "manager": manager,
                    "balance_sampler": None,
                },
                "acc02": {
                    "mode": "full",
                    "strategy": acc02,
                    "manager": manager,
                    "balance_sampler": None,
                },
            },
            max_account_workers=2,
        )
        service._build_shared_top_gainers = lambda _account_ids: None  # type: ignore[method-assign]

        now_local = datetime(2026, 2, 13, 7, 40, tzinfo=ZoneInfo("UTC"))
        service.run_cycle(now_local=now_local, now_monotonic=10.0)
        service.run_cycle(now_local=now_local, now_monotonic=70.0)
        self.assertTrue(_wait_until(lambda: acc02.entry_calls == 1))

        self.assertEqual(acc01.entry_calls, 1)
        self.assertEqual(acc02.entry_calls, 1)


if __name__ == "__main__":
    unittest.main()


def test_parse_accounts_with_modes_and_overrides() -> None:
    from core.account_config import parse_account_settings

    cfg_text = """
[accounts]
enabled = acc01,55
mode.acc01 = full
mode.55 = loss_cut_only
[binance]
api_key = k
api_secret = s
[account.55.binance]
api_key = k55
api_secret = s55
"""
    settings = parse_account_settings(cfg_text)
    assert settings["acc01"].mode == "full"
    assert settings["55"].mode == "loss_cut_only"
    assert settings["55"].binance["api_key"] == "k55"


def test_service_dispatches_manage_concurrently_per_account() -> None:
    class ManagerStub:
        def __init__(self) -> None:
            self.calls = 0

        def run_once(self):
            self.calls += 1
            return {"total": 1}

    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    m1 = ManagerStub()
    m2 = ManagerStub()
    s1 = StrategyStub()
    s2 = StrategyStub()

    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=60,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=True,
    )

    service = StrategyRuntimeService(
        strategy=s1,
        manager=m1,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"strategy": s1, "manager": m1, "balance_sampler": None},
            "acc02": {"strategy": s2, "manager": m2, "balance_sampler": None},
        },
        max_account_workers=2,
    )

    now_local = datetime(2026, 2, 13, 1, 0, tzinfo=ZoneInfo("UTC"))
    service.run_cycle(now_local=now_local, now_monotonic=10.0)

    assert m1.calls == 1
    assert m2.calls == 1


def test_account_breaker_trips_after_consecutive_failures() -> None:
    class FlakyManager:
        def __init__(self, should_fail: bool) -> None:
            self.should_fail = should_fail
            self.calls = 0

        def run_once(self):
            self.calls += 1
            if self.should_fail:
                raise RuntimeError("boom")
            return {"total": 1}

    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    bad = FlakyManager(True)
    good = FlakyManager(False)
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=1,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=0.1,
        run_manage_on_startup=True,
        account_failure_threshold=2,
        account_cooldown_cycles=2,
        account_task_timeout_sec=1.0,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=bad,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc-bad": {"strategy": StrategyStub(), "manager": bad, "balance_sampler": None},
            "acc-good": {"strategy": StrategyStub(), "manager": good, "balance_sampler": None},
        },
        max_account_workers=2,
    )
    now_local = datetime(2026, 2, 13, 1, 0, tzinfo=ZoneInfo("UTC"))

    service.run_cycle(now_local=now_local, now_monotonic=0.0)
    service.run_cycle(now_local=now_local, now_monotonic=1.0)
    # Breaker tripped here for acc-bad; next two cycles should skip it.
    service.run_cycle(now_local=now_local, now_monotonic=2.0)
    service.run_cycle(now_local=now_local, now_monotonic=3.0)

    assert bad.calls == 2
    assert good.calls == 4


def test_entry_dispatches_to_full_accounts_only() -> None:
    class StrategyStub:
        def __init__(self) -> None:
            self.entry_calls = 0

        def run_entry(self):
            self.entry_calls += 1
            return {"status": "SUCCESS"}

    class ManagerStub:
        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

    s1 = StrategyStub()
    s2 = StrategyStub()
    s3 = StrategyStub()
    m = ManagerStub()

    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=11,
        entry_minute=0,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=23,
        daily_loss_cut_minute=59,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
    )
    service = StrategyRuntimeService(
        strategy=s1,
        manager=m,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": s1, "manager": m, "balance_sampler": None},
            "acc02": {"mode": "full", "strategy": s2, "manager": m, "balance_sampler": None},
            "acc03": {"mode": "loss_cut_only", "strategy": s3, "manager": m, "balance_sampler": None},
        },
        max_account_workers=2,
    )
    now_local = datetime(2026, 2, 13, 11, 0, tzinfo=ZoneInfo("UTC"))
    service.run_cycle(now_local=now_local, now_monotonic=10.0)
    assert _wait_until(lambda: s2.entry_calls == 1)
    assert s1.entry_calls == 1
    assert s2.entry_calls == 1
    assert s3.entry_calls == 0


def test_daily_loss_cut_runs_for_full_and_loss_cut_only() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.daily_calls = 0

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            self.daily_calls += 1
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

    m1 = ManagerStub()
    m2 = ManagerStub()
    s = StrategyStub()

    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=True,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
    )
    service = StrategyRuntimeService(
        strategy=s,
        manager=m1,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": s, "manager": m1, "balance_sampler": None},
            "acc55": {"mode": "loss_cut_only", "strategy": s, "manager": m2, "balance_sampler": None},
        },
        max_account_workers=2,
    )
    now_local = datetime(2026, 2, 13, 11, 55, tzinfo=ZoneInfo("UTC"))
    service.run_cycle(now_local=now_local, now_monotonic=10.0)
    assert m1.daily_calls == 1
    assert m2.daily_calls == 1


def test_manage_timeout_returns_without_blocking_and_collects_completion_later() -> None:
    class SlowManager:
        def run_once(self):
            time.sleep(0.2)
            return {"total": 1}

    class FastManager:
        def run_once(self):
            return {"total": 1}

    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=1,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=True,
        account_task_timeout_sec=0.05,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=FastManager(),
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "slow": {"mode": "full", "strategy": StrategyStub(), "manager": SlowManager(), "balance_sampler": None},
            "fast": {"mode": "full", "strategy": StrategyStub(), "manager": FastManager(), "balance_sampler": None},
        },
        max_account_workers=2,
    )
    now_local = datetime(2026, 2, 13, 1, 0, tzinfo=ZoneInfo("UTC"))
    service.run_cycle(now_local=now_local, now_monotonic=0.0)
    summary = service.run_manage_tick()
    assert "error" not in summary["slow"]
    assert summary["slow"].get("slow") is True
    assert summary["slow"].get("running") is True

    time.sleep(0.25)
    completed = service.run_manage_tick()
    assert completed["slow"]["summary"]["total"] == 1


def test_entry_uses_shared_ranking_once_for_multi_accounts() -> None:
    class StoreStub:
        def __init__(self, symbols):
            self._symbols = symbols

        def list_open_symbols(self):
            return set(self._symbols)

    class ClientStub:
        def __init__(self):
            self.session = object()
            self.base_url = "https://fapi.binance.com"

    class StrategyStub:
        def __init__(self, open_symbols):
            self.top_n = 10
            self.entry_rank_fetch_multiplier = 3
            self.volume_threshold = 0.0
            self.ranker_max_workers = 24
            self.ranker_weight_limit_per_minute = 1000
            self.ranker_min_request_interval_ms = 20
            self.store = StoreStub(open_symbols)
            self.client = ClientStub()
            self.received = None

        def run_entry(self, shared_top_gainers=None):
            self.received = shared_top_gainers
            return {"status": "SUCCESS"}

    class ManagerStub:
        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

    s1 = StrategyStub({"A"})
    s2 = StrategyStub({"B", "C"})
    manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=11,
        entry_minute=0,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=23,
        daily_loss_cut_minute=59,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
    )
    service = StrategyRuntimeService(
        strategy=s1,
        manager=manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": s1, "manager": manager, "balance_sampler": None},
            "acc02": {"mode": "full", "strategy": s2, "manager": manager, "balance_sampler": None},
        },
        max_account_workers=2,
    )
    shared = [{"symbol": "AAAUSDT", "change": 1.23, "current_price": 2.0, "volume": 1000.0}]
    with patch.object(service, "_build_shared_top_gainers", return_value=shared) as mocked_ranker:
        service.run_cycle(now_local=datetime(2026, 2, 13, 11, 0, tzinfo=ZoneInfo("UTC")), now_monotonic=1.0)

    assert _wait_until(lambda: s2.received == shared)
    assert mocked_ranker.call_count == 1
    assert s1.received == shared
    assert s2.received == shared


def test_entry_runs_in_background_and_does_not_block_manage_cycle() -> None:
    started = Event()
    release = Event()

    class BlockingStrategy:
        def __init__(self):
            self.calls = 0

        def run_entry(self, shared_top_gainers=None):
            self.calls += 1
            started.set()
            release.wait(timeout=5)
            return {"status": "SUCCESS"}

    class ManagerStub:
        def __init__(self):
            self.calls = 0

        def run_once(self):
            self.calls += 1
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

    strategy = BlockingStrategy()
    manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=7,
        entry_minute=40,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=23,
        daily_loss_cut_minute=59,
        manager_interval_sec=1,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=True,
        account_task_timeout_sec=0.1,
    )
    service = StrategyRuntimeService(
        strategy=strategy,
        manager=manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": strategy, "manager": manager, "balance_sampler": None},
        },
        max_account_workers=1,
    )
    service._get_entry_ranking = MagicMock(return_value=None)
    now_local = datetime(2026, 2, 13, 7, 40, tzinfo=ZoneInfo("UTC"))

    start = time.monotonic()
    service.run_cycle(now_local=now_local, now_monotonic=1.0)
    elapsed = time.monotonic() - start

    assert elapsed < 1.0
    assert started.wait(timeout=1)
    service.run_cycle(now_local=now_local, now_monotonic=2.0)
    assert manager.calls >= 1
    assert strategy.calls == 1

    release.set()


def test_noon_protection_runs_once_for_full_and_loss_cut_only_accounts() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.noon_calls = 0
            self.last_day_start = None
            self.last_noon_time = None

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        def run_noon_protection_stop(self, day_start_utc, noon_time_utc):
            self.noon_calls += 1
            self.last_day_start = day_start_utc
            self.last_noon_time = noon_time_utc
            return {"total": 0, "updated_sl": 0, "errors": 0}

    full_manager = ManagerStub()
    loss_cut_only_manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
        noon_protection_enabled=True,
        noon_protection_hour=12,
        noon_protection_minute=0,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=full_manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": StrategyStub(), "manager": full_manager, "balance_sampler": None},
            "acc55": {
                "mode": "loss_cut_only",
                "strategy": StrategyStub(),
                "manager": loss_cut_only_manager,
                "balance_sampler": None,
            },
        },
        max_account_workers=2,
    )

    service.run_cycle(now_local=datetime(2026, 2, 13, 11, 59, tzinfo=ZoneInfo("UTC")), now_monotonic=1.0)
    assert full_manager.noon_calls == 0
    assert loss_cut_only_manager.noon_calls == 0

    service.run_cycle(now_local=datetime(2026, 2, 13, 12, 0, tzinfo=ZoneInfo("UTC")), now_monotonic=2.0)
    assert full_manager.noon_calls == 1
    assert loss_cut_only_manager.noon_calls == 1
    assert full_manager.last_day_start == datetime(2026, 2, 13, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert full_manager.last_noon_time == datetime(2026, 2, 13, 12, 0, tzinfo=ZoneInfo("UTC"))
    assert loss_cut_only_manager.last_day_start == datetime(2026, 2, 13, 0, 0, tzinfo=ZoneInfo("UTC"))
    assert loss_cut_only_manager.last_noon_time == datetime(2026, 2, 13, 12, 0, tzinfo=ZoneInfo("UTC"))

    service.run_cycle(now_local=datetime(2026, 2, 13, 16, 0, tzinfo=ZoneInfo("UTC")), now_monotonic=3.0)
    assert full_manager.noon_calls == 1
    assert loss_cut_only_manager.noon_calls == 1


def test_noon_protection_skips_restart_beyond_two_hour_grace() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.noon_calls = 0

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        def run_noon_protection_stop(self, day_start_utc, noon_time_utc):
            self.noon_calls += 1
            return {"total": 0, "updated_sl": 0, "errors": 0}

    manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
        noon_protection_enabled=True,
        noon_protection_hour=12,
        noon_protection_minute=0,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": StrategyStub(), "manager": manager, "balance_sampler": None},
        },
        max_account_workers=1,
    )

    service._run_noon_protection_if_due(
        datetime(2026, 2, 13, 14, 1, tzinfo=ZoneInfo("UTC"))
    )
    assert manager.noon_calls == 0

    service._run_noon_protection_if_due(
        datetime(2026, 2, 13, 14, 30, tzinfo=ZoneInfo("UTC"))
    )
    assert manager.noon_calls == 0


def test_noon_protection_retries_only_failed_accounts_and_symbols() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self, fail_first: bool = False) -> None:
            self.fail_first = fail_first
            self.noon_calls = []

        def run_once(self):
            return {"total": 0}

        def run_noon_protection_stop(self, day_start_utc, noon_time_utc, symbols=None):
            self.noon_calls.append(symbols)
            if self.fail_first and len(self.noon_calls) == 1:
                return {
                    "total": 1,
                    "updated_sl": 0,
                    "errors": 1,
                    "failed_symbols": ["AKEUSDT"],
                }
            return {
                "total": 1,
                "updated_sl": 1,
                "errors": 0,
                "failed_symbols": [],
            }

    healthy = ManagerStub()
    flaky = ManagerStub(fail_first=True)
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
        noon_protection_enabled=True,
        noon_protection_hour=12,
        noon_protection_minute=0,
        noon_protection_retry_interval_sec=60.0,
        orphan_exit_order_cleanup_enabled=False,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=healthy,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": StrategyStub(), "manager": healthy, "balance_sampler": None},
            "acc04": {"mode": "full", "strategy": StrategyStub(), "manager": flaky, "balance_sampler": None},
        },
        max_account_workers=2,
    )

    noon = datetime(2026, 2, 13, 12, 0, tzinfo=ZoneInfo("UTC"))
    service.run_cycle(now_local=noon, now_monotonic=1.0)
    assert len(healthy.noon_calls) == 1
    assert len(flaky.noon_calls) == 1
    assert flaky.noon_calls[0] is None

    service.run_cycle(now_local=noon + timedelta(seconds=30), now_monotonic=2.0)
    assert len(healthy.noon_calls) == 1
    assert len(flaky.noon_calls) == 1

    service.run_cycle(now_local=noon + timedelta(seconds=60), now_monotonic=3.0)
    assert len(healthy.noon_calls) == 1
    assert len(flaky.noon_calls) == 2
    assert flaky.noon_calls[1] == {"AKEUSDT"}

    service.run_cycle(now_local=noon + timedelta(seconds=120), now_monotonic=4.0)
    assert len(healthy.noon_calls) == 1
    assert len(flaky.noon_calls) == 2

def test_morning_protection_runs_once_for_full_and_loss_cut_only_accounts() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.morning_calls = 0
            self.last_check_time = None
            self.last_min_hold_hours = None

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        def run_morning_protection_stop(self, check_time_utc, min_hold_hours):
            self.morning_calls += 1
            self.last_check_time = check_time_utc
            self.last_min_hold_hours = min_hold_hours
            return {"total": 0, "updated_sl": 0, "errors": 0}

    full_manager = ManagerStub()
    loss_cut_only_manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
        morning_protection_enabled=True,
        morning_protection_hour=7,
        morning_protection_minute=55,
        morning_protection_min_hold_hours=6.0,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=full_manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "acc01": {"mode": "full", "strategy": StrategyStub(), "manager": full_manager, "balance_sampler": None},
            "acc55": {
                "mode": "loss_cut_only",
                "strategy": StrategyStub(),
                "manager": loss_cut_only_manager,
                "balance_sampler": None,
            },
        },
        max_account_workers=2,
    )

    service.run_cycle(now_local=datetime(2026, 2, 13, 7, 54, tzinfo=ZoneInfo("UTC")), now_monotonic=1.0)
    assert full_manager.morning_calls == 0
    assert loss_cut_only_manager.morning_calls == 0

    service.run_cycle(now_local=datetime(2026, 2, 13, 7, 55, tzinfo=ZoneInfo("UTC")), now_monotonic=2.0)
    assert full_manager.morning_calls == 1
    assert loss_cut_only_manager.morning_calls == 1
    assert full_manager.last_check_time == datetime(2026, 2, 13, 7, 55, tzinfo=timezone.utc)
    assert full_manager.last_min_hold_hours == 6.0
    assert loss_cut_only_manager.last_check_time == datetime(2026, 2, 13, 7, 55, tzinfo=timezone.utc)
    assert loss_cut_only_manager.last_min_hold_hours == 6.0

    service.run_cycle(now_local=datetime(2026, 2, 13, 8, 10, tzinfo=ZoneInfo("UTC")), now_monotonic=3.0)
    assert full_manager.morning_calls == 1
    assert loss_cut_only_manager.morning_calls == 1


def test_morning_protection_skips_restart_beyond_two_hour_grace() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.morning_calls = 0

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        def run_morning_protection_stop(self, check_time_utc, min_hold_hours):
            self.morning_calls += 1
            return {"total": 0, "updated_sl": 0, "errors": 0}

    manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
        morning_protection_enabled=True,
        morning_protection_hour=7,
        morning_protection_minute=55,
        morning_protection_min_hold_hours=6.0,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "55": {
                "mode": "loss_cut_only",
                "strategy": StrategyStub(),
                "manager": manager,
                "balance_sampler": None,
                "morning_protection_enabled": True,
                "morning_protection_hour": 7,
                "morning_protection_minute": 55,
                "morning_protection_min_hold_hours": 8.0,
            },
        },
        max_account_workers=1,
    )

    service._run_morning_protection_if_due(
        datetime(2026, 2, 13, 10, 0, 1, tzinfo=ZoneInfo("UTC"))
    )
    assert manager.morning_calls == 0

    service._run_morning_protection_if_due(
        datetime(2026, 2, 13, 10, 30, tzinfo=ZoneInfo("UTC"))
    )
    assert manager.morning_calls == 0


def test_morning_protection_runs_only_once_per_local_day() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.morning_calls = 0

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        def run_morning_protection_stop(self, check_time_utc, min_hold_hours):
            self.morning_calls += 1
            return {"total": 0, "updated_sl": 0, "errors": 0}

    manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
        morning_protection_enabled=True,
        morning_protection_hour=7,
        morning_protection_minute=55,
        morning_protection_min_hold_hours=6.0,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "55": {
                "mode": "loss_cut_only",
                "strategy": StrategyStub(),
                "manager": manager,
                "balance_sampler": None,
                "morning_protection_enabled": True,
                "morning_protection_hour": 7,
                "morning_protection_minute": 55,
                "morning_protection_min_hold_hours": 8.0,
            },
        },
        max_account_workers=1,
    )

    service._run_morning_protection_if_due(
        datetime(2026, 2, 13, 7, 55, tzinfo=ZoneInfo("UTC"))
    )
    service._run_morning_protection_if_due(
        datetime(2026, 2, 13, 7, 55, 30, tzinfo=ZoneInfo("UTC"))
    )
    service._run_morning_protection_if_due(
        datetime(2026, 2, 14, 7, 55, tzinfo=ZoneInfo("UTC"))
    )

    assert manager.morning_calls == 2


def test_hourly_exchange_take_profit_runs_for_loss_cut_only_account_at_configured_minute() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.hourly_take_profit_calls = 0
            self.last_now_local = None
            self.last_drop_pct = None

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        def run_hourly_exchange_take_profit(self, now_local, drop_pct):
            self.hourly_take_profit_calls += 1
            self.last_now_local = now_local
            self.last_drop_pct = drop_pct
            return {"closed_take_profit": 0, "errors": 0}

    manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "55": {
                "mode": "loss_cut_only",
                "strategy": StrategyStub(),
                "manager": manager,
                "balance_sampler": None,
                "hourly_exchange_take_profit_enabled": True,
                "hourly_exchange_take_profit_minute": 0,
                "hourly_exchange_take_profit_drop_pct": 20.0,
            },
        },
        max_account_workers=1,
    )

    service._run_hourly_exchange_take_profit_if_due(
        datetime(2026, 2, 13, 10, 0, tzinfo=ZoneInfo("UTC"))
    )

    assert manager.hourly_take_profit_calls == 1
    assert manager.last_now_local == datetime(2026, 2, 13, 10, 0, tzinfo=ZoneInfo("UTC"))
    assert manager.last_drop_pct == 20.0


def test_hourly_exchange_take_profit_runs_only_once_per_local_hour() -> None:
    class StrategyStub:
        def run_entry(self):
            return {"status": "SKIPPED"}

    class ManagerStub:
        def __init__(self) -> None:
            self.hourly_take_profit_calls = 0

        def run_once(self):
            return {"total": 0}

        def run_daily_loss_cut(self):
            return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        def run_hourly_exchange_take_profit(self, now_local, drop_pct):
            self.hourly_take_profit_calls += 1
            return {"closed_take_profit": 0, "errors": 0}

    manager = ManagerStub()
    cfg = ServiceRuntimeConfig(
        timezone_name="UTC",
        entry_hour=23,
        entry_minute=59,
        entry_misfire_grace_min=120,
        entry_catchup_enabled=True,
        daily_loss_cut_enabled=False,
        daily_loss_cut_hour=11,
        daily_loss_cut_minute=55,
        manager_interval_sec=3600,
        manager_max_catch_up_runs=1,
        loop_sleep_sec=1.0,
        run_manage_on_startup=False,
    )
    service = StrategyRuntimeService(
        strategy=StrategyStub(),
        manager=manager,
        cfg=cfg,
        now_monotonic=0.0,
        account_runtimes={
            "55": {
                "mode": "loss_cut_only",
                "strategy": StrategyStub(),
                "manager": manager,
                "balance_sampler": None,
                "hourly_exchange_take_profit_enabled": True,
                "hourly_exchange_take_profit_minute": 0,
                "hourly_exchange_take_profit_drop_pct": 20.0,
            },
        },
        max_account_workers=1,
    )

    service._run_hourly_exchange_take_profit_if_due(
        datetime(2026, 2, 13, 10, 0, tzinfo=ZoneInfo("UTC"))
    )
    service._run_hourly_exchange_take_profit_if_due(
        datetime(2026, 2, 13, 10, 0, 30, tzinfo=ZoneInfo("UTC"))
    )
    service._run_hourly_exchange_take_profit_if_due(
        datetime(2026, 2, 13, 11, 0, tzinfo=ZoneInfo("UTC"))
    )

    assert manager.hourly_take_profit_calls == 2
