import unittest
from datetime import datetime
from threading import Event, Thread
import time
from unittest.mock import patch
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


def test_manage_timeout_marks_account_as_slow_but_waits_for_completion() -> None:
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
    assert summary["slow"]["summary"]["total"] == 1


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

    assert mocked_ranker.call_count == 1
    assert s1.received == shared
    assert s2.received == shared
