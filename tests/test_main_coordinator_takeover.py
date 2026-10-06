"""Test verifying that AccountCoordinator.step() is the unified executor across main.py and runtime_service."""

from datetime import datetime, timezone
import unittest
from unittest.mock import MagicMock, patch

import main
from core.runtime_service import StrategyRuntimeService, ServiceRuntimeConfig


class TestMainCoordinatorTakeover(unittest.TestCase):
    """Verify that main.py CLI commands route strictly through AccountCoordinator.step()."""

    def test_main_entry_calls_coordinator_step(self):
        mock_coord = MagicMock()
        mock_coord.step.return_value = {"status": "SUCCESS"}

        account_runtimes = {
            "acc01": {
                "coordinator": mock_coord,
                "strategy": MagicMock(),
                "manager": MagicMock(),
            }
        }

        with patch("main.parse_args") as mock_args, \
             patch("main.load_config"), \
             patch("main.setup_logging"), \
             patch("main.file_lock"), \
             patch("main.create_components", return_value=(None, None, None, {"timezone": "UTC"}, None, account_runtimes)), \
             patch("main.warn_if_outside_entry_window"):
            mock_args.return_value = MagicMock(
                command="entry",
                config="config.ini",
                trade_day_utc="2026-10-06",
            )
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        mock_coord.step.assert_called_once_with(action="entry", trade_day_utc="2026-10-06")

    def test_main_manage_calls_coordinator_step(self):
        mock_coord = MagicMock()
        mock_coord.step.return_value = {"status": "COMPLETED", "total": 2, "errors": 0}

        account_runtimes = {
            "acc01": {
                "coordinator": mock_coord,
                "strategy": MagicMock(),
                "manager": MagicMock(),
            }
        }

        mock_runtime_cfg = MagicMock()
        mock_runtime_cfg.getint.return_value = 60
        mock_runtime_cfg.get.return_value = "UTC"

        with patch("main.parse_args") as mock_args, \
             patch("main.load_config"), \
             patch("main.setup_logging"), \
             patch("main.file_lock"), \
             patch("main.create_components", return_value=(None, None, None, mock_runtime_cfg, None, account_runtimes)):
            mock_args.return_value = MagicMock(
                command="manage",
                config="config.ini",
                loop=False,
            )
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        mock_coord.step.assert_called_once_with(action="manage", config=account_runtimes["acc01"])

    def test_main_loss_cut_calls_coordinator_step(self):
        mock_coord = MagicMock()
        mock_coord.step.return_value = {"status": "COMPLETED", "errors": 0}

        account_runtimes = {
            "acc01": {
                "coordinator": mock_coord,
                "strategy": MagicMock(),
                "manager": MagicMock(),
            }
        }

        with patch("main.parse_args") as mock_args, \
             patch("main.load_config"), \
             patch("main.setup_logging"), \
             patch("main.file_lock"), \
             patch("main.create_components", return_value=(None, None, None, {"timezone": "UTC"}, None, account_runtimes)):
            mock_args.return_value = MagicMock(
                command="loss-cut",
                config="config.ini",
            )
            exit_code = main.main()

        self.assertEqual(exit_code, 0)
        mock_coord.step.assert_called_once_with(action="loss_cut", config=account_runtimes["acc01"])


class TestRuntimeServiceCoordinatorTakeover(unittest.TestCase):
    """Verify that StrategyRuntimeService executes all actions via coordinator.step()."""

    def setUp(self):
        self.mock_coord = MagicMock()
        self.mock_coord.step.return_value = {"status": "COMPLETED", "total": 0, "errors": 0}
        self.account_runtimes = {
            "acc01": {
                "mode": "full",
                "coordinator": self.mock_coord,
                "strategy": MagicMock(),
                "manager": MagicMock(),
                "balance_sampler": None,
                "daily_loss_cut_enabled": True,
                "morning_protection_enabled": True,
                "hourly_exchange_take_profit_enabled": True,
            }
        }
        self.cfg = ServiceRuntimeConfig(
            timezone_name="UTC",
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=30,
            entry_catchup_enabled=True,
            daily_loss_cut_enabled=True,
            daily_loss_cut_hour=16,
            daily_loss_cut_minute=0,
            manager_interval_sec=60,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=False,
            max_account_workers=1,
            account_task_timeout_sec=5.0,
            account_failure_threshold=3,
            account_cooldown_cycles=1,
            noon_protection_hour=12,
            noon_protection_minute=0,
            morning_protection_hour=8,
            morning_protection_minute=30,
            hourly_exchange_take_profit_minute=45,
            orphan_exit_order_cleanup_enabled=True,
            orphan_exit_order_cleanup_hour=0,
            orphan_exit_order_cleanup_minute=10,
        )
        self.service = StrategyRuntimeService(
            strategy=MagicMock(),
            manager=MagicMock(),
            cfg=self.cfg,
            account_runtimes=self.account_runtimes,
        )

    def test_manage_for_account_calls_coordinator_step_manage(self):
        res = self.service._run_manage_for_account("acc01")
        self.mock_coord.step.assert_called()
        call_kwargs = self.mock_coord.step.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "manage")
        self.assertEqual(res["summary"]["status"], "COMPLETED")

    def test_run_entry_calls_coordinator_step_entry(self):
        trade_day = datetime(2026, 10, 6, tzinfo=timezone.utc).date()
        self.service._run_entry_with_shared(
            strategy=self.account_runtimes["acc01"]["strategy"],
            shared_top_gainers=None,
            trade_day=trade_day,
            coordinator=self.mock_coord,
        )
        self.mock_coord.step.assert_called_with(
            action="entry",
            trade_day_utc="2026-10-06",
            shared_top_gainers=None,
            strategy=self.account_runtimes["acc01"]["strategy"],
        )

    def test_daily_loss_cut_calls_coordinator_step_loss_cut(self):
        now_local = datetime(2026, 10, 6, 16, 5, tzinfo=timezone.utc)
        self.service._run_daily_loss_cut_if_due(now_local)
        self.mock_coord.step.assert_called()
        call_kwargs = self.mock_coord.step.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "loss_cut")

    def test_noon_protection_calls_coordinator_step_noon_protection(self):
        now_local = datetime(2026, 10, 6, 12, 1, tzinfo=timezone.utc)
        self.service._run_noon_protection_if_due(now_local)
        self.mock_coord.step.assert_called()
        call_kwargs = self.mock_coord.step.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "noon_protection")

    def test_morning_protection_calls_coordinator_step_morning_protection(self):
        now_local = datetime(2026, 10, 6, 8, 35, tzinfo=timezone.utc)
        self.service._run_morning_protection_if_due(now_local)
        self.mock_coord.step.assert_called()
        call_kwargs = self.mock_coord.step.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "morning_protection")

    def test_hourly_take_profit_calls_coordinator_step_hourly_tp(self):
        now_local = datetime(2026, 10, 6, 14, 50, tzinfo=timezone.utc)
        self.service._run_hourly_exchange_take_profit_if_due(now_local)
        self.mock_coord.step.assert_called()
        call_kwargs = self.mock_coord.step.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "hourly_take_profit")

    def test_orphan_cleanup_calls_coordinator_step_orphan_cleanup(self):
        now_local = datetime(2026, 10, 6, 0, 15, tzinfo=timezone.utc)
        self.service._run_orphan_exit_order_cleanup_if_due(now_local)
        self.mock_coord.step.assert_called()
        call_kwargs = self.mock_coord.step.call_args.kwargs
        self.assertEqual(call_kwargs.get("action"), "orphan_cleanup")
