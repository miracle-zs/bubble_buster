import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from core.position_manager import PositionManager
from core.runtime_service import ServiceRuntimeConfig, StrategyRuntimeService
from core.state_store import StateStore
import tests.test_runtime_service  # noqa: F401


class PortfolioLossCutTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=db_path, schema_path=schema_path, account_id="acc01")
        self.store.init_schema()
        self.run_id, _ = self.store.create_run("2026-07-28", account_id="acc01")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert_short(self, symbol: str) -> int:
        opened_at = datetime(2026, 7, 28, 0, 5, tzinfo=timezone.utc)
        return self.store.insert_position(
            run_id=self.run_id,
            symbol=symbol,
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=200.0,
            tp_price=None,
            sl_price=110.0,
            tp_order_id=101,
            sl_order_id=102,
            tp_client_order_id="tp-old",
            sl_client_order_id="sl-old",
            opened_at_utc=opened_at.isoformat(),
            expire_at_utc=(opened_at.replace(hour=12)).isoformat(),
            status="OPEN",
        )

    def _manager(self, client: MagicMock) -> PositionManager:
        return PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            account_id="acc01",
        )

    def test_preclose_new_position_is_not_closed_by_previous_cycle_stop(self):
        self._insert_short("BTCUSDT")
        self.store.save_risk_cycle_target_set("LOSS_CUT", "2026-07-27", {
            "cycle_date": "2026-07-27", "triggered": True, "close_complete": True,
            "baseline_equity_usdt": 100.0,
        }, status="TRIGGERED")
        client = MagicMock()
        manager = self._manager(client)
        tz = ZoneInfo("Asia/Shanghai")
        result = manager.run_portfolio_loss_cut(
            current_equity_usdt=80, now_local=datetime(2026, 7, 28, 7, 59, 50, tzinfo=tz),
        )
        self.assertEqual(result["status"], "PRE_RESET")
        client.create_order.assert_not_called()
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 80.0)
        result = manager.run_portfolio_loss_cut(
            current_equity_usdt=80, now_local=datetime(2026, 7, 28, 8, 0, 1, tzinfo=tz),
        )
        self.assertEqual(result["status"], "MONITORING")
        self.assertEqual(result["baseline_equity"], 80.0)
        self.assertFalse(self.store.get_risk_cycle_target_set("LOSS_CUT", "2026-07-28")["targets"]["triggered"])
        client.create_order.assert_not_called()

    def test_uses_first_08_snapshot_and_closes_all_position_directions(self) -> None:
        short_id = self._insert_short("BTCUSDT")
        # 08:00 Asia/Shanghai is 00:00 UTC. The first snapshot must be used as
        # the baseline, not the lower later snapshot.
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        self.store.add_wallet_snapshot("2026-07-28T00:01:00+00:00", 90.0)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {"symbol": "BTCUSDT", "positionAmt": "-1", "positionSide": "BOTH"},
            {"symbol": "ETHUSDT", "positionAmt": "2", "positionSide": "BOTH"},
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = [
            {"orderId": 201, "clientOrderId": "plc-btc", "status": "FILLED", "side": "BUY"},
            {"orderId": 202, "clientOrderId": "plc-eth", "status": "FILLED", "side": "SELL"},
        ]

        manager = self._manager(client)
        result = manager.run_portfolio_loss_cut(
            current_equity_usdt=96.0,
            now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            loss_pct=3.5,
        )

        self.assertEqual(result["status"], "TRIGGERED")
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["closed_loss_cut"], 2)
        self.assertEqual(result["errors"], 0)
        rec = self.store.get_risk_cycle_target_set("LOSS_CUT", "2026-07-28")
        assert rec is not None
        state = rec["targets"]
        self.assertAlmostEqual(float(state["baseline_equity_usdt"]), 100.0)
        self.assertAlmostEqual(float(state["threshold_equity_usdt"]), 96.5)
        self.assertTrue(state["triggered"])
        self.assertTrue(state["close_complete"])

        first_order = client.create_order.call_args_list[0].kwargs
        self.assertEqual(first_order["side"], "BUY")
        self.assertTrue(first_order["reduceOnly"])
        second_order = client.create_order.call_args_list[1].kwargs
        self.assertEqual(second_order["side"], "SELL")
        self.assertTrue(second_order["reduceOnly"])

        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute("SELECT status, close_reason FROM positions WHERE id = ?", (short_id,)).fetchone()
        self.assertEqual(row["status"], "CLOSED_PORTFOLIO_LOSS_CUT")
        self.assertEqual(row["close_reason"], "PORTFOLIO_EQUITY_LOSS_CUT")

    def test_trigger_state_deduplicates_close_until_next_08_cycle(self) -> None:
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        client = MagicMock()
        client.get_position_risk.return_value = []
        manager = self._manager(client)

        monitoring = manager.run_portfolio_loss_cut(
            current_equity_usdt=99.0,
            now_local=datetime(2026, 7, 28, 8, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
            loss_pct=3.5,
        )
        self.assertEqual(monitoring["status"], "MONITORING")

        triggered = manager.run_portfolio_loss_cut(
            current_equity_usdt=96.0,
            now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            loss_pct=3.5,
        )
        self.assertEqual(triggered["status"], "TRIGGERED")
        rec = self.store.get_risk_cycle_target_set("LOSS_CUT", "2026-07-28")
        assert rec is not None
        self.assertTrue(rec["targets"]["triggered"])

        same_cycle = manager.run_portfolio_loss_cut(
            current_equity_usdt=95.0,
            now_local=datetime(2026, 7, 28, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            loss_pct=3.5,
        )
        self.assertEqual(same_cycle["status"], "ALREADY_TRIGGERED")

        next_cycle = manager.run_portfolio_loss_cut(
            current_equity_usdt=100.0,
            now_local=datetime(2026, 7, 29, 8, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
            loss_pct=3.5,
        )
        self.assertEqual(next_cycle["status"], "MONITORING")

    def test_runtime_checks_after_wallet_snapshot_without_blocking_entry_wait(self) -> None:
        class StrategyStub:
            def __init__(self) -> None:
                self.stop_calls = 0

            def request_entry_wait_stop(self) -> None:
                self.stop_calls += 1

            def run_equity_recovery_take_profit(self):
                return {"status": "SKIPPED"}

        class ManagerStub:
            def __init__(self) -> None:
                self.portfolio_calls = 0

            def run_once(self):
                return {"total": 0}

            def run_portfolio_loss_cut(self, **_kwargs):
                self.portfolio_calls += 1
                return {"status": "TRIGGERED", "triggered": True, "close_complete": True}

        class WalletSamplerStub:
            def run_once(self):
                return {"equity": 96.0, "snapshot_id": 1}

        strategy = StrategyStub()
        manager = ManagerStub()
        cfg = ServiceRuntimeConfig(
            timezone_name="Asia/Shanghai",
            entry_hour=7,
            entry_minute=40,
            entry_misfire_grace_min=120,
            entry_catchup_enabled=True,
            daily_loss_cut_enabled=False,
            daily_loss_cut_hour=11,
            daily_loss_cut_minute=55,
            manager_interval_sec=60,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=True,
            portfolio_loss_cut_enabled=True,
            portfolio_loss_cut_pct=3.5,
        )
        service = StrategyRuntimeService(
            strategy=strategy,
            manager=manager,
            balance_sampler=WalletSamplerStub(),
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": strategy,
                    "manager": manager,
                    "balance_sampler": WalletSamplerStub(),
                }
            },
        )
        try:
            output = service.run_manage_tick(
                now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
            )
            self.assertEqual(manager.portfolio_calls, 1)
            self.assertEqual(output["acc01"]["portfolio_loss_cut"]["status"], "TRIGGERED")
            self.assertEqual(strategy.stop_calls, 0)
        finally:
            service._entry_executor.shutdown(wait=True, cancel_futures=True)
            service._manage_executor.shutdown(wait=True, cancel_futures=True)
            service._scheduled_executor.shutdown(wait=True, cancel_futures=True)

    def test_legacy_daily_loss_cut_remains_enabled_alongside_portfolio_stop(self) -> None:
        class StrategyStub:
            def run_entry(self):
                return {"status": "SKIPPED"}

        class ManagerStub:
            def __init__(self) -> None:
                self.daily_calls = 0

            def run_daily_loss_cut(self):
                self.daily_calls += 1
                return {"total": 0, "closed_loss_cut": 0, "errors": 0}

        strategy = StrategyStub()
        manager = ManagerStub()
        cfg = ServiceRuntimeConfig(
            timezone_name="Asia/Shanghai",
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
            portfolio_loss_cut_enabled=True,
        )
        service = StrategyRuntimeService(
            strategy=strategy,
            manager=manager,
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": strategy,
                    "manager": manager,
                    "daily_loss_cut_enabled": True,
                    "portfolio_loss_cut_enabled": True,
                }
            },
        )
        try:
            service._run_daily_loss_cut_if_due(
                datetime(2026, 7, 28, 11, 55, tzinfo=ZoneInfo("Asia/Shanghai"))
            )
            self.assertEqual(manager.daily_calls, 1)
        finally:
            service._entry_executor.shutdown(wait=True, cancel_futures=True)
            service._manage_executor.shutdown(wait=True, cancel_futures=True)
            service._scheduled_executor.shutdown(wait=True, cancel_futures=True)

    def test_has_pending_entry_wait_clears_plan_when_portfolio_latched(self) -> None:
        from core.strategy_top10_short import Top10ShortStrategy

        client = MagicMock()
        client.get_position_risk.return_value = []
        strategy = Top10ShortStrategy(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            leverage=2,
            top_n=10,
            volume_threshold=0.0,
            tp_price_drop_pct=20.0,
            sl_liq_buffer_pct=1.0,
            max_hold_hours=47.5,
            trigger_price_type="CONTRACT_PRICE",
            allocation_splits=10,
            entry_fee_buffer_pct=1.0,
            entry_shrink_retry_count=3,
            entry_shrink_step_pct=10.0,
            entry_rank_fetch_multiplier=3,
            ranker_max_workers=4,
            ranker_weight_limit_per_minute=1000,
            ranker_min_request_interval_ms=20,
            account_id="acc01",
        )
        self.store.save_entry_plan(
            plan_id="bearish_hour_entry_wait_v1",
            symbol="ALL",
            status="WAITING_KLINE",
            hour_open_utc="2026-07-28T01:00:00+00:00",
            next_wakeup_utc="2026-07-28T02:00:01+00:00",
            plan_payload={"run_id": self.run_id, "pending": {"1": {"symbol": "BTCUSDT"}}},
        )
        self.assertTrue(strategy.has_pending_entry_wait())

        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.store.save_risk_cycle_target_set(
            cycle_type="LOSS_CUT",
            cycle_key=today_str,
            targets={"cycle_date": today_str, "triggered": True},
            status="TRIGGERED",
        )
        self.assertTrue(strategy._portfolio_entry_latched())
        self.assertFalse(strategy.has_pending_entry_wait())
        plan = self.store.get_entry_plan("bearish_hour_entry_wait_v1")
        self.assertEqual(plan["status"], "COMPLETED")
        self.assertEqual(plan["plan_payload"], {})


if __name__ == "__main__":
    unittest.main()
