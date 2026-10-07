"""Unit tests for AccountCoordinator serialized execution steps."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from core.decision.kernel import DecisionKernel
from core.execution.coordinator import AccountCoordinator
from core.execution.engine import ExecutionEngine
from core.execution.ledger import TradingLedger
from core.execution.models import (
    AttemptStatus,
    EpisodeStatus,
    IntentStatus,
    OrderAttempt,
    OrderIntent,
    PositionEpisode,
)
from core.state_store import StateStore


class TestAccountCoordinator(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_coordinator.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(
            db_path=self.db_path,
            schema_path=schema_path,
            account_id="acc_coord",
        )
        self.store.init_schema()

        self.client = MagicMock()
        self.client.format_order_qty.side_effect = lambda sym, qty: str(qty)
        self.ledger = TradingLedger(store=self.store)
        self.engine = ExecutionEngine(
            client=self.client,
            store=self.store,
            ledger=self.ledger,
        )
        self.kernel = DecisionKernel()
        self.coordinator = AccountCoordinator(
            account_id="acc_coord",
            store=self.store,
            engine=self.engine,
            ledger=self.ledger,
            client=self.client,
            kernel=self.kernel,
            now_iso_fn=lambda: "2026-10-05T12:00:00Z",
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_step_recovers_unknown_attempt_before_dispatch(self):
        """In-flight UNKNOWN order attempts must be queried and recovered."""
        # 1. Setup an intent and UNKNOWN attempt in store
        self.store.save_order_intent(
            intent_id="intent_unk",
            client_intent_key="key_unk",
            symbol="BTCUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=1.0,
            status=IntentStatus.SUBMITTED.value,
        )
        self.store.save_order_attempt(
            attempt_id="att_unk",
            intent_id="intent_unk",
            symbol="BTCUSDT",
            client_order_id="cid_unk",
            status=AttemptStatus.UNKNOWN.value,
            submitted_qty=1.0,
        )

        # Exchange says it was FILLED
        self.client.get_order.return_value = {
            "orderId": 8888,
            "status": "FILLED",
            "executedQty": "1.0",
            "avgPrice": "50000.0",
            "cumQuote": "50000.0",
            "clientOrderId": "cid_unk",
        }

        result = self.coordinator.step(max_duration_sec=10.0)

        self.assertEqual(result["recovered_attempts_count"], 1)
        self.assertEqual(result["uncertain_symbols"], [])
        self.assertEqual(result["status"], "COMPLETED")

        # Verify attempt transitioned to FILLED
        att = self.store.get_order_attempt("att_unk")
        self.assertEqual(att["status"], AttemptStatus.FILLED.value)

    def test_step_freezes_uncertain_symbol_if_recovery_remains_unknown(self):
        """If an attempt cannot be verified, its symbol is marked uncertain and frozen from entry."""
        self.store.save_order_intent(
            intent_id="intent_unk_2",
            client_intent_key="key_unk_2",
            symbol="ETHUSDT",
            side="SELL",
            order_type="MARKET",
            target_qty=5.0,
            status=IntentStatus.SUBMITTED.value,
        )
        self.store.save_order_attempt(
            attempt_id="att_unk_2",
            intent_id="intent_unk_2",
            symbol="ETHUSDT",
            client_order_id="cid_unk_2",
            status=AttemptStatus.UNKNOWN.value,
            submitted_qty=5.0,
        )

        # Exchange query raises network/API error
        from infra.binance_futures_client import BinanceAPIError, OrderStateUnknownError
        self.client.get_order.side_effect = OrderStateUnknownError(
            symbol="ETHUSDT",
            client_order_id="cid_unk_2",
            cause=BinanceAPIError(code=-1001, message="timeout"),
        )

        # Top gainers include ETHUSDT and SOLUSDT
        top_gainers = [{"symbol": "ETHUSDT"}, {"symbol": "SOLUSDT"}]
        prices = {"ETHUSDT": 3000.0, "SOLUSDT": 150.0}

        self.client.create_order.return_value = {
            "orderId": 9999,
            "status": "FILLED",
            "executedQty": "1.0",
            "avgPrice": "150.0",
        }

        result = self.coordinator.step(
            max_duration_sec=10.0,
            prices=prices,
            top_gainers=top_gainers,
            config={"max_positions": 10},
        )

        self.assertIn("ETHUSDT", result["uncertain_symbols"])
        # ETHUSDT must NOT have been submitted, but SOLUSDT can enter!
        # Check created orders
        calls = self.client.create_order.call_args_list
        submitted_symbols = [c.kwargs.get("symbol") for c in calls]
        self.assertNotIn("ETHUSDT", submitted_symbols)
        self.assertIn("SOLUSDT", submitted_symbols)

    def test_process_due_task_occurrences(self):
        """Due TaskOccurrences are executed and marked COMPLETED."""
        self.store.save_task_occurrence(
            task_occurrence_id="task_1",
            task_type="NOON_PROTECTION",
            cycle_key="2026-10-05_noon",
            due_at_utc="2026-10-05T11:59:00Z",  # Due relative to 12:00:00Z
            status="PENDING",
        )

        executed_tasks = []

        def handle_noon(task):
            executed_tasks.append(task["task_occurrence_id"])

        result = self.coordinator.step(
            max_duration_sec=10.0,
            task_handlers={"NOON_PROTECTION": handle_noon},
        )

        self.assertEqual(result["processed_tasks_count"], 1)
        self.assertEqual(executed_tasks, ["task_1"])

        saved = self.store.get_task_occurrence("task_1")
        self.assertEqual(saved["status"], "COMPLETED")

    def test_check_entry_plan_wakeups(self):
        """Entry plans with WAITING_KLINE and due wakeup are detected."""
        self.store.save_entry_plan(
            plan_id="plan_1",
            symbol="BTCUSDT",
            status="WAITING_KLINE",
            hour_open_utc="2026-10-05T11:00:00Z",
            next_wakeup_utc="2026-10-05T12:00:00Z",
            plan_payload={"stage": "WAITING_CONFIRMATION"},
        )

        result = self.coordinator.step(max_duration_sec=10.0)
        self.assertTrue(result["entry_plan_ready"])

    def test_step_entry_natively_creates_intents_and_episode(self):
        """Native entry creates OrderIntent, creates order attempt, and creates PositionEpisode."""
        self.client.create_order.return_value = {
            "orderId": 101,
            "status": "FILLED",
            "executedQty": "2.0",
            "avgPrice": "50.0",
        }
        mock_strategy = MagicMock()
        self.coordinator.strategy = mock_strategy

        result = self.coordinator.step(
            action="entry",
            shared_top_gainers=[{"symbol": "SOLUSDT", "current_price": 50.0}],
            config={"max_positions": 10, "target_notional_per_pos": 100.0},
        )

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["opened"], 1)
        mock_strategy.run_entry.assert_not_called()

        # Check PositionEpisode was created with status OPEN
        ep = self.store.get_active_position_episode("SOLUSDT")
        self.assertIsNotNone(ep)
        self.assertEqual(ep["status"], EpisodeStatus.OPEN.value)
        self.assertAlmostEqual(float(ep["current_qty"]), 2.0)

    def test_step_loss_cut_natively_executes_portfolio_loss_cut(self):
        """Native loss cut evaluates DecisionKernel and market-closes positions."""
        run_id, _ = self.store.create_run("2026-10-05_loss", account_id=self.store.account_id)
        # Setup open position and episode
        self.store.save_position_episode(
            episode_id="ep_loss",
            symbol="DOGEUSDT",
            status=EpisodeStatus.OPEN.value,
            current_qty=100.0,
        )
        self.store.insert_position(
            run_id=run_id,
            symbol="DOGEUSDT",
            side="SHORT",
            qty=100.0,
            entry_price=0.2,
            liq_price_open=None,
            tp_price=None,
            sl_price=None,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc="2026-10-05T00:00:00Z",
            expire_at_utc="2026-10-06T00:00:00Z",
            status="OPEN",
            episode_id="ep_loss",
        )
        mock_manager = MagicMock()
        self.coordinator.manager = mock_manager

        self.client.create_order.return_value = {
            "orderId": 202,
            "status": "FILLED",
            "executedQty": "100.0",
            "avgPrice": "0.2",
        }

        result = self.coordinator.step(
            action="loss_cut",
            wallet_balance=1000.0,
            equity=950.0,  # 5% loss > 3.5% threshold
            config={"baseline_equity": 1000.0, "portfolio_loss_cut_pct": 3.5},
        )

        self.assertEqual(result["status"], "TRIGGERED")
        self.assertTrue(result["triggered"])
        mock_manager.run_daily_loss_cut.assert_not_called()

        # Check position episode was closed
        ep = self.store.get_position_episode("ep_loss")
        self.assertEqual(ep["status"], EpisodeStatus.CLOSED.value)

    def test_step_noon_protection_natively_updates_sl_and_policy(self):
        """Native noon protection evaluates noon high, submits new stop loss, cancels old stop loss, and updates store."""
        run_id, _ = self.store.create_run("2026-10-05_noon", account_id=self.store.account_id)
        pos_id = self.store.insert_position(
            run_id=run_id,
            symbol="XRPUSDT",
            side="SHORT",
            qty=100.0,
            entry_price=1.0,
            liq_price_open=None,
            tp_price=None,
            sl_price=1.20,  # Old SL
            tp_order_id=None,
            sl_order_id=777,
            tp_client_order_id=None,
            sl_client_order_id="old_xrp_sl",
            opened_at_utc="2026-10-05T00:00:00Z",
            expire_at_utc="2026-10-06T00:00:00Z",
            status="OPEN",
        )
        mock_manager = MagicMock()
        self.coordinator.manager = mock_manager

        self.client.create_order.return_value = {
            "orderId": 999,
            "status": "NEW",
            "clientOrderId": "nsl_xrp_1",
        }

        # Mark price is 1.10 (lower than 1.20, tightens stop loss)
        result = self.coordinator.step(
            action="noon_protection",
            prices={"XRPUSDT": 1.10},
        )

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["updated_sl"], 1)
        mock_manager.run_noon_protection_stop.assert_not_called()

        # 1. Assert new stop order submitted to exchange
        self.client.create_order.assert_called()
        submitted_kwargs = self.client.create_order.call_args.kwargs
        self.assertEqual(submitted_kwargs["symbol"], "XRPUSDT")
        self.assertEqual(submitted_kwargs["type"], "STOP_MARKET")
        self.assertEqual(submitted_kwargs["side"], "BUY")
        self.assertEqual(submitted_kwargs["closePosition"], True)

        # 2. Assert old stop order was canceled on exchange
        self.client.cancel_order.assert_called_with(
            symbol="XRPUSDT",
            order_id=777,
            orig_client_order_id="old_xrp_sl",
        )

        # 3. Verify updated sl_price and new sl_order_id in store
        pos = self.store.get_position(pos_id)
        self.assertAlmostEqual(float(pos["sl_price"]), 1.10)
        self.assertEqual(pos["sl_order_id"], 999)

        # 4. Verify protection policy state saved
        state = self.store.get_protection_policy_state("NOON_PROTECTION")
        self.assertIsNotNone(state)
        self.assertEqual(state["order_id"], "999")

    def test_step_morning_protection_natively_updates_sl_and_policy(self):
        """Native morning protection submits new stop loss, cancels old stop loss, and updates store."""
        run_id, _ = self.store.create_run("2026-10-05_morn", account_id=self.store.account_id)
        pos_id = self.store.insert_position(
            run_id=run_id,
            symbol="ADAUSDT",
            side="SHORT",
            qty=200.0,
            entry_price=0.50,
            liq_price_open=None,
            tp_price=None,
            sl_price=0.60,
            tp_order_id=None,
            sl_order_id=555,
            tp_client_order_id=None,
            sl_client_order_id="old_ada_sl",
            opened_at_utc="2026-10-05T00:00:00Z",
            expire_at_utc="2026-10-06T00:00:00Z",
            status="OPEN",
        )
        mock_manager = MagicMock()
        self.coordinator.manager = mock_manager

        self.client.create_order.return_value = {
            "orderId": 888,
            "status": "NEW",
            "clientOrderId": "msl_ada_1",
        }

        result = self.coordinator.step(
            action="morning_protection",
            prices={"ADAUSDT": 0.55},
        )

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["updated_sl"], 1)
        mock_manager.run_morning_protection_stop.assert_not_called()

        self.client.create_order.assert_called()
        submitted_kwargs = self.client.create_order.call_args.kwargs
        self.assertEqual(submitted_kwargs["symbol"], "ADAUSDT")
        self.assertEqual(submitted_kwargs["type"], "STOP_MARKET")

        self.client.cancel_order.assert_called_with(
            symbol="ADAUSDT",
            order_id=555,
            orig_client_order_id="old_ada_sl",
        )

        pos = self.store.get_position(pos_id)
        self.assertAlmostEqual(float(pos["sl_price"]), 0.55)
        self.assertEqual(pos["sl_order_id"], 888)
        state = self.store.get_protection_policy_state("MORNING_PROTECTION")
        self.assertIsNotNone(state)

    def test_step_protection_order_rejected_does_not_falsely_update_local_sl(self):
        """If exchange rejects new protection order, local database is NOT updated to false tighter stop."""
        from infra.binance_futures_client import BinanceAPIError

        run_id, _ = self.store.create_run("2026-10-05_fail", account_id=self.store.account_id)
        pos_id = self.store.insert_position(
            run_id=run_id,
            symbol="DOGEUSDT",
            side="SHORT",
            qty=1000.0,
            entry_price=0.20,
            liq_price_open=None,
            tp_price=None,
            sl_price=0.25,  # Original SL
            tp_order_id=None,
            sl_order_id=111,
            tp_client_order_id=None,
            sl_client_order_id="orig_sl",
            opened_at_utc="2026-10-05T00:00:00Z",
            expire_at_utc="2026-10-06T00:00:00Z",
            status="OPEN",
        )
        self.client.create_order.side_effect = BinanceAPIError(code=-4000, message="Price protect error")

        result = self.coordinator.step(
            action="noon_protection",
            prices={"DOGEUSDT": 0.22},
        )

        self.assertEqual(result["updated_sl"], 0)
        # Verify local DB stop loss was NOT updated
        pos = self.store.get_position(pos_id)
        self.assertAlmostEqual(float(pos["sl_price"]), 0.25)
        self.assertEqual(pos["sl_order_id"], 111)
        # Verify old order was NOT canceled
        self.client.cancel_order.assert_not_called()

    def test_immediate_protection_close_rejected_keeps_existing_stop(self):
        from infra.binance_futures_client import BinanceAPIError

        run_id, _ = self.store.create_run("2026-10-05-rejected-close", account_id=self.store.account_id)
        pos_id = self.store.insert_position(
            run_id=run_id, symbol="DOGEUSDT", side="SHORT", qty=1000.0,
            entry_price=0.20, liq_price_open=None, tp_price=None, sl_price=0.25,
            tp_order_id=None, sl_order_id=111, tp_client_order_id=None,
            sl_client_order_id="original-sl", opened_at_utc="2026-10-05T00:00:00Z",
            expire_at_utc="2026-10-06T00:00:00Z", status="OPEN",
        )
        self.client.get_position_risk.return_value = [
            {"symbol": "DOGEUSDT", "positionSide": "BOTH", "positionAmt": "-1000"}
        ]
        self.client.create_order.side_effect = [
            BinanceAPIError(-2021, "Order would immediately trigger."),
            BinanceAPIError(-2022, "ReduceOnly Order is rejected."),
        ]
        view = self.coordinator.build_account_view()
        intent = OrderIntent(
            intent_id="protect-rejected", account_id=self.store.account_id,
            client_intent_key="protect-rejected", symbol="DOGEUSDT", side="BUY",
            order_type="STOP_MARKET", target_qty=1000, target_price=0.22,
            intent_scope="PROTECTION", position_id=pos_id, reason="NOON_PROTECTION_UPDATE",
        )
        updated = self.coordinator._execute_protection_update(
            intent, "NOON_PROTECTION", "NOON_CAPS", "nsl", view,
        )
        self.assertFalse(updated)
        self.client.cancel_order.assert_not_called()
        self.assertEqual(self.store.get_position(pos_id)["status"], "OPEN")
        self.assertEqual(self.store.get_position(pos_id)["sl_order_id"], 111)

    def test_step_manage_hold_expiry_closes_position(self):
        """Positions past expire_at_utc generate HOLD_EXPIRY exit and mark position closed."""
        run_id, _ = self.store.create_run("2026-10-05_exp", account_id=self.store.account_id)
        self.store.save_position_episode(
            episode_id="ep_exp",
            symbol="DOTUSDT",
            status=EpisodeStatus.OPEN.value,
            current_qty=50.0,
        )
        pos_id = self.store.insert_position(
            run_id=run_id,
            symbol="DOTUSDT",
            side="SHORT",
            qty=50.0,
            entry_price=10.0,
            liq_price_open=None,
            tp_price=None,
            sl_price=12.0,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc="2026-10-05T00:00:00Z",
            expire_at_utc="2026-10-05T10:00:00Z",  # In the past relative to now 12:00:00Z
            status="OPEN",
            episode_id="ep_exp",
        )

        self.client.create_order.return_value = {
            "orderId": 404,
            "status": "FILLED",
            "executedQty": "50.0",
            "avgPrice": "9.5",
        }

        result = self.coordinator.step(
            action="manage",
            prices={"DOTUSDT": 9.5},
        )

        self.assertEqual(result["status"], "COMPLETED")
        self.client.create_order.assert_called()
        submitted_kwargs = self.client.create_order.call_args.kwargs
        self.assertEqual(submitted_kwargs["symbol"], "DOTUSDT")
        self.assertEqual(submitted_kwargs["type"], "MARKET")
        self.assertEqual(submitted_kwargs["side"], "BUY")

        # Verify position is closed in store
        pos = self.store.get_position(pos_id)
        self.assertEqual(pos["status"], "CLOSED_HOLD_EXPIRY")
        self.assertEqual(pos["close_reason"], "HOLD_EXPIRY")

        # Verify episode is closed
        ep = self.store.get_position_episode("ep_exp")
        self.assertEqual(ep["status"], EpisodeStatus.CLOSED.value)

    def test_step_hourly_take_profit_natively_closes_position(self):
        """Native hourly take profit market-closes eligible positions."""
        run_id, _ = self.store.create_run("2026-10-05_htp", account_id=self.store.account_id)
        self.store.save_position_episode(
            episode_id="ep_htp",
            symbol="AVAXUSDT",
            status=EpisodeStatus.OPEN.value,
            current_qty=10.0,
        )
        self.store.insert_position(
            run_id=run_id,
            symbol="AVAXUSDT",
            side="SHORT",
            qty=10.0,
            entry_price=30.0,
            liq_price_open=None,
            tp_price=None,
            sl_price=None,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc="2026-10-05T00:00:00Z",
            expire_at_utc="2026-10-06T00:00:00Z",
            status="OPEN",
            episode_id="ep_htp",
        )
        mock_manager = MagicMock()
        self.coordinator.manager = mock_manager

        self.client.create_order.return_value = {
            "orderId": 303,
            "status": "FILLED",
            "executedQty": "10.0",
            "avgPrice": "25.0",
        }

        # Price dropped from 30.0 to 25.0 (> 16% drop, drop_pct threshold = 5%)
        result = self.coordinator.step(
            action="hourly_take_profit",
            prices={"AVAXUSDT": 25.0},
            config={"hourly_exchange_take_profit_drop_pct": 5.0},
        )

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["closed_tp"], 1)
        mock_manager.run_hourly_exchange_take_profit.assert_not_called()

        ep = self.store.get_position_episode("ep_htp")
        self.assertEqual(ep["status"], EpisodeStatus.CLOSED.value)

    def test_step_orphan_cleanup_natively_cancels_orders(self):
        """Native orphan cleanup cancels orders on inactive symbols."""
        mock_manager = MagicMock()
        self.coordinator.manager = mock_manager

        # Open order on LINKUSDT, but no open positions
        self.client.get_open_orders.return_value = [
            {"symbol": "LINKUSDT", "orderId": 404, "clientOrderId": "cid_orphan"}
        ]
        self.client.cancel_order.return_value = {"orderId": 404, "status": "CANCELED"}

        result = self.coordinator.step(action="orphan_cleanup")

        self.assertEqual(result["status"], "COMPLETED")
        self.assertEqual(result["canceled"], 1)
        mock_manager.cleanup_orphan_exit_orders_once_per_day.assert_not_called()

        self.client.cancel_order.assert_called_once()
        state = self.store.get_protection_policy_state("ORPHAN_CLEANUP")
        self.assertIsNotNone(state)

    def test_build_account_view_reconciles_flat_exchange_position(self):
        """When exchange position risk shows amt == 0 for an open position, it is marked CLOSED_EXTERNAL."""
        run_id, _ = self.store.create_run("2026-10-05_flat", account_id=self.store.account_id)
        pos_id = self.store.insert_position(
            run_id=run_id,
            symbol="RESOLVUSDT",
            side="SHORT",
            qty=1222.0,
            entry_price=0.0218,
            liq_price_open=None,
            tp_price=None,
            sl_price=0.0215,
            tp_order_id=None,
            sl_order_id=99901,
            tp_client_order_id=None,
            sl_client_order_id="sl_cid_1",
            opened_at_utc="2026-10-01T00:00:00Z",
            expire_at_utc="2026-10-02T00:00:00Z",
            status="OPEN",
        )

        # Exchange says positionAmt is 0.0
        self.client.get_position_risk.return_value = [
            {"symbol": "RESOLVUSDT", "positionAmt": "0.0"}
        ]
        self.client.cancel_order.return_value = {"orderId": 99901, "status": "CANCELED"}

        acct_view = self.coordinator.build_account_view()

        # Position should not be in active account view
        self.assertNotIn("RESOLVUSDT", acct_view.positions)

        # Position in DB should be marked CLOSED_EXTERNAL
        pos = self.store.get_position(pos_id)
        self.assertEqual(pos["status"], "CLOSED_EXTERNAL")
        self.assertEqual(pos["close_reason"], "EXCHANGE_POSITION_FLAT")

        # Orphan order was canceled
        self.client.cancel_order.assert_called_once()


if __name__ == "__main__":
    unittest.main()
