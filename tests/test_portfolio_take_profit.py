import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from core.position_manager import PositionManager
from core.runtime_service import ServiceRuntimeConfig, StrategyRuntimeService
from core.state_store import StateStore
from infra.binance_futures_client import BinanceAPIError


class PortfolioTakeProfitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=db_path, schema_path=schema_path, account_id="acc01")
        self.store.init_schema()
        self.run_id, _ = self.store.create_run("2026-07-28", account_id="acc01")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _insert_short(self, symbol: str, qty: float = 1.0) -> int:
        opened_at = datetime(2026, 7, 28, 0, 5, tzinfo=timezone.utc)
        return self.store.insert_position(
            run_id=self.run_id,
            symbol=symbol,
            side="SHORT",
            qty=qty,
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

    def _manager(self, client: MagicMock, notifier: MagicMock | None = None) -> PositionManager:
        if client.format_trigger_price.side_effect is None:
            client.format_trigger_price.side_effect = (
                lambda _symbol, price, round_up=False: str(price)
            )
        return PositionManager(
            client=client,
            store=self.store,
            notifier=notifier or MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            account_id="acc01",
        )

    def test_uses_first_08_snapshot_and_closes_all_position_directions_at_9_pct(self) -> None:
        short_id = self._insert_short("BTCUSDT")
        # 08:00 Asia/Shanghai is 00:00 UTC. A later snapshot must not move the
        # day's fixed take-profit baseline.
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        self.store.add_wallet_snapshot("2026-07-28T00:01:00+00:00", 105.0)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {"symbol": "BTCUSDT", "positionAmt": "-1", "positionSide": "BOTH", "markPrice": "99"},
            {"symbol": "ETHUSDT", "positionAmt": "2", "positionSide": "BOTH", "markPrice": "101"},
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = [
            {
                "orderId": 301,
                "clientOrderId": "pft-btc",
                "status": "FILLED",
                "side": "BUY",
                "executedQty": "1",
            },
            {
                "orderId": 302,
                "clientOrderId": "pft-eth",
                "status": "FILLED",
                "side": "SELL",
                "executedQty": "2",
            },
        ]
        notifier = MagicMock()

        result = self._manager(client, notifier).run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )

        self.assertEqual(result["status"], "TRIGGERED")
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["closed_take_profit"], 2)
        self.assertEqual(result["errors"], 0)
        self.assertAlmostEqual(result["actual_profit_pct"], 9.0)

        state = self.store.get_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME)
        assert state is not None
        self.assertAlmostEqual(float(state["baseline_equity_usdt"]), 100.0)
        self.assertAlmostEqual(float(state["threshold_equity_usdt"]), 109.0)
        self.assertTrue(state["triggered"])
        self.assertTrue(state["close_complete"])

        first_order = client.create_order.call_args_list[0].kwargs
        self.assertEqual(first_order["side"], "BUY")
        self.assertTrue(first_order["reduceOnly"])
        self.assertIn("-pftlim-", str(first_order["newClientOrderId"]))
        second_order = client.create_order.call_args_list[1].kwargs
        self.assertEqual(second_order["side"], "SELL")
        self.assertTrue(second_order["reduceOnly"])

        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute("SELECT status, close_reason FROM positions WHERE id = ?", (short_id,)).fetchone()
        self.assertEqual(row["status"], "CLOSED_PORTFOLIO_TAKE_PROFIT")
        self.assertEqual(row["close_reason"], "PORTFOLIO_EQUITY_TAKE_PROFIT")
        notifier.send.assert_called_once()
        self.assertIn("组合止盈", notifier.send.call_args.args[0])

    def test_low_price_portfolio_limit_uses_fixed_point_price_on_new_and_legacy_retry(self) -> None:
        position_id = self._insert_short("NEIROUSDT", qty=498977.0)
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        risk = {
            "symbol": "NEIROUSDT",
            "positionAmt": "-498977",
            "positionSide": "BOTH",
            "markPrice": "0.00008598",
        }
        client = MagicMock()
        client.get_position_risk.return_value = [risk]
        client.format_trigger_price.side_effect = (
            lambda _symbol, _price, round_up=False: "0.00008598"
        )
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = RuntimeError("temporary submit failure")
        manager = self._manager(client)
        now_local = datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        first = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=now_local,
            profit_pct=9.0,
        )
        self.assertEqual(first["errors"], 1)
        state = self.store.get_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME)
        assert state is not None
        self.assertEqual(
            state["portfolio_limit_plan"][0]["limit_price"],
            "0.00008598",
        )

        # Simulate a plan persisted by the buggy version before the retry.
        state["portfolio_limit_plan"][0]["limit_price"] = 8.598e-05
        self.store.set_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)
        client.create_order.side_effect = None
        client.create_order.return_value = {
            "orderId": 701,
            "clientOrderId": "pftlim-neiro",
            "type": "LIMIT",
            "status": "FILLED",
            "executedQty": "498977",
            "side": "BUY",
        }

        retry = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=now_local,
            profit_pct=9.0,
        )

        self.assertTrue(retry["close_complete"])
        self.assertEqual(client.create_order.call_args.kwargs["price"], "0.00008598")
        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute(
                "SELECT status, close_reason FROM positions WHERE id = ?",
                (position_id,),
            ).fetchone()
        self.assertEqual(row["status"], "CLOSED_PORTFOLIO_TAKE_PROFIT")
        self.assertEqual(row["close_reason"], "PORTFOLIO_EQUITY_TAKE_PROFIT")

    def test_full_take_profit_limit_waits_without_timeout_and_cleans_up_after_fill(self) -> None:
        position_id = self._insert_short("BTCUSDT")
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        open_risk = {
            "symbol": "BTCUSDT",
            "positionAmt": "-1",
            "positionSide": "BOTH",
            "markPrice": "99",
        }
        client = MagicMock()
        client.get_position_risk.side_effect = [[open_risk], [open_risk], [open_risk], [{
            "symbol": "BTCUSDT",
            "positionAmt": "0",
            "positionSide": "BOTH",
            "markPrice": "99",
        }]]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 501,
            "clientOrderId": "pftlim-btc",
            "type": "LIMIT",
            "status": "NEW",
            "side": "BUY",
        }
        client.get_order.side_effect = [
            {"orderId": 501, "clientOrderId": "pftlim-btc", "type": "LIMIT", "status": "NEW"},
            {
                "orderId": 501,
                "clientOrderId": "pftlim-btc",
                "type": "LIMIT",
                "status": "FILLED",
                "side": "BUY",
                "executedQty": "1",
            },
        ]
        client.get_user_trades.return_value = []
        client.cancel_order.return_value = {}
        manager = self._manager(client)
        now_local = datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        triggered = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=now_local,
            profit_pct=9.0,
        )
        self.assertEqual(triggered["status"], "TRIGGERED")
        self.assertFalse(triggered["close_complete"])
        self.assertEqual(triggered["pending"], 1)
        self.assertEqual(client.create_order.call_count, 1)
        self.assertEqual(client.cancel_order.call_count, 0)

        waiting = manager.run_portfolio_take_profit(
            current_equity_usdt=108.0,
            now_local=now_local,
            profit_pct=9.0,
        )
        self.assertEqual(waiting["status"], "TRIGGERED_RETRY")
        self.assertFalse(waiting["close_complete"])
        self.assertEqual(client.create_order.call_count, 1)
        self.assertEqual(client.cancel_order.call_count, 0)

        filled = manager.run_portfolio_take_profit(
            current_equity_usdt=108.0,
            now_local=now_local,
            profit_pct=9.0,
        )
        self.assertEqual(filled["status"], "TRIGGERED_RETRY")
        self.assertTrue(filled["close_complete"])
        self.assertEqual(client.cancel_order.call_count, 2)

        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute(
                "SELECT status, close_reason, close_order_id FROM positions WHERE id = ?",
                (position_id,),
            ).fetchone()
        self.assertEqual(row["status"], "CLOSED_PORTFOLIO_TAKE_PROFIT")
        self.assertEqual(row["close_reason"], "PORTFOLIO_EQUITY_TAKE_PROFIT")
        self.assertEqual(row["close_order_id"], 501)

    def test_reuses_existing_local_limit_when_persisted_plan_lost_order_id(self) -> None:
        self._insert_short("BTCUSDT")
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        risk = {
            "symbol": "BTCUSDT",
            "positionAmt": "-1",
            "positionSide": "BOTH",
            "markPrice": "99",
        }
        client = MagicMock()
        client.get_position_risk.return_value = [risk]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 701,
            "clientOrderId": "t10s-pftlim-BTCUS-abc123-xyz98765",
            "type": "LIMIT",
            "side": "BUY",
            "positionSide": "BOTH",
            "price": "99",
            "origQty": "1",
            "executedQty": "0",
            "reduceOnly": True,
            "status": "NEW",
        }
        user_stream = MagicMock()
        manager = self._manager(client)
        manager.order_state = user_stream

        first = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )
        self.assertFalse(first["close_complete"])
        self.assertEqual(client.create_order.call_count, 1)

        state = self.store.get_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME)
        assert state is not None
        item = state["portfolio_limit_plan"][0]
        item["portfolio_order_id"] = None
        item["portfolio_client_order_id"] = None
        item["portfolio_order_status"] = "REJECTED"
        item["retry_count"] = 1
        self.store.set_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME, state)

        second = manager.run_portfolio_take_profit(
            current_equity_usdt=108.0,
            now_local=datetime(2026, 7, 28, 9, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )

        self.assertFalse(second["close_complete"])
        self.assertEqual(second["errors"], 0)
        self.assertEqual(second["pending"], 1)
        self.assertEqual(client.create_order.call_count, 1)
        state = self.store.get_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME)
        assert state is not None
        self.assertEqual(state["portfolio_limit_plan"][0]["portfolio_order_id"], 701)

    def test_reduce_only_rejection_adopts_existing_remote_limit(self) -> None:
        self._insert_short("BTCUSDT")
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-1",
                "positionSide": "BOTH",
                "markPrice": "99",
            }
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = BinanceAPIError(-2022, "ReduceOnly Order is rejected")
        client.get_open_orders.return_value = [
            {
                "orderId": 702,
                "clientOrderId": "t10s-pftlim-BTCUS-abc123-remote1",
                "symbol": "BTCUSDT",
                "type": "LIMIT",
                "side": "BUY",
                "positionSide": "BOTH",
                "price": "99",
                "origQty": "1",
                "executedQty": "0",
                "reduceOnly": True,
                "status": "NEW",
            }
        ]
        manager = self._manager(client)

        first = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )

        self.assertFalse(first["close_complete"])
        self.assertEqual(first["errors"], 0)
        self.assertEqual(first["pending"], 1)
        self.assertEqual(client.create_order.call_count, 1)
        client.get_open_orders.assert_called_once_with(symbol="BTCUSDT")

        state = self.store.get_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME)
        assert state is not None
        self.assertEqual(state["portfolio_limit_plan"][0]["portfolio_order_id"], 702)

        client.get_order.return_value = client.get_open_orders.return_value[0]
        second = manager.run_portfolio_take_profit(
            current_equity_usdt=108.0,
            now_local=datetime(2026, 7, 28, 9, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )
        self.assertEqual(second["errors"], 0)
        self.assertEqual(client.create_order.call_count, 1)

    def test_pending_limit_plan_is_carried_across_daily_reset(self) -> None:
        self._insert_short("BTCUSDT")
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        self.store.add_wallet_snapshot("2026-07-29T00:00:00+00:00", 100.0)
        risk = {
            "symbol": "BTCUSDT",
            "positionAmt": "-1",
            "positionSide": "BOTH",
            "markPrice": "99",
        }
        client = MagicMock()
        client.get_position_risk.side_effect = [[risk], [risk], [risk]]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 601,
            "clientOrderId": "pftlim-btc",
            "type": "LIMIT",
            "status": "NEW",
            "side": "BUY",
        }
        client.get_order.return_value = {
            "orderId": 601,
            "clientOrderId": "pftlim-btc",
            "type": "LIMIT",
            "status": "NEW",
        }
        manager = self._manager(client)
        first = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )
        self.assertFalse(first["close_complete"])

        next_cycle = manager.run_portfolio_take_profit(
            current_equity_usdt=100.0,
            now_local=datetime(2026, 7, 29, 8, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )
        self.assertEqual(next_cycle["status"], "TRIGGERED_RETRY")
        self.assertFalse(next_cycle["close_complete"])
        self.assertEqual(client.create_order.call_count, 1)
        state = self.store.get_lock_state(PositionManager.PORTFOLIO_TAKE_PROFIT_LOCK_NAME)
        assert state is not None
        self.assertEqual(state["cycle_date"], "2026-07-29")
        self.assertEqual(state["portfolio_limit_plan"][0]["portfolio_order_id"], 601)

    def test_monitoring_continues_before_08_using_previous_daily_cycle(self) -> None:
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        client = MagicMock()
        client.get_position_risk.return_value = []
        manager = self._manager(client)
        before_reset = datetime(2026, 7, 29, 7, 59, tzinfo=ZoneInfo("Asia/Shanghai"))

        monitoring = manager.run_portfolio_take_profit(
            current_equity_usdt=108.99,
            now_local=before_reset,
            profit_pct=9.0,
        )
        self.assertEqual(monitoring["status"], "MONITORING")
        self.assertEqual(monitoring["cycle_date"], "2026-07-28")

        triggered = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=before_reset,
            profit_pct=9.0,
        )
        self.assertEqual(triggered["status"], "TRIGGERED")
        self.assertEqual(triggered["cycle_date"], "2026-07-28")

    def test_trailing_take_profit_arms_at_2_5_and_triggers_after_15_pct_peak_giveback(self) -> None:
        self._insert_short("BTCUSDT")
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        client = MagicMock()
        client.get_position_risk.return_value = [
            {"symbol": "BTCUSDT", "positionAmt": "-1", "positionSide": "BOTH"},
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 351,
            "clientOrderId": "pft-btc",
            "status": "FILLED",
            "side": "BUY",
        }
        manager = self._manager(client)
        now_local = datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        monitoring = manager.run_portfolio_take_profit(
            current_equity_usdt=102.49,
            now_local=now_local,
            profit_pct=2.5,
            giveback_pct=15.0,
        )
        self.assertEqual(monitoring["status"], "MONITORING")
        self.assertFalse(monitoring["armed"])

        armed = manager.run_portfolio_take_profit(
            current_equity_usdt=102.5,
            now_local=now_local,
            profit_pct=2.5,
            giveback_pct=15.0,
        )
        self.assertEqual(armed["status"], "ARMED")
        self.assertTrue(armed["armed"])
        self.assertAlmostEqual(armed["threshold_equity"], 102.125)

        trailing = manager.run_portfolio_take_profit(
            current_equity_usdt=105.0,
            now_local=now_local,
            profit_pct=2.5,
            giveback_pct=15.0,
        )
        self.assertEqual(trailing["status"], "TRAILING")
        self.assertAlmostEqual(trailing["peak_profit_pct"], 5.0)
        self.assertAlmostEqual(trailing["threshold_equity"], 104.25)

        above_trigger = manager.run_portfolio_take_profit(
            current_equity_usdt=104.26,
            now_local=now_local,
            profit_pct=2.5,
            giveback_pct=15.0,
        )
        self.assertEqual(above_trigger["status"], "TRAILING")

        triggered = manager.run_portfolio_take_profit(
            current_equity_usdt=104.25,
            now_local=now_local,
            profit_pct=2.5,
            giveback_pct=15.0,
        )
        self.assertEqual(triggered["status"], "TRIGGERED")
        self.assertAlmostEqual(triggered["actual_profit_pct"], 4.25)
        self.assertEqual(client.get_position_risk.call_count, 2)

    def test_trailing_take_profit_recovers_cycle_peak_from_wallet_snapshots(self) -> None:
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        self.store.add_wallet_snapshot("2026-07-28T00:20:00+00:00", 109.0)
        self.store.add_wallet_snapshot("2026-07-28T01:00:00+00:00", 107.65)
        client = MagicMock()
        client.get_position_risk.return_value = []

        result = self._manager(client).run_portfolio_take_profit(
            current_equity_usdt=107.65,
            now_local=datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=2.5,
            giveback_pct=15.0,
        )

        self.assertEqual(result["status"], "TRIGGERED")
        self.assertAlmostEqual(result["peak_equity"], 109.0)
        self.assertAlmostEqual(result["peak_profit_pct"], 9.0)
        self.assertAlmostEqual(result["threshold_equity"], 107.65)

    def test_50_pct_take_profit_uses_limit_and_keeps_original_protection(self) -> None:
        position_id = self._insert_short("BTCUSDT", qty=2.0)
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)

        initial_risk = {
            "symbol": "BTCUSDT",
            "positionAmt": "-2",
            "positionSide": "BOTH",
            "entryPrice": "100",
            "markPrice": "99",
        }
        client = MagicMock()
        client.get_position_risk.side_effect = [
            [initial_risk],  # persisted reduction plan
            [initial_risk],  # first execution
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 401,
            "clientOrderId": "pft-btc",
            "type": "LIMIT",
            "status": "FILLED",
            "executedQty": "1",
            "side": "BUY",
        }
        manager = self._manager(client)
        now_local = datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

        first = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=now_local,
            profit_pct=9.0,
            reduce_ratio=0.50,
        )
        self.assertEqual(first["status"], "TRIGGERED")
        self.assertTrue(first["close_complete"])
        self.assertEqual(first["adjusted_take_profit"], 1)

        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["type"], "LIMIT")
        self.assertEqual(order_kwargs["timeInForce"], "GTC")
        self.assertEqual(float(order_kwargs["price"]), 99.0)
        self.assertTrue(order_kwargs["reduceOnly"])
        self.assertEqual(float(order_kwargs["quantity"]), 1.0)
        client.cancel_order.assert_not_called()

        already_complete = manager.run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=now_local,
            profit_pct=9.0,
            reduce_ratio=0.50,
        )
        self.assertEqual(already_complete["status"], "ALREADY_TRIGGERED")

        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute(
                "SELECT qty, status, sl_order_id, sl_client_order_id FROM positions WHERE id = ?",
                (position_id,),
            ).fetchone()
        self.assertAlmostEqual(float(row["qty"]), 1.0)
        self.assertEqual(row["status"], "OPEN")
        self.assertEqual(row["sl_order_id"], 102)
        self.assertEqual(row["sl_client_order_id"], "sl-old")

    def test_trigger_latch_survives_restart_and_resets_at_next_08(self) -> None:
        self.store.add_wallet_snapshot("2026-07-28T00:00:00+00:00", 100.0)
        client = MagicMock()
        client.get_position_risk.return_value = []

        first = self._manager(client).run_portfolio_take_profit(
            current_equity_usdt=109.0,
            now_local=datetime(2026, 7, 28, 12, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )
        self.assertEqual(first["status"], "TRIGGERED")

        restarted = self._manager(client).run_portfolio_take_profit(
            current_equity_usdt=110.0,
            now_local=datetime(2026, 7, 28, 23, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )
        self.assertEqual(restarted["status"], "ALREADY_TRIGGERED")
        self.assertEqual(client.get_position_risk.call_count, 2)

        self.store.add_wallet_snapshot("2026-07-29T00:00:00+00:00", 110.0)
        next_cycle = self._manager(client).run_portfolio_take_profit(
            current_equity_usdt=110.0,
            now_local=datetime(2026, 7, 29, 8, 1, tzinfo=ZoneInfo("Asia/Shanghai")),
            profit_pct=9.0,
        )
        self.assertEqual(next_cycle["status"], "MONITORING")
        self.assertEqual(next_cycle["cycle_date"], "2026-07-29")
        self.assertAlmostEqual(next_cycle["threshold_equity"], 119.9)

    def test_runtime_applies_account_override_and_returns_take_profit_result(self) -> None:
        class StrategyStub:
            def __init__(self) -> None:
                self.legacy_calls = 0

            def run_equity_recovery_take_profit(self):
                self.legacy_calls += 1
                return {"status": "DISABLED"}

        class ManagerStub:
            def __init__(self) -> None:
                self.take_profit_calls: list[dict[str, object]] = []

            def run_once(self):
                return {"total": 0}

            def run_portfolio_take_profit(self, **kwargs):
                self.take_profit_calls.append(kwargs)
                return {"status": "TRIGGERED", "triggered": True, "close_complete": True}

        class WalletSamplerStub:
            def run_once(self):
                return {"equity": 109.0, "snapshot_id": 1}

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
            portfolio_take_profit_enabled=False,
        )
        acc01_manager = ManagerStub()
        acc04_manager = ManagerStub()
        strategy = StrategyStub()
        sampler = WalletSamplerStub()
        service = StrategyRuntimeService(
            strategy=strategy,
            manager=acc01_manager,
            balance_sampler=sampler,
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                "acc01": {
                    "mode": "full",
                    "strategy": strategy,
                    "manager": acc01_manager,
                    "balance_sampler": sampler,
                    "portfolio_take_profit_enabled": True,
                    "portfolio_take_profit_pct": 9.0,
                    "portfolio_take_profit_hour": 8,
                    "portfolio_take_profit_minute": 0,
                    "portfolio_take_profit_reduce_ratio": 0.50,
                    "portfolio_take_profit_giveback_pct": 15.0,
                },
                "acc04": {
                    "mode": "full",
                    "strategy": strategy,
                    "manager": acc04_manager,
                    "balance_sampler": sampler,
                    "portfolio_take_profit_enabled": False,
                },
            },
        )
        try:
            now_local = datetime(2026, 7, 28, 9, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
            acc01_result = service._run_manage_for_account("acc01", now_local=now_local)
            self.assertEqual(strategy.legacy_calls, 0)
            acc04_result = service._run_manage_for_account("acc04", now_local=now_local)

            self.assertEqual(acc01_result["portfolio_take_profit"]["status"], "TRIGGERED")
            self.assertEqual(len(acc01_manager.take_profit_calls), 1)
            self.assertEqual(acc01_manager.take_profit_calls[0]["profit_pct"], 9.0)
            self.assertEqual(acc01_manager.take_profit_calls[0]["reduce_ratio"], 0.50)
            self.assertEqual(acc01_manager.take_profit_calls[0]["giveback_pct"], 15.0)
            self.assertIsNone(acc04_result["portfolio_take_profit"])
            self.assertEqual(acc04_manager.take_profit_calls, [])
            self.assertEqual(strategy.legacy_calls, 1)
        finally:
            service._entry_executor.shutdown(wait=True, cancel_futures=True)
            service._manage_executor.shutdown(wait=True, cancel_futures=True)
            service._scheduled_executor.shutdown(wait=True, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
