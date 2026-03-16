import importlib.util
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import MagicMock, patch

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from core.position_manager import PositionManager
from core.state_store import StateStore


class PositionManagerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=self.db_path, schema_path=schema_path)
        self.store.init_schema()
        self.run_id, _ = self.store.create_run("2026-02-13")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_new_client_id_handles_non_ascii_symbol(self) -> None:
        client_id = PositionManager._new_client_id("dl", "币安人生USDT")
        self.assertLessEqual(len(client_id), 36)
        self.assertRegex(client_id, r"^[.A-Z:/a-z0-9_-]{1,36}$")

    def test_dynamic_stop_loss_update(self) -> None:
        position_id = self._insert_open_position(
            symbol="BTCUSDT",
            qty=0.01,
            tp_order_id=11,
            sl_order_id=22,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=24,
        )

        client = MagicMock()
        client.get_order.side_effect = [{"status": "NEW"}, {"status": "NEW"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.01",
                "liquidationPrice": "61000",
            }
        ]
        client.get_symbol_rules.return_value = {
            "BTCUSDT": SimpleNamespace(tick_size=0.1)
        }
        client.normalize_trigger_price.return_value = 60390.0
        client.create_order.return_value = {
            "orderId": 333,
            "clientOrderId": "sl-new",
            "type": "STOP_MARKET",
            "side": "BUY",
            "price": "0",
            "origQty": "0.01",
            "status": "NEW",
        }

        notifier = MagicMock()
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=notifier,
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["closed_timeout"], 0)

        client.cancel_order.assert_called_once_with(
            symbol="BTCUSDT",
            order_id=22,
            orig_client_order_id="sl-old",
        )
        client.create_order.assert_called_once()
        create_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(create_kwargs["type"], "STOP_MARKET")
        self.assertEqual(create_kwargs["workingType"], "CONTRACT_PRICE")
        self.assertEqual(create_kwargs["symbol"], "BTCUSDT")
        self.assertEqual(create_kwargs["quantity"], "0.01")
        self.assertTrue(create_kwargs["reduceOnly"])
        self.assertNotIn("closePosition", create_kwargs)

        row = self._get_position(position_id)
        self.assertEqual(row["status"], "OPEN")
        self.assertEqual(row["sl_order_id"], 333)
        self.assertAlmostEqual(float(row["sl_price"]), 60390.0)
        self.assertAlmostEqual(float(row["liq_price_latest"]), 61000.0)

        notifier.send.assert_called_once()
        title, content = notifier.send.call_args.args
        self.assertEqual(title, "【Top10做空】巡检动作汇总")
        self.assertIn("| updated_sl | 1 |", content)
        self.assertIn("止损更新明细", content)
        self.assertIn("BTCUSDT", content)

    def test_timeout_close_position(self) -> None:
        position_id = self._insert_open_position(
            symbol="BTCUSDT",
            qty=0.02,
            tp_order_id=101,
            sl_order_id=202,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=-1,
        )

        client = MagicMock()
        client.get_order.side_effect = [{"status": "NEW"}, {"status": "NEW"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.02",
                "liquidationPrice": "60000",
            }
        ]
        client.create_order.return_value = {
            "orderId": 999,
            "clientOrderId": "to-close",
            "type": "MARKET",
            "side": "BUY",
            "origQty": "0.02",
            "status": "FILLED",
        }

        notifier = MagicMock()
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=notifier,
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["closed_timeout"], 1)
        self.assertEqual(summary["updated_sl"], 0)
        self.assertEqual(client.cancel_order.call_count, 2)

        client.create_order.assert_called_once()
        create_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(create_kwargs["symbol"], "BTCUSDT")
        self.assertEqual(create_kwargs["side"], "BUY")
        self.assertEqual(create_kwargs["type"], "MARKET")
        self.assertTrue(create_kwargs["reduceOnly"])

        row = self._get_position(position_id)
        self.assertEqual(row["status"], "CLOSED_TIMEOUT")
        self.assertEqual(row["close_reason"], "MAX_HOLD_EXCEEDED")
        self.assertEqual(row["close_order_id"], 999)
        self.assertIsNotNone(row["closed_at_utc"])

        notifier.send.assert_called_once()
        title, content = notifier.send.call_args.args
        self.assertEqual(title, "【Top10做空】巡检动作汇总")
        self.assertIn("| closed_timeout | 1 |", content)
        self.assertIn("超时平仓明细", content)
        self.assertIn("BTCUSDT", content)

    def test_stale_error_is_cleared_after_successful_manage(self) -> None:
        position_id = self._insert_open_position(
            symbol="BTCUSDT",
            qty=0.01,
            tp_order_id=11,
            sl_order_id=22,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=24,
        )
        self.store.set_position_error(position_id, "old network error")

        client = MagicMock()
        client.get_order.side_effect = [{"status": "NEW"}, {"status": "NEW"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.01",
                "liquidationPrice": "61000",
            }
        ]
        client.get_symbol_rules.return_value = {
            "BTCUSDT": SimpleNamespace(tick_size=0.1)
        }
        client.normalize_trigger_price.return_value = 60390.0
        client.create_order.return_value = {
            "orderId": 333,
            "clientOrderId": "sl-new",
            "type": "STOP_MARKET",
            "side": "BUY",
            "price": "0",
            "origQty": "0.01",
            "status": "NEW",
        }

        notifier = MagicMock()
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=notifier,
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()
        self.assertEqual(summary["errors"], 0)
        self.assertEqual(summary["updated_sl"], 1)

        row = self._get_position(position_id)
        self.assertIsNone(row["last_error"])

    def test_daily_loss_cut_closes_only_losing_positions(self) -> None:
        losing_id = self._insert_open_position(
            symbol="BTCUSDT",
            qty=0.02,
            tp_order_id=101,
            sl_order_id=202,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=24,
        )
        winning_id = self._insert_open_position(
            symbol="ETHUSDT",
            qty=0.03,
            tp_order_id=301,
            sl_order_id=302,
            tp_price=3000.0,
            sl_price=4200.0,
            expire_in_hours=24,
        )

        client = MagicMock()
        client.get_position_risk.side_effect = [
            [{"symbol": "BTCUSDT", "positionAmt": "-0.02", "unRealizedProfit": "-1.2"}],
            [{"symbol": "ETHUSDT", "positionAmt": "-0.03", "unRealizedProfit": "2.8"}],
        ]
        client.create_order.return_value = {
            "orderId": 888,
            "clientOrderId": "dl-close",
            "type": "MARKET",
            "side": "BUY",
            "origQty": "0.02",
            "status": "FILLED",
        }

        notifier = MagicMock()
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=notifier,
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_daily_loss_cut()
        self.assertEqual(summary["closed_loss_cut"], 1)
        self.assertEqual(summary["errors"], 0)

        losing_row = self._get_position(losing_id)
        self.assertEqual(losing_row["status"], "CLOSED_DAILY_LOSS_CUT")
        self.assertEqual(losing_row["close_reason"], "DAILY_FLOATING_LOSS_CHECK")

        winning_row = self._get_position(winning_id)
        self.assertEqual(winning_row["status"], "OPEN")

        notifier.send.assert_called_once()
        title, content = notifier.send.call_args.args
        self.assertEqual(title, "【Top10做空】11:55浮亏止损汇总")
        self.assertIn("| closed_loss_cut | 1 |", content)

    def test_daily_loss_cut_exchange_scope_closes_losing_long_and_short(self) -> None:
        client = MagicMock()
        client.get_position_risk.return_value = [
            {"symbol": "BTCUSDT", "positionAmt": "-0.02", "unRealizedProfit": "-1.2"},
            {"symbol": "ETHUSDT", "positionAmt": "0.30", "unRealizedProfit": "-2.4"},
            {"symbol": "BNBUSDT", "positionAmt": "0.10", "unRealizedProfit": "3.8"},
            {"symbol": "XRPUSDT", "positionAmt": "0", "unRealizedProfit": "-0.2"},
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = [
            {
                "orderId": 1001,
                "clientOrderId": "dl-short",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "0.02",
                "status": "FILLED",
            },
            {
                "orderId": 1002,
                "clientOrderId": "dl-long",
                "type": "MARKET",
                "side": "SELL",
                "origQty": "0.30",
                "status": "FILLED",
            },
        ]

        notifier = MagicMock()
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=notifier,
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_daily_loss_cut()
        self.assertEqual(summary["total"], 3)
        self.assertEqual(summary["closed_loss_cut"], 2)
        self.assertEqual(summary["errors"], 0)

        self.assertEqual(client.create_order.call_count, 2)
        first_order = client.create_order.call_args_list[0].kwargs
        self.assertEqual(first_order["symbol"], "BTCUSDT")
        self.assertEqual(first_order["side"], "BUY")
        self.assertEqual(first_order["type"], "MARKET")
        self.assertTrue(first_order["reduceOnly"])

        second_order = client.create_order.call_args_list[1].kwargs
        self.assertEqual(second_order["symbol"], "ETHUSDT")
        self.assertEqual(second_order["side"], "SELL")
        self.assertEqual(second_order["type"], "MARKET")
        self.assertTrue(second_order["reduceOnly"])

        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute("SELECT COUNT(*) AS c FROM order_events").fetchone()
            self.assertEqual(int(row["c"]), 2)

        notifier.send.assert_called_once()
        title, content = notifier.send.call_args.args
        self.assertEqual(title, "【Top10做空】11:55浮亏止损汇总")
        self.assertIn("| closed_loss_cut | 2 |", content)

    def test_daily_loss_cut_exchange_scope_hedge_mode_uses_position_side(self) -> None:
        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.02",
                "positionSide": "LONG",
                "unRealizedProfit": "-1.0",
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "-0.30",
                "positionSide": "SHORT",
                "unRealizedProfit": "-2.0",
            },
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = [
            {
                "orderId": 2001,
                "clientOrderId": "dl-long",
                "type": "MARKET",
                "side": "SELL",
                "origQty": "0.02",
                "status": "FILLED",
            },
            {
                "orderId": 2002,
                "clientOrderId": "dl-short",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "0.30",
                "status": "FILLED",
            },
        ]

        notifier = MagicMock()
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=notifier,
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_daily_loss_cut()
        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["closed_loss_cut"], 2)
        self.assertEqual(summary["errors"], 0)

        self.assertEqual(client.create_order.call_count, 2)
        first_order = client.create_order.call_args_list[0].kwargs
        self.assertEqual(first_order["symbol"], "BTCUSDT")
        self.assertEqual(first_order["side"], "SELL")
        self.assertEqual(first_order["positionSide"], "LONG")
        self.assertNotIn("reduceOnly", first_order)

        second_order = client.create_order.call_args_list[1].kwargs
        self.assertEqual(second_order["symbol"], "ETHUSDT")
        self.assertEqual(second_order["side"], "BUY")
        self.assertEqual(second_order["positionSide"], "SHORT")
        self.assertNotIn("reduceOnly", second_order)

        notifier.send.assert_called_once()
        title, content = notifier.send.call_args.args
        self.assertEqual(title, "【Top10做空】11:55浮亏止损汇总")
        self.assertIn("| closed_loss_cut | 2 |", content)

    def test_noon_protection_tightens_stop_using_max_of_day_start_and_opened_at(self) -> None:
        opened_at = datetime(2026, 2, 13, 8, 30, tzinfo=timezone.utc)
        noon_utc = datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc)
        day_start_utc = datetime(2026, 2, 13, 0, 0, tzinfo=timezone.utc)
        position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="DENTUSDT",
            side="SHORT",
            qty=1000.0,
            entry_price=0.001,
            liq_price_open=0.01,
            tp_price=0.0008,
            sl_price=0.0020,
            tp_order_id=100,
            sl_order_id=200,
            tp_client_order_id="tp-old",
            sl_client_order_id="sl-old",
            opened_at_utc=opened_at.isoformat(),
            expire_at_utc=(opened_at + timedelta(hours=12)).isoformat(),
            status="OPEN",
        )

        client = MagicMock()
        client.get_klines.return_value = [
            [0, "0", "0.0017", "0", "0", 0],
            [0, "0", "0.0015", "0", "0", 0],
        ]
        client.get_position_risk.return_value = [
            {
                "symbol": "DENTUSDT",
                "positionAmt": "-1000",
                "liquidationPrice": "0.01",
            }
        ]
        client.get_symbol_rules.return_value = {
            "DENTUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 3001,
            "clientOrderId": "sl-noon",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "1000",
            "status": "NEW",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_noon_protection_stop(
            day_start_utc=day_start_utc,
            noon_time_utc=noon_utc,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["errors"], 0)

        klines_kwargs = client.get_klines.call_args.kwargs
        self.assertEqual(klines_kwargs["symbol"], "DENTUSDT")
        self.assertEqual(klines_kwargs["interval"], "1m")
        self.assertEqual(klines_kwargs["start_time"], int(opened_at.timestamp() * 1000))
        self.assertEqual(klines_kwargs["end_time"], int(noon_utc.timestamp() * 1000))

        row = self._get_position(position_id)
        self.assertAlmostEqual(float(row["sl_price"]), 0.0017, places=10)
        lock_state = self.store.get_lock_state(PositionManager.NOON_PROTECTION_LOCK_NAME)
        self.assertIsNotNone(lock_state)
        assert lock_state is not None
        self.assertAlmostEqual(float(lock_state["caps"][str(position_id)]), 0.0017, places=10)

    def test_dynamic_stop_does_not_widen_when_noon_cap_is_tighter(self) -> None:
        position_id = self._insert_open_position(
            symbol="BTCUSDT",
            qty=0.01,
            tp_order_id=11,
            sl_order_id=22,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=24,
        )
        self.store.set_lock_state(
            PositionManager.NOON_PROTECTION_LOCK_NAME,
            {"caps": {str(position_id): 59000.0}},
        )

        client = MagicMock()
        client.get_order.side_effect = [{"status": "NEW"}, {"status": "NEW"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.01",
                "liquidationPrice": "61000",
            }
        ]
        client.get_symbol_rules.return_value = {
            "BTCUSDT": SimpleNamespace(tick_size=0.1)
        }
        client.normalize_trigger_price.return_value = 60390.0

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["updated_sl"], 0)
        client.create_order.assert_not_called()
        row = self._get_position(position_id)
        self.assertAlmostEqual(float(row["sl_price"]), 59000.0)

    def test_noon_protection_applies_to_untracked_exchange_positions(self) -> None:
        noon_utc = datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc)
        day_start_utc = datetime(2026, 2, 13, 0, 0, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "XRPUSDT",
                "positionAmt": "-1500",
                "positionSide": "BOTH",
                "liquidationPrice": "5.0",
            }
        ]
        client.get_klines.return_value = [
            [0, "0", "0.62", "0.51", "0", 0],
            [0, "0", "0.60", "0.52", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "XRPUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 4001,
            "clientOrderId": "sl-xrp-noon",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "1500",
            "status": "NEW",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_noon_protection_stop(
            day_start_utc=day_start_utc,
            noon_time_utc=noon_utc,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["errors"], 0)
        client.create_order.assert_called_once()
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["symbol"], "XRPUSDT")
        self.assertEqual(order_kwargs["side"], "BUY")
        self.assertEqual(order_kwargs["quantity"], "1500.0")
        self.assertTrue(order_kwargs["reduceOnly"])
        self.assertNotIn("closePosition", order_kwargs)
        lock_state = self.store.get_lock_state(PositionManager.NOON_PROTECTION_LOCK_NAME)
        self.assertIsNotNone(lock_state)
        assert lock_state is not None
        self.assertIn("EX:XRPUSDT:BOTH_SHORT", lock_state["caps"])

    def test_noon_protection_does_not_skip_untracked_exchange_position_from_cached_cap(self) -> None:
        noon_utc = datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc)
        day_start_utc = datetime(2026, 2, 13, 0, 0, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "SKYAIUSDT",
                "positionAmt": "-1500",
                "positionSide": "SHORT",
                "liquidationPrice": "5.0",
            }
        ]
        client.get_klines.return_value = [
            [0, "0", "0.0548", "0.0510", "0", 0],
            [0, "0", "0.0548", "0.0520", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "SKYAIUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 4002,
            "clientOrderId": "sl-skyai-noon",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "1500",
            "status": "NEW",
        }

        self.store.set_lock_state(
            PositionManager.NOON_PROTECTION_LOCK_NAME,
            {"caps": {"EX:SKYAIUSDT:SHORT": 0.0548}},
        )

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_noon_protection_stop(
            day_start_utc=day_start_utc,
            noon_time_utc=noon_utc,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["skipped"], 0)
        client.create_order.assert_called_once()

    def test_hourly_exchange_take_profit_initializes_state_from_true_open_time(self) -> None:
        opened_at_utc = datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc)
        now_local = datetime(2026, 3, 16, 10, 18, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-1",
                "entryPrice": "100",
                "positionSide": "BOTH",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(opened_at_utc.timestamp() * 1000),
                "qty": "1",
                "side": "SELL",
            }
        ]
        client.get_klines.return_value = [
            [int(opened_at_utc.timestamp() * 1000), "100", "101", "79", "80", 0],
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.refresh_hourly_exchange_take_profit_state(
            now_local=now_local,
            drop_pct=20.0,
        )

        self.assertEqual(summary["initialized"], 1)
        self.assertEqual(summary["updated"], 0)
        self.assertEqual(summary["pruned"], 0)

        lock_state = self.store.get_lock_state(PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME)
        self.assertIsNotNone(lock_state)
        assert lock_state is not None
        monitor = lock_state["symbols"]["BTCUSDT"]
        self.assertEqual(monitor["opened_at_utc"], opened_at_utc.isoformat())
        self.assertEqual(monitor["entry_price"], 100.0)
        self.assertEqual(monitor["lowest_price_since_open"], 79.0)
        self.assertTrue(monitor["eligible_reached"])

    def test_hourly_exchange_take_profit_keeps_eligibility_after_retrace(self) -> None:
        first_seen_local = datetime(2026, 3, 16, 10, 18, tzinfo=timezone.utc)
        second_seen_local = datetime(2026, 3, 16, 10, 19, tzinfo=timezone.utc)
        opened_at_utc = datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.side_effect = [
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "positionSide": "BOTH",
                }
            ],
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "positionSide": "BOTH",
                }
            ],
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(opened_at_utc.timestamp() * 1000),
                "qty": "1",
                "side": "SELL",
            }
        ]
        client.get_klines.side_effect = [
            [[int(opened_at_utc.timestamp() * 1000), "100", "101", "79", "80", 0]],
            [[int(opened_at_utc.timestamp() * 1000), "100", "101", "92", "95", 0]],
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        manager.refresh_hourly_exchange_take_profit_state(
            now_local=first_seen_local,
            drop_pct=20.0,
        )
        summary = manager.refresh_hourly_exchange_take_profit_state(
            now_local=second_seen_local,
            drop_pct=20.0,
        )

        self.assertEqual(summary["initialized"], 0)
        self.assertEqual(summary["updated"], 1)
        lock_state = self.store.get_lock_state(PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME)
        self.assertIsNotNone(lock_state)
        assert lock_state is not None
        monitor = lock_state["symbols"]["BTCUSDT"]
        self.assertEqual(monitor["lowest_price_since_open"], 79.0)
        self.assertTrue(monitor["eligible_reached"])

    def test_hourly_exchange_take_profit_closes_eligible_short_on_bullish_hour(self) -> None:
        self.store.set_lock_state(
            PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": {
                    "BTCUSDT": {
                        "symbol": "BTCUSDT",
                        "position_amt": -1.0,
                        "entry_price": 100.0,
                        "opened_at_utc": datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc).isoformat(),
                        "lowest_price_since_open": 79.0,
                        "eligible_reached": True,
                    }
                }
            },
        )

        client = MagicMock()
        client.get_position_risk.side_effect = [
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "85",
                    "positionSide": "BOTH",
                }
            ],
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "85",
                    "positionSide": "BOTH",
                }
            ],
        ]
        client.get_klines.side_effect = [
            [[0, "100", "101", "79", "80", 0]],
            [[0, "84", "86", "83", "85", 0]],
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "1",
                "side": "SELL",
            }
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 5001,
            "clientOrderId": "tp-hourly",
            "type": "MARKET",
            "side": "BUY",
            "origQty": "1",
            "status": "FILLED",
        }

        notifier = MagicMock()
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=notifier,
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        result = manager.run_hourly_exchange_take_profit(
            now_local=datetime(2026, 3, 16, 10, 59, tzinfo=timezone.utc),
            drop_pct=20.0,
        )

        self.assertEqual(result["closed_take_profit"], 1)
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["symbol"], "BTCUSDT")
        self.assertEqual(order_kwargs["side"], "BUY")
        self.assertTrue(order_kwargs["reduceOnly"])

    def test_hourly_exchange_take_profit_skips_ineligible_or_bearish_positions(self) -> None:
        self.store.set_lock_state(
            PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": {
                    "BTCUSDT": {
                        "symbol": "BTCUSDT",
                        "position_amt": -1.0,
                        "entry_price": 100.0,
                        "opened_at_utc": datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc).isoformat(),
                        "lowest_price_since_open": 79.0,
                        "eligible_reached": True,
                    },
                    "ETHUSDT": {
                        "symbol": "ETHUSDT",
                        "position_amt": -2.0,
                        "entry_price": 100.0,
                        "opened_at_utc": datetime(2026, 3, 16, 2, 0, tzinfo=timezone.utc).isoformat(),
                        "lowest_price_since_open": 90.0,
                        "eligible_reached": False,
                    },
                }
            },
        )

        client = MagicMock()
        client.get_position_risk.side_effect = [
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "83",
                    "positionSide": "BOTH",
                },
                {
                    "symbol": "ETHUSDT",
                    "positionAmt": "-2",
                    "entryPrice": "100",
                    "markPrice": "95",
                    "positionSide": "BOTH",
                },
            ],
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "83",
                    "positionSide": "BOTH",
                },
                {
                    "symbol": "ETHUSDT",
                    "positionAmt": "-2",
                    "entryPrice": "100",
                    "markPrice": "95",
                    "positionSide": "BOTH",
                },
            ],
        ]
        client.get_klines.side_effect = [
            [[0, "100", "101", "79", "80", 0]],
            [[0, "100", "101", "90", "95", 0]],
            [[0, "84", "85", "82", "83", 0]],
        ]
        client.get_user_trades.side_effect = [
            [
                {
                    "time": int(datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    "qty": "1",
                    "side": "SELL",
                }
            ],
            [
                {
                    "time": int(datetime(2026, 3, 16, 2, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    "qty": "2",
                    "side": "SELL",
                }
            ],
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        result = manager.run_hourly_exchange_take_profit(
            now_local=datetime(2026, 3, 16, 10, 59, tzinfo=timezone.utc),
            drop_pct=20.0,
        )

        self.assertEqual(result["closed_take_profit"], 0)
        client.create_order.assert_not_called()

    def test_hourly_exchange_take_profit_logs_symbol_when_close_fails(self) -> None:
        self.store.set_lock_state(
            PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": {
                    "COSUSDT": {
                        "symbol": "COSUSDT",
                        "position_amt": -10.0,
                        "entry_price": 0.0023,
                        "opened_at_utc": datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc).isoformat(),
                        "lowest_price_since_open": 0.001755,
                        "eligible_reached": True,
                    }
                }
            },
        )

        client = MagicMock()
        client.get_position_risk.side_effect = [
            [
                {
                    "symbol": "COSUSDT",
                    "positionAmt": "-10",
                    "entryPrice": "0.0023",
                    "markPrice": "0.0021",
                    "positionSide": "BOTH",
                }
            ],
            [
                {
                    "symbol": "COSUSDT",
                    "positionAmt": "-10",
                    "entryPrice": "0.0023",
                    "markPrice": "0.0021",
                    "positionSide": "BOTH",
                }
            ],
        ]
        client.get_klines.side_effect = [
            [[0, "0.0023", "0.0023", "0.001755", "0.0019", 0]],
            [[0, "0.0020", "0.0022", "0.0019", "0.0021", 0]],
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 15, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "10",
                "side": "SELL",
            }
        ]
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = RuntimeError("close rejected")

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        with patch("core.position_manager.LOGGER.exception") as log_exception:
            result = manager.run_hourly_exchange_take_profit(
                now_local=datetime(2026, 3, 16, 7, 59, tzinfo=timezone.utc),
                drop_pct=20.0,
            )

        self.assertEqual(result["errors"], 1)
        log_args = log_exception.call_args.args
        self.assertIn("Hourly exchange take-profit failed", log_args[0])
        self.assertEqual(log_args[1], "COSUSDT")

    def _insert_open_position(
        self,
        symbol: str,
        qty: float,
        tp_order_id: int,
        sl_order_id: int,
        tp_price: float,
        sl_price: float,
        expire_in_hours: float,
    ) -> int:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        expire_at = now + timedelta(hours=expire_in_hours)
        return self.store.insert_position(
            run_id=self.run_id,
            symbol=symbol,
            side="SHORT",
            qty=qty,
            entry_price=50000.0,
            liq_price_open=60000.0,
            tp_price=tp_price,
            sl_price=sl_price,
            tp_order_id=tp_order_id,
            sl_order_id=sl_order_id,
            tp_client_order_id="tp-old",
            sl_client_order_id="sl-old",
            opened_at_utc=now.isoformat(),
            expire_at_utc=expire_at.isoformat(),
            status="OPEN",
        )

    def _get_position(self, position_id: int) -> Dict[str, Any]:
        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute(
                "SELECT * FROM positions WHERE id = ?",
                (position_id,),
            ).fetchone()
            if row is None:
                raise AssertionError(f"Position not found: id={position_id}")
            return dict(row)


if __name__ == "__main__":
    unittest.main()
