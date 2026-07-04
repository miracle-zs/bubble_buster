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
from infra.binance_futures_client import BinanceAPIError


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
        client.format_trigger_price.return_value = "60390.0"
        client.format_order_qty.return_value = "0.01"
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

    def test_zero_position_checks_filled_tp_before_marking_external_and_records_fill(self) -> None:
        position_id = self._insert_open_position(
            symbol="BTCUSDT",
            qty=0.02,
            tp_order_id=101,
            sl_order_id=202,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=24,
        )

        client = MagicMock()
        filled_tp_order = {
            "orderId": 101,
            "clientOrderId": "tp-old",
            "type": "TAKE_PROFIT_MARKET",
            "side": "BUY",
            "status": "FILLED",
            "reduceOnly": True,
        }
        client.get_order.side_effect = [
            filled_tp_order,
            {"orderId": 202, "clientOrderId": "sl-old", "status": "NEW"},
            filled_tp_order,
        ]
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0",
                "liquidationPrice": "0",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "symbol": "BTCUSDT",
                "orderId": 101,
                "side": "BUY",
                "qty": "0.01",
                "quoteQty": "400",
                "price": "40000",
                "commission": "0.1",
                "commissionAsset": "USDT",
                "realizedPnl": "10",
                "time": 1771000000000,
            },
            {
                "symbol": "BTCUSDT",
                "orderId": 101,
                "side": "BUY",
                "qty": "0.01",
                "quoteQty": "398",
                "price": "39800",
                "commission": "0.1",
                "commissionAsset": "USDT",
                "realizedPnl": "12",
                "time": 1771000001000,
            },
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["closed_tp"], 1)
        self.assertEqual(summary["closed_external"], 0)
        row = self._get_position(position_id)
        self.assertEqual(row["status"], "CLOSED_TP")
        self.assertEqual(row["close_reason"], "TAKE_PROFIT_FILLED")

        fill = self._get_buy_fill(position_id)
        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertEqual(fill["order_id"], 101)
        self.assertAlmostEqual(float(fill["executed_qty"]), 0.02)
        self.assertAlmostEqual(float(fill["avg_price"]), 798 / 0.02)
        self.assertAlmostEqual(float(fill["realized_pnl"]), 22.0)

    def test_missing_position_risk_cancels_stale_exit_orders_and_marks_external(self) -> None:
        position_id = self._insert_open_position(
            symbol="BTCUSDT",
            qty=0.02,
            tp_order_id=101,
            sl_order_id=202,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=24,
        )

        client = MagicMock()
        client.get_position_risk.return_value = []
        client.get_order.side_effect = [
            {"orderId": 101, "clientOrderId": "tp-old", "status": "NEW"},
            {"orderId": 202, "clientOrderId": "sl-old", "status": "NEW"},
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["closed_external"], 1)
        self.assertEqual(client.cancel_order.call_count, 2)
        client.cancel_order.assert_any_call(
            symbol="BTCUSDT",
            order_id=101,
            orig_client_order_id="tp-old",
        )
        client.cancel_order.assert_any_call(
            symbol="BTCUSDT",
            order_id=202,
            orig_client_order_id="sl-old",
        )

        row = self._get_position(position_id)
        self.assertEqual(row["status"], "CLOSED_EXTERNAL")
        self.assertEqual(row["close_reason"], "SHORT_POSITION_NOT_FOUND")
        self.assertIsNone(row["last_error"])

    def test_cleanup_orphan_exit_orders_once_per_day_cancels_without_exchange_short(self) -> None:
        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "MAGMAUSDT",
                "positionAmt": "-10",
                "liquidationPrice": "1.2",
            }
        ]
        client.get_open_orders.return_value = [
            {
                "symbol": "BASUSDT",
                "orderId": 301,
                "clientOrderId": "old-bas-sl",
                "type": "STOP_MARKET",
                "side": "BUY",
                "status": "NEW",
                "reduceOnly": True,
            },
            {
                "symbol": "MAGMAUSDT",
                "orderId": 302,
                "clientOrderId": "live-magma-sl",
                "type": "STOP_MARKET",
                "side": "BUY",
                "status": "NEW",
                "reduceOnly": True,
            },
            {
                "symbol": "NOUSDT",
                "orderId": 303,
                "clientOrderId": "entry-sell",
                "type": "LIMIT",
                "side": "SELL",
                "status": "NEW",
            },
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.cleanup_orphan_exit_orders_once_per_day()

        self.assertEqual(summary["canceled"], 1)
        client.cancel_order.assert_called_once_with(
            symbol="BASUSDT",
            order_id=301,
            orig_client_order_id="old-bas-sl",
        )

    def test_run_once_does_not_scan_orphan_exit_orders(self) -> None:
        client = MagicMock()
        client.get_position_risk.return_value = []

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertNotIn("orphan_exit_orders", summary)
        client.get_open_orders.assert_not_called()

    def test_cleanup_orphan_exit_orders_once_per_day_skips_after_daily_run(self) -> None:
        today_key = datetime.now().astimezone().date().isoformat()
        self.store.set_lock_state(
            "orphan_exit_order_cleanup_v1",
            {
                "day_key": today_key,
                "canceled": 0,
                "updated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            },
        )

        client = MagicMock()
        client.get_position_risk.return_value = []

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.cleanup_orphan_exit_orders_once_per_day()

        self.assertEqual(summary["canceled"], 0)
        self.assertTrue(summary["skipped"])
        client.get_open_orders.assert_not_called()

    def test_filled_sl_on_open_position_records_close_fill(self) -> None:
        position_id = self._insert_open_position(
            symbol="ETHUSDT",
            qty=0.5,
            tp_order_id=303,
            sl_order_id=404,
            tp_price=2000.0,
            sl_price=3000.0,
            expire_in_hours=24,
        )

        client = MagicMock()
        filled_sl_order = {
            "orderId": 404,
            "clientOrderId": "sl-old",
            "type": "STOP_MARKET",
            "side": "BUY",
            "status": "FILLED",
            "reduceOnly": True,
        }
        client.get_order.side_effect = [
            {"orderId": 303, "clientOrderId": "tp-old", "status": "NEW"},
            filled_sl_order,
            filled_sl_order,
        ]
        client.get_position_risk.return_value = [
            {
                "symbol": "ETHUSDT",
                "positionAmt": "-0.5",
                "liquidationPrice": "3500",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "symbol": "ETHUSDT",
                "orderId": 404,
                "side": "BUY",
                "qty": "0.5",
                "quoteQty": "1500",
                "price": "3000",
                "commission": "0.6",
                "commissionAsset": "USDT",
                "realizedPnl": "-25",
                "time": 1771000000000,
            }
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["closed_sl"], 1)
        row = self._get_position(position_id)
        self.assertEqual(row["status"], "CLOSED_SL")
        fill = self._get_buy_fill(position_id)
        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertEqual(fill["order_id"], 404)
        self.assertAlmostEqual(float(fill["avg_price"]), 3000.0)

    def test_filled_sl_is_detected_when_position_risk_is_missing(self) -> None:
        position_id = self._insert_open_position(
            symbol="TAIKOUSDT",
            qty=10.0,
            tp_order_id=303,
            sl_order_id=404,
            tp_price=0.05,
            sl_price=0.084,
            expire_in_hours=24,
        )

        client = MagicMock()
        filled_sl_order = {
            "orderId": 404,
            "clientOrderId": "sl-old",
            "type": "STOP_MARKET",
            "side": "BUY",
            "status": "FILLED",
            "reduceOnly": True,
        }
        client.get_order.side_effect = [
            {"orderId": 303, "clientOrderId": "tp-old", "status": "NEW"},
            filled_sl_order,
            filled_sl_order,
        ]
        client.get_position_risk.return_value = []
        client.get_user_trades.return_value = [
            {
                "symbol": "TAIKOUSDT",
                "orderId": 404,
                "side": "BUY",
                "qty": "10",
                "quoteQty": "0.84",
                "price": "0.084",
                "commission": "0.001",
                "commissionAsset": "USDT",
                "realizedPnl": "-0.04",
                "time": 1782880186829,
            }
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["closed_sl"], 1)
        self.assertEqual(summary["closed_external"], 0)
        row = self._get_position(position_id)
        self.assertEqual(row["status"], "CLOSED_SL")
        self.assertEqual(row["close_reason"], "STOP_LOSS_FILLED")
        self.assertIsNone(row["last_error"])

        fill = self._get_buy_fill(position_id)
        self.assertIsNotNone(fill)
        assert fill is not None
        self.assertEqual(fill["order_id"], 404)
        self.assertAlmostEqual(float(fill["avg_price"]), 0.084)

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
            expire_at_utc=(opened_at + timedelta(days=7)).isoformat(),
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

    def test_run_once_backfills_noon_cap_for_position_opened_after_noon(self) -> None:
        day_start = datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc)
        noon = datetime(2026, 7, 1, 4, 0, tzinfo=timezone.utc)
        opened_at = datetime(2026, 7, 1, 5, 0, tzinfo=timezone.utc)
        position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="MUSDT",
            side="SHORT",
            qty=100.0,
            entry_price=0.78,
            liq_price_open=1.12,
            tp_price=None,
            sl_price=1.10,
            tp_order_id=None,
            sl_order_id=22,
            tp_client_order_id=None,
            sl_client_order_id="sl-old",
            opened_at_utc=opened_at.isoformat(),
            expire_at_utc=(opened_at + timedelta(days=7)).isoformat(),
            status="OPEN",
        )
        self.store.set_lock_state(
            PositionManager.NOON_PROTECTION_LOCK_NAME,
            {
                "caps": {},
                "day_start_utc": day_start.isoformat(),
                "noon_time_utc": noon.isoformat(),
            },
        )

        client = MagicMock()
        client.get_order.return_value = {"status": "NEW"}
        client.get_position_risk.return_value = [
            {
                "symbol": "MUSDT",
                "positionAmt": "-100",
                "liquidationPrice": "1.12",
                "markPrice": "0.86",
                "positionSide": "BOTH",
            }
        ]
        client.get_klines.return_value = [[0, "0.7", "0.90", "0.6", "0.8", 0]]
        client.get_symbol_rules.return_value = {"MUSDT": SimpleNamespace(tick_size=0.0001)}
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 333,
            "clientOrderId": "sl-noon-late",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "100",
            "status": "NEW",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["updated_sl"], 1)
        klines_kwargs = client.get_klines.call_args.kwargs
        self.assertEqual(klines_kwargs["start_time"], int(day_start.timestamp() * 1000))
        self.assertEqual(klines_kwargs["end_time"], int(noon.timestamp() * 1000))
        row = self._get_position(position_id)
        self.assertAlmostEqual(float(row["sl_price"]), 0.90)
        self.assertEqual(row["sl_order_id"], 333)
        lock_state = self.store.get_lock_state(PositionManager.NOON_PROTECTION_LOCK_NAME)
        self.assertAlmostEqual(float(lock_state["caps"][str(position_id)]), 0.90)

    def test_run_once_uses_noon_to_open_high_when_morning_noon_cap_already_breached(self) -> None:
        day_start = datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc)
        noon = datetime(2026, 7, 1, 4, 0, tzinfo=timezone.utc)
        opened_at = datetime(2026, 7, 1, 5, 0, tzinfo=timezone.utc)
        position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="MUSDT",
            side="SHORT",
            qty=100.0,
            entry_price=0.78,
            liq_price_open=1.12,
            tp_price=None,
            sl_price=1.10,
            tp_order_id=None,
            sl_order_id=22,
            tp_client_order_id=None,
            sl_client_order_id="sl-old",
            opened_at_utc=opened_at.isoformat(),
            expire_at_utc=(opened_at + timedelta(days=7)).isoformat(),
            status="OPEN",
        )
        self.store.set_lock_state(
            PositionManager.NOON_PROTECTION_LOCK_NAME,
            {
                "caps": {},
                "day_start_utc": day_start.isoformat(),
                "noon_time_utc": noon.isoformat(),
            },
        )

        client = MagicMock()
        client.get_order.return_value = {"status": "NEW"}
        client.get_position_risk.return_value = [
            {
                "symbol": "MUSDT",
                "positionAmt": "-100",
                "liquidationPrice": "1.12",
                "markPrice": "1.02",
                "positionSide": "BOTH",
            }
        ]
        client.get_klines.side_effect = [
            [[0, "0.7", "0.90", "0.6", "0.8", 0]],
            [[0, "0.9", "1.05", "0.8", "0.95", 0]],
        ]
        client.get_symbol_rules.return_value = {"MUSDT": SimpleNamespace(tick_size=0.0001)}
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 444,
            "clientOrderId": "sl-noon-late",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "100",
            "status": "NEW",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        summary = manager.run_once()

        self.assertEqual(summary["closed_sl"], 0)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(client.get_klines.call_count, 2)
        second_klines_kwargs = client.get_klines.call_args_list[1].kwargs
        self.assertEqual(second_klines_kwargs["start_time"], int(noon.timestamp() * 1000))
        self.assertEqual(second_klines_kwargs["end_time"], int(opened_at.timestamp() * 1000))
        create_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(create_kwargs["type"], "STOP_MARKET")
        self.assertEqual(create_kwargs["side"], "BUY")
        self.assertEqual(create_kwargs["quantity"], "100.0")
        self.assertTrue(create_kwargs["reduceOnly"])
        self.assertEqual(create_kwargs["stopPrice"], "1.05")
        row = self._get_position(position_id)
        self.assertEqual(row["status"], "OPEN")
        self.assertAlmostEqual(float(row["sl_price"]), 1.05)
        self.assertEqual(row["sl_order_id"], 444)

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

    def test_noon_protection_uses_0800_start_for_untracked_exchange_positions(self) -> None:
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
        ]
        client.get_symbol_rules.return_value = {
            "XRPUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 4003,
            "clientOrderId": "sl-xrp-noon-0800",
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

        manager.run_noon_protection_stop(
            day_start_utc=day_start_utc,
            noon_time_utc=noon_utc,
        )

        klines_kwargs = client.get_klines.call_args.kwargs
        self.assertEqual(
            klines_kwargs["start_time"],
            int((day_start_utc + timedelta(hours=8)).timestamp() * 1000),
        )

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

    def test_noon_protection_does_not_persist_new_cap_when_order_creation_fails(self) -> None:
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
            [0, "0", "0.0550", "0.0510", "0", 0],
            [0, "0", "0.0549", "0.0520", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "SKYAIUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = RuntimeError("create noon stop failed")

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
        self.assertEqual(summary["updated_sl"], 0)
        self.assertEqual(summary["skipped"], 0)
        self.assertEqual(summary["errors"], 1)

        lock_state = self.store.get_lock_state(PositionManager.NOON_PROTECTION_LOCK_NAME)
        self.assertIsNotNone(lock_state)
        assert lock_state is not None
        self.assertAlmostEqual(float(lock_state["caps"]["EX:SKYAIUSDT:SHORT"]), 0.0548, places=10)

    def test_noon_protection_market_closes_when_stop_would_immediately_trigger(self) -> None:
        noon_utc = datetime(2026, 2, 13, 12, 0, tzinfo=timezone.utc)
        day_start_utc = datetime(2026, 2, 13, 0, 0, tzinfo=timezone.utc)
        position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="DYDXUSDT",
            side="SHORT",
            qty=12.0,
            entry_price=0.80,
            liq_price_open=1.20,
            tp_price=None,
            sl_price=1.10,
            tp_order_id=None,
            sl_order_id=22,
            tp_client_order_id=None,
            sl_client_order_id="sl-old",
            opened_at_utc=datetime(2026, 2, 13, 1, 0, tzinfo=timezone.utc).isoformat(),
            expire_at_utc=datetime(2026, 2, 14, 1, 0, tzinfo=timezone.utc).isoformat(),
            status="OPEN",
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "DYDXUSDT",
                "positionAmt": "-12",
                "positionSide": "BOTH",
                "liquidationPrice": "1.20",
            }
        ]
        client.get_klines.return_value = [
            [0, "0", "0.92", "0.70", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "DYDXUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = [
            BinanceAPIError(-2021, "Order would immediately trigger."),
            {
                "orderId": 999,
                "clientOrderId": "nsl-close",
                "type": "MARKET",
                "side": "BUY",
                "origQty": "12",
                "status": "FILLED",
            },
        ]

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
        self.assertEqual(summary["updated_sl"], 0)
        self.assertEqual(summary["closed_immediate"], 1)
        self.assertEqual(summary["errors"], 0)
        self.assertEqual(client.create_order.call_count, 2)
        stop_kwargs = client.create_order.call_args_list[0].kwargs
        self.assertEqual(stop_kwargs["type"], "STOP_MARKET")
        close_kwargs = client.create_order.call_args_list[1].kwargs
        self.assertEqual(close_kwargs["symbol"], "DYDXUSDT")
        self.assertEqual(close_kwargs["side"], "BUY")
        self.assertEqual(close_kwargs["type"], "MARKET")
        self.assertEqual(close_kwargs["quantity"], "12.0")
        self.assertTrue(close_kwargs["reduceOnly"])

        row = self._get_position(position_id)
        self.assertEqual(row["status"], "CLOSED_NOON_PROTECTION")
        self.assertEqual(row["close_reason"], "NOON_PROTECTION_IMMEDIATE_TRIGGER")
        self.assertEqual(row["close_order_id"], 999)
        self.assertIsNone(row["last_error"])

    def test_morning_protection_tightens_tracked_short_older_than_min_hold_to_current_hour_high(self) -> None:
        opened_at = datetime(2026, 3, 16, 23, 0, tzinfo=timezone.utc)
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="DENTUSDT",
            side="SHORT",
            qty=1000.0,
            entry_price=0.001,
            liq_price_open=0.01,
            tp_price=0.0008,
            sl_price=0.0025,
            tp_order_id=100,
            sl_order_id=200,
            tp_client_order_id="tp-old",
            sl_client_order_id="sl-old",
            opened_at_utc=opened_at.isoformat(),
            expire_at_utc=(opened_at + timedelta(hours=12)).isoformat(),
            status="OPEN",
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "DENTUSDT",
                "positionAmt": "-1000",
                "positionSide": "BOTH",
                "liquidationPrice": "0.01",
            }
        ]
        client.get_klines.return_value = [
            [0, "0", "0.0021", "0.0019", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "DENTUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 5001,
            "clientOrderId": "sl-morning",
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

        summary = manager.run_morning_protection_stop(
            check_time_utc=check_time,
            min_hold_hours=6.0,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["errors"], 0)
        row = self._get_position(position_id)
        self.assertAlmostEqual(float(row["sl_price"]), 0.0021, places=10)

    def test_morning_protection_applies_to_exchange_short_positions(self) -> None:
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "XRPUSDT",
                "positionAmt": "-1500",
                "positionSide": "BOTH",
                "entryPrice": "0.7",
                "liquidationPrice": "5.0",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 22, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "1500",
                "side": "SELL",
            }
        ]
        client.get_klines.return_value = [
            [0, "0", "0.62", "0.51", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "XRPUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 5002,
            "clientOrderId": "sl-xrp-morning",
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
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_morning_protection_stop(
            check_time_utc=check_time,
            min_hold_hours=6.0,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["errors"], 0)
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["symbol"], "XRPUSDT")
        self.assertEqual(order_kwargs["side"], "BUY")
        self.assertTrue(order_kwargs["reduceOnly"])

    def test_morning_protection_skips_reopened_exchange_short_that_does_not_meet_min_hold(self) -> None:
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "ONTUSDT",
                "positionAmt": "-1000",
                "positionSide": "BOTH",
                "entryPrice": "0.06",
                "liquidationPrice": "0.2",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 0, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "1000",
                "side": "SELL",
            },
            {
                "time": int(datetime(2026, 3, 17, 0, 30, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "1000",
                "side": "BUY",
            },
            {
                "time": int(datetime(2026, 3, 17, 3, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "1000",
                "side": "SELL",
            },
        ]
        client.get_klines.return_value = [
            [0, "0", "0.061", "0.058", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "ONTUSDT": SimpleNamespace(tick_size=0.0001),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_morning_protection_stop(
            check_time_utc=check_time,
            min_hold_hours=6.0,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 0)
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["errors"], 0)
        client.create_order.assert_not_called()

    def test_morning_protection_ignores_stale_exchange_cap_from_previous_position(self) -> None:
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        self.store.set_lock_state(
            PositionManager.MORNING_PROTECTION_LOCK_NAME,
            {
                "caps": {"EX:BTCUSDT:BOTH_SHORT": 70800.0},
                "cap_updated_at_utc_by_key": {
                    "EX:BTCUSDT:BOTH_SHORT": "2026-03-16T23:55:03+00:00",
                },
                "updated_at_utc": "2026-03-16T23:55:03+00:00",
            },
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.042",
                "positionSide": "BOTH",
                "entryPrice": "70641.1",
                "liquidationPrice": "90000",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 23, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "SELL",
            },
            {
                "time": int(datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "BUY",
            },
            {
                "time": int(datetime(2026, 3, 17, 1, 36, 35, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "SELL",
            },
        ]
        client.get_klines.return_value = [
            [0, "0", "70900", "70500", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "BTCUSDT": SimpleNamespace(tick_size=0.1),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 5004,
            "clientOrderId": "sl-btc-morning",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "0.042",
            "status": "NEW",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_morning_protection_stop(
            check_time_utc=check_time,
            min_hold_hours=6.0,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["skipped"], 0)
        self.assertEqual(summary["errors"], 0)
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["symbol"], "BTCUSDT")
        self.assertEqual(order_kwargs["stopPrice"], "70900.0")

    def test_morning_protection_does_not_reuse_btc_cap_when_other_symbol_updates_lock_later(self) -> None:
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        self.store.set_lock_state(
            PositionManager.MORNING_PROTECTION_LOCK_NAME,
            {
                "caps": {
                    "EX:BTCUSDT:BOTH_SHORT": 70800.0,
                    "EX:ETHUSDT:BOTH_SHORT": 2100.0,
                },
                "cap_updated_at_utc_by_key": {
                    "EX:BTCUSDT:BOTH_SHORT": "2026-03-16T23:55:03+00:00",
                    "EX:ETHUSDT:BOTH_SHORT": "2026-03-17T03:00:00+00:00",
                },
                "updated_at_utc": "2026-03-17T03:00:00+00:00",
            },
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.042",
                "positionSide": "BOTH",
                "entryPrice": "70641.1",
                "liquidationPrice": "90000",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 23, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "SELL",
            },
            {
                "time": int(datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "BUY",
            },
            {
                "time": int(datetime(2026, 3, 17, 1, 36, 35, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "SELL",
            },
        ]
        client.get_klines.return_value = [
            [0, "0", "70900", "70500", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "BTCUSDT": SimpleNamespace(tick_size=0.1),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 5005,
            "clientOrderId": "sl-btc-morning-2",
            "type": "STOP_MARKET",
            "side": "BUY",
            "origQty": "0.042",
            "status": "NEW",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_morning_protection_stop(
            check_time_utc=check_time,
            min_hold_hours=6.0,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 1)
        self.assertEqual(summary["skipped"], 0)
        self.assertEqual(summary["errors"], 0)
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["symbol"], "BTCUSDT")
        self.assertEqual(order_kwargs["stopPrice"], "70900.0")

    def test_morning_protection_does_not_persist_new_cap_when_order_creation_fails(self) -> None:
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)
        self.store.set_lock_state(
            PositionManager.MORNING_PROTECTION_LOCK_NAME,
            {
                "caps": {"EX:BTCUSDT:BOTH_SHORT": 70800.0},
                "cap_updated_at_utc_by_key": {
                    "EX:BTCUSDT:BOTH_SHORT": "2026-03-16T23:55:03+00:00",
                },
                "updated_at_utc": "2026-03-16T23:55:03+00:00",
            },
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.042",
                "positionSide": "BOTH",
                "entryPrice": "70641.1",
                "liquidationPrice": "90000",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 23, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "SELL",
            },
            {
                "time": int(datetime(2026, 3, 17, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "BUY",
            },
            {
                "time": int(datetime(2026, 3, 17, 1, 36, 35, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "0.042",
                "side": "SELL",
            },
        ]
        client.get_klines.return_value = [
            [0, "0", "70900", "70500", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "BTCUSDT": SimpleNamespace(tick_size=0.1),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.side_effect = RuntimeError("create stop failed")

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_morning_protection_stop(
            check_time_utc=check_time,
            min_hold_hours=6.0,
        )

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["updated_sl"], 0)
        self.assertEqual(summary["skipped"], 0)
        self.assertEqual(summary["errors"], 1)

        state = self.store.get_lock_state(PositionManager.MORNING_PROTECTION_LOCK_NAME) or {}
        holder_caps = state.get("caps") if isinstance(state, dict) else None
        self.assertIsInstance(holder_caps, dict)
        self.assertEqual(float(holder_caps["EX:BTCUSDT:BOTH_SHORT"]), 70800.0)

    def test_morning_protection_uses_current_hour_low_for_long_positions(self) -> None:
        check_time = datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "SOLUSDT",
                "positionAmt": "3",
                "positionSide": "LONG",
                "entryPrice": "120",
                "liquidationPrice": "80",
            }
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 22, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "3",
                "side": "BUY",
            }
        ]
        client.get_klines.return_value = [
            [0, "0", "122", "118", "0", 0],
        ]
        client.get_symbol_rules.return_value = {
            "SOLUSDT": SimpleNamespace(tick_size=0.1),
        }
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        client.format_trigger_price.side_effect = lambda _symbol, price, round_up=False: str(price)
        client.format_order_qty.side_effect = lambda _symbol, qty: str(qty)
        client.create_order.return_value = {
            "orderId": 5003,
            "clientOrderId": "sl-sol-morning",
            "type": "STOP_MARKET",
            "side": "SELL",
            "origQty": "3",
            "status": "NEW",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        summary = manager.run_morning_protection_stop(
            check_time_utc=check_time,
            min_hold_hours=6.0,
        )

        self.assertEqual(summary["updated_sl"], 1)
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["side"], "SELL")
        self.assertEqual(order_kwargs["positionSide"], "LONG")
        self.assertEqual(order_kwargs["stopPrice"], "118.0")

    def test_dynamic_stop_respects_tighter_morning_protection_cap(self) -> None:
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
            PositionManager.MORNING_PROTECTION_LOCK_NAME,
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

    def test_run_once_skips_timeout_and_dynamic_stop_for_exempt_symbol(self) -> None:
        position_id = self._insert_open_position(
            symbol="XAUUSDT",
            qty=0.01,
            tp_order_id=11,
            sl_order_id=22,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=-1,
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "XAUUSDT",
                "positionAmt": "-0.01",
                "liquidationPrice": "61000",
            }
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            protection_exempt_symbols={"XAUUSDT"},
        )

        summary = manager.run_once()

        self.assertEqual(summary["closed_timeout"], 0)
        self.assertEqual(summary["updated_sl"], 0)
        client.get_order.assert_not_called()
        client.create_order.assert_not_called()
        row = self._get_position(position_id)
        self.assertEqual(row["status"], "OPEN")

    def test_daily_loss_cut_skips_exempt_symbol(self) -> None:
        position_id = self._insert_open_position(
            symbol="XAUUSDT",
            qty=0.02,
            tp_order_id=101,
            sl_order_id=202,
            tp_price=40000.0,
            sl_price=59000.0,
            expire_in_hours=24,
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {"symbol": "XAUUSDT", "positionAmt": "-0.02", "unRealizedProfit": "-1.2"}
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            protection_exempt_symbols={"XAUUSDT"},
        )

        summary = manager.run_daily_loss_cut()

        self.assertEqual(summary["closed_loss_cut"], 0)
        client.create_order.assert_not_called()
        row = self._get_position(position_id)
        self.assertEqual(row["status"], "OPEN")

    def test_morning_and_noon_protection_skip_exempt_symbol(self) -> None:
        opened_at = datetime(2026, 3, 17, 0, 30, tzinfo=timezone.utc)
        position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="XAUUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=120.0,
            tp_price=80.0,
            sl_price=118.0,
            tp_order_id=11,
            sl_order_id=22,
            tp_client_order_id="tp-old",
            sl_client_order_id="sl-old",
            status="OPEN",
            opened_at_utc=opened_at.isoformat(),
            expire_at_utc=(opened_at + timedelta(hours=24)).isoformat(),
        )

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "XAUUSDT",
                "positionAmt": "-1",
                "entryPrice": "100",
                "liquidationPrice": "120",
                "positionSide": "BOTH",
            }
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
            protection_exempt_symbols={"XAUUSDT"},
        )

        morning = manager.run_morning_protection_stop(
            check_time_utc=datetime(2026, 3, 17, 7, 55, tzinfo=timezone.utc),
            min_hold_hours=6.0,
        )
        noon = manager.run_noon_protection_stop(
            day_start_utc=datetime(2026, 3, 17, 0, 0, tzinfo=timezone.utc),
            noon_time_utc=datetime(2026, 3, 17, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(morning["updated_sl"], 0)
        self.assertEqual(noon["updated_sl"], 0)
        client.get_klines.assert_not_called()
        client.create_order.assert_not_called()
        row = self._get_position(position_id)
        self.assertEqual(row["status"], "OPEN")

    def test_hourly_exchange_take_profit_skips_exempt_symbol(self) -> None:
        self.store.set_lock_state(
            PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": {
                    "XAUUSDT": {
                        "symbol": "XAUUSDT",
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
                    "symbol": "XAUUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "83",
                    "positionSide": "BOTH",
                }
            ],
            [
                {
                    "symbol": "XAUUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "83",
                    "positionSide": "BOTH",
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
            protection_exempt_symbols={"XAUUSDT"},
        )

        summary = manager.run_hourly_exchange_take_profit(
            now_local=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            drop_pct=20.0,
        )

        self.assertEqual(summary["closed_take_profit"], 0)
        client.get_klines.assert_not_called()
        client.create_order.assert_not_called()

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

    def test_hourly_exchange_take_profit_refresh_keeps_other_symbols_when_one_symbol_fails(self) -> None:
        opened_at_utc = datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc)
        now_local = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)

        client = MagicMock()
        client.get_position_risk.return_value = [
            {
                "symbol": "NFPUSDT",
                "positionAmt": "-10",
                "entryPrice": "100",
                "positionSide": "BOTH",
            },
            {
                "symbol": "TAIKOUSDT",
                "positionAmt": "-1",
                "entryPrice": "100",
                "positionSide": "BOTH",
            },
        ]

        def get_user_trades(symbol: str, limit: int) -> list[dict[str, object]]:
            del limit
            if symbol == "NFPUSDT":
                return [
                    {
                        "time": int(opened_at_utc.timestamp() * 1000),
                        "qty": "5",
                        "side": "SELL",
                    }
                ]
            if symbol == "TAIKOUSDT":
                return [
                    {
                        "time": int(opened_at_utc.timestamp() * 1000),
                        "qty": "1",
                        "side": "SELL",
                    }
                ]
            return []

        client.get_user_trades.side_effect = get_user_trades
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
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["error_symbols"], ["NFPUSDT"])

        lock_state = self.store.get_lock_state(PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME)
        self.assertIsNotNone(lock_state)
        assert lock_state is not None
        self.assertNotIn("NFPUSDT", lock_state["symbols"])
        monitor = lock_state["symbols"]["TAIKOUSDT"]
        self.assertEqual(monitor["opened_at_utc"], opened_at_utc.isoformat())
        self.assertEqual(monitor["lowest_price_since_open"], 79.0)
        self.assertTrue(monitor["eligible_reached"])

    def test_fetch_symbol_extremes_between_paginates_beyond_1000_bars(self) -> None:
        start_utc = datetime(2026, 3, 16, 0, 0, tzinfo=timezone.utc)
        end_utc = start_utc + timedelta(minutes=1002)
        first_open_ms = int(start_utc.timestamp() * 1000)
        second_open_ms = int((start_utc + timedelta(minutes=1000)).timestamp() * 1000)
        first_batch = [
            [first_open_ms + idx * 60_000, "100", "110", "79", "90", 0]
            for idx in range(1000)
        ]

        client = MagicMock()
        client.get_klines.side_effect = [
            first_batch,
            [[second_open_ms, "90", "95", "70", "75", 0]],
        ]

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
        )

        high_price, low_price = manager._fetch_symbol_extremes_between(
            symbol="BTCUSDT",
            start_utc=start_utc,
            end_utc=end_utc,
        )

        self.assertEqual(high_price, 110.0)
        self.assertEqual(low_price, 70.0)
        self.assertEqual(client.get_klines.call_count, 2)

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

    def test_hourly_exchange_take_profit_resets_state_for_reopened_symbol(self) -> None:
        old_opened_at_utc = datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc)
        new_opened_at_utc = datetime(2026, 3, 16, 11, 10, tzinfo=timezone.utc)
        now_local = datetime(2026, 3, 16, 12, 59, tzinfo=timezone.utc)

        self.store.set_lock_state(
            PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": {
                    "BTCUSDT": {
                        "symbol": "BTCUSDT",
                        "position_amt": -1.0,
                        "entry_price": 100.0,
                        "opened_at_utc": old_opened_at_utc.isoformat(),
                        "lowest_price_since_open": 79.0,
                        "eligible_reached": True,
                        "eligible_reached_at_utc": old_opened_at_utc.isoformat(),
                    }
                }
            },
        )

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
                "time": int(old_opened_at_utc.timestamp() * 1000),
                "qty": "1",
                "side": "SELL",
            },
            {
                "time": int(datetime(2026, 3, 16, 10, 59, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "1",
                "side": "BUY",
            },
            {
                "time": int(new_opened_at_utc.timestamp() * 1000),
                "qty": "1",
                "side": "SELL",
            },
        ]
        client.get_klines.return_value = [
            [int(new_opened_at_utc.timestamp() * 1000), "100", "101", "95", "98", 0],
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

        self.assertEqual(summary["initialized"], 0)
        self.assertEqual(summary["updated"], 1)
        lock_state = self.store.get_lock_state(PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME)
        self.assertIsNotNone(lock_state)
        assert lock_state is not None
        monitor = lock_state["symbols"]["BTCUSDT"]
        self.assertEqual(monitor["opened_at_utc"], new_opened_at_utc.isoformat())
        self.assertEqual(monitor["lowest_price_since_open"], 95.0)
        self.assertFalse(monitor["eligible_reached"])
        self.assertIsNone(monitor["eligible_reached_at_utc"])

    def test_hourly_exchange_take_profit_closes_eligible_short_on_previous_closed_bullish_hour(self) -> None:
        position_id = self.store.insert_position(
            run_id=self.run_id,
            symbol="BTCUSDT",
            side="SHORT",
            qty=1.0,
            entry_price=100.0,
            liq_price_open=160.0,
            tp_price=None,
            sl_price=150.0,
            tp_order_id=None,
            sl_order_id=222,
            tp_client_order_id=None,
            sl_client_order_id="sl-old",
            opened_at_utc=datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc).isoformat(),
            expire_at_utc=(datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc) + timedelta(days=7)).isoformat(),
            status="OPEN",
        )
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
                    "markPrice": "83",
                    "positionSide": "BOTH",
                }
            ],
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "83",
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
            now_local=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            drop_pct=20.0,
        )

        self.assertEqual(result["closed_take_profit"], 1)
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["symbol"], "BTCUSDT")
        self.assertEqual(order_kwargs["side"], "BUY")
        self.assertTrue(order_kwargs["reduceOnly"])
        client.cancel_order.assert_called_once_with(
            symbol="BTCUSDT",
            order_id=222,
            orig_client_order_id="sl-old",
        )

        row = self._get_position(position_id)
        self.assertEqual(row["status"], "CLOSED_HOURLY_TAKE_PROFIT")
        self.assertEqual(row["close_reason"], "HOURLY_EXCHANGE_TAKE_PROFIT")
        self.assertEqual(row["close_order_id"], 5001)

        buy_fill = self._get_buy_fill(position_id)
        self.assertIsNotNone(buy_fill)
        assert buy_fill is not None
        self.assertEqual(buy_fill["order_id"], 5001)
        self.assertEqual(buy_fill["client_order_id"], "tp-hourly")

    def test_hourly_exchange_take_profit_skips_when_previous_closed_hour_is_bearish_even_if_current_hour_is_green(self) -> None:
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
                    "markPrice": "91",
                    "positionSide": "BOTH",
                }
            ],
            [
                {
                    "symbol": "BTCUSDT",
                    "positionAmt": "-1",
                    "entryPrice": "100",
                    "markPrice": "91",
                    "positionSide": "BOTH",
                }
            ],
        ]
        client.get_klines.side_effect = [
            [[0, "100", "101", "79", "80", 0]],
            [[0, "90", "92", "83", "84", 0]],
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 16, 1, 0, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "1",
                "side": "SELL",
            }
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
            now_local=datetime(2026, 3, 16, 10, 0, tzinfo=timezone.utc),
            drop_pct=20.0,
        )

        self.assertEqual(result["closed_take_profit"], 0)
        client.create_order.assert_not_called()

    def test_hourly_exchange_take_profit_closes_hedge_mode_short_with_position_side(self) -> None:
        self.store.set_lock_state(
            PositionManager.HOURLY_EXCHANGE_TP_LOCK_NAME,
            {
                "symbols": {
                    "MYXUSDT": {
                        "symbol": "MYXUSDT",
                        "position_amt": -7371.0,
                        "entry_price": 0.407,
                        "opened_at_utc": datetime(2026, 3, 15, 23, 50, 44, tzinfo=timezone.utc).isoformat(),
                        "lowest_price_since_open": 0.3231,
                        "eligible_reached": True,
                    }
                }
            },
        )

        client = MagicMock()
        client.get_position_risk.side_effect = [
            [
                {
                    "symbol": "MYXUSDT",
                    "positionAmt": "-7371",
                    "entryPrice": "0.407",
                    "markPrice": "0.3395",
                    "positionSide": "SHORT",
                }
            ],
            [
                {
                    "symbol": "MYXUSDT",
                    "positionAmt": "-7371",
                    "entryPrice": "0.407",
                    "markPrice": "0.3395",
                    "positionSide": "SHORT",
                }
            ],
        ]
        client.get_klines.side_effect = [
            [[0, "0.407", "0.410", "0.3231", "0.3300", 0]],
            [[0, "0.3385", "0.3400", "0.3300", "0.3395", 0]],
        ]
        client.get_user_trades.return_value = [
            {
                "time": int(datetime(2026, 3, 15, 23, 50, 44, tzinfo=timezone.utc).timestamp() * 1000),
                "qty": "7371",
                "side": "SELL",
            }
        ]
        client.format_order_qty.return_value = "7371"
        client.create_order.return_value = {
            "orderId": 9988,
            "clientOrderId": "tp-close-short",
            "type": "MARKET",
            "side": "BUY",
            "origQty": "7371",
            "status": "FILLED",
            "positionSide": "SHORT",
        }

        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            daily_loss_cut_scope="exchange",
        )

        result = manager.run_hourly_exchange_take_profit(
            now_local=datetime(2026, 3, 17, 0, 59, tzinfo=timezone.utc),
            drop_pct=20.0,
        )

        self.assertEqual(result["closed_take_profit"], 1)
        order_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(order_kwargs["side"], "BUY")
        self.assertEqual(order_kwargs["positionSide"], "SHORT")
        self.assertNotIn("reduceOnly", order_kwargs)

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

    def _get_buy_fill(self, position_id: int) -> Dict[str, Any] | None:
        with self.store._connect() as conn:  # pylint: disable=protected-access
            row = conn.execute(
                "SELECT * FROM fills WHERE position_id = ? AND side = 'BUY'",
                (position_id,),
            ).fetchone()
            return dict(row) if row is not None else None


if __name__ == "__main__":
    unittest.main()
