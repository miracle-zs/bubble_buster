import importlib.util
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict
from unittest.mock import MagicMock

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
