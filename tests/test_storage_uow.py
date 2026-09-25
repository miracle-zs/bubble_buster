"""Unit tests for Unit of Work (UoW) and dual-access storage models."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict

from core.state_store import StateStore
from core.storage.models import (
    DualAccessRecord,
    FillRecord,
    OrderEventRecord,
    PositionRecord,
    PositionState,
    RunRecord,
    RunState,
    TaskExecutionRecord,
    WalletSnapshotRecord,
)
from core.storage.uow import UnitOfWork, get_current_uow


class DualAccessModelsTest(unittest.TestCase):
    def test_dual_access_record_attributes_and_dict_keys(self) -> None:
        rec = DualAccessRecord(symbol="ETHUSDT", price=3000.0)
        # Attribute access
        self.assertEqual(rec.symbol, "ETHUSDT")
        self.assertEqual(rec.price, 3000.0)
        # Dict access
        self.assertEqual(rec["symbol"], "ETHUSDT")
        self.assertEqual(rec["price"], 3000.0)
        self.assertEqual(rec.get("symbol"), "ETHUSDT")
        self.assertIsNone(rec.get("missing"))
        self.assertEqual(rec.get("missing", 99), 99)
        # Mutation
        rec.price = 3050.0
        self.assertEqual(rec["price"], 3050.0)
        rec["status"] = "ACTIVE"
        self.assertEqual(rec.status, "ACTIVE")
        # Dict conversion & isinstance
        self.assertTrue(isinstance(rec, dict))
        self.assertEqual(dict(rec), {"symbol": "ETHUSDT", "price": 3050.0, "status": "ACTIVE"})
        self.assertEqual(rec.to_dict(), {"symbol": "ETHUSDT", "price": 3050.0, "status": "ACTIVE"})

    def test_position_record_and_position_state_compatibility(self) -> None:
        # Legacy positional instantiation
        pos = PositionState(
            1, "run-100", "BTCUSDT", "SHORT", 0.5, 50000.0,
            60000.0, 59500.0, 45000.0, 55000.0,
            11, 22, "2026-07-18T00:00:00Z", "2026-07-19T00:00:00Z",
            None, "OPEN", None,
        )
        self.assertEqual(pos.id, 1)
        self.assertEqual(pos.symbol, "BTCUSDT")
        self.assertEqual(pos["symbol"], "BTCUSDT")
        self.assertEqual(pos["qty"], 0.5)
        self.assertEqual(pos.entry_price, 50000.0)
        self.assertTrue(pos.is_open)
        self.assertTrue(pos.is_active)
        self.assertFalse(pos.is_closed)

        # Dict instantiation
        pos2 = PositionRecord({
            "id": 2,
            "run_id": "run-101",
            "symbol": "SOLUSDT",
            "status": "CLOSED_TP",
        })
        self.assertEqual(pos2.id, 2)
        self.assertEqual(pos2.symbol, "SOLUSDT")
        self.assertTrue(pos2.is_closed)
        self.assertFalse(pos2.is_open)

    def test_run_record_and_run_state_compatibility(self) -> None:
        # Legacy positional instantiation
        run = RunState(
            "run-200", "acc01", "2026-07-18", "2026-07-18T00:00:00Z",
            None, "RUNNING", "Started ok",
        )
        self.assertEqual(run.run_id, "run-200")
        self.assertEqual(run["account_id"], "acc01")
        self.assertEqual(run.reason, "Started ok")
        self.assertEqual(run.message, "Started ok")
        self.assertEqual(run["reason"], "Started ok")
        self.assertEqual(run["message"], "Started ok")
        self.assertTrue(run.is_running)
        self.assertFalse(run.is_success)

        # DB row compatibility with 'message' column
        db_row = {
            "run_id": "run-201",
            "account_id": "acc02",
            "trade_day_utc": "2026-07-18",
            "started_at_utc": "t1",
            "completed_at_utc": "t2",
            "status": "SUCCESS",
            "message": "All symbols processed",
        }
        run2 = RunRecord.from_row(db_row)
        self.assertIsNotNone(run2)
        self.assertEqual(run2.run_id, "run-201")
        self.assertEqual(run2.reason, "All symbols processed")
        self.assertEqual(run2.message, "All symbols processed")
        self.assertTrue(run2.is_success)

    def test_order_event_and_fill_record_payloads(self) -> None:
        order_event = OrderEventRecord(
            id=10,
            symbol="BTCUSDT",
            order_id=12345,
            raw_json='{"orderId": 12345, "status": "FILLED", "avgPrice": "50000"}',
        )
        self.assertEqual(order_event.symbol, "BTCUSDT")
        self.assertEqual(order_event["order_id"], 12345)
        payload = order_event.parsed_payload()
        self.assertEqual(payload.get("status"), "FILLED")
        self.assertEqual(payload.get("avgPrice"), "50000")

        fill = FillRecord(
            id=1,
            order_event_id=10,
            symbol="BTCUSDT",
            executed_qty=0.5,
            avg_price=50000.0,
            raw_json='{"commission": 0.05}',
        )
        self.assertEqual(fill.executed_qty, 0.5)
        self.assertEqual(fill.parsed_payload().get("commission"), 0.05)


class UnitOfWorkTransactionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(db_path=self.db_path, schema_path=schema_path)
        self.store.init_schema()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_unit_of_work_commits_multiple_operations_atomically(self) -> None:
        run_id, _ = self.store.create_run("2026-07-18")

        with self.store.unit_of_work() as uow:
            self.assertTrue(uow.is_active)
            self.assertFalse(uow.is_nested)

            # Insert position inside UoW
            pos_id = self.store.insert_position(
                run_id=run_id,
                symbol="BTCUSDT",
                side="SHORT",
                qty=0.1,
                entry_price=60000.0,
                liq_price_open=70000.0,
                tp_price=50000.0,
                sl_price=65000.0,
                tp_order_id=101,
                sl_order_id=102,
                tp_client_order_id="tp-1",
                sl_client_order_id="sl-1",
                opened_at_utc="2026-07-18T00:00:00Z",
                expire_at_utc="2026-07-19T00:00:00Z",
            )
            # Add order event inside UoW
            event_id = self.store.add_order_event(
                symbol="BTCUSDT",
                position_id=pos_id,
                event_time_utc="2026-07-18T00:00:01Z",
                order_payload={
                    "orderId": 102,
                    "clientOrderId": "sl-1",
                    "status": "NEW",
                    "type": "STOP_MARKET",
                    "side": "BUY",
                    "price": "65000",
                },
            )

        # After block exit, transaction is committed
        pos = self.store.get_position(pos_id)
        self.assertIsNotNone(pos)
        self.assertEqual(pos.symbol, "BTCUSDT")
        self.assertEqual(pos.status, "OPEN")

        event = self.store.get_order_event(event_id)
        self.assertIsNotNone(event)
        self.assertEqual(event.symbol, "BTCUSDT")
        self.assertEqual(event.position_id, pos_id)

    def test_unit_of_work_rolls_back_all_operations_on_exception(self) -> None:
        run_id, _ = self.store.create_run("2026-07-18")

        pos_id = None
        try:
            with self.store.unit_of_work() as uow:
                pos_id = self.store.insert_position(
                    run_id=run_id,
                    symbol="ETHUSDT",
                    side="SHORT",
                    qty=1.0,
                    entry_price=3000.0,
                    liq_price_open=4000.0,
                    tp_price=2500.0,
                    sl_price=3500.0,
                    tp_order_id=201,
                    sl_order_id=202,
                    tp_client_order_id="tp-2",
                    sl_client_order_id="sl-2",
                    opened_at_utc="2026-07-18T00:00:00Z",
                    expire_at_utc="2026-07-19T00:00:00Z",
                )
                self.store.add_order_event(
                    symbol="ETHUSDT",
                    position_id=pos_id,
                    event_time_utc="2026-07-18T00:00:01Z",
                    order_payload={"orderId": 201, "status": "NEW"},
                )
                # Simulate mid-workflow failure
                raise RuntimeError("Simulated mid-transaction failure")
        except RuntimeError:
            pass

        # Since it raised, the entire transaction must have rolled back
        self.assertIsNotNone(pos_id)
        self.assertIsNone(self.store.get_position(pos_id))
        self.assertNotIn("ETHUSDT", self.store.list_open_symbols())
        self.assertEqual(self.store.list_order_events_for_position(pos_id), [])

    def test_nested_unit_of_work_savepoints(self) -> None:
        run_id, _ = self.store.create_run("2026-07-18")

        with self.store.unit_of_work() as outer_uow:
            pos1_id = self.store.insert_position(
                run_id=run_id,
                symbol="BTCUSDT",
                side="SHORT",
                qty=0.1,
                entry_price=60000.0,
                liq_price_open=70000.0,
                tp_price=50000.0,
                sl_price=65000.0,
                tp_order_id=1,
                sl_order_id=2,
                tp_client_order_id="t1",
                sl_client_order_id="s1",
                opened_at_utc="2026-07-18T00:00:00Z",
                expire_at_utc="2026-07-19T00:00:00Z",
            )

            # Nested UoW fails and rolls back to savepoint
            try:
                with self.store.unit_of_work() as inner_uow:
                    self.assertTrue(inner_uow.is_nested)
                    self.assertIsNotNone(inner_uow.savepoint_name)
                    pos2_id = self.store.insert_position(
                        run_id=run_id,
                        symbol="ETHUSDT",
                        side="SHORT",
                        qty=1.0,
                        entry_price=3000.0,
                        liq_price_open=4000.0,
                        tp_price=2500.0,
                        sl_price=3500.0,
                        tp_order_id=3,
                        sl_order_id=4,
                        tp_client_order_id="t2",
                        sl_client_order_id="s2",
                        opened_at_utc="2026-07-18T00:00:00Z",
                        expire_at_utc="2026-07-19T00:00:00Z",
                    )
                    raise ValueError("Inner failure to trigger savepoint rollback")
            except ValueError:
                pass

            # Outer continues and adds another position
            pos3_id = self.store.insert_position(
                run_id=run_id,
                symbol="SOLUSDT",
                side="SHORT",
                qty=10.0,
                entry_price=150.0,
                liq_price_open=200.0,
                tp_price=100.0,
                sl_price=180.0,
                tp_order_id=5,
                sl_order_id=6,
                tp_client_order_id="t3",
                sl_client_order_id="s3",
                opened_at_utc="2026-07-18T00:00:00Z",
                expire_at_utc="2026-07-19T00:00:00Z",
            )

        # BTC and SOL were committed; ETH was rolled back by savepoint
        symbols = self.store.list_open_symbols()
        self.assertIn("BTCUSDT", symbols)
        self.assertIn("SOLUSDT", symbols)
        self.assertNotIn("ETHUSDT", symbols)

    def test_after_commit_and_after_rollback_callbacks(self) -> None:
        commit_calls = []
        rollback_calls = []

        # Successful transaction
        with self.store.unit_of_work() as uow:
            uow.add_after_commit(lambda: commit_calls.append("committed"))
            uow.add_after_rollback(lambda: rollback_calls.append("rolled_back"))

        self.assertEqual(commit_calls, ["committed"])
        self.assertEqual(rollback_calls, [])

        # Failed transaction
        try:
            with self.store.unit_of_work() as uow:
                uow.add_after_commit(lambda: commit_calls.append("committed2"))
                uow.add_after_rollback(lambda: rollback_calls.append("rolled_back2"))
                raise RuntimeError("Boom")
        except RuntimeError:
            pass

        self.assertEqual(commit_calls, ["committed"])
        self.assertEqual(rollback_calls, ["rolled_back2"])

    def test_atomic_order_event_and_fills_integrity(self) -> None:
        run_id, _ = self.store.create_run("2026-07-18")
        pos_id = self.store.insert_position(
            run_id=run_id,
            symbol="BTCUSDT",
            side="SHORT",
            qty=0.5,
            entry_price=50000.0,
            liq_price_open=60000.0,
            tp_price=40000.0,
            sl_price=55000.0,
            tp_order_id=None,
            sl_order_id=None,
            tp_client_order_id=None,
            sl_client_order_id=None,
            opened_at_utc="2026-07-18T00:00:00Z",
            expire_at_utc="2026-07-19T00:00:00Z",
        )

        with self.store.unit_of_work():
            event_id = self.store.add_order_event(
                symbol="BTCUSDT",
                position_id=pos_id,
                event_time_utc="2026-07-18T00:01:00Z",
                order_payload={
                    "orderId": 9999,
                    "clientOrderId": "close-btc",
                    "status": "FILLED",
                    "side": "BUY",
                    "executedQty": "0.5",
                    "avgPrice": "48000.0",
                    "cumQuote": "24000.0",
                    "realizedPnl": "1000.0",
                    "commission": "12.0",
                    "commissionAsset": "USDT",
                },
            )
            self.store.mark_position_closed(
                position_id=pos_id,
                status="CLOSED_TP",
                close_reason="TAKE_PROFIT_FILLED",
                close_order_id=9999,
            )

        # Verify all 3 tables (positions, order_events, fills) updated atomically
        pos = self.store.get_position(pos_id)
        self.assertEqual(pos.status, "CLOSED_TP")
        self.assertEqual(pos.close_order_id, 9999)

        events = self.store.list_order_events_for_position(pos_id)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].id, event_id)

        fill = self.store.get_fill_by_order_event_id(event_id)
        self.assertIsNotNone(fill)
        self.assertEqual(fill.executed_qty, 0.5)
        self.assertEqual(fill.avg_price, 48000.0)
        self.assertEqual(fill.realized_pnl, 1000.0)
        self.assertEqual(fill.commission, 12.0)

        fills = self.store.list_fills_for_position(pos_id)
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].id, fill.id)
