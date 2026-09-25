"""Regression tests based on the architecture audit probe.

Verifies:
1. Terminal order states are not resurrected by older trade updates.
2. Terminal order states are not overwritten by stale REST openOrders reconciliations.
3. Stream updates during REST account fetches survive in the database.
4. Cashflow sync waits do not block protection and manage dispatch in the main cycle.
"""
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone
from threading import Event, Thread
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
from core.state_store import StateStore
from core.account_snapshot import AccountSnapshotProvider
from core.runtime_service import ServiceRuntimeConfig, StrategyRuntimeService


class AuditProbeRegressionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = StateStore(
            str(Path(self.tmp.name) / "state.db"),
            str(ROOT / "schema.sql"),
            account_id="audit",
        )
        self.store.init_schema()

    def test_old_order_event_must_not_resurrect_filled_order(self) -> None:
        order = dict(symbol="TESTUSDT", orderId=123, type="LIMIT", side="BUY")
        self.store.upsert_exchange_order_state(
            dict(order, status="FILLED", executedQty="1"),
            source="ORDER_TRADE_UPDATE",
            event_time_utc="2026-09-22T10:00:02+00:00",
        )
        self.store.upsert_exchange_order_state(
            dict(order, status="NEW", executedQty="0"),
            source="ORDER_TRADE_UPDATE",
            event_time_utc="2026-09-22T10:00:01+00:00",
        )
        row = self.store.get_exchange_order_state(symbol="TESTUSDT", order_id=123)
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "FILLED", str(row))

    def test_stream_update_during_rest_must_survive_in_database(self) -> None:
        entered, release, applied = Event(), Event(), Event()
        errors = []
        client = MagicMock()
        payload = dict(
            assets=[dict(asset="USDT", walletBalance="100", unrealizedProfit="0", marginBalance="100")],
            positions=[],
        )
        client.get_account.return_value = payload
        provider = AccountSnapshotProvider(client, self.store, account_id="audit")
        provider.capture(now_utc=datetime(2026, 9, 22, 10, 0, tzinfo=timezone.utc))

        def blocked_rest():
            entered.set()
            if not release.wait(3):
                raise TimeoutError("probe cleanup deadline")
            return payload

        client.get_account.side_effect = blocked_rest
        original_apply = self.store.apply_account_stream_update

        def observed_apply(**kwargs):
            original_apply(**kwargs)
            applied.set()

        self.store.apply_account_stream_update = observed_apply

        def rest():
            try:
                provider.capture(force=True, now_utc=datetime(2026, 9, 22, 10, 1, tzinfo=timezone.utc))
            except Exception as exc:
                errors.append(exc)

        def stream():
            try:
                provider.apply_stream_update(
                    balances=[dict(a="USDT", wb="200")],
                    positions=[],
                    captured_at_utc="2026-09-22T10:01:01+00:00",
                )
            except Exception as exc:
                errors.append(exc)

        a, b = Thread(target=rest), Thread(target=stream)
        a.start()
        try:
            self.assertTrue(entered.wait(2))
            b.start()
            self.assertTrue(applied.wait(2))
        finally:
            release.set()
            a.join(3)
            if b.ident is not None:
                b.join(3)

        self.assertEqual(errors, [])
        state = self.store.get_latest_account_state()
        self.assertIsNotNone(state)
        self.assertEqual(state["wallet_balance"], 200)

    def test_stale_rest_open_orders_must_not_resurrect_filled_order(self) -> None:
        order = dict(symbol="TESTUSDT", orderId=123, type="LIMIT", side="BUY")
        self.store.upsert_exchange_order_state(
            dict(order, status="FILLED", executedQty="1"),
            source="ORDER_TRADE_UPDATE",
            event_time_utc="2026-09-22T10:00:02+00:00",
        )
        self.store.reconcile_open_order_state([dict(order, status="NEW", executedQty="0")])
        row = self.store.get_exchange_order_state(symbol="TESTUSDT", order_id=123)
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "FILLED")

    def test_cashflow_wait_must_not_block_protection_dispatch(self) -> None:
        entered, release, protection = Event(), Event(), Event()
        sampler = MagicMock(sync_cashflows=True)

        def sync(**kwargs):
            entered.set()
            release.wait(3)
            return 0

        sampler.sync_cashflows_once.side_effect = sync
        cfg = ServiceRuntimeConfig(
            "UTC", 7, 40, 120, True, True, 11, 55, 60, 3, 1, False,
            account_task_timeout_sec=0.1,
        )
        service = StrategyRuntimeService(
            MagicMock(),
            MagicMock(),
            cfg,
            account_runtimes={"audit": dict(mode="full", balance_sampler=sampler)},
        )
        for method in [
            "_run_entry_if_due",
            "_run_noon_protection_if_due",
            "_run_morning_protection_if_due",
            "_run_hourly_exchange_take_profit_if_due",
            "_run_orphan_exit_order_cleanup_if_due",
            "_run_manage_if_due",
            "_run_balance_snapshot_for_readonly_accounts",
        ]:
            setattr(service, method, MagicMock())

        service._run_daily_loss_cut_if_due = lambda now: protection.set()
        worker = Thread(
            target=service.run_cycle,
            kwargs={"now_local": datetime(2026, 9, 22, 11, 55, 5, tzinfo=timezone.utc)},
        )
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            dispatched = protection.wait(0.25)
        finally:
            release.set()
            worker.join(3)
            for executor in [service._entry_executor, service._manage_executor, service._scheduled_executor]:
                executor.shutdown(wait=True, cancel_futures=True)

        self.assertTrue(dispatched, "Protection dispatch blocked behind cashflow I/O despite 0.1s task timeout")


if __name__ == "__main__":
    unittest.main(verbosity=2)
