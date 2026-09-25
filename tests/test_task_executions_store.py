"""Unit tests for task_executions recording, querying, and listing in StateStore."""

import os
import tempfile
import pytest

from core.state_store import StateStore


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    schema_path = os.path.join(os.path.dirname(__file__), "..", "schema.sql")
    store = StateStore(db_path=db_path, schema_path=schema_path, account_id="acc01")
    store.init_schema()
    yield store
    if os.path.exists(db_path):
        os.remove(db_path)


class TestTaskExecutionsStore:
    def test_record_and_get_latest_for_account(self, store: StateStore):
        # Initial empty state
        latest = store.get_latest_task_executions(account_id="acc01")
        assert latest == {}

        # Record entry execution
        row_id1 = store.record_task_execution(
            task_name="entry",
            status="SUCCESS",
            summary="opened=10 failed=0 skipped=0",
            payload={"opened": 10},
            task_cycle="2026-03-01",
            time_local="2026-03-01 07:30:00",
            account_id="acc01",
        )
        assert row_id1 > 0

        # Record daily_loss_cut execution
        row_id2 = store.record_task_execution(
            task_name="daily_loss_cut",
            status="SUCCESS",
            summary="total=1 closed=0 errors=0",
            payload={"total": 1},
            task_cycle="2026-03-01",
            time_local="2026-03-01 08:00:00",
            account_id="acc01",
        )
        assert row_id2 > row_id1

        # Query latest for acc01
        latest = store.get_latest_task_executions(account_id="acc01")
        assert len(latest) == 2
        assert latest["entry"]["status"] == "SUCCESS"
        assert latest["entry"]["summary"] == "opened=10 failed=0 skipped=0"
        assert latest["entry"]["payload"] == {"opened": 10}
        assert latest["daily_loss_cut"]["status"] == "SUCCESS"

        # Update entry with a newer execution
        store.record_task_execution(
            task_name="entry",
            status="PARTIAL",
            summary="opened=5 failed=1 skipped=0",
            payload={"opened": 5, "failed": 1},
            task_cycle="2026-03-02",
            time_local="2026-03-02 07:30:00",
            account_id="acc01",
        )
        latest_after = store.get_latest_task_executions(account_id="acc01")
        assert latest_after["entry"]["status"] == "PARTIAL"
        assert latest_after["entry"]["summary"] == "opened=5 failed=1 skipped=0"

    def test_multi_account_grouped_query(self, store: StateStore):
        store.record_task_execution(
            task_name="entry",
            status="SUCCESS",
            summary="opened=1",
            account_id="acc01",
        )
        store.record_task_execution(
            task_name="entry",
            status="FAILED",
            summary="opened=0 failed=1",
            account_id="acc02",
        )
        store.record_task_execution(
            task_name="manage",
            status="SUCCESS",
            summary="ok",
            account_id="acc02",
        )

        all_latest = store.get_latest_task_executions(account_id=None)
        assert "acc01" in all_latest
        assert "acc02" in all_latest
        assert all_latest["acc01"]["entry"]["status"] == "SUCCESS"
        assert all_latest["acc02"]["entry"]["status"] == "FAILED"
        assert all_latest["acc02"]["manage"]["status"] == "SUCCESS"

    def test_list_task_executions(self, store: StateStore):
        store.record_task_execution(task_name="manage", status="SUCCESS", summary="r1", account_id="acc01")
        store.record_task_execution(task_name="manage", status="SUCCESS", summary="r2", account_id="acc01")
        store.record_task_execution(task_name="manage", status="SUCCESS", summary="r3", account_id="acc02")

        history_acc01 = store.list_task_executions(account_id="acc01", limit=10)
        assert len(history_acc01) == 2
        assert history_acc01[0]["summary"] == "r2"
        assert history_acc01[1]["summary"] == "r1"

        history_all = store.list_task_executions(limit=10)
        assert len(history_all) == 3
