import json
import sqlite3
from pathlib import Path

from core.state_store import StateStore


SCHEMA = Path(__file__).resolve().parents[1] / "schema.sql"


def test_order_ids_are_scoped_by_symbol_and_order_kind(tmp_path):
    store = StateStore(str(tmp_path / "state.db"), str(SCHEMA), account_id="acc01")
    store.init_schema()
    store.upsert_exchange_order_state({"symbol": "BTCUSDT", "orderId": 7, "status": "FILLED", "clientOrderId": "normal"}, source="STREAM")
    store.upsert_exchange_order_state({"symbol": "ETHUSDT", "orderId": 7, "status": "NEW"}, source="STREAM")
    store.upsert_exchange_order_state({"symbol": "BTCUSDT", "algoId": 7, "orderId": 7, "clientAlgoId": "algo", "status": "NEW"}, source="STREAM")
    assert len(store.list_exchange_order_state()) == 3
    assert store.get_exchange_order_state(symbol="BTCUSDT", order_id=7, order_kind="order")["status"] == "FILLED"
    assert store.get_exchange_order_state(symbol="BTCUSDT", order_id=7, order_kind="algo")["status"] == "NEW"
    assert store.get_exchange_order_state(symbol="BTCUSDT", order_id=7, client_order_id="algo")["client_order_id"] == "algo"
    store.reconcile_open_order_state([{"symbol": "ETHUSDT", "orderId": 7, "status": "NEW"}, {"symbol": "BTCUSDT", "algoId": 7, "clientAlgoId": "algo", "status": "NEW"}])
    assert store.get_exchange_order_state(symbol="BTCUSDT", order_id=7, order_kind="algo")["status"] == "NEW"
    assert store.get_exchange_order_state(symbol="ETHUSDT", order_id=7)["status"] == "NEW"


def test_actual_order_links_only_to_parent_of_same_symbol(tmp_path):
    store = StateStore(str(tmp_path / "state.db"), str(SCHEMA), account_id="acc01")
    store.init_schema()
    for symbol in ("BTCUSDT", "ETHUSDT"):
        store.upsert_exchange_order_state({"symbol": symbol, "algoId": 10, "actualOrderId": 99, "status": "TRIGGERED"}, source="ALGO_UPDATE")
    store.reconcile_open_order_state([{"symbol": "BTCUSDT", "orderId": 99, "status": "NEW"}])
    assert store.get_exchange_order_state(symbol="BTCUSDT", order_id=10)["status"] == "NEW"
    assert store.get_exchange_order_state(symbol="ETHUSDT", order_id=10)["status"] == "MISSING"
    store.update_parent_algo_order_status(symbol="BTCUSDT", actual_order_id=99, status="FILLED", event_time_utc="2026-01-01")
    assert store.get_exchange_order_state(symbol="BTCUSDT", order_id=10)["status"] == "FILLED"
    assert store.get_exchange_order_state(symbol="ETHUSDT", order_id=10)["status"] == "MISSING"


def test_legacy_order_keys_are_migrated_once_without_losing_rows(tmp_path):
    db = tmp_path / "state.db"
    store = StateStore(str(db), str(SCHEMA), account_id="acc01")
    store.init_schema()
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM schema_migrations WHERE version = ?", ("20261028_01_exchange_order_identity",))
        for symbol, order_id, source, raw in (
            ("BTCUSDT", "1", "ORDER_TRADE_UPDATE", {"symbol": "BTCUSDT", "orderId": 1}),
            ("ETHUSDT", "2", "ALGO_UPDATE", {"s": "ETHUSDT", "aid": 2}),
        ):
            conn.execute("INSERT INTO exchange_order_state (account_id, order_key, symbol, order_id, status, event_time_utc, source, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                         ("acc01", "id:" + order_id, symbol, order_id, "NEW", "2026-01-01", source, json.dumps(raw)))
    store.init_schema()
    store.init_schema()
    assert len(store.list_exchange_order_state()) == 2
    store.upsert_exchange_order_state({"symbol": "BTCUSDT", "orderId": 1, "status": "FILLED"}, source="STREAM", event_time_utc="2026-01-02")
    store.upsert_exchange_order_state({"symbol": "ETHUSDT", "algoId": 2, "status": "FILLED"}, source="STREAM", event_time_utc="2026-01-02")
    assert len(store.list_exchange_order_state()) == 2
    assert store.get_exchange_order_state(symbol="ETHUSDT", order_id=2, order_kind="algo")["status"] == "FILLED"
