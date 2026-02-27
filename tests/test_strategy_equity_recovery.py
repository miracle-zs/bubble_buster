import importlib.util
import sys
import types
import unittest
from unittest.mock import MagicMock

if importlib.util.find_spec("requests") is None:
    requests_stub = types.ModuleType("requests")

    class _DummySession:
        def __init__(self):
            self.headers = {}
            self.proxies = {}

        def mount(self, *_args, **_kwargs):
            return None

    class _DummyRequestException(Exception):
        pass

    requests_stub.Session = _DummySession
    requests_stub.RequestException = _DummyRequestException

    adapters_stub = types.ModuleType("requests.adapters")

    class _DummyHTTPAdapter:
        def __init__(self, *args, **kwargs):
            pass

    adapters_stub.HTTPAdapter = _DummyHTTPAdapter
    requests_stub.adapters = adapters_stub
    sys.modules["requests"] = requests_stub
    sys.modules["requests.adapters"] = adapters_stub

from core.strategy_top10_short import Top10ShortStrategy


class StrategyEquityRecoveryTest(unittest.TestCase):
    def _build_strategy(self, client: MagicMock, store: MagicMock) -> Top10ShortStrategy:
        client.get_position_risk.return_value = []
        return Top10ShortStrategy(
            client=client,
            store=store,
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
            equity_recovery_take_profit_enabled=True,
            equity_recovery_lookback_hours=24.0,
            equity_recovery_trigger_pct=0.10,
            equity_recovery_reduce_ratio=0.5,
        )

    @staticmethod
    def _mock_order_factory():
        state = {"seq": 0}

        def _create_order(**kwargs):
            state["seq"] += 1
            return {
                "orderId": 3000 + state["seq"],
                "symbol": kwargs["symbol"],
                "side": kwargs["side"],
                "type": kwargs["type"],
                "origQty": kwargs["quantity"],
                "status": "FILLED",
            }

        return _create_order

    def test_equity_recovery_triggers_once_per_cycle(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.get_latest_wallet_snapshot.return_value = {
            "captured_at_utc": "2026-02-23T07:40:00+00:00",
            "balance_usdt": 990.0,
        }
        store.get_wallet_snapshot_min_since.return_value = {
            "captured_at_utc": "2026-02-23T01:00:00+00:00",
            "balance_usdt": 900.0,
        }
        store.get_lock_state.side_effect = [
            None,
            {
                "cycle_key": "2026-02-23T07:40:00+00:00",
                "triggered": True,
                "window_start_utc": "2026-02-23T07:40:00+00:00",
            },
        ]
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0},
            {"id": 2, "symbol": "BUSDT", "entry_price": 20.0},
        ]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-5", "markPrice": "20", "entryPrice": "20"},
            ]
        )
        strategy._sync_position_after_adjustment = MagicMock(return_value=True)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        first = strategy.run_equity_recovery_take_profit()
        second = strategy.run_equity_recovery_take_profit()

        self.assertEqual(first["status"], "TRIGGERED")
        self.assertEqual(first["adjusted"], 2)
        self.assertEqual(first["cycle_key"], "2026-02-23T07:40:00+00:00")
        self.assertEqual(client.create_order.call_count, 2)
        self.assertEqual(second["status"], "SKIPPED")
        self.assertEqual(second["reason"], "ALREADY_TRIGGERED_IN_CYCLE")

    def test_equity_recovery_resets_window_start_at_trigger_time(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.get_latest_wallet_snapshot.side_effect = [
            {"captured_at_utc": "2026-02-23T07:40:00+00:00", "balance_usdt": 990.0},
            {"captured_at_utc": "2026-02-23T07:41:00+00:00", "balance_usdt": 992.0},
        ]
        store.get_wallet_snapshot_min_since.side_effect = [
            {"captured_at_utc": "2026-02-23T01:00:00+00:00", "balance_usdt": 900.0},
            {"captured_at_utc": "2026-02-23T07:40:00+00:00", "balance_usdt": 990.0},
        ]
        store.get_lock_state.side_effect = [
            None,
            {
                "cycle_key": "2026-02-23T07:40:00+00:00",
                "triggered": True,
                "window_start_utc": "2026-02-23T07:40:00+00:00",
            },
        ]
        store.list_open_positions.return_value = [{"id": 1, "symbol": "AUSDT", "entry_price": 10.0}]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            return_value={"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"}
        )
        strategy._sync_position_after_adjustment = MagicMock(return_value=True)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        first = strategy.run_equity_recovery_take_profit()
        second = strategy.run_equity_recovery_take_profit()

        self.assertEqual(first["status"], "TRIGGERED")
        self.assertEqual(second["status"], "SKIPPED")
        first_call = store.get_wallet_snapshot_min_since.call_args_list[0].kwargs
        second_call = store.get_wallet_snapshot_min_since.call_args_list[1].kwargs
        self.assertNotEqual(first_call["start_captured_at_utc"], "2026-02-23T07:40:00+00:00")
        self.assertEqual(second_call["start_captured_at_utc"], "2026-02-23T07:40:00+00:00")

    def test_equity_recovery_does_not_lock_cycle_when_partial_failures_exist(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = [
            {"orderId": 3101, "symbol": "AUSDT", "side": "BUY", "type": "MARKET", "origQty": "5", "status": "FILLED"},
            RuntimeError("temporary api failure"),
        ]

        store = MagicMock()
        store.get_latest_wallet_snapshot.return_value = {
            "captured_at_utc": "2026-02-23T07:40:00+00:00",
            "balance_usdt": 990.0,
        }
        store.get_wallet_snapshot_min_since.return_value = {
            "captured_at_utc": "2026-02-23T01:00:00+00:00",
            "balance_usdt": 900.0,
        }
        store.get_lock_state.return_value = None
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0},
            {"id": 2, "symbol": "BUSDT", "entry_price": 10.0},
        ]

        strategy = self._build_strategy(client, store)
        strategy._refresh_exit_orders_for_positions = MagicMock()
        strategy._sync_positions_after_adjustment_bulk = MagicMock(return_value={1})
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
            ]
        )

        result = strategy.run_equity_recovery_take_profit()

        self.assertEqual(result["status"], "PARTIAL")
        last_lock = store.set_lock_state.call_args_list[-1].args[1]
        self.assertFalse(last_lock["triggered"])
        self.assertEqual(last_lock["window_start_utc"], "2026-02-22T07:40:00+00:00")

    def test_equity_recovery_updates_anchor_when_no_open_positions(self) -> None:
        client = MagicMock()
        store = MagicMock()
        store.get_latest_wallet_snapshot.return_value = {
            "captured_at_utc": "2026-02-23T07:40:00+00:00",
            "balance_usdt": 990.0,
        }
        store.get_wallet_snapshot_min_since.return_value = {
            "captured_at_utc": "2026-02-23T01:00:00+00:00",
            "balance_usdt": 900.0,
        }
        store.get_lock_state.return_value = None
        store.list_open_positions.return_value = []

        strategy = self._build_strategy(client, store)
        result = strategy.run_equity_recovery_take_profit()

        self.assertEqual(result["status"], "SKIPPED")
        self.assertEqual(result["reason"], "NO_OPEN_POSITIONS")
        lock_payload = store.set_lock_state.call_args.args[1]
        self.assertEqual(lock_payload["window_start_utc"], "2026-02-23T07:40:00+00:00")

    def test_equity_recovery_triggers_when_equity_equals_threshold_with_float_rounding(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()
        client.get_position_risk.return_value = [{"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10"}]

        store = MagicMock()
        store.get_latest_wallet_snapshot.return_value = {
            "captured_at_utc": "2026-02-23T07:40:00+00:00",
            "balance_usdt": 990.0,
        }
        store.get_wallet_snapshot_min_since.return_value = {
            "captured_at_utc": "2026-02-23T01:00:00+00:00",
            "balance_usdt": 900.0,
        }
        store.get_lock_state.return_value = None
        store.list_open_positions.return_value = [{"id": 1, "symbol": "AUSDT", "entry_price": 10.0}]

        strategy = self._build_strategy(client, store)
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
        ]
        strategy._refresh_exit_orders_for_positions = MagicMock()

        result = strategy.run_equity_recovery_take_profit()
        self.assertEqual(result["status"], "TRIGGERED")

    def test_equity_recovery_prefers_batch_risk_rows_over_per_symbol_fetch(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
            {"symbol": "BUSDT", "positionAmt": "-5", "markPrice": "20", "entryPrice": "20"},
        ]

        store = MagicMock()
        store.get_latest_wallet_snapshot.return_value = {
            "captured_at_utc": "2026-02-23T07:40:00+00:00",
            "balance_usdt": 1000.0,
        }
        store.get_wallet_snapshot_min_since.return_value = {
            "captured_at_utc": "2026-02-23T01:00:00+00:00",
            "balance_usdt": 900.0,
        }
        store.get_lock_state.return_value = None
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0},
            {"id": 2, "symbol": "BUSDT", "entry_price": 20.0},
        ]

        strategy = self._build_strategy(client, store)
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
            {"symbol": "BUSDT", "positionAmt": "-5", "markPrice": "20", "entryPrice": "20"},
        ]
        strategy._load_short_position = MagicMock()
        strategy._sync_positions_after_adjustment_bulk = MagicMock(return_value={1, 2})
        strategy._refresh_exit_orders_for_positions = MagicMock()

        result = strategy.run_equity_recovery_take_profit()

        self.assertEqual(result["status"], "TRIGGERED")
        strategy._load_short_position.assert_not_called()
        self.assertEqual(client.get_position_risk.call_count, 1)

    def test_equity_recovery_uses_position_qty_path_instead_of_notional_conversion(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.side_effect = RuntimeError("should not be called")
        client.format_order_qty.side_effect = lambda _s, qty: f"{qty:.3f}"
        client.create_order.side_effect = self._mock_order_factory()
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
        ]
        client.get_symbol_rules.return_value = {"AUSDT": types.SimpleNamespace(min_qty=0.001, min_notional=5.0)}

        store = MagicMock()
        store.get_latest_wallet_snapshot.return_value = {
            "captured_at_utc": "2026-02-23T07:40:00+00:00",
            "balance_usdt": 1000.0,
        }
        store.get_wallet_snapshot_min_since.return_value = {
            "captured_at_utc": "2026-02-23T01:00:00+00:00",
            "balance_usdt": 900.0,
        }
        store.get_lock_state.return_value = None
        store.list_open_positions.return_value = [{"id": 1, "symbol": "AUSDT", "entry_price": 10.0}]

        strategy = self._build_strategy(client, store)
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10"},
        ]
        strategy._refresh_exit_orders_for_positions = MagicMock()

        result = strategy.run_equity_recovery_take_profit()

        self.assertEqual(result["status"], "TRIGGERED")
        self.assertEqual(client.create_order.call_count, 1)
        client.normalize_order_qty.assert_not_called()
        call_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(call_kwargs["quantity"], "5.000")


if __name__ == "__main__":
    unittest.main()
