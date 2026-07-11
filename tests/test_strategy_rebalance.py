import importlib.util
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

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
from infra.binance_futures_client import BinanceAPIError


class StrategyRebalanceTest(unittest.TestCase):
    def _build_strategy(self, client: MagicMock, store: MagicMock, **overrides) -> Top10ShortStrategy:
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
            rebalance_enabled=overrides.get("rebalance_enabled", True),
            rebalance_pre_entry_reduce=True,
            rebalance_after_entry=True,
            rebalance_utilization=overrides.get("rebalance_utilization", 0.9),
            rebalance_deadband_pct=overrides.get("rebalance_deadband_pct", 0.10),
            rebalance_min_adjust_notional_usdt=overrides.get("rebalance_min_adjust_notional_usdt", 5.0),
            rebalance_max_single_adjust_pct=overrides.get("rebalance_max_single_adjust_pct", 0.95),
            rebalance_max_adjust_orders=30,
            rebalance_mode=overrides.get("rebalance_mode", "equal_risk"),
            rebalance_age_decay_half_life_hours=overrides.get("rebalance_age_decay_half_life_hours", 36.0),
            entry_initial_delay_sec=overrides.get("entry_initial_delay_sec", 0),
            entry_symbol_interval_sec=overrides.get("entry_symbol_interval_sec", 0),
            cooling_off_retry_count=overrides.get("cooling_off_retry_count", 0),
            cooling_off_retry_delay_sec=overrides.get("cooling_off_retry_delay_sec", 0),
            entry_wait_bearish_hour_enabled=overrides.get("entry_wait_bearish_hour_enabled", False),
            entry_wait_poll_sec=overrides.get("entry_wait_poll_sec", 30),
            entry_wait_close_grace_sec=overrides.get("entry_wait_close_grace_sec", 5),
        )

    @staticmethod
    def _mock_order_factory():
        state = {"seq": 0}

        def _create_order(**kwargs):
            state["seq"] += 1
            return {
                "orderId": 1000 + state["seq"],
                "symbol": kwargs["symbol"],
                "side": kwargs["side"],
                "type": kwargs["type"],
                "origQty": kwargs["quantity"],
                "status": "FILLED",
            }

        return _create_order

    def test_new_client_id_handles_non_ascii_symbol(self) -> None:
        client_id = Top10ShortStrategy._new_client_id("ent", "币安人生USDT")
        self.assertLessEqual(len(client_id), 36)
        self.assertRegex(client_id, r"^[.A-Z:/a-z0-9_-]{1,36}$")

    def test_rebalance_reduce_only_sells_nothing_and_reduces_old_positions(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
            {"symbol": "BUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
            {"id": 2, "symbol": "BUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-6", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-6", "entryPrice": "10"},
            ]
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=4, reduce_only=True, reason_tag="pre")

        self.assertEqual(int(summary["planned"]), 2)
        self.assertEqual(int(summary["adjusted"]), 2)
        self.assertEqual(int(summary["errors"]), 0)
        self.assertAlmostEqual(float(summary["reduced_notional"]), 110.0, places=6)
        self.assertAlmostEqual(float(summary["added_notional"]), 0.0, places=6)
        self.assertEqual(summary["mode"], "equal_risk")
        self.assertEqual(client.create_order.call_count, 2)
        for call in client.create_order.call_args_list:
            kwargs = call.kwargs
            self.assertEqual(kwargs["side"], "BUY")
            self.assertTrue(kwargs["reduceOnly"])
        client.ensure_isolated_and_leverage.assert_not_called()

    def test_rebalance_full_runs_reduce_then_increase(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "AUSDT",
                "positionAmt": "-10",
                "markPrice": "10",
                "entryPrice": "10",
                "unRealizedProfit": "0",
                "marginType": "isolated",
                "leverage": "2",
            },
            {
                "symbol": "BUSDT",
                "positionAmt": "-2",
                "markPrice": "10",
                "entryPrice": "10",
                "unRealizedProfit": "0",
                "marginType": "isolated",
                "leverage": "2",
            },
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
            {"id": 2, "symbol": "BUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-9", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-3.9", "entryPrice": "10"},
            ]
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=2, reduce_only=False, reason_tag="post")

        self.assertEqual(int(summary["planned"]), 2)
        self.assertEqual(int(summary["adjusted"]), 2)
        self.assertEqual(client.create_order.call_count, 2)
        self.assertEqual(client.create_order.call_args_list[0].kwargs["side"], "BUY")
        self.assertEqual(client.create_order.call_args_list[1].kwargs["side"], "SELL")
        self.assertTrue(client.create_order.call_args_list[0].kwargs["reduceOnly"])
        self.assertNotIn("reduceOnly", client.create_order.call_args_list[1].kwargs)
        client.ensure_isolated_and_leverage.assert_not_called()

    def test_rebalance_sell_continues_when_ensure_hits_open_orders_error(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "AUSDT",
                "positionAmt": "-10",
                "markPrice": "10",
                "entryPrice": "10",
                "unRealizedProfit": "0",
                "marginType": "isolated",
                "leverage": "2",
            },
            {
                "symbol": "BUSDT",
                "positionAmt": "-2",
                "markPrice": "10",
                "entryPrice": "10",
                "unRealizedProfit": "0",
                "marginType": "cross",
                "leverage": "1",
            },
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()
        client.ensure_isolated_and_leverage.side_effect = BinanceAPIError(
            code=-4067,
            message="Position side cannot be changed if there exists open orders.",
        )

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
            {"id": 2, "symbol": "BUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store)
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AUSDT", "positionAmt": "-9", "entryPrice": "10"},
                {"symbol": "BUSDT", "positionAmt": "-3.9", "entryPrice": "10"},
            ]
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=2, reduce_only=False, reason_tag="post")

        self.assertEqual(int(summary["errors"]), 0)
        self.assertEqual(int(summary["adjusted"]), 2)
        self.assertEqual(client.create_order.call_count, 2)
        self.assertEqual(client.create_order.call_args_list[1].kwargs["side"], "SELL")

    def test_rebalance_skips_when_within_deadband(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "55"}]
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store, rebalance_deadband_pct=0.10)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=1, reduce_only=False, reason_tag="post")

        self.assertEqual(int(summary["planned"]), 0)
        self.assertEqual(int(summary["adjusted"]), 0)
        client.create_order.assert_not_called()

    def test_rebalance_age_decay_biases_to_newer_positions(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {"symbol": "OLDUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
            {"symbol": "NEWUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = self._mock_order_factory()

        store = MagicMock()
        store.list_open_positions.return_value = [
            {
                "id": 1,
                "symbol": "OLDUSDT",
                "entry_price": 10.0,
                "opened_at_utc": "2026-02-13T00:00:00+00:00",
                "tp_order_id": None,
                "sl_order_id": None,
            },
            {
                "id": 2,
                "symbol": "NEWUSDT",
                "entry_price": 10.0,
                "opened_at_utc": "2026-02-15T00:00:00+00:00",
                "tp_order_id": None,
                "sl_order_id": None,
            },
        ]

        strategy = self._build_strategy(
            client,
            store,
            rebalance_mode="age_decay",
            rebalance_age_decay_half_life_hours=24.0,
            rebalance_deadband_pct=0.0,
        )
        strategy._utc_now_datetime = MagicMock(return_value=strategy._parse_iso_utc("2026-02-15T00:00:00+00:00"))
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "OLDUSDT", "positionAmt": "-6.4", "entryPrice": "10"},
                {"symbol": "NEWUSDT", "positionAmt": "-13.6", "entryPrice": "10"},
            ]
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=2, reduce_only=False, reason_tag="post")

        self.assertEqual(summary["mode"], "age_decay")
        self.assertEqual(int(summary["planned"]), 2)
        self.assertEqual(int(summary["adjusted"]), 2)
        self.assertEqual(client.create_order.call_args_list[0].kwargs["symbol"], "OLDUSDT")
        self.assertEqual(client.create_order.call_args_list[0].kwargs["side"], "BUY")
        self.assertEqual(client.create_order.call_args_list[1].kwargs["symbol"], "NEWUSDT")
        self.assertEqual(client.create_order.call_args_list[1].kwargs["side"], "SELL")

    def test_rebalance_age_decay_falls_back_when_opened_time_missing(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {"symbol": "AUSDT", "positionAmt": "-10", "markPrice": "10", "entryPrice": "10", "unRealizedProfit": "0"},
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(client, store, rebalance_mode="age_decay", rebalance_deadband_pct=0.10)
        strategy._refresh_exit_orders_for_positions = MagicMock()

        summary = strategy._rebalance_to_target(target_count=1, reduce_only=False, reason_tag="post")

        self.assertEqual(summary["mode"], "age_decay")
        self.assertEqual(int(summary["planned"]), 1)
        self.assertEqual(int(summary["adjusted"]), 1)
        self.assertEqual(client.create_order.call_count, 1)
        self.assertEqual(client.create_order.call_args.kwargs["side"], "SELL")

    @patch("core.strategy_top10_short.build_top_gainers")
    def test_entry_uses_equity_target_notional_when_rebalance_enabled(self, mock_top_gainers) -> None:
        mock_top_gainers.return_value = [
            {
                "symbol": "NEWUSDT",
                "change": "11.5",
                "current_price": "10",
                "volume": "12345",
            }
        ]

        client = MagicMock()
        client.session = MagicMock()
        client.base_url = "https://fapi.binance.com"
        client.get_available_balance.return_value = 500.0
        client.get_balance.return_value = [{"asset": "USDT", "balance": "920"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "OLDUSDT",
                "positionAmt": "-1",
                "markPrice": "80",
                "entryPrice": "80",
                "unRealizedProfit": "0",
            }
        ]
        client.normalize_order_qty.return_value = 1.0

        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = {"OLDUSDT"}
        store.insert_position.return_value = 1001
        store.list_open_positions.return_value = [{"id": 1001, "symbol": "NEWUSDT"}]

        strategy = self._build_strategy(client, store, rebalance_utilization=0.9)
        strategy.top_n = 1
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "NEWUSDT",
                "entryPrice": "10",
                "liquidationPrice": "12",
                "positionAmt": "-1",
            }
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            return_value=(
                {
                    "orderId": 2001,
                    "clientOrderId": "ent-new-1",
                    "status": "FILLED",
                    "origQty": "1",
                    "side": "SELL",
                    "type": "MARKET",
                    "symbol": "NEWUSDT",
                },
                0,
            )
        )
        strategy._place_exit_orders = MagicMock()
        strategy._rebalance_to_target = MagicMock(return_value={"planned": 0, "adjusted": 0, "errors": 0, "mode": "equal_risk"})

        result = strategy.run_entry(trade_day_utc="2026-02-16-test-entry-notional")

        self.assertEqual(result["status"], "SUCCESS")
        target_notional_used = float(strategy._place_market_short_with_shrink_retry.call_args.kwargs["target_notional"])
        expected_total_positions = 2.0  # 1 old + 1 new
        expected_target_notional = 920.0 * 2.0 * 0.9 / expected_total_positions
        self.assertAlmostEqual(target_notional_used, expected_target_notional, places=6)
        old_available_formula_notional = 500.0 * 0.99 / 10.0 * 2.0
        self.assertGreater(target_notional_used, old_available_formula_notional)
        self.assertEqual(strategy._rebalance_to_target.call_count, 1)
        self.assertEqual(strategy._rebalance_to_target.call_args.kwargs["reason_tag"], "pre")
        self.assertTrue(strategy._rebalance_to_target.call_args.kwargs["reduce_only"])

    def test_entry_paces_each_processed_symbol_when_configured(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.diagnose_order_qty.side_effect = [
            {"normalized_qty": 1.0},
            {"normalized_qty": 1.0},
            {
                "normalized_qty": 0.0,
                "has_rules": True,
                "raw_qty": 0.5,
                "normalized_notional": 0.0,
                "step_size": 1.0,
                "min_qty": 1.0,
                "min_notional": 5.0,
                "reject_reason": "qty_below_min_qty",
            },
        ]

        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = set()
        store.insert_position.return_value = 1001
        store.list_open_positions.return_value = [{"id": 1001, "symbol": "AAAUSDT"}]

        strategy = self._build_strategy(client, store, rebalance_enabled=False, entry_symbol_interval_sec=30)
        strategy.top_n = 3
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "AAAUSDT",
                "entryPrice": "10",
                "liquidationPrice": "12",
                "positionAmt": "-1",
            }
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            side_effect=[
                (
                    {
                        "orderId": 2001,
                        "clientOrderId": "ent-aaa-1",
                        "status": "FILLED",
                        "origQty": "1",
                        "side": "SELL",
                        "type": "MARKET",
                        "symbol": "AAAUSDT",
                    },
                    0,
                ),
                RuntimeError("boom"),
            ]
        )
        strategy._place_exit_orders = MagicMock()
        strategy._redistribute_failed_notional = MagicMock()

        candidates = [
            {"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"},
            {"symbol": "BBBUSDT", "change": "14", "current_price": "11", "volume": "100"},
            {"symbol": "CCCUSDT", "change": "13", "current_price": "12", "volume": "100"},
        ]

        with patch("core.strategy_top10_short.time.sleep") as sleep_mock:
            result = strategy.run_entry(
                trade_day_utc="2026-03-01-test-entry-pacing",
                shared_top_gainers=candidates,
            )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["opened"], 1)
        self.assertEqual(result["entry_failed"], 2)
        self.assertEqual(strategy._place_market_short_with_shrink_retry.call_count, 2)
        self.assertEqual(sleep_mock.call_args_list, [unittest.mock.call(30), unittest.mock.call(30)])

    def test_entry_waits_for_each_symbol_first_bearish_closed_hour_from_signal_time(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.diagnose_order_qty.side_effect = [{"normalized_qty": 1.0}, {"normalized_qty": 1.0}]
        client.get_klines.side_effect = [
            [[1764547200000, "100", "105", "99", "101", "0", 1764550799999]],  # AAA 00:00 bullish
            [[1764550800000, "101", "102", "90", "95", "0", 1764554399999]],  # AAA 01:00 bearish
            [[1764547200000, "200", "201", "180", "190", "0", 1764550799999]],  # BBB 00:00 bearish
        ]

        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = set()
        store.insert_position.side_effect = [1001, 1002]
        store.list_open_positions.return_value = [
            {"id": 1001, "symbol": "AAAUSDT"},
            {"id": 1002, "symbol": "BBBUSDT"},
        ]

        strategy = self._build_strategy(
            client,
            store,
            rebalance_enabled=False,
            entry_wait_bearish_hour_enabled=True,
            entry_symbol_interval_sec=30,
        )
        strategy.top_n = 2
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "BBBUSDT", "entryPrice": "190", "liquidationPrice": "220", "positionAmt": "-1"},
                {"symbol": "AAAUSDT", "entryPrice": "95", "liquidationPrice": "120", "positionAmt": "-1"},
            ]
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            side_effect=[
                ({"orderId": 2001, "status": "FILLED", "origQty": "1", "side": "SELL", "type": "MARKET", "symbol": "BBBUSDT"}, 0),
                ({"orderId": 2002, "status": "FILLED", "origQty": "1", "side": "SELL", "type": "MARKET", "symbol": "AAAUSDT"}, 0),
            ]
        )
        strategy._place_exit_orders = MagicMock()
        strategy._utc_now_datetime = MagicMock(
            side_effect=[
                datetime.fromisoformat("2025-12-01T00:10:00+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:10+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:11+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:12+00:00"),
            ]
        )

        result = strategy.run_entry(
            trade_day_utc="2025-12-01-test-bearish-hour-entry",
            shared_top_gainers=[
                {"symbol": "AAAUSDT", "change": "15", "current_price": "100", "volume": "100"},
                {"symbol": "BBBUSDT", "change": "14", "current_price": "200", "volume": "100"},
            ],
        )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["opened"], 2)
        self.assertEqual(
            [
                call.kwargs["symbol"]
                for call in strategy._place_market_short_with_shrink_retry.call_args_list
            ],
            ["BBBUSDT", "AAAUSDT"],
        )
        self.assertEqual(
            [
                call.kwargs["reference_price"]
                for call in strategy._place_market_short_with_shrink_retry.call_args_list
            ],
            [190.0, 95.0],
        )

    def test_bearish_wait_state_roundtrips_for_restart_recovery(self) -> None:
        client = MagicMock()
        state = {}
        store = MagicMock()
        store.get_lock_state.side_effect = lambda _name: dict(state)

        def save_state(_name, payload):
            state.clear()
            state.update(payload)

        store.set_lock_state.side_effect = save_state
        strategy = self._build_strategy(
            client,
            store,
            rebalance_enabled=False,
            entry_wait_bearish_hour_enabled=True,
        )
        candidates = strategy._build_ranked_entries(
            [{"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"}]
        )
        signal_time = datetime.fromisoformat("2026-07-11T00:10:00+00:00")

        first = strategy._restore_or_create_entry_wait(candidates, signal_time, "run-1", "2026-07-11")
        restored = strategy._restore_or_create_entry_wait([], signal_time, "run-1", "2026-07-11")

        self.assertEqual(first[0]["entry"].symbol, "AAAUSDT")
        self.assertEqual(restored[0]["entry"].symbol, "AAAUSDT")
        self.assertEqual(state["run_id"], "run-1")
        self.assertIn("deadline_utc", state)

    def test_initial_exit_setup_cleans_take_profit_when_stop_creation_fails(self) -> None:
        client = MagicMock()
        client.normalize_trigger_price.side_effect = [8.0, 11.9]
        client.format_trigger_price.side_effect = ["8", "11.9"]
        client.create_order.side_effect = [
            {"orderId": 101, "clientOrderId": "tp-new", "status": "NEW"},
            BinanceAPIError(code=-2021, message="Order would immediately trigger"),
        ]
        store = MagicMock()
        strategy = self._build_strategy(client, store, rebalance_enabled=False)
        strategy.fixed_take_profit_enabled = True
        strategy._load_short_position = MagicMock(
            return_value={"symbol": "AAAUSDT", "entryPrice": "10", "liquidationPrice": "12", "positionAmt": "-1"}
        )

        with self.assertRaises(BinanceAPIError):
            strategy._place_exit_orders(position_id=1, symbol="AAAUSDT")

        client.cancel_order.assert_called_once_with(
            symbol="AAAUSDT",
            order_id=101,
            orig_client_order_id="tp-new",
        )

    def test_entry_waits_before_first_symbol_when_initial_delay_configured(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.normalize_order_qty.side_effect = [1.0, 1.0]

        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = set()
        store.insert_position.return_value = 1001
        store.list_open_positions.return_value = [{"id": 1001, "symbol": "AAAUSDT"}]

        strategy = self._build_strategy(
            client,
            store,
            rebalance_enabled=False,
            entry_initial_delay_sec=30,
            entry_symbol_interval_sec=30,
        )
        strategy.top_n = 2
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "AAAUSDT",
                "entryPrice": "10",
                "liquidationPrice": "12",
                "positionAmt": "-1",
            }
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            side_effect=[
                (
                    {
                        "orderId": 2001,
                        "clientOrderId": "ent-aaa-1",
                        "status": "FILLED",
                        "origQty": "1",
                        "side": "SELL",
                        "type": "MARKET",
                        "symbol": "AAAUSDT",
                    },
                    0,
                ),
                (
                    {
                        "orderId": 2002,
                        "clientOrderId": "ent-bbb-1",
                        "status": "FILLED",
                        "origQty": "1",
                        "side": "SELL",
                        "type": "MARKET",
                        "symbol": "BBBUSDT",
                    },
                    0,
                ),
            ]
        )
        strategy._place_exit_orders = MagicMock()

        with patch("core.strategy_top10_short.time.sleep") as sleep_mock:
            result = strategy.run_entry(
                trade_day_utc="2026-03-03-test-entry-initial-delay",
                shared_top_gainers=[
                    {"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"},
                    {"symbol": "BBBUSDT", "change": "14", "current_price": "11", "volume": "100"},
                ],
            )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["opened"], 2)
        self.assertEqual(
            sleep_mock.call_args_list,
            [unittest.mock.call(30), unittest.mock.call(30)],
        )

    def test_entry_does_not_sleep_without_symbol_interval(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.normalize_order_qty.return_value = 1.0

        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = set()
        store.insert_position.return_value = 1001
        store.list_open_positions.return_value = [{"id": 1001, "symbol": "AAAUSDT"}]

        strategy = self._build_strategy(client, store, rebalance_enabled=False)
        strategy.top_n = 1
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "AAAUSDT",
                "entryPrice": "10",
                "liquidationPrice": "12",
                "positionAmt": "-1",
            }
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            return_value=(
                {
                    "orderId": 2001,
                    "clientOrderId": "ent-aaa-1",
                    "status": "FILLED",
                    "origQty": "1",
                    "side": "SELL",
                    "type": "MARKET",
                    "symbol": "AAAUSDT",
                },
                0,
            )
        )
        strategy._place_exit_orders = MagicMock()

        with patch("core.strategy_top10_short.time.sleep") as sleep_mock:
            result = strategy.run_entry(
                trade_day_utc="2026-03-01-test-entry-no-pacing",
                shared_top_gainers=[
                    {"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"},
                ],
            )

        self.assertEqual(result["status"], "SUCCESS")
        sleep_mock.assert_not_called()

    def test_entry_keeps_new_position_pending_until_initial_exit_orders_are_ready(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.diagnose_order_qty.return_value = {"normalized_qty": 1.0}

        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = set()
        store.insert_position.return_value = 1001
        store.list_open_positions.return_value = []

        strategy = self._build_strategy(client, store, rebalance_enabled=False)
        strategy.top_n = 1
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "AAAUSDT",
                "entryPrice": "10",
                "liquidationPrice": "12",
                "positionAmt": "-1",
            }
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            return_value=(
                {
                    "orderId": 2001,
                    "clientOrderId": "ent-aaa-1",
                    "status": "FILLED",
                    "origQty": "1",
                    "side": "SELL",
                    "type": "MARKET",
                    "symbol": "AAAUSDT",
                },
                0,
            )
        )

        def assert_position_is_pending_during_exit_setup(**_kwargs):
            self.assertEqual(store.insert_position.call_args.kwargs["status"], "PENDING_EXIT_SETUP")

        strategy._place_exit_orders = MagicMock(side_effect=assert_position_is_pending_during_exit_setup)

        result = strategy.run_entry(
            trade_day_utc="2026-03-01-test-pending-exit-setup",
            shared_top_gainers=[
                {"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"},
            ],
        )

        self.assertEqual(result["status"], "SUCCESS")
        store.mark_position_open.assert_called_once_with(1001)

    def test_entry_places_initial_exit_orders_before_waiting_for_later_bearish_symbols(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.diagnose_order_qty.return_value = {"normalized_qty": 1.0}

        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = set()
        store.insert_position.side_effect = [1001, 1002]
        store.list_open_positions.return_value = []

        strategy = self._build_strategy(client, store, rebalance_enabled=False)
        strategy.top_n = 2
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AAAUSDT", "entryPrice": "10", "liquidationPrice": "12", "positionAmt": "-1"},
                {"symbol": "BBBUSDT", "entryPrice": "20", "liquidationPrice": "24", "positionAmt": "-1"},
            ]
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            side_effect=[
                ({"orderId": 2001, "status": "FILLED", "origQty": "1", "side": "SELL", "type": "MARKET", "symbol": "AAAUSDT"}, 0),
                ({"orderId": 2002, "status": "FILLED", "origQty": "1", "side": "SELL", "type": "MARKET", "symbol": "BBBUSDT"}, 0),
            ]
        )
        strategy._place_exit_orders = MagicMock()

        candidates = [
            {"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"},
            {"symbol": "BBBUSDT", "change": "14", "current_price": "20", "volume": "100"},
        ]
        ranked_entries = strategy._build_ranked_entries(candidates)

        def ready_entries():
            yield type("ReadyEntry", (), {"entry": ranked_entries[0], "reference_price": 10.0})()
            strategy._place_exit_orders.assert_called_once_with(position_id=1001, symbol="AAAUSDT")
            store.mark_position_open.assert_called_once_with(1001)
            yield type("ReadyEntry", (), {"entry": ranked_entries[1], "reference_price": 20.0})()

        strategy._iter_ready_entries_after_bearish_hour = MagicMock(return_value=ready_entries())

        result = strategy.run_entry(
            trade_day_utc="2026-03-01-test-immediate-exit-setup",
            shared_top_gainers=candidates,
        )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(strategy._place_exit_orders.call_count, 2)
        self.assertEqual(store.mark_position_open.call_count, 2)

    def test_market_short_retries_once_after_cooling_off_and_sleeps(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.return_value = 1.0
        client.format_order_qty.return_value = "1"
        client.create_order.side_effect = [
            BinanceAPIError(code=-4192, message="Trade forbidden due to Cooling-off Period."),
            {
                "orderId": 3001,
                "clientOrderId": "ent-aaa-1",
                "status": "FILLED",
                "origQty": "1",
                "side": "SELL",
                "type": "MARKET",
                "symbol": "AAAUSDT",
            },
        ]

        strategy = self._build_strategy(
            client,
            MagicMock(),
            cooling_off_retry_count=1,
            cooling_off_retry_delay_sec=30,
        )

        with patch("core.strategy_top10_short.time.sleep") as sleep_mock:
            order, retries_used = strategy._place_market_short_with_shrink_retry(
                symbol="AAAUSDT",
                target_notional=10.0,
                reference_price=10.0,
                client_id_tag="ent",
            )

        self.assertEqual(order["orderId"], 3001)
        self.assertEqual(retries_used, 0)
        self.assertEqual(client.create_order.call_count, 2)
        sleep_mock.assert_called_once_with(30)

    def test_cooling_off_retry_disabled_by_default(self) -> None:
        client = MagicMock()
        client.normalize_order_qty.return_value = 1.0
        client.format_order_qty.return_value = "1"
        client.create_order.side_effect = BinanceAPIError(
            code=-4192,
            message="Trade forbidden due to Cooling-off Period.",
        )

        strategy = self._build_strategy(client, MagicMock())

        with patch("core.strategy_top10_short.time.sleep") as sleep_mock:
            with self.assertRaises(BinanceAPIError):
                strategy._place_market_short_with_shrink_retry(
                    symbol="AAAUSDT",
                    target_notional=10.0,
                    reference_price=10.0,
                    client_id_tag="ent",
                )

        self.assertEqual(client.create_order.call_count, 1)
        sleep_mock.assert_not_called()

    def test_rebalance_sell_retries_once_after_cooling_off_and_sleeps(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "100"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "AAAUSDT",
                "positionAmt": "-2",
                "markPrice": "10",
                "entryPrice": "10",
                "unRealizedProfit": "0",
                "marginType": "isolated",
                "leverage": "2",
            },
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = [
            BinanceAPIError(code=-4192, message="Trade forbidden due to Cooling-off Period."),
            {
                "orderId": 4001,
                "clientOrderId": "rbpost-aaa-1",
                "status": "FILLED",
                "origQty": "16",
                "side": "SELL",
                "type": "MARKET",
                "symbol": "AAAUSDT",
            },
        ]

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AAAUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(
            client,
            store,
            cooling_off_retry_count=1,
            cooling_off_retry_delay_sec=30,
        )
        strategy._load_short_position = MagicMock(
            return_value={"symbol": "AAAUSDT", "positionAmt": "-18", "entryPrice": "10"}
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        with patch("core.strategy_top10_short.time.sleep") as sleep_mock:
            summary = strategy._rebalance_to_target(target_count=1, reduce_only=False, reason_tag="post")

        self.assertEqual(int(summary["adjusted"]), 1)
        self.assertEqual(int(summary["errors"]), 0)
        self.assertEqual(client.create_order.call_count, 2)
        sleep_mock.assert_called_once_with(30)

    def test_rebalance_buy_does_not_retry_after_cooling_off(self) -> None:
        client = MagicMock()
        client.get_balance.return_value = [{"asset": "USDT", "balance": "50"}]
        client.get_position_risk.return_value = [
            {
                "symbol": "AAAUSDT",
                "positionAmt": "-10",
                "markPrice": "10",
                "entryPrice": "10",
                "unRealizedProfit": "0",
                "marginType": "isolated",
                "leverage": "2",
            },
        ]
        client.normalize_order_qty.side_effect = lambda _s, notional, price: notional / price
        client.format_order_qty.side_effect = lambda _s, qty: str(qty)
        client.create_order.side_effect = BinanceAPIError(
            code=-4192,
            message="Trade forbidden due to Cooling-off Period.",
        )

        store = MagicMock()
        store.list_open_positions.return_value = [
            {"id": 1, "symbol": "AAAUSDT", "entry_price": 10.0, "tp_order_id": None, "sl_order_id": None},
        ]

        strategy = self._build_strategy(
            client,
            store,
            cooling_off_retry_count=1,
            cooling_off_retry_delay_sec=30,
        )
        strategy._refresh_exit_orders_for_positions = MagicMock()

        with patch("core.strategy_top10_short.time.sleep") as sleep_mock:
            summary = strategy._rebalance_to_target(target_count=2, reduce_only=False, reason_tag="post")

        self.assertEqual(int(summary["adjusted"]), 0)
        self.assertEqual(int(summary["errors"]), 1)
        self.assertEqual(client.create_order.call_count, 1)
        sleep_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
