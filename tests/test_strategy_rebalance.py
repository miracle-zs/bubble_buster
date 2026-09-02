import importlib.util
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
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

from core.entry_structure_protection import EntryStructureProtection
from core.state_store import RunState
from core.strategy_top10_short import EntryStructureWindow, RankEntry, ReadyEntry, Top10ShortStrategy
from infra.binance_futures_client import BinanceAPIError, BinanceRateLimitError


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
            entry_wait_close_grace_sec=overrides.get("entry_wait_close_grace_sec", 1),
            entry_wait_close_retry_sec=overrides.get("entry_wait_close_retry_sec", 1.0),
            entry_wait_close_retry_count=overrides.get("entry_wait_close_retry_count", 5),
            entry_preclose_sec=overrides.get("entry_preclose_sec", 0),
            entry_scale_in_mode=overrides.get("entry_scale_in_mode", "none"),
            entry_scale_in_first_ratio=overrides.get("entry_scale_in_first_ratio", 0.50),
            runtime_timezone=overrides.get("runtime_timezone", "Asia/Shanghai"),
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

        def get_klines(**kwargs):
            rows = {
                ("AAAUSDT", 1764547200000): [[1764547200000, "100", "105", "99", "101", "0", 1764550799999]],
                ("AAAUSDT", 1764550800000): [[1764550800000, "101", "102", "90", "95", "0", 1764554399999]],
                ("BBBUSDT", 1764547200000): [[1764547200000, "200", "201", "180", "190", "0", 1764550799999]],
            }
            return rows.get((kwargs["symbol"], kwargs["start_time"]), [])

        client.get_klines.side_effect = get_klines

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
        strategy._prepare_entry_structure_window = MagicMock(return_value=None)
        strategy._utc_now_datetime = MagicMock(
            side_effect=[
                datetime.fromisoformat("2025-12-01T00:10:00+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:10+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:11+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:12+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:13+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:14+00:00"),
                datetime.fromisoformat("2025-12-01T03:00:15+00:00"),
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

    def test_scale_in_waits_for_bullish_then_later_bearish_candle(self) -> None:
        client = MagicMock()
        state = {}
        store = MagicMock()
        store.list_active_symbols.return_value = set()
        store.get_lock_state.side_effect = lambda _name: dict(state)

        def save_state(_name, payload):
            state.clear()
            state.update(payload)

        store.set_lock_state.side_effect = save_state
        strategy = self._build_strategy(
            client,
            store,
            rebalance_enabled=False,
            entry_scale_in_mode="after_bullish_bearish",
            entry_scale_in_first_ratio=0.50,
        )
        hour_zero = datetime(2025, 12, 1, 0, tzinfo=timezone.utc)
        hour_ms = int(hour_zero.timestamp() * 1000)
        candles = {
            hour_ms: (100.0, 90.0),
            hour_ms + 3_600_000: (90.0, 95.0),
            hour_ms + 7_200_000: (95.0, 92.0),
        }

        def get_klines(**kwargs):
            prices = candles.get(kwargs["start_time"])
            if prices is None:
                return []
            open_price, close_price = prices
            return [
                [
                    kwargs["start_time"],
                    str(open_price),
                    str(max(open_price, close_price)),
                    str(min(open_price, close_price)),
                    str(close_price),
                    "0",
                    kwargs["start_time"] + 3_599_999,
                ]
            ]

        client.get_klines.side_effect = get_klines
        strategy._utc_now_datetime = MagicMock(
            return_value=datetime(2025, 12, 1, 3, 1, tzinfo=timezone.utc)
        )
        strategy._entry_wait_stop_event.wait = MagicMock(return_value=False)

        events = strategy._iter_ready_entries_after_bearish_hour(
            candidates=[RankEntry("AAAUSDT", 15.0, 100.0, 100.0)],
            signal_base_time_utc=datetime(2025, 12, 1, 0, 10, tzinfo=timezone.utc),
            run_id="run-1",
            trade_day_utc="2025-12-01",
        )

        first = next(events)
        self.assertEqual(first.entry_stage, strategy.ENTRY_STAGE_INITIAL)
        self.assertEqual(first.reference_price, 90.0)
        self.assertEqual(state["pending"]["0"]["phase"], strategy.ENTRY_PHASE_WAIT_BULLISH)

        second = next(events)
        self.assertEqual(second.entry_stage, strategy.ENTRY_STAGE_SCALE_IN)
        self.assertEqual(second.reference_price, 92.0)
        self.assertEqual(state["pending"]["0"]["phase"], strategy.ENTRY_PHASE_COMPLETE)

        with self.assertRaises(StopIteration):
            next(events)
        self.assertEqual(state, {})

    def test_scale_in_ratio_is_half_and_default_mode_stays_full_size(self) -> None:
        client = MagicMock()
        store = MagicMock()
        split_strategy = self._build_strategy(
            client,
            store,
            entry_scale_in_mode="after_bullish_bearish",
            entry_scale_in_first_ratio=0.50,
        )
        normal_strategy = self._build_strategy(client, store)

        self.assertAlmostEqual(
            split_strategy._entry_ratio_for_stage(split_strategy.ENTRY_STAGE_INITIAL),
            0.50,
        )
        self.assertAlmostEqual(
            split_strategy._entry_ratio_for_stage(split_strategy.ENTRY_STAGE_SCALE_IN),
            0.50,
        )
        self.assertAlmostEqual(
            normal_strategy._entry_ratio_for_stage(normal_strategy.ENTRY_STAGE_INITIAL),
            1.0,
        )

    def test_scale_in_updates_existing_position_and_replaces_exit_orders(self) -> None:
        client = MagicMock()
        client.diagnose_order_qty.return_value = {"normalized_qty": 5.0}
        store = MagicMock()
        store.list_open_positions.return_value = [
            {
                "id": 7,
                "symbol": "AAAUSDT",
                "status": "OPEN",
                "entry_price": 10.0,
                "tp_order_id": 11,
                "tp_client_order_id": "old-tp",
                "sl_order_id": 12,
                "sl_client_order_id": "old-sl",
            }
        ]
        store.has_position_order_event_with_client_prefix.return_value = False
        strategy = self._build_strategy(
            client,
            store,
            entry_scale_in_mode="after_bullish_bearish",
        )
        strategy._load_short_position = MagicMock(
            side_effect=[
                {"symbol": "AAAUSDT", "positionAmt": "-10", "entryPrice": "10"},
                {"symbol": "AAAUSDT", "positionAmt": "-20", "entryPrice": "9.5"},
            ]
        )
        strategy._place_market_short_with_shrink_retry = MagicMock(
            return_value=(
                {
                    "orderId": 99,
                    "clientOrderId": "t10s-add-AAAUSDT",
                    "symbol": "AAAUSDT",
                    "side": "SELL",
                    "type": "MARKET",
                    "status": "FILLED",
                    "origQty": "10",
                    "executedQty": "10",
                    "avgPrice": "9.0",
                },
                0,
            )
        )
        strategy._place_exit_orders = MagicMock()
        strategy._cancel_order_if_exists = MagicMock()
        strategy._utc_now_datetime = MagicMock(return_value=datetime(2025, 12, 1, 2, tzinfo=timezone.utc))

        result = strategy._add_scale_in_tranche(
            ready_entry=ReadyEntry(
                entry=RankEntry("AAAUSDT", 15.0, 9.0, 100.0),
                reference_price=9.0,
                signal_time_utc=datetime(2025, 12, 1, 0, tzinfo=timezone.utc),
                bearish_close_time_utc=datetime(2025, 12, 1, 2, tzinfo=timezone.utc),
                entry_stage="SCALE_IN",
                signal_hour_open_utc=datetime(2025, 12, 1, 1, tzinfo=timezone.utc),
            ),
            full_target_notional=100.0,
        )

        self.assertEqual(result["status"], "ADDED")
        store.set_position_qty.assert_called_once_with(7, 20.0, 9.5)
        store.add_order_event.assert_called_once()
        self.assertEqual(store.add_order_event.call_args.kwargs["position_id"], 7)
        strategy._place_exit_orders.assert_called_once_with(position_id=7, symbol="AAAUSDT")
        self.assertEqual(
            strategy._cancel_order_if_exists.call_args_list,
            [
                unittest.mock.call("AAAUSDT", 11, "old-tp"),
                unittest.mock.call("AAAUSDT", 12, "old-sl"),
            ],
        )

    def test_closed_hour_kline_retry_uses_second_level_interval(self) -> None:
        strategy = self._build_strategy(
            MagicMock(),
            MagicMock(),
            entry_wait_close_retry_sec=0.25,
            entry_wait_close_retry_count=3,
        )
        candle = (100.0, 95.0, datetime(2025, 12, 1, 1, tzinfo=timezone.utc))
        strategy._fetch_hour_candle = MagicMock(side_effect=[None, candle])
        strategy._entry_wait_stop_event.wait = MagicMock(return_value=False)

        result = strategy._fetch_hour_candle_with_retry(
            symbol="AAAUSDT",
            hour_open_utc=datetime(2025, 12, 1, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(result, candle)
        self.assertEqual(strategy._fetch_hour_candle.call_count, 2)
        strategy._entry_wait_stop_event.wait.assert_called_once_with(timeout=0.25)

    def test_preclose_snapshot_can_ready_entry_before_hour_boundary(self) -> None:
        client = MagicMock()
        store = MagicMock()
        store.list_active_symbols.return_value = set()
        strategy = self._build_strategy(
            client,
            store,
            entry_wait_bearish_hour_enabled=True,
            entry_preclose_sec=10,
        )
        hour_open = datetime(2025, 12, 1, 7, tzinfo=timezone.utc)
        preclose_time = datetime(2025, 12, 1, 7, 59, 50, tzinfo=timezone.utc)
        strategy._utc_now_datetime = MagicMock(return_value=preclose_time)
        client.get_klines.return_value = [
            [
                int(hour_open.timestamp() * 1000),
                "100",
                "101",
                "94",
                "95",
                "0",
                int((hour_open + timedelta(hours=1)).timestamp() * 1000) - 1,
            ]
        ]

        ready = next(
            strategy._iter_ready_entries_after_bearish_hour(
                candidates=[RankEntry("AAAUSDT", 15.0, 100.0, 100.0)],
                signal_base_time_utc=datetime(2025, 12, 1, 7, 40, tzinfo=timezone.utc),
                run_id="run-1",
                trade_day_utc="2025-12-01",
            )
        )

        self.assertTrue(ready.preclose_entry)
        self.assertEqual(ready.reference_price, 95.0)
        self.assertIsNone(ready.bearish_close_time_utc)
        self.assertEqual(ready.preclose_time_utc, preclose_time)
        self.assertEqual(ready.signal_hour_open_utc, hour_open)
        self.assertEqual(client.get_klines.call_args.kwargs["end_time"], int(preclose_time.timestamp() * 1000))

    def test_preclose_audit_records_final_candle_direction(self) -> None:
        client = MagicMock()
        store = MagicMock()
        strategy = self._build_strategy(client, store, entry_preclose_sec=10)
        final_time = datetime(2025, 12, 1, 8, 0, 2, tzinfo=timezone.utc)
        hour_open = datetime(2025, 12, 1, 7, tzinfo=timezone.utc)
        strategy._utc_now_datetime = MagicMock(return_value=final_time)
        strategy._fetch_hour_candle_with_retry = MagicMock(
            return_value=(100.0, 101.0, datetime(2025, 12, 1, 8, tzinfo=timezone.utc))
        )
        strategy._build_finalized_preclose_structure_protection = MagicMock(
            return_value=EntryStructureProtection(
                stop_price=105.0,
                bearish_close_time_utc=datetime(2025, 12, 1, 8, tzinfo=timezone.utc),
                window_start_utc=datetime(2025, 12, 1, 6, tzinfo=timezone.utc),
                window_end_utc=datetime(2025, 12, 1, 8, tzinfo=timezone.utc),
            )
        )
        strategy._apply_finalized_preclose_structure_protection = MagicMock(
            return_value="SKIPPED_POSITION_NOT_OPEN"
        )

        strategy._finalize_preclose_entry_audits(
            [
                {
                    "order_event_id": 12,
                    "position_id": 34,
                    "symbol": "AAAUSDT",
                    "hour_open_utc": hour_open,
                    "order_payload": {
                        "orderId": 99,
                        "symbol": "AAAUSDT",
                        "side": "SELL",
                        "type": "MARKET",
                        "status": "FILLED",
                        "entry_audit": {"entry_mode": "PRECLOSE", "filled_at_utc": final_time.isoformat()},
                    },
                }
            ]
        )

        audit = store.update_order_event.call_args.kwargs["order_payload"]["entry_audit"]
        self.assertTrue(audit["final_candle_available"])
        self.assertFalse(audit["final_candle_bearish"])
        self.assertEqual(audit["final_candle_close_price"], 101.0)
        self.assertEqual(audit["structure_stop_status"], "SKIPPED_POSITION_NOT_OPEN")

    def test_preclose_before_noon_does_not_apply_structure_stop(self) -> None:
        client = MagicMock()
        store = MagicMock()
        strategy = self._build_strategy(client, store, entry_preclose_sec=10)
        hour_open = datetime(2025, 12, 1, 0, tzinfo=timezone.utc)
        hour_close = hour_open + timedelta(hours=1)
        strategy._utc_now_datetime = MagicMock(return_value=hour_close + timedelta(seconds=2))
        strategy._fetch_hour_candle_with_retry = MagicMock(
            return_value=(100.0, 95.0, hour_close - timedelta(milliseconds=1))
        )
        strategy._build_finalized_preclose_structure_protection = MagicMock()
        strategy._apply_finalized_preclose_structure_protection = MagicMock()

        strategy._finalize_preclose_entry_audits(
            [
                {
                    "order_event_id": 12,
                    "position_id": 34,
                    "symbol": "AAAUSDT",
                    "hour_open_utc": hour_open,
                    "order_payload": {
                        "orderId": 99,
                        "symbol": "AAAUSDT",
                        "side": "SELL",
                        "type": "MARKET",
                        "status": "FILLED",
                        "entry_audit": {
                            "entry_mode": "PRECLOSE",
                            "filled_at_utc": (hour_close - timedelta(seconds=9)).isoformat(),
                        },
                    },
                }
            ]
        )

        strategy._build_finalized_preclose_structure_protection.assert_not_called()
        strategy._apply_finalized_preclose_structure_protection.assert_not_called()
        audit = store.update_order_event.call_args.kwargs["order_payload"]["entry_audit"]
        self.assertEqual(audit["structure_stop_status"], "DEFERRED_BEFORE_NOON")

    def test_preclose_at_noon_uses_logical_candle_boundary_for_structure_stop(self) -> None:
        client = MagicMock()
        store = MagicMock()
        strategy = self._build_strategy(
            client,
            store,
            entry_wait_bearish_hour_enabled=True,
            entry_preclose_sec=10,
            runtime_timezone="Asia/Shanghai",
        )
        hour_open = datetime(2026, 8, 23, 3, tzinfo=timezone.utc)
        hour_close = hour_open + timedelta(hours=1)
        strategy._utc_now_datetime = MagicMock(
            return_value=hour_close + timedelta(seconds=2)
        )
        strategy._fetch_hour_candle_with_retry = MagicMock(
            return_value=(100.0, 95.0, hour_close - timedelta(milliseconds=1))
        )
        protection = EntryStructureProtection(
            stop_price=105.0,
            bearish_close_time_utc=hour_close,
            window_start_utc=hour_close - timedelta(hours=2),
            window_end_utc=hour_close,
        )
        strategy._build_finalized_preclose_structure_protection = MagicMock(
            return_value=protection
        )
        strategy._apply_finalized_preclose_structure_protection = MagicMock(
            return_value="REPLACED"
        )
        store.list_open_preclose_entry_audits_needing_structure.return_value = [
            {"order_event_id": 12}
        ]

        strategy._finalize_preclose_entry_audits(
            [
                {
                    "order_event_id": 12,
                    "position_id": 34,
                    "symbol": "PORTALUSDT",
                    "hour_open_utc": hour_open,
                    "order_payload": {
                        "orderId": 99,
                        "symbol": "PORTALUSDT",
                        "side": "SELL",
                        "type": "MARKET",
                        "status": "FILLED",
                        "entry_audit": {
                            "entry_mode": "PRECLOSE",
                            "filled_at_utc": (
                                hour_close + timedelta(seconds=2)
                            ).isoformat(),
                        },
                    },
                }
            ]
        )

        strategy._build_finalized_preclose_structure_protection.assert_called_once_with(
            symbol="PORTALUSDT",
            final_close_time_utc=hour_close,
        )
        strategy._apply_finalized_preclose_structure_protection.assert_called_once_with(
            position_id=34,
            symbol="PORTALUSDT",
            protection=protection,
        )
        audit = store.update_order_event.call_args.kwargs["order_payload"]["entry_audit"]
        self.assertEqual(audit["final_candle_close_time_utc"], (hour_close - timedelta(milliseconds=1)).isoformat())
        self.assertEqual(audit["final_candle_logical_close_time_utc"], hour_close.isoformat())
        self.assertEqual(audit["structure_stop_status"], "REPLACED")

    def test_preclose_finalization_is_idempotent_after_persisted_completion(self) -> None:
        client = MagicMock()
        store = MagicMock()
        pending_row = {"order_event_id": 12}
        store.list_open_preclose_entry_audits_needing_structure.side_effect = [
            [pending_row],
            [],
        ]
        strategy = self._build_strategy(client, store, entry_preclose_sec=10)
        hour_open = datetime(2025, 12, 1, 7, tzinfo=timezone.utc)
        hour_close = hour_open + timedelta(hours=1)
        strategy._utc_now_datetime = MagicMock(return_value=hour_close + timedelta(seconds=2))
        strategy._fetch_hour_candle_with_retry = MagicMock(
            return_value=(100.0, 95.0, hour_close - timedelta(milliseconds=1))
        )
        strategy._build_finalized_preclose_structure_protection = MagicMock()
        strategy._apply_finalized_preclose_structure_protection = MagicMock(
            return_value="REPLACED"
        )
        audit = {
            "order_event_id": 12,
            "position_id": 34,
            "symbol": "AAAUSDT",
            "hour_open_utc": hour_open,
            "order_payload": {
                "orderId": 99,
                "symbol": "AAAUSDT",
                "side": "SELL",
                "type": "MARKET",
                "status": "FILLED",
                "entry_audit": {
                    "entry_mode": "PRECLOSE",
                    "filled_at_utc": (hour_close - timedelta(seconds=9)).isoformat(),
                },
            },
        }

        strategy._finalize_preclose_entry_audits([audit])
        strategy._finalize_preclose_entry_audits([audit])

        strategy._apply_finalized_preclose_structure_protection.assert_called_once()
        store.update_order_event.assert_called_once()

    def test_preclose_finalization_replaces_fallback_stop_with_two_candle_structure_stop(self) -> None:
        client = MagicMock()
        store = MagicMock()
        strategy = self._build_strategy(
            client,
            store,
            entry_wait_bearish_hour_enabled=True,
            entry_preclose_sec=10,
        )
        hour_open = datetime(2025, 12, 1, 7, tzinfo=timezone.utc)
        hour_close = hour_open + timedelta(hours=1)
        strategy._utc_now_datetime = MagicMock(
            return_value=hour_close + timedelta(seconds=2)
        )
        strategy._fetch_hour_candle_with_retry = MagicMock(
            return_value=(100.0, 95.0, hour_close - timedelta(milliseconds=1))
        )
        client.get_klines.return_value = [
            [
                int((hour_open - timedelta(hours=1)).timestamp() * 1000),
                "105",
                "108",
                "99",
                "104",
                "0",
                int(hour_open.timestamp() * 1000) - 1,
            ],
            [
                int(hour_open.timestamp() * 1000),
                "100",
                "112",
                "90",
                "95",
                "0",
                int(hour_close.timestamp() * 1000) - 1,
            ],
        ]
        client.normalize_trigger_price.side_effect = (
            lambda _symbol, price, round_up=False: float(price)
        )
        client.format_trigger_price.side_effect = (
            lambda _symbol, price, round_up=False: str(float(price))
        )
        call_order = []

        def create_order(**_kwargs):
            call_order.append("create")
            return {
                "orderId": 333,
                "clientOrderId": "sl-structure-new",
                "status": "NEW",
                "type": "STOP_MARKET",
            }

        def cancel_order(**_kwargs):
            call_order.append("cancel")
            return {}

        client.create_order.side_effect = create_order
        client.cancel_order.side_effect = cancel_order
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "AAAUSDT",
                "entryPrice": "95",
                "liquidationPrice": "140",
                "positionAmt": "-1",
            }
        )
        store.get_position.return_value = {
            "id": 34,
            "symbol": "AAAUSDT",
            "status": "OPEN",
            "sl_price": 130.0,
            "sl_order_id": 22,
            "sl_client_order_id": "sl-fallback-old",
        }
        store.get_lock_state.return_value = {}

        strategy._finalize_preclose_entry_audits(
            [
                {
                    "order_event_id": 12,
                    "position_id": 34,
                    "symbol": "AAAUSDT",
                    "hour_open_utc": hour_open,
                    "order_payload": {
                        "orderId": 99,
                        "symbol": "AAAUSDT",
                        "side": "SELL",
                        "type": "MARKET",
                        "status": "FILLED",
                        "entry_audit": {
                            "entry_mode": "PRECLOSE",
                            "filled_at_utc": (
                                hour_close - timedelta(seconds=9)
                            ).isoformat(),
                        },
                    },
                }
            ]
        )

        create_kwargs = client.create_order.call_args.kwargs
        self.assertEqual(create_kwargs["type"], "STOP_MARKET")
        self.assertEqual(create_kwargs["stopPrice"], "112.0")
        self.assertEqual(call_order, ["create", "cancel"])
        store.update_stop_loss.assert_called_once_with(
            position_id=34,
            sl_order_id=333,
            sl_client_order_id="sl-structure-new",
            sl_price=112.0,
            liq_price_latest=140.0,
        )
        protection_payload = store.set_lock_state.call_args.args[1]["positions"]["34"]
        self.assertEqual(protection_payload["stop_price"], 112.0)

    def test_preclose_structure_stop_never_widens_a_tighter_existing_stop(self) -> None:
        client = MagicMock()
        store = MagicMock()
        strategy = self._build_strategy(
            client,
            store,
            entry_wait_bearish_hour_enabled=True,
            entry_preclose_sec=10,
        )
        client.normalize_trigger_price.side_effect = (
            lambda _symbol, price, round_up=False: float(price)
        )
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "AAAUSDT",
                "liquidationPrice": "140",
                "positionAmt": "-1",
            }
        )
        store.get_position.return_value = {
            "id": 34,
            "symbol": "AAAUSDT",
            "status": "OPEN",
            "sl_price": 110.0,
            "sl_order_id": 22,
            "sl_client_order_id": "sl-tighter",
        }
        store.get_lock_state.return_value = {}
        protection = EntryStructureProtection(
            stop_price=112.0,
            bearish_close_time_utc=datetime(2025, 12, 1, 8, tzinfo=timezone.utc),
            window_start_utc=datetime(2025, 12, 1, 6, tzinfo=timezone.utc),
            window_end_utc=datetime(2025, 12, 1, 8, tzinfo=timezone.utc),
        )

        status = strategy._apply_finalized_preclose_structure_protection(
            position_id=34,
            symbol="AAAUSDT",
            protection=protection,
        )

        self.assertEqual(status, "KEPT_TIGHTER_EXISTING_STOP")
        client.create_order.assert_not_called()
        client.cancel_order.assert_not_called()
        store.update_stop_loss.assert_not_called()
        protection_payload = store.set_lock_state.call_args.args[1]["positions"]["34"]
        self.assertEqual(protection_payload["stop_price"], 112.0)

    def test_preclose_structure_recovery_replays_persisted_entry_audit(self) -> None:
        store = MagicMock()
        store.list_open_preclose_entry_audits_needing_structure.return_value = [
            {
                "order_event_id": 12,
                "position_id": 34,
                "symbol": "AAAUSDT",
                "hour_open_utc": "2025-12-01T07:00:00+00:00",
                "order_payload": {
                    "entry_audit": {
                        "entry_mode": "PRECLOSE",
                        "signal_hour_open_utc": "2025-12-01T07:00:00+00:00",
                    }
                },
            }
        ]
        strategy = self._build_strategy(
            MagicMock(),
            store,
            entry_wait_bearish_hour_enabled=True,
            entry_preclose_sec=10,
        )
        expected_summary = strategy._empty_preclose_structure_summary(total=1)
        expected_summary["replaced"] = 1
        strategy._finalize_preclose_entry_audits = MagicMock(
            return_value=expected_summary
        )

        summary = strategy.recover_preclose_structure_protections()

        self.assertEqual(summary, expected_summary)
        replayed = strategy._finalize_preclose_entry_audits.call_args.args[0]
        self.assertEqual(len(replayed), 1)
        self.assertEqual(
            replayed[0]["hour_open_utc"],
            datetime(2025, 12, 1, 7, tzinfo=timezone.utc),
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

    def test_bearish_wait_restore_drops_symbol_that_is_already_active(self) -> None:
        client = MagicMock()
        state = {}
        store = MagicMock()
        store.get_lock_state.side_effect = lambda _name: dict(state)
        store.set_lock_state.side_effect = lambda _name, payload: (state.clear(), state.update(payload))
        store.list_active_symbols.return_value = set()
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
        strategy._restore_or_create_entry_wait(candidates, signal_time, "run-1", "2026-07-11")

        store.list_active_symbols.return_value = {"AAAUSDT"}
        restored = strategy._restore_or_create_entry_wait([], signal_time, "run-1", "2026-07-11")

        self.assertEqual(restored, {})
        self.assertEqual(state, {})

    def test_post_noon_entry_structure_uses_previous_and_trigger_hour_high(self) -> None:
        client = MagicMock()
        client.get_klines.return_value = [
            [1784253600000, "0.0238200", "0.0259000", "0.0235100", "0.0239200", "0", 1784257199999],
            [1784257200000, "0.0239300", "0.0240600", "0.0196600", "0.0213900", "0", 1784260799999],
        ]
        client.get_agg_trades.return_value = [
            {"a": 1, "p": "0.0215700", "T": 1784260805321},
            {"a": 2, "p": "0.0216100", "T": 1784260805804},
        ]
        client.normalize_trigger_price.side_effect = lambda _symbol, price, round_up=False: float(price)
        strategy = self._build_strategy(
            client,
            MagicMock(),
            rebalance_enabled=False,
            entry_wait_bearish_hour_enabled=True,
        )
        ready_entry = ReadyEntry(
            entry=RankEntry("ESPORTSUSDT", 50.17, 0.02139, 1.0),
            reference_price=0.02139,
            signal_time_utc=datetime(2026, 7, 16, 23, 40, tzinfo=timezone.utc),
            bearish_close_time_utc=datetime(2026, 7, 17, 3, 59, 59, 999000, tzinfo=timezone.utc),
        )

        protection = strategy._build_entry_structure_protection(
            ready_entry=ready_entry,
            fill_time_utc=datetime(2026, 7, 17, 4, 0, 5, 804000, tzinfo=timezone.utc),
            entry_price=0.02159,
        )

        self.assertIsNotNone(protection)
        assert protection is not None
        self.assertAlmostEqual(protection.stop_price, 0.02590)
        self.assertEqual(protection.window_start_utc, datetime(2026, 7, 17, 2, 0, tzinfo=timezone.utc))
        self.assertEqual(protection.bearish_close_time_utc, datetime(2026, 7, 17, 4, 0, tzinfo=timezone.utc))
        self.assertEqual(protection.window_end_utc, datetime(2026, 7, 17, 4, 0, 5, 804000, tzinfo=timezone.utc))
        klines_kwargs = client.get_klines.call_args.kwargs
        self.assertEqual(klines_kwargs["interval"], "1h")
        self.assertEqual(klines_kwargs["limit"], 2)
        agg_kwargs = client.get_agg_trades.call_args.kwargs
        self.assertEqual(agg_kwargs["start_time"], 1784260800000)
        self.assertEqual(agg_kwargs["end_time"], 1784260805804)

    def test_aggregate_trade_high_splits_requests_into_valid_one_hour_windows(self) -> None:
        client = MagicMock()

        def agg_trades(**kwargs):
            return [{"a": kwargs["start_time"], "p": str(kwargs["start_time"]), "T": kwargs["start_time"]}]

        client.get_agg_trades.side_effect = agg_trades
        strategy = self._build_strategy(client, MagicMock(), rebalance_enabled=False)
        start = datetime(2026, 7, 17, 4, 0, tzinfo=timezone.utc)

        high = strategy._fetch_agg_trade_high(
            symbol="BTCUSDT",
            start_utc=start,
            end_utc=datetime(2026, 7, 17, 6, 30, tzinfo=timezone.utc),
        )

        self.assertIsNotNone(high)
        self.assertEqual(client.get_agg_trades.call_count, 3)
        for request in client.get_agg_trades.call_args_list:
            self.assertLessEqual(
                request.kwargs["end_time"] - request.kwargs["start_time"],
                60 * 60 * 1000 - 1,
            )

    def test_running_entry_without_wait_state_resumes_instead_of_skipping_forever(self) -> None:
        client = MagicMock()
        store = MagicMock()
        store.create_run.return_value = ("run-1", False)
        store.get_run.return_value = RunState(
            run_id="run-1",
            account_id="acc01",
            trade_day_utc="2026-07-18",
            started_at_utc="2026-07-18T00:00:00+00:00",
            completed_at_utc=None,
            status="RUNNING",
            reason=None,
        )
        store.get_lock_state.return_value = {}
        store.list_active_symbols.return_value = set()
        strategy = self._build_strategy(client, store, rebalance_enabled=False)

        result = strategy.run_entry(
            trade_day_utc="2026-07-18",
            shared_top_gainers=[],
        )

        self.assertEqual(result["status"], "SUCCESS")
        store.finalize_run.assert_called_once_with("run-1", "SUCCESS", "No ranked symbols")

    def test_resumed_run_summary_accumulates_persisted_opened_positions(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.diagnose_order_qty.return_value = {"normalized_qty": 1.0}

        store = MagicMock()
        store.create_run.return_value = ("run-1", False)
        store.get_run.return_value = RunState(
            run_id="run-1",
            account_id="acc01",
            trade_day_utc="2026-07-18",
            started_at_utc="2026-07-18T00:00:00+00:00",
            completed_at_utc=None,
            status="RUNNING",
            reason=None,
        )
        store.get_lock_state.return_value = {}
        store.list_active_symbols.return_value = set()
        store.count_run_opened_positions.return_value = 9
        store.insert_position.return_value = 1001

        strategy = self._build_strategy(client, store, rebalance_enabled=False)
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

        result = strategy.run_entry(
            trade_day_utc="2026-07-18",
            shared_top_gainers=[
                {"symbol": "NEWUSDT", "change": "15", "current_price": "10", "volume": "100"},
            ],
        )

        self.assertEqual(result["opened"], 10)
        finalize_message = store.finalize_run.call_args.args[2]
        self.assertIn("opened=10", finalize_message)

    def test_run_entry_applies_structure_stop_immediately_after_fill(self) -> None:
        client = MagicMock()
        client.get_available_balance.return_value = 500.0
        client.diagnose_order_qty.return_value = {"normalized_qty": 100.0}
        store = MagicMock()
        store.create_run.return_value = ("run-1", True)
        store.list_open_symbols.return_value = set()
        store.insert_position.return_value = 5618
        store.list_open_positions.return_value = []
        strategy = self._build_strategy(
            client,
            store,
            rebalance_enabled=False,
            entry_wait_bearish_hour_enabled=True,
        )
        strategy.top_n = 1
        ready_entry = ReadyEntry(
            entry=RankEntry("ESPORTSUSDT", 50.17, 0.02139, 1.0),
            reference_price=0.02139,
            signal_time_utc=datetime(2026, 7, 16, 23, 40, tzinfo=timezone.utc),
            bearish_close_time_utc=datetime(2026, 7, 17, 3, 59, 59, 999000, tzinfo=timezone.utc),
        )
        structure_window = EntryStructureWindow(
            bearish_close_time_utc=datetime(2026, 7, 17, 4, 0, tzinfo=timezone.utc),
            window_start_utc=datetime(2026, 7, 17, 2, 0, tzinfo=timezone.utc),
            highest_price=0.02590,
        )
        protection = EntryStructureProtection(
            stop_price=0.02590,
            bearish_close_time_utc=structure_window.bearish_close_time_utc,
            window_start_utc=structure_window.window_start_utc,
            window_end_utc=datetime(2026, 7, 17, 4, 0, 5, 804000, tzinfo=timezone.utc),
        )
        strategy._iter_ready_entries_after_bearish_hour = MagicMock(return_value=iter([ready_entry]))
        strategy._prepare_entry_structure_window = MagicMock(return_value=structure_window)
        strategy._complete_entry_structure_protection = MagicMock(return_value=protection)
        strategy._place_market_short_with_shrink_retry = MagicMock(
            return_value=(
                {
                    "orderId": 1696249220,
                    "status": "FILLED",
                    "side": "SELL",
                    "type": "MARKET",
                    "symbol": "ESPORTSUSDT",
                    "updateTime": 1784260805804,
                },
                0,
            )
        )
        strategy._load_short_position = MagicMock(
            return_value={
                "symbol": "ESPORTSUSDT",
                "entryPrice": "0.02159",
                "liquidationPrice": "0.03083257",
                "positionAmt": "-1582",
            }
        )
        strategy._place_exit_orders = MagicMock()

        result = strategy.run_entry(
            trade_day_utc="2026-07-17-entry-structure",
            shared_top_gainers=[
                {"symbol": "ESPORTSUSDT", "change": "50.17", "current_price": "0.02139", "volume": "1"},
            ],
        )

        self.assertEqual(result["opened"], 1)
        strategy._place_exit_orders.assert_called_once_with(
            position_id=5618,
            symbol="ESPORTSUSDT",
            entry_structure_stop_price=0.02590,
        )
        inserted = store.insert_position.call_args.kwargs
        self.assertEqual(inserted["status"], "PENDING_ENTRY")
        entry_fill = store.set_position_entry_fill.call_args.kwargs
        self.assertEqual(entry_fill["opened_at_utc"], "2026-07-17T04:00:05.804000+00:00")
        lock_payload = store.set_lock_state.call_args.args[1]
        self.assertEqual(lock_payload["positions"]["5618"]["stop_price"], 0.02590)

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
            self.assertEqual(store.insert_position.call_args.kwargs["status"], "PENDING_ENTRY")
            store.set_position_entry_fill.assert_called_once()

        strategy._place_exit_orders = MagicMock(side_effect=assert_position_is_pending_during_exit_setup)

        result = strategy.run_entry(
            trade_day_utc="2026-03-01-test-pending-exit-setup",
            shared_top_gainers=[
                {"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"},
            ],
        )

        self.assertEqual(result["status"], "SUCCESS")
        store.mark_position_open.assert_called_once_with(1001)

    def test_entry_defers_initial_exit_setup_on_rate_limit(self) -> None:
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
        strategy._place_exit_orders = MagicMock(
            side_effect=BinanceRateLimitError(
                code=-1003,
                message="Too many requests",
                http_status=429,
                retry_after_sec=60.0,
            )
        )
        strategy._force_close_position = MagicMock()

        result = strategy.run_entry(
            trade_day_utc="2026-03-01-test-rate-limit-exit-setup",
            shared_top_gainers=[
                {"symbol": "AAAUSDT", "change": "15", "current_price": "10", "volume": "100"},
            ],
        )

        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["exit_setup_deferred"], 1)
        self.assertEqual(result["exit_setup_failed"], 0)
        strategy._force_close_position.assert_not_called()
        store.mark_position_open.assert_not_called()

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
