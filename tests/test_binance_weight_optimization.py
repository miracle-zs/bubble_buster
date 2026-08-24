import tempfile
import unittest
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

from core.account_snapshot import AccountSnapshotProvider
from core.position_manager import PositionManager
from core.runtime_service import ServiceRuntimeConfig, StrategyRuntimeService
from core.state_store import StateStore
from infra.binance_futures_client import BinanceFuturesClient
from infra.binance_rate_limit import BinanceRateLimitCoordinator
from infra.binance_top10_monitor import DailyOpenPriceStream, build_top_gainers
from infra.binance_user_stream import BinanceUserStreamState
from infra.trade_stats_fetcher import TradeStatsFetcher


class BinanceWeightOptimizationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "state.db")
        schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
        self.store = StateStore(
            db_path=self.db_path,
            schema_path=schema_path,
            account_id="acc01",
        )
        self.store.init_schema()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    @staticmethod
    def _account_payload(position_count: int = 1) -> dict:
        return {
            "totalWalletBalance": "1000",
            "totalUnrealizedProfit": "20",
            "totalMarginBalance": "1020",
            "availableBalance": "750",
            "assets": [
                {
                    "asset": "USDT",
                    "walletBalance": "1000",
                    "unrealizedProfit": "20",
                    "marginBalance": "1020",
                    "availableBalance": "750",
                }
            ],
            "positions": [
                {
                    "symbol": f"S{index:02d}USDT",
                    "positionSide": "BOTH",
                    "positionAmt": "-1",
                    "unrealizedProfit": "0.5",
                    "notional": "100",
                    "isolatedWallet": "50",
                }
                for index in range(position_count)
            ],
        }

    def test_one_account_snapshot_is_shared_with_forty_position_lookups(self) -> None:
        client = MagicMock()
        client.get_account.return_value = self._account_payload(position_count=40)
        provider = AccountSnapshotProvider(
            client=client,
            store=self.store,
            account_id="acc01",
        )
        manager = PositionManager(
            client=client,
            store=self.store,
            notifier=MagicMock(),
            sl_liq_buffer_pct=1.0,
            trigger_price_type="CONTRACT_PRICE",
            account_id="acc01",
            snapshot_provider=provider,
        )

        for index in range(40):
            risk = manager._get_symbol_position_risk(f"S{index:02d}USDT")
            self.assertIsNotNone(risk)

        client.get_account.assert_called_once_with()
        client.get_position_risk.assert_not_called()

    def test_position_verification_merges_missing_risk_fields_once(self) -> None:
        client = MagicMock()
        client.base_url = "https://fapi.binance.com"
        client.background_requests.side_effect = lambda: nullcontext()
        client.get_account.return_value = self._account_payload(position_count=1)
        client.get_position_risk.return_value = [
            {
                "symbol": "S00USDT",
                "positionSide": "BOTH",
                "positionAmt": "-1",
                "entryPrice": "100",
                "markPrice": "102",
                "liquidationPrice": "145",
                "leverage": "2",
            }
        ]
        client.get_open_orders.return_value = []
        provider = AccountSnapshotProvider(
            client=client,
            store=self.store,
            account_id="acc01",
        )
        stream = BinanceUserStreamState(
            client=client,
            store=self.store,
            snapshot_provider=provider,
            account_id="acc01",
        )

        self.assertEqual(stream.websocket_base_url, "wss://fstream.binance.com/private/ws")
        self.assertTrue(stream.verify_rest())

        client.get_account.assert_called_once_with()
        client.get_position_risk.assert_called_once_with()
        client.get_open_orders.assert_called_once_with()
        client.get_order.assert_not_called()
        cached = provider.cached()
        self.assertIsNotNone(cached)
        risk = cached.position_risk_by_symbol()["S00USDT"]
        self.assertEqual(float(risk["liquidationPrice"]), 145.0)

    def test_periodic_empty_account_verification_skips_risk_and_order_scans(self) -> None:
        client = MagicMock()
        client.base_url = "https://fapi.binance.com"
        client.background_requests.side_effect = lambda: nullcontext()
        client.get_account.return_value = self._account_payload(position_count=0)
        provider = AccountSnapshotProvider(
            client=client,
            store=self.store,
            account_id="acc01",
        )
        stream = BinanceUserStreamState(
            client=client,
            store=self.store,
            snapshot_provider=provider,
            account_id="acc01",
        )

        self.assertTrue(
            stream.verify_rest(
                full_order_scan=False,
                force_account_snapshot=False,
            )
        )

        client.get_account.assert_called_once_with()
        client.get_position_risk.assert_not_called()
        client.get_open_orders.assert_not_called()

    def test_readonly_refresh_queries_no_user_trades_when_income_is_unchanged(self) -> None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - 10_000

        class ClientStub:
            def __init__(self) -> None:
                self.income_calls = []
                self.trade_calls = []

            def background_requests(self):
                return nullcontext()

            def get_income_history(self, **params):
                self.income_calls.append(params)
                return [
                    {
                        "time": now_ms,
                        "incomeType": "REALIZED_PNL",
                        "asset": "USDT",
                        "income": "5",
                        "symbol": "BTCUSDT",
                        "tradeId": "11",
                        "tranId": "realized-11",
                    },
                    {
                        "time": now_ms,
                        "incomeType": "COMMISSION",
                        "asset": "USDT",
                        "income": "-0.1",
                        "symbol": "BTCUSDT",
                        "tradeId": "11",
                        "tranId": "commission-11",
                    },
                ]

            def get_user_trades(self, **params):
                self.trade_calls.append(params)
                return [
                    {
                        "symbol": "BTCUSDT",
                        "id": 11,
                        "orderId": 7,
                        "time": now_ms,
                        "realizedPnl": "5",
                        "commission": "0.1",
                    }
                ]

        client = ClientStub()
        fetcher = TradeStatsFetcher(
            client=client,
            store=self.store,
            cache_ttl_sec=900,
        )

        first = fetcher.refresh_stats(account_id="acc01", lookback_days=30)
        second = fetcher.refresh_stats(account_id="acc01", lookback_days=30)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(first.total_trades, 1)
        self.assertEqual(first.net_realized_pnl, 4.9)
        self.assertEqual(len(client.income_calls), 2)
        self.assertTrue(all(call.get("income_type") is None for call in client.income_calls))
        self.assertEqual(len(client.trade_calls), 1)

    def test_readonly_full_page_resumes_with_persisted_page_cursor(self) -> None:
        event_ms = int(datetime.now(timezone.utc).timestamp() * 1000) - 10_000

        class ClientStub:
            def __init__(self) -> None:
                self.income_calls = []
                self.trade_calls = []

            def background_requests(self):
                return nullcontext()

            def get_income_history(self, **params):
                self.income_calls.append(params)
                page = int(params.get("page") or 1)
                if page == 1:
                    return [
                        {
                            "time": event_ms,
                            "incomeType": "COMMISSION",
                            "asset": "USDT",
                            "income": "-0.01",
                            "symbol": "BTCUSDT",
                            "tranId": f"commission-{index}",
                        }
                        for index in range(1000)
                    ]
                return [
                    {
                        "time": event_ms,
                        "incomeType": "COMMISSION",
                        "asset": "USDT",
                        "income": "-0.01",
                        "symbol": "BTCUSDT",
                        "tranId": "commission-1000",
                    }
                ]

            def get_user_trades(self, **params):
                self.trade_calls.append(params)
                return []

        self.store.set_lock_state(
            "readonly_trade_stats_cursor_v2",
            {
                "bootstrapped": True,
                "income_cursor_ms": event_ms - 60_000,
            },
        )
        client = ClientStub()
        fetcher = TradeStatsFetcher(client=client, store=self.store)

        first = fetcher._sync_incremental(lookback_days=30)
        first_state = self.store.get_lock_state("readonly_trade_stats_cursor_v2")
        second = fetcher._sync_incremental(lookback_days=30)
        second_state = self.store.get_lock_state("readonly_trade_stats_cursor_v2")

        self.assertEqual(first["income_requests"], 1)
        self.assertEqual(second["income_requests"], 1)
        self.assertEqual([call["page"] for call in client.income_calls], [1, 2])
        self.assertEqual(
            client.income_calls[1]["start_time"],
            client.income_calls[0]["start_time"],
        )
        self.assertEqual(
            client.income_calls[1]["end_time"],
            client.income_calls[0]["end_time"],
        )
        self.assertTrue(first_state["income_draining_full_page"])
        self.assertFalse(second_state["income_draining_full_page"])
        self.assertEqual(
            second_state["income_cursor_ms"],
            first_state["income_drain_end_ms"],
        )
        self.assertEqual(client.trade_calls, [])

    def test_ranking_cache_hit_uses_no_rest_weight(self) -> None:
        now = datetime.now(timezone.utc)
        day_utc = now.date().isoformat()
        self.store.put_market_data_cache(
            "exchange_info",
            ["AAAUSDT", "BBBUSDT"],
            (now + timedelta(days=1)).isoformat(),
        )
        self.store.put_market_data_cache(
            f"ticker_24hr:{day_utc}",
            [
                {"symbol": "AAAUSDT", "lastPrice": "12", "quoteVolume": "100"},
                {"symbol": "BBBUSDT", "lastPrice": "9", "quoteVolume": "100"},
            ],
            (now + timedelta(days=1)).isoformat(),
        )
        self.store.put_daily_open_price(day_utc, "AAAUSDT", 10.0, "TEST")
        self.store.put_daily_open_price(day_utc, "BBBUSDT", 10.0, "TEST")
        coordinator = BinanceRateLimitCoordinator()

        with patch("infra.binance_top10_monitor.get_exchange_info") as exchange_info, patch(
            "infra.binance_top10_monitor.get_24hr_ticker_data"
        ) as ticker, patch("infra.binance_top10_monitor.get_open_price_at_midnight") as open_price:
            ranking = build_top_gainers(
                top_n=2,
                volume_threshold=0,
                state_store=self.store,
                rate_limit_coordinator=coordinator,
            )

        self.assertEqual([row["symbol"] for row in ranking], ["AAAUSDT", "BBBUSDT"])
        exchange_info.assert_not_called()
        ticker.assert_not_called()
        open_price.assert_not_called()
        self.assertLessEqual(coordinator.usage()["background_weight_1m"], 50)

    def test_ranking_requests_all_market_ticker_at_most_once_per_day(self) -> None:
        now = datetime.now(timezone.utc)
        self.store.put_market_data_cache(
            "exchange_info",
            ["AAAUSDT"],
            (now + timedelta(days=1)).isoformat(),
        )
        with patch(
            "infra.binance_top10_monitor.get_24hr_ticker_data",
            return_value=[],
        ) as ticker:
            with self.assertRaises(Exception):
                build_top_gainers(state_store=self.store)
            with self.assertRaises(Exception):
                build_top_gainers(state_store=self.store)

        ticker.assert_called_once()

    def test_daily_open_uses_current_market_stream_namespace(self) -> None:
        stream = DailyOpenPriceStream(state_store=self.store)
        self.assertEqual(
            stream._websocket_url(),
            "wss://fstream.binance.com/market/stream",
        )

    def test_daily_open_stream_persists_each_symbol_once_per_utc_day(self) -> None:
        store = MagicMock()
        store.get_daily_open_prices.return_value = {}
        stream = DailyOpenPriceStream(state_store=store)
        payload = {
            "e": "kline",
            "s": "BTCUSDT",
            "k": {
                "s": "BTCUSDT",
                "i": "1d",
                "t": 1_777_075_200_000,
                "o": "60000",
            },
        }

        stream.handle_message(payload)
        stream.handle_message(payload)

        store.get_daily_open_prices.assert_called_once()
        store.put_daily_open_price.assert_called_once()

    def test_project_limiter_holds_non_trading_and_background_budgets(self) -> None:
        clock = [0.0]

        def advance(seconds: float) -> None:
            clock[0] += float(seconds)

        coordinator = BinanceRateLimitCoordinator(
            non_trading_weight_per_minute=50,
            background_weight_per_minute=40,
        )
        with patch("infra.binance_rate_limit.time.monotonic", side_effect=lambda: clock[0]), patch(
            "infra.binance_rate_limit.time.sleep",
            side_effect=advance,
        ):
            coordinator.acquire(path="/fapi/v1/income", weight=30, background=True)
            coordinator.acquire(path="/fapi/v1/income", weight=30, background=True)
            usage = coordinator.usage()

        self.assertGreaterEqual(clock[0], 60.0)
        self.assertLessEqual(usage["non_trading_weight_1m"], 50)
        self.assertLessEqual(usage["background_weight_1m"], 40)

    def test_user_stream_events_update_local_order_and_account_state(self) -> None:
        client = MagicMock()
        client.base_url = "https://fapi.binance.com"
        client.get_account.return_value = self._account_payload(position_count=0)
        provider = AccountSnapshotProvider(
            client=client,
            store=self.store,
            account_id="acc01",
        )
        provider.capture()
        stream = BinanceUserStreamState(
            client=client,
            store=self.store,
            snapshot_provider=provider,
            account_id="acc01",
        )

        stream.handle_event(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1_777_000_000_000,
                "o": {
                    "s": "BTCUSDT",
                    "i": 123,
                    "c": "tp-local",
                    "o": "TAKE_PROFIT_MARKET",
                    "S": "BUY",
                    "ps": "BOTH",
                    "X": "NEW",
                    "sp": "60000",
                },
            }
        )
        stream.handle_event(
            {
                "e": "ACCOUNT_UPDATE",
                "E": 1_777_000_000_100,
                "a": {
                    "B": [{"a": "USDT", "wb": "999"}],
                    "P": [
                        {
                            "s": "BTCUSDT",
                            "ps": "BOTH",
                            "pa": "-0.1",
                            "ep": "61000",
                            "up": "4",
                            "iw": "100",
                        }
                    ],
                },
            }
        )

        order = self.store.get_exchange_order_state(
            symbol="BTCUSDT",
            order_id=123,
            client_order_id="tp-local",
        )
        self.assertIsNotNone(order)
        self.assertEqual(order["status"], "NEW")
        positions = self.store.list_account_position_state(active_only=True)
        self.assertEqual(len(positions), 1)
        self.assertEqual(float(positions[0]["position_amt"]), -0.1)
        refreshed = provider.capture()
        self.assertEqual(refreshed.wallet_balance, 999.0)
        self.assertEqual(refreshed.unrealized_pnl, 4.0)
        self.assertEqual(refreshed.equity, 1003.0)
        client.get_account.assert_called_once_with()
        self.assertFalse(stream.entry_allowed())

    def test_algo_order_is_filled_only_after_actual_order_fill_event(self) -> None:
        client = MagicMock()
        client.base_url = "https://fapi.binance.com"
        client._map_algo_status.side_effect = BinanceFuturesClient._map_algo_status
        provider = AccountSnapshotProvider(
            client=client,
            store=self.store,
            account_id="acc01",
        )
        stream = BinanceUserStreamState(
            client=client,
            store=self.store,
            snapshot_provider=provider,
            account_id="acc01",
        )

        stream.handle_event(
            {
                "e": "ALGO_UPDATE",
                "E": 1_777_000_000_000,
                "o": {
                    "symbol": "BTCUSDT",
                    "algoId": 777,
                    "clientAlgoId": "sl-parent",
                    "orderType": "STOP_MARKET",
                    "algoStatus": "TRIGGERED",
                    "actualOrderId": 888,
                },
            }
        )
        parent = self.store.get_exchange_order_state(
            symbol="BTCUSDT",
            order_id=777,
        )
        self.assertEqual(parent["status"], "TRIGGERED")

        self.store.reconcile_open_order_state(
            [{"symbol": "BTCUSDT", "orderId": 888, "status": "NEW"}]
        )
        linked_open_parent = self.store.get_exchange_order_state(
            symbol="BTCUSDT",
            order_id=777,
        )
        self.assertEqual(linked_open_parent["status"], "NEW")

        stream.handle_event(
            {
                "e": "ORDER_TRADE_UPDATE",
                "E": 1_777_000_000_100,
                "o": {
                    "s": "BTCUSDT",
                    "i": 888,
                    "c": "actual-stop",
                    "o": "MARKET",
                    "S": "BUY",
                    "X": "FILLED",
                    "z": "0.1",
                },
            }
        )

        linked_parent = self.store.get_exchange_order_state(
            symbol="BTCUSDT",
            order_id=777,
        )
        self.assertEqual(linked_parent["status"], "FILLED")

    def test_four_cashflow_accounts_are_staggered_at_default_seconds(self) -> None:
        class CashflowSampler:
            sync_cashflows = True

            def __init__(self) -> None:
                self.calls = []

            def sync_cashflows_once(self, now_utc):
                self.calls.append(now_utc)
                return 0

        samplers = {f"acc0{index}": CashflowSampler() for index in range(1, 5)}
        cfg = ServiceRuntimeConfig(
            timezone_name="UTC",
            entry_hour=23,
            entry_minute=59,
            entry_misfire_grace_min=1,
            entry_catchup_enabled=False,
            daily_loss_cut_enabled=False,
            daily_loss_cut_hour=23,
            daily_loss_cut_minute=59,
            manager_interval_sec=60,
            manager_max_catch_up_runs=1,
            loop_sleep_sec=1.0,
            run_manage_on_startup=False,
        )
        service = StrategyRuntimeService(
            strategy=MagicMock(),
            manager=MagicMock(),
            cfg=cfg,
            now_monotonic=0.0,
            account_runtimes={
                account_id: {
                    "mode": "full",
                    "balance_sampler": sampler,
                }
                for account_id, sampler in samplers.items()
            },
        )
        base = datetime(2026, 8, 24, 12, 0, tzinfo=ZoneInfo("UTC"))
        try:
            for second in (4, 5, 19, 20, 34, 35, 49, 50, 59):
                service._run_cashflow_sync_if_due(base.replace(second=second))

            self.assertEqual([len(samplers[f"acc0{index}"].calls) for index in range(1, 5)], [1, 1, 1, 1])
            self.assertEqual(
                [samplers[f"acc0{index}"].calls[0].second for index in range(1, 5)],
                [5, 20, 35, 50],
            )
        finally:
            service._entry_executor.shutdown(wait=False, cancel_futures=True)
            service._manage_executor.shutdown(wait=False, cancel_futures=True)
            service._scheduled_executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
