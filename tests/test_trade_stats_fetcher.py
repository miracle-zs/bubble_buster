import importlib.util
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import unittest

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from core.state_store import StateStore
from infra.trade_stats_fetcher import TradeStats, TradeStatsFetcher


def _income_record(
    seq: int,
    income: float,
    income_type: str = "REALIZED_PNL",
    *,
    time_ms: int | None = None,
) -> dict:
    return {
        "symbol": "BTCUSDT",
        "incomeType": income_type,
        "income": str(income),
        "asset": "USDT",
        "time": time_ms if time_ms is not None else int(time.time() * 1000) - 60_000 + seq,
        "tranId": seq,
        "tradeId": str(seq),
    }


def _make_store(tmp_dir: str, account_id: str = "readonly01") -> StateStore:
    db_path = str(Path(tmp_dir) / "state.db")
    schema_path = str(Path(__file__).resolve().parents[1] / "schema.sql")
    store = StateStore(db_path=db_path, schema_path=schema_path, account_id=account_id)
    store.init_schema()
    return store


class FakeIncomeClient:
    def __init__(
        self,
        pages: dict[int, list[dict]],
        trades_by_symbol: dict[str, list[dict]] | None = None,
    ):
        self.pages = pages
        self.trades_by_symbol = trades_by_symbol or {}
        self.calls: list[dict] = []
        self.trade_calls: list[dict] = []

    def get_income_history(self, **params):
        self.calls.append(dict(params))
        return list(self.pages.get(int(params.get("page") or 1), []))

    def get_user_trades(self, **params):
        self.trade_calls.append(dict(params))
        if params["symbol"] in self.trades_by_symbol:
            rows = list(self.trades_by_symbol[params["symbol"]])
        else:
            rows = [
                {
                    "symbol": record["symbol"],
                    "id": int(record["tradeId"]),
                    "orderId": int(record["tradeId"]),
                    "realizedPnl": record["income"],
                    "time": record["time"],
                }
                for page in self.pages.values()
                for record in page
                if record.get("symbol") == params["symbol"]
                and record.get("incomeType") == "REALIZED_PNL"
                and float(record.get("income", 0)) != 0
            ]

        if params.get("from_id") is not None:
            rows = [row for row in rows if int(row["id"]) >= int(params["from_id"])]
        return rows[: int(params.get("limit") or 1000)]


class FailingTradeStatsClient:
    def __init__(self):
        self.income_calls = 0

    def get_income_history(self, **params):
        self.income_calls += 1
        raise RuntimeError("temporary Binance rate limit")


def _trade_record(trade_id: int, order_id: int, income: float, time_ms: int | None = None) -> dict:
    return {
        "symbol": "ONUSDT",
        "id": trade_id,
        "orderId": order_id,
        "realizedPnl": str(income),
        "time": time_ms if time_ms is not None else int(time.time() * 1000) - 60_000,
    }


def test_refresh_stats_paginates_unfiltered_income_backfill() -> None:
    page_one = [
        *[_income_record(i, 1.0) for i in range(600)],
        *[_income_record(600 + i, -0.5) for i in range(400)],
    ]
    page_two = [
        _income_record(1000, 2.0),
        _income_record(1001, -1.0),
    ]
    client = FakeIncomeClient({1: page_one, 2: page_two})
    with tempfile.TemporaryDirectory() as td:
        fetcher = TradeStatsFetcher(client=client, store=_make_store(td, "readonly01"))
        stats = fetcher.refresh_stats(account_id="readonly01", lookback_days=30)

    assert stats is not None
    assert [call["page"] for call in client.calls] == [1, 2]
    assert all(call["income_type"] is None for call in client.calls)
    assert all(call["limit"] == 1000 for call in client.calls)
    assert stats.total_trades == 1002
    assert stats.win_count == 601
    assert stats.loss_count == 401
    assert stats.total_realized_pnl == 401.0
    assert stats.gross_profit == 602.0
    assert stats.gross_loss == 201.0
    assert stats.profit_factor == 2.995


def test_refresh_stats_counts_completed_orders_not_partial_fills() -> None:
    incomes = [0.0616, 0.21175, 0.16852, 0.20574, 0.20466, 0.0567, 0.07182, 0.08671]
    income_time_ms = int(time.time() * 1000) - 86400 * 1000
    income_rows = [
        _income_record(i, income, time_ms=income_time_ms)
        | {"symbol": "ONUSDT", "tradeId": str(96910192 + i)}
        for i, income in enumerate(incomes)
    ]
    trade_rows = [
        _trade_record(96910192 + i, 1394222026, income, time_ms=income_time_ms)
        for i, income in enumerate(incomes)
    ]
    client = FakeIncomeClient({1: income_rows}, trades_by_symbol={"ONUSDT": trade_rows})
    with tempfile.TemporaryDirectory() as td:
        fetcher = TradeStatsFetcher(client=client, store=_make_store(td, "readonly02"))
        stats = fetcher.refresh_stats(account_id="readonly02", lookback_days=30)

    assert stats is not None
    assert stats.total_trades == 1
    assert stats.win_count == 1
    assert stats.loss_count == 0
    assert stats.total_realized_pnl == 1.0675
    assert stats.gross_profit == 1.0675
    assert stats.avg_win == 1.0675
    assert len(client.trade_calls) == 1
    assert client.trade_calls[0]["start_time"] <= income_time_ms - 1000
    assert client.trade_calls[0]["end_time"] >= income_time_ms + 1000


def test_refresh_stats_exposes_net_pnl_after_commission_and_funding() -> None:
    income_rows = [
        _income_record(1, 1.0675),
        _income_record(2, -0.14974522, "COMMISSION"),
        _income_record(3, -0.0125, "FUNDING_FEE"),
    ]
    client = FakeIncomeClient({1: income_rows})
    with tempfile.TemporaryDirectory() as td:
        fetcher = TradeStatsFetcher(client=client, store=_make_store(td, "readonly02"))
        stats = fetcher.refresh_stats(account_id="readonly02", lookback_days=30)

    assert stats is not None
    assert stats.total_realized_pnl == 1.0675
    assert stats.commission_usdt == -0.14974522
    assert stats.funding_fee_usdt == -0.0125
    assert stats.net_realized_pnl == 0.90525478


def test_refresh_stats_calculates_outcomes_per_completed_order() -> None:
    incomes = [0.6, 0.4, -0.3, -0.2]
    income_rows = [
        _income_record(i, income) | {"symbol": "ONUSDT", "tradeId": str(100 + i)}
        for i, income in enumerate(incomes)
    ]
    trade_rows = [
        _trade_record(100, 2001, 0.6),
        _trade_record(101, 2001, 0.4),
        _trade_record(102, 2002, -0.3),
        _trade_record(103, 2002, -0.2),
    ]
    client = FakeIncomeClient({1: income_rows}, trades_by_symbol={"ONUSDT": trade_rows})
    with tempfile.TemporaryDirectory() as td:
        fetcher = TradeStatsFetcher(client=client, store=_make_store(td, "readonly02"))
        stats = fetcher.refresh_stats(account_id="readonly02", lookback_days=30)

    assert stats is not None
    assert stats.total_trades == 2
    assert stats.win_count == 1
    assert stats.loss_count == 1
    assert stats.win_rate_pct == 50.0
    assert stats.gross_profit == 1.0
    assert stats.gross_loss == 0.5
    assert stats.profit_factor == 2.0


def test_refresh_stats_counts_breakeven_completed_order() -> None:
    income_rows = [_income_record(300, 0.0) | {"symbol": "ONUSDT", "tradeId": "300"}]
    trade_rows = [_trade_record(300, 3001, 0.0)]
    client = FakeIncomeClient({1: income_rows}, trades_by_symbol={"ONUSDT": trade_rows})
    with tempfile.TemporaryDirectory() as td:
        fetcher = TradeStatsFetcher(client=client, store=_make_store(td, "readonly02"))
        stats = fetcher.refresh_stats(account_id="readonly02", lookback_days=30)

    assert stats is not None
    assert stats.total_trades == 1
    assert stats.win_count == 0
    assert stats.loss_count == 0
    assert stats.win_rate_pct == 0.0


def test_refresh_stats_returns_none_on_initial_failure() -> None:
    client = FailingTradeStatsClient()
    with tempfile.TemporaryDirectory() as td:
        fetcher = TradeStatsFetcher(client=client, store=_make_store(td, "readonly02"))
        assert fetcher.refresh_stats(account_id="readonly02", lookback_days=30) is None
        assert fetcher.get_cached_stats(account_id="readonly02", lookback_days=30) is None
    assert client.income_calls == 1


def test_refresh_stats_deduplicates_concurrent_refreshes() -> None:
    class SlowFetcher(TradeStatsFetcher):
        def __init__(self, store: StateStore) -> None:
            super().__init__(client=object(), store=store)  # type: ignore[arg-type]
            self.calls = 0
            self.entered = threading.Event()
            self.release = threading.Event()

        def _sync_incremental(self, *, lookback_days: int) -> dict[str, int]:
            return {"income_inserted": 0, "trades_inserted": 0, "income_requests": 0, "trade_requests": 0}

        def _calculate_stats_from_store(self, *, lookback_days: int) -> TradeStats:
            self.calls += 1
            self.entered.set()
            assert self.release.wait(timeout=2.0)
            return TradeStats(
                total_realized_pnl=1.0,
                total_trades=1,
                win_count=1,
                loss_count=0,
                win_rate_pct=100.0,
                gross_profit=1.0,
                gross_loss=0.0,
                profit_factor=None,
                avg_win=1.0,
                avg_loss=0.0,
                last_updated_utc="2026-08-15T00:00:00+00:00",
            )

    with tempfile.TemporaryDirectory() as td:
        fetcher = SlowFetcher(_make_store(td, "readonly01"))
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(fetcher.refresh_stats, account_id="readonly01", lookback_days=30)
            assert fetcher.entered.wait(timeout=1.0)
            second = pool.submit(fetcher.refresh_stats, account_id="readonly01", lookback_days=30)
            fetcher.release.set()
            first_result = first.result(timeout=2.0)
            second_result = second.result(timeout=2.0)

    assert fetcher.calls == 1
    assert first_result == second_result


def test_refresh_failure_keeps_last_good_stats() -> None:
    stats = TradeStats(
        total_realized_pnl=1.0,
        total_trades=1,
        win_count=1,
        loss_count=0,
        win_rate_pct=100.0,
        gross_profit=1.0,
        gross_loss=0.0,
        profit_factor=None,
        avg_win=1.0,
        avg_loss=0.0,
        last_updated_utc="2026-08-15T00:00:00+00:00",
    )
    responses: list[object] = [stats, RuntimeError("temporary Binance rate limit")]

    with tempfile.TemporaryDirectory() as td:
        fetcher = TradeStatsFetcher(client=object(), store=_make_store(td, "readonly01"))  # type: ignore[arg-type]
        fetcher._sync_incremental = lambda *, lookback_days: {"income_inserted": 0, "trades_inserted": 0, "income_requests": 0, "trade_requests": 0}  # type: ignore[method-assign]

        def fake_calc(*, lookback_days: int) -> TradeStats:
            response = responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response  # type: ignore[return-value]

        fetcher._calculate_stats_from_store = fake_calc  # type: ignore[method-assign]

        assert fetcher.refresh_stats(account_id="readonly01", lookback_days=30) == stats
        assert fetcher.refresh_stats(account_id="readonly01", lookback_days=30) == stats
        assert fetcher.get_cached_stats(account_id="readonly01", lookback_days=30) == stats
