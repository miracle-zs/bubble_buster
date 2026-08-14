import importlib.util
import unittest

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from infra.trade_stats_fetcher import TradeStatsFetcher


def _income_record(seq: int, income: float, income_type: str = "REALIZED_PNL") -> dict:
    return {
        "symbol": "BTCUSDT",
        "incomeType": income_type,
        "income": str(income),
        "asset": "USDT",
        "time": 1770000000000 + seq,
        "tranId": seq,
        "tradeId": str(seq),
    }


class FakeIncomeClient:
    def __init__(
        self,
        pages: dict[int, list[dict]],
        trades_by_symbol: dict[str, list[dict]] | None = None,
        income_pages_by_type: dict[str, dict[int, list[dict]]] | None = None,
    ):
        self.pages = pages
        self.income_pages_by_type = {"REALIZED_PNL": pages, **(income_pages_by_type or {})}
        self.trades_by_symbol = trades_by_symbol or {}
        self.calls: list[dict] = []
        self.trade_calls: list[dict] = []

    def get_income_history(self, **params):
        self.calls.append(dict(params))
        income_type = str(params.get("income_type") or "")
        pages = self.income_pages_by_type.get(income_type, {})
        return list(pages.get(int(params.get("page") or 1), []))

    def get_user_trades(self, **params):
        self.trade_calls.append(dict(params))
        if params["symbol"] in self.trades_by_symbol:
            rows = list(self.trades_by_symbol[params["symbol"]])
        else:
            # Default fixture behavior: one completed order per non-zero income row.
            income_rows = [
                record
                for page in self.pages.values()
                for record in page
                if record.get("symbol") == params["symbol"] and float(record.get("income", 0)) != 0
            ]
            rows = [
                {
                    "symbol": record["symbol"],
                    "id": int(record["tradeId"]),
                    "orderId": int(record["tradeId"]),
                    "realizedPnl": record["income"],
                    "time": record["time"],
                }
                for record in income_rows
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


def _trade_record(trade_id: int, order_id: int, income: float) -> dict:
    return {
        "symbol": "ONUSDT",
        "id": trade_id,
        "orderId": order_id,
        "realizedPnl": str(income),
        "time": 1770000000000,
    }


def test_fetch_stats_paginates_realized_pnl_income_history() -> None:
    page_one = [
        *[_income_record(i, 1.0) for i in range(600)],
        *[_income_record(600 + i, -0.5) for i in range(400)],
    ]
    page_two = [
        _income_record(1000, 2.0),
        _income_record(1001, -1.0),
    ]
    client = FakeIncomeClient({1: page_one, 2: page_two})
    fetcher = TradeStatsFetcher(client=client)

    stats = fetcher.fetch_stats(account_id="readonly01", lookback_days=30)

    assert stats is not None
    realized_calls = [call for call in client.calls if call["income_type"] == "REALIZED_PNL"]
    assert [call["page"] for call in realized_calls] == [1, 2]
    assert all(call["limit"] == 1000 for call in realized_calls)
    assert stats.total_trades == 1002
    assert stats.win_count == 601
    assert stats.loss_count == 401
    assert stats.total_realized_pnl == 401.0
    assert stats.gross_profit == 602.0
    assert stats.gross_loss == 201.0
    assert stats.profit_factor == 2.995


def test_fetch_stats_counts_completed_orders_not_partial_fills() -> None:
    incomes = [0.0616, 0.21175, 0.16852, 0.20574, 0.20466, 0.0567, 0.07182, 0.08671]
    income_time_ms = 1786708800000
    income_rows = [
        _income_record(i, income)
        | {"symbol": "ONUSDT", "tradeId": str(96910192 + i), "time": income_time_ms}
        for i, income in enumerate(incomes)
    ]
    trade_rows = [_trade_record(96910192 + i, 1394222026, income) for i, income in enumerate(incomes)]
    client = FakeIncomeClient(
        {1: income_rows},
        trades_by_symbol={"ONUSDT": trade_rows},
    )
    fetcher = TradeStatsFetcher(client=client)

    stats = fetcher.fetch_stats(account_id="readonly02", lookback_days=30)

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


def test_fetch_stats_exposes_net_pnl_after_commission_and_funding() -> None:
    income_rows = [_income_record(1, 1.0675)]
    client = FakeIncomeClient(
        {1: income_rows},
        income_pages_by_type={
            "COMMISSION": {1: [_income_record(2, -0.14974522, "COMMISSION")]},
            "FUNDING_FEE": {1: [_income_record(3, -0.0125, "FUNDING_FEE")]},
        },
    )
    fetcher = TradeStatsFetcher(client=client)

    stats = fetcher.fetch_stats(account_id="readonly02", lookback_days=30)

    assert stats is not None
    assert stats.total_realized_pnl == 1.0675
    assert stats.commission_usdt == -0.14974522
    assert stats.funding_fee_usdt == -0.0125
    assert stats.net_realized_pnl == 0.90525478


def test_fetch_stats_calculates_outcomes_per_completed_order() -> None:
    incomes = [0.6, 0.4, -0.3, -0.2]
    income_rows = [
        _income_record(i, income)
        | {"symbol": "ONUSDT", "tradeId": str(100 + i)}
        for i, income in enumerate(incomes)
    ]
    trade_rows = [
        _trade_record(100, 2001, 0.6),
        _trade_record(101, 2001, 0.4),
        _trade_record(102, 2002, -0.3),
        _trade_record(103, 2002, -0.2),
    ]
    client = FakeIncomeClient(
        {1: income_rows},
        trades_by_symbol={"ONUSDT": trade_rows},
    )
    fetcher = TradeStatsFetcher(client=client)

    stats = fetcher.fetch_stats(account_id="readonly02", lookback_days=30)

    assert stats is not None
    assert stats.total_trades == 2
    assert stats.win_count == 1
    assert stats.loss_count == 1
    assert stats.win_rate_pct == 50.0
    assert stats.gross_profit == 1.0
    assert stats.gross_loss == 0.5
    assert stats.profit_factor == 2.0


def test_fetch_stats_counts_breakeven_completed_order() -> None:
    income_rows = [_income_record(300, 0.0) | {"symbol": "ONUSDT", "tradeId": "300"}]
    trade_rows = [_trade_record(300, 3001, 0.0)]
    client = FakeIncomeClient(
        {1: income_rows},
        trades_by_symbol={"ONUSDT": trade_rows},
    )
    fetcher = TradeStatsFetcher(client=client)

    stats = fetcher.fetch_stats(account_id="readonly02", lookback_days=30)

    assert stats is not None
    assert stats.total_trades == 1
    assert stats.win_count == 0
    assert stats.loss_count == 0
    assert stats.win_rate_pct == 0.0


def test_fetch_stats_caches_temporary_fetch_failure() -> None:
    client = FailingTradeStatsClient()
    fetcher = TradeStatsFetcher(client=client, cache_ttl_sec=300)

    assert fetcher.fetch_stats(account_id="readonly02", lookback_days=30) is None
    assert fetcher.fetch_stats(account_id="readonly02", lookback_days=30) is None

    assert client.income_calls == 1
