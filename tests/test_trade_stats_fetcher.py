import importlib.util
import unittest

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from infra.trade_stats_fetcher import TradeStatsFetcher


def _income_record(seq: int, income: float) -> dict:
    return {
        "symbol": "BTCUSDT",
        "incomeType": "REALIZED_PNL",
        "income": str(income),
        "asset": "USDT",
        "time": 1770000000000 + seq,
        "tranId": seq,
        "tradeId": str(seq),
    }


class FakeIncomeClient:
    def __init__(self, pages: dict[int, list[dict]]):
        self.pages = pages
        self.calls: list[dict] = []

    def get_income_history(self, **params):
        self.calls.append(dict(params))
        return list(self.pages.get(int(params.get("page") or 1), []))


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
    assert [call["page"] for call in client.calls] == [1, 2]
    assert all(call["limit"] == 1000 for call in client.calls)
    assert all(call["income_type"] == "REALIZED_PNL" for call in client.calls)
    assert stats.total_trades == 1002
    assert stats.win_count == 601
    assert stats.loss_count == 401
    assert stats.total_realized_pnl == 401.0
    assert stats.gross_profit == 602.0
    assert stats.gross_loss == 201.0
    assert stats.profit_factor == 2.995
