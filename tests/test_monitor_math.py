import importlib.util
import unittest
from unittest.mock import patch

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from infra.binance_top10_monitor import (
    ApiWeightLimiter,
    build_top_gainers,
    calculate_daily_percentage_change,
    get_open_price_at_midnight,
)


class MonitorMathTest(unittest.TestCase):
    def test_daily_change(self) -> None:
        self.assertAlmostEqual(calculate_daily_percentage_change(110, 100), 10.0)
        self.assertAlmostEqual(calculate_daily_percentage_change(90, 100), -10.0)
        self.assertEqual(calculate_daily_percentage_change(100, 0), 0.0)

    @patch("infra.binance_top10_monitor.get_klines_data")
    def test_midnight_open_uses_single_kline_fast_path(self, mock_klines) -> None:
        midnight = 1700000000000
        mock_klines.return_value = [[midnight, "1.2345"]]
        limiter = ApiWeightLimiter(max_weight_per_minute=1000, min_request_interval_ms=0)

        price = get_open_price_at_midnight("BTCUSDT", midnight, rate_limiter=limiter)

        self.assertAlmostEqual(price or 0.0, 1.2345)
        self.assertEqual(mock_klines.call_count, 1)
        kwargs = mock_klines.call_args.kwargs
        self.assertEqual(kwargs.get("limit"), 1)
        self.assertIs(kwargs.get("rate_limiter"), limiter)

    @patch("infra.binance_top10_monitor.get_open_price_at_midnight")
    @patch("infra.binance_top10_monitor.get_utc_midnight_timestamp")
    @patch("infra.binance_top10_monitor.get_24hr_ticker_data")
    @patch("infra.binance_top10_monitor.get_exchange_info")
    def test_build_top_gainers_keeps_ranking_logic(
        self,
        mock_exchange_info,
        mock_ticker,
        mock_midnight_ts,
        mock_midnight_open,
    ) -> None:
        mock_exchange_info.return_value = ["AAAUSDT", "BBBUSDT", "CCCUSDT"]
        mock_ticker.return_value = [
            {"symbol": "AAAUSDT", "lastPrice": "12", "quoteVolume": "100"},
            {"symbol": "BBBUSDT", "lastPrice": "9", "quoteVolume": "100"},
            {"symbol": "CCCUSDT", "lastPrice": "20", "quoteVolume": "1"},
        ]
        mock_midnight_ts.return_value = 1700000000000
        mock_midnight_open.side_effect = lambda symbol, *_args, **_kwargs: {
            "AAAUSDT": 10.0,
            "BBBUSDT": 10.0,
            "CCCUSDT": 10.0,
        }.get(symbol)

        top = build_top_gainers(top_n=2, volume_threshold=10, max_workers=4)

        self.assertEqual(len(top), 2)
        self.assertEqual(top[0]["symbol"], "AAAUSDT")
        self.assertEqual(top[1]["symbol"], "BBBUSDT")
        called_symbols = {str(call.args[0]) for call in mock_midnight_open.call_args_list}
        self.assertEqual(called_symbols, {"AAAUSDT", "BBBUSDT"})
        self.assertEqual(mock_exchange_info.call_args.kwargs.get("request_weight"), 1)
        self.assertEqual(mock_ticker.call_args.kwargs.get("request_weight"), 40)


if __name__ == "__main__":
    unittest.main()
