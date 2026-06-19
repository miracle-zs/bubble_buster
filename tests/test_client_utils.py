import importlib.util
import unittest
from unittest.mock import MagicMock, patch

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from infra.binance_futures_client import (
    BinanceAPIError,
    BinanceFuturesClient,
    SymbolRules,
    ceil_to_step,
    floor_to_step,
)


class ClientUtilsTest(unittest.TestCase):
    def test_signed_request_refreshes_timestamp_and_signature_on_retry(self) -> None:
        client = BinanceFuturesClient(
            api_key="k",
            api_secret="s",
            retry_count=2,
            retry_delay_sec=0.1,
        )

        first_response = MagicMock()
        first_response.status_code = 400
        first_response.text = '{"code": -1021, "msg": "Timestamp outside recvWindow"}'
        first_response.json.return_value = {
            "code": -1021,
            "msg": "Timestamp outside recvWindow",
        }
        second_response = MagicMock()
        second_response.status_code = 200
        second_response.text = '{"ok": true}'
        second_response.json.return_value = {"ok": True}
        client.session.request = MagicMock(side_effect=[first_response, second_response])

        with patch("infra.binance_futures_client.time.time", side_effect=[1000.0, 1000.5, 1001.0]):
            with patch("infra.binance_futures_client.time.sleep"):
                result = client._request("GET", "/signed", params={"symbol": "BTCUSDT"}, signed=True)

        self.assertEqual(result, {"ok": True})
        request_params = [
            call.kwargs["params"]
            for call in client.session.request.call_args_list
        ]
        self.assertEqual([params["timestamp"] for params in request_params], [1000000, 1001000])
        self.assertNotEqual(request_params[0]["signature"], request_params[1]["signature"])

    def test_set_margin_type_preserves_non_numeric_api_error(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        network_error = BinanceAPIError("NETWORK", "connection reset")
        client._request = MagicMock(side_effect=network_error)  # type: ignore[method-assign]

        with self.assertRaises(BinanceAPIError) as caught:
            client.set_margin_type("BTCUSDT")

        self.assertIs(caught.exception, network_error)

    def test_floor_and_ceil_step(self) -> None:
        self.assertAlmostEqual(floor_to_step(1.2345, 0.01), 1.23)
        self.assertAlmostEqual(ceil_to_step(1.2345, 0.01), 1.24)

    def test_normalize_qty_and_trigger_price(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._symbol_rules_cache = {
            "BTCUSDT": SymbolRules(
                symbol="BTCUSDT",
                tick_size=0.1,
                step_size=0.001,
                min_qty=0.001,
                min_notional=5.0,
            )
        }

        qty = client.normalize_order_qty("BTCUSDT", notional=100.0, price=50000.0)
        self.assertAlmostEqual(qty, 0.002)

        sl_price = client.normalize_trigger_price("BTCUSDT", price=59000.03, round_up=True)
        tp_price = client.normalize_trigger_price("BTCUSDT", price=40000.08, round_up=False)
        self.assertAlmostEqual(sl_price, 59000.1)
        self.assertAlmostEqual(tp_price, 40000.0)

    def test_diagnose_order_qty_reports_reject_reason(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._symbol_rules_cache = {
            "ALTUSDT": SymbolRules(
                symbol="ALTUSDT",
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1000.0,
                min_notional=5.0,
            )
        }

        diagnostic = client.diagnose_order_qty("ALTUSDT", notional=92.0, price=0.16834)

        self.assertTrue(diagnostic["has_rules"])
        self.assertEqual(diagnostic["reject_reason"], "qty_below_min_qty")
        self.assertAlmostEqual(diagnostic["raw_qty"], 546.5130093858)
        self.assertAlmostEqual(diagnostic["normalized_qty"], 546.0)
        self.assertAlmostEqual(diagnostic["normalized_notional"], 91.91364)
        self.assertEqual(client.normalize_order_qty("ALTUSDT", notional=92.0, price=0.16834), 0.0)

    def test_diagnose_order_qty_reports_missing_symbol_rules(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client.get_symbol_rules = MagicMock(return_value={})  # type: ignore[method-assign]

        diagnostic = client.diagnose_order_qty("UNKNOWNUSDT", notional=92.0, price=0.16834)

        self.assertFalse(diagnostic["has_rules"])
        self.assertEqual(diagnostic["reject_reason"], "missing_symbol_rules")
        self.assertEqual(client.normalize_order_qty("UNKNOWNUSDT", notional=92.0, price=0.16834), 0.0)

    def test_format_order_params(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._symbol_rules_cache = {
            "ALTUSDT": SymbolRules(
                symbol="ALTUSDT",
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
            )
        }

        self.assertEqual(client.format_order_qty("ALTUSDT", 123.999), "123")
        self.assertEqual(client.format_trigger_price("ALTUSDT", 0.01234567, round_up=False), "0.0123")

    def test_create_conditional_order_uses_algo_endpoint(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        seen = {}

        def _fake_request(method, path, params=None, signed=False):  # type: ignore[no-untyped-def]
            if method == "POST" and path == "/fapi/v1/algoOrder":
                seen["algo_params"] = dict(params or {})
                return {
                    "algoId": 123456,
                    "clientAlgoId": "cid-1",
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "orderType": "STOP_MARKET",
                    "algoStatus": "NEW",
                    "triggerPrice": "50000",
                    "quantity": "0.01",
                }
            raise AssertionError(f"Unexpected request: {method} {path} params={params} signed={signed}")

        client._request = MagicMock(side_effect=_fake_request)  # type: ignore[method-assign]

        order = client.create_order(
            symbol="BTCUSDT",
            side="BUY",
            type="STOP_MARKET",
            stopPrice="50000",
            closePosition=True,
            workingType="CONTRACT_PRICE",
            newClientOrderId="cid-1",
        )

        first_call = client._request.call_args_list[0]  # type: ignore[attr-defined]
        self.assertEqual(first_call.args[0], "POST")
        self.assertEqual(first_call.args[1], "/fapi/v1/algoOrder")
        self.assertEqual(order["orderId"], 123456)
        self.assertEqual(order["clientOrderId"], "cid-1")
        self.assertEqual(order["status"], "NEW")
        self.assertEqual(order["stopPrice"], "50000")
        algo_params = seen["algo_params"]
        self.assertEqual(algo_params["type"], "STOP_MARKET")
        self.assertEqual(algo_params["triggerPrice"], "50000")
        self.assertEqual(algo_params["clientAlgoId"], "cid-1")

    def test_get_order_falls_back_to_algo_endpoint(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")

        def _fake_request(method, path, params=None, signed=False):  # type: ignore[no-untyped-def]
            if method == "GET" and path == "/fapi/v1/order":
                raise BinanceAPIError(-2013, "Order does not exist.")
            if method == "GET" and path == "/fapi/v1/algoOrder":
                return {
                    "algoId": 654321,
                    "clientAlgoId": "cid-2",
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "orderType": "TAKE_PROFIT_MARKET",
                    "algoStatus": "TRIGGERED",
                    "actualOrderId": 9988,
                }
            raise AssertionError(f"Unexpected request: {method} {path} params={params} signed={signed}")

        client._request = MagicMock(side_effect=_fake_request)  # type: ignore[method-assign]

        order = client.get_order(symbol="BTCUSDT", order_id=654321, orig_client_order_id="cid-2")
        self.assertEqual(order["orderId"], 654321)
        self.assertEqual(order["clientOrderId"], "cid-2")
        self.assertEqual(order["status"], "FILLED")

    def test_cancel_order_falls_back_to_algo_endpoint(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")

        def _fake_request(method, path, params=None, signed=False):  # type: ignore[no-untyped-def]
            if method == "DELETE" and path == "/fapi/v1/order":
                raise BinanceAPIError(-2011, "Unknown order sent.")
            if method == "DELETE" and path == "/fapi/v1/algoOrder":
                return {
                    "algoId": 654322,
                    "clientAlgoId": "cid-3",
                    "symbol": "BTCUSDT",
                    "side": "BUY",
                    "orderType": "STOP_MARKET",
                    "algoStatus": "CANCELED",
                }
            raise AssertionError(f"Unexpected request: {method} {path} params={params} signed={signed}")

        client._request = MagicMock(side_effect=_fake_request)  # type: ignore[method-assign]

        order = client.cancel_order(symbol="BTCUSDT", order_id=654322, orig_client_order_id="cid-3")
        self.assertEqual(order["orderId"], 654322)
        self.assertEqual(order["clientOrderId"], "cid-3")
        self.assertEqual(order["status"], "CANCELED")


if __name__ == "__main__":
    unittest.main()
