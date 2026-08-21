import importlib.util
import unittest
from unittest.mock import MagicMock, patch

import requests

if importlib.util.find_spec("requests") is None:
    raise unittest.SkipTest("requests is not installed")

from infra.binance_futures_client import (
    BinanceAPIError,
    BinanceRateLimitError,
    BinanceFuturesClient,
    BinanceRateLimitCoordinator,
    OrderStateUnknownError,
    SymbolRules,
    ceil_to_step,
    floor_to_step,
)


class ClientUtilsTest(unittest.TestCase):
    def test_rate_limit_does_not_retry_immediately_and_records_retry_after(self) -> None:
        coordinator = BinanceRateLimitCoordinator(fallback_retry_after_sec=60.0)
        client = BinanceFuturesClient(
            api_key="k",
            api_secret="s",
            retry_count=3,
            rate_limit_coordinator=coordinator,
        )
        response = MagicMock()
        response.status_code = 429
        response.text = '{"code": -1003, "msg": "Too many requests"}'
        response.headers = {"Retry-After": "17"}
        response.json.return_value = {"code": -1003, "msg": "Too many requests"}
        client.session.request = MagicMock(return_value=response)

        with self.assertRaises(BinanceRateLimitError) as caught:
            client._request("GET", "/fapi/v2/positionRisk", signed=True)

        self.assertEqual(client.session.request.call_count, 1)
        self.assertEqual(caught.exception.retry_after_sec, 17.0)
        self.assertTrue(coordinator.is_blocked())

    def test_clients_can_share_one_ip_rate_limit_coordinator(self) -> None:
        coordinator = BinanceRateLimitCoordinator()
        first = BinanceFuturesClient(api_key="k1", api_secret="s1", rate_limit_coordinator=coordinator)
        second = BinanceFuturesClient(api_key="k2", api_secret="s2", rate_limit_coordinator=coordinator)

        self.assertIs(first.rate_limit_coordinator, coordinator)
        self.assertIs(second.rate_limit_coordinator, coordinator)

    def test_entry_prewarm_warms_signed_path_rules_leverage_and_qty(self) -> None:
        client = BinanceFuturesClient(
            api_key="k",
            api_secret="s",
            retry_count=1,
        )
        client.sync_server_time = MagicMock(return_value=0)  # type: ignore[method-assign]
        client.get_symbol_rules = MagicMock(return_value={})  # type: ignore[method-assign]
        client.ensure_isolated_and_leverage = MagicMock()  # type: ignore[method-assign]
        client.diagnose_order_qty = MagicMock(  # type: ignore[method-assign]
            side_effect=lambda **kwargs: {"symbol": kwargs["symbol"], "normalized_qty": 1.0}
        )

        result = client.prewarm_entry(
            symbols=["BTCUSDT", "ETHUSDT"],
            leverage=2,
            target_notional=100.0,
            reference_prices={"BTCUSDT": 100.0, "ETHUSDT": 0.0},
        )

        client.sync_server_time.assert_called_once_with()
        client.get_symbol_rules.assert_called_once_with()
        self.assertEqual(
            client.ensure_isolated_and_leverage.call_args_list,
            [
                unittest.mock.call("BTCUSDT", 2),
                unittest.mock.call("ETHUSDT", 2),
            ],
        )
        client.diagnose_order_qty.assert_called_once_with(
            symbol="BTCUSDT",
            notional=100.0,
            price=100.0,
        )
        self.assertEqual(result["BTCUSDT"]["normalized_qty"], 1.0)

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
        client._sync_server_time = MagicMock()  # type: ignore[method-assign]

        time_values = iter(1000.0 + (idx * 0.1) for idx in range(20))
        with patch("infra.binance_futures_client.time.time", side_effect=lambda: next(time_values)):
            with patch("infra.binance_futures_client.time.sleep"):
                result = client._request("GET", "/signed", params={"symbol": "BTCUSDT"}, signed=True)

        self.assertEqual(result, {"ok": True})
        request_params = [
            call.kwargs["params"]
            for call in client.session.request.call_args_list
        ]
        self.assertGreater(request_params[1]["timestamp"], request_params[0]["timestamp"])
        self.assertNotEqual(request_params[0]["signature"], request_params[1]["signature"])

    def test_idempotent_order_timestamp_error_syncs_and_retries(self) -> None:
        client = BinanceFuturesClient(
            api_key="k",
            api_secret="s",
            retry_count=2,
            retry_delay_sec=0.1,
        )
        sync_mock = MagicMock(
            side_effect=lambda: setattr(client, "_server_time_offset_ms", 2500)
        )
        client._sync_server_time = sync_mock  # type: ignore[method-assign]

        first_response = MagicMock()
        first_response.status_code = 400
        first_response.text = '{"code": -1021, "msg": "Timestamp outside recvWindow"}'
        first_response.json.return_value = {
            "code": -1021,
            "msg": "Timestamp outside recvWindow",
        }
        second_response = MagicMock()
        second_response.status_code = 200
        second_response.text = '{"algoId": 123, "clientAlgoId": "test-1", "algoStatus": "NEW"}'
        second_response.json.return_value = {
            "algoId": 123,
            "clientAlgoId": "test-1",
            "algoStatus": "NEW",
        }
        client.session.request = MagicMock(side_effect=[first_response, second_response])

        with patch("infra.binance_futures_client.time.time", return_value=1000.0):
            result = client._request(
                "POST",
                "/fapi/v1/algoOrder",
                params={"symbol": "BTCUSDT", "clientAlgoId": "test-1"},
                signed=True,
            )

        self.assertEqual(result["algoId"], 123)
        sync_mock.assert_called_once_with()
        request_params = [
            call.kwargs["data"]
            for call in client.session.request.call_args_list
        ]
        self.assertEqual(request_params[0]["clientAlgoId"], "test-1")
        self.assertEqual(request_params[1]["clientAlgoId"], "test-1")
        self.assertGreater(request_params[1]["timestamp"], request_params[0]["timestamp"])

    def test_order_write_network_error_is_not_blindly_retried(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s", retry_count=3)
        client.session.request = MagicMock(side_effect=requests.RequestException("reset"))

        with self.assertRaises(BinanceAPIError) as caught:
            client._request(
                "POST",
                "/fapi/v1/order",
                params={"symbol": "BTCUSDT", "newClientOrderId": "test-1"},
                signed=True,
            )

        self.assertEqual(caught.exception.code, "NETWORK")
        self.assertEqual(client.session.request.call_count, 1)

    def test_create_order_does_not_treat_recovered_new_market_order_as_filled(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._request = MagicMock(side_effect=BinanceAPIError("NETWORK", "reset"))  # type: ignore[method-assign]
        client.get_order = MagicMock(  # type: ignore[method-assign]
            return_value={"orderId": 123, "clientOrderId": "test-1", "status": "NEW"}
        )

        with self.assertRaises(OrderStateUnknownError):
            client.create_order(
                symbol="BTCUSDT",
                side="SELL",
                type="MARKET",
                quantity="1",
                newClientOrderId="test-1",
            )

        self.assertEqual(client.get_order.call_count, client.order_reconcile_attempts)

    def test_create_order_reconciles_filled_market_order_by_client_id(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._request = MagicMock(side_effect=BinanceAPIError("NETWORK", "reset"))  # type: ignore[method-assign]
        client.get_order = MagicMock(  # type: ignore[method-assign]
            return_value={"orderId": 123, "clientOrderId": "test-1", "status": "FILLED"}
        )

        order = client.create_order(
            symbol="BTCUSDT",
            side="SELL",
            type="MARKET",
            quantity="1",
            newClientOrderId="test-1",
        )

        self.assertEqual(order["orderId"], 123)

    def test_create_order_reconciles_open_limit_order_by_client_id(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._request = MagicMock(side_effect=BinanceAPIError("NETWORK", "reset"))  # type: ignore[method-assign]
        client.get_order = MagicMock(  # type: ignore[method-assign]
            return_value={"orderId": 124, "clientOrderId": "test-limit-1", "status": "NEW"}
        )

        order = client.create_order(
            symbol="BTCUSDT",
            side="BUY",
            type="LIMIT",
            timeInForce="GTC",
            price="50000",
            quantity="1",
            newClientOrderId="test-limit-1",
        )

        self.assertEqual(order["orderId"], 124)
        self.assertEqual(order["status"], "NEW")
        client.get_order.assert_called_once_with(
            symbol="BTCUSDT",
            orig_client_order_id="test-limit-1",
        )

    @patch("infra.binance_futures_client.time.sleep")
    def test_create_order_waits_for_exchange_eventual_consistency(self, sleep_mock: MagicMock) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._request = MagicMock(side_effect=BinanceAPIError("NETWORK", "reset"))  # type: ignore[method-assign]
        client.get_order = MagicMock(  # type: ignore[method-assign]
            side_effect=[
                BinanceAPIError(-2013, "Order does not exist."),
                BinanceAPIError(-2013, "Order does not exist."),
                {"orderId": 123, "clientOrderId": "test-1", "status": "FILLED"},
            ]
        )

        order = client.create_order(
            symbol="BTCUSDT",
            side="SELL",
            type="MARKET",
            quantity="1",
            newClientOrderId="test-1",
        )

        self.assertEqual(order["orderId"], 123)
        self.assertEqual(client.get_order.call_count, 3)
        self.assertEqual(sleep_mock.call_count, 2)

    @patch("infra.binance_futures_client.time.sleep")
    def test_create_order_reports_unknown_state_after_reconciliation_exhausted(
        self,
        _sleep_mock: MagicMock,
    ) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._request = MagicMock(side_effect=BinanceAPIError("NETWORK", "reset"))  # type: ignore[method-assign]
        client.get_order = MagicMock(  # type: ignore[method-assign]
            side_effect=BinanceAPIError(-2013, "Order does not exist.")
        )

        with self.assertRaises(OrderStateUnknownError) as caught:
            client.create_order(
                symbol="BTCUSDT",
                side="SELL",
                type="MARKET",
                quantity="1",
                newClientOrderId="test-1",
            )

        self.assertEqual(caught.exception.symbol, "BTCUSDT")
        self.assertEqual(caught.exception.client_order_id, "test-1")
        self.assertEqual(client.get_order.call_count, 5)

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

    def test_diagnose_order_qty_refreshes_when_symbol_missing_from_cache(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        rules = {
            "NEWUSDT": SymbolRules(
                symbol="NEWUSDT",
                tick_size=0.0001,
                step_size=1.0,
                min_qty=1.0,
                min_notional=5.0,
            )
        }
        client.get_symbol_rules = MagicMock(side_effect=[{}, rules])  # type: ignore[method-assign]

        diagnostic = client.diagnose_order_qty("NEWUSDT", notional=92.0, price=0.16834)

        self.assertTrue(diagnostic["has_rules"])
        self.assertIsNone(diagnostic["reject_reason"])
        self.assertAlmostEqual(diagnostic["normalized_qty"], 546.0)
        client.get_symbol_rules.assert_any_call()
        client.get_symbol_rules.assert_any_call(refresh=True)

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

    def test_get_open_orders_combines_legacy_and_algo_orders(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")

        def _fake_request(method, path, params=None, signed=False):  # type: ignore[no-untyped-def]
            if method == "GET" and path == "/fapi/v1/openOrders":
                self.assertEqual(params, {})
                self.assertTrue(signed)
                return [
                    {
                        "orderId": 11,
                        "clientOrderId": "legacy-sl",
                        "symbol": "BTCUSDT",
                        "type": "STOP_MARKET",
                        "side": "BUY",
                        "status": "NEW",
                    }
                ]
            if method == "GET" and path == "/fapi/v1/openAlgoOrders":
                self.assertEqual(params, {})
                self.assertTrue(signed)
                return [
                    {
                        "algoId": 22,
                        "clientAlgoId": "algo-sl",
                        "symbol": "ETHUSDT",
                        "orderType": "STOP_MARKET",
                        "side": "BUY",
                        "algoStatus": "NEW",
                    }
                ]
            raise AssertionError(f"Unexpected request: {method} {path} params={params} signed={signed}")

        client._request = MagicMock(side_effect=_fake_request)  # type: ignore[method-assign]

        orders = client.get_open_orders()

        self.assertEqual(len(orders), 2)
        self.assertEqual(orders[0]["orderId"], 11)
        self.assertEqual(orders[1]["orderId"], 22)
        self.assertEqual(orders[1]["clientOrderId"], "algo-sl")
        self.assertEqual(orders[1]["type"], "STOP_MARKET")
        self.assertEqual(orders[1]["status"], "NEW")

    def test_get_account_reads_position_margin_payload(self) -> None:
        client = BinanceFuturesClient(api_key="k", api_secret="s")
        client._request = MagicMock(return_value={"positions": []})  # type: ignore[method-assign]

        account = client.get_account()

        self.assertEqual(account, {"positions": []})
        client._request.assert_called_once_with("GET", "/fapi/v2/account", signed=True)


if __name__ == "__main__":
    unittest.main()
