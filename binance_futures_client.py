import hashlib
import hmac
import logging
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter

LOGGER = logging.getLogger(__name__)


class BinanceAPIError(Exception):
    def __init__(self, code: Any, message: str, http_status: Optional[int] = None):
        self.code = code
        self.message = message
        self.http_status = http_status
        super().__init__(f"Binance API Error {code}: {message}")


@dataclass(frozen=True)
class SymbolRules:
    symbol: str
    tick_size: float
    step_size: float
    min_qty: float
    min_notional: float


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def floor_to_step(value: float, step: float) -> float:
    value_d = _to_decimal(value)
    step_d = _to_decimal(step)
    if step_d <= 0:
        return float(value_d)
    quantized = (value_d / step_d).to_integral_value(rounding=ROUND_DOWN) * step_d
    return float(quantized)


def ceil_to_step(value: float, step: float) -> float:
    value_d = _to_decimal(value)
    step_d = _to_decimal(step)
    if step_d <= 0:
        return float(value_d)
    quantized = (value_d / step_d).to_integral_value(rounding=ROUND_CEILING) * step_d
    return float(quantized)


def _precision_from_step(step: float) -> int:
    if step <= 0:
        return 8
    normalized = _to_decimal(step).normalize()
    exponent = normalized.as_tuple().exponent
    return max(0, -exponent)


def _format_by_step(value: float, step: float) -> str:
    precision = _precision_from_step(step)
    rounded = round(float(value), precision)
    if precision == 0:
        return str(int(round(rounded)))
    text = f"{rounded:.{precision}f}"
    text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


class BinanceFuturesClient:
    RETRIABLE_HTTP_STATUS = {408, 429, 500, 502, 503, 504}
    RETRIABLE_ERROR_CODES = {-1001, -1003, -1006, -1007, -1008, -1021}
    CONDITIONAL_ORDER_TYPES = {
        "STOP",
        "STOP_MARKET",
        "TAKE_PROFIT",
        "TAKE_PROFIT_MARKET",
        "TRAILING_STOP_MARKET",
    }
    ORDER_NOT_FOUND_CODES = {-2011, -2013}

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.binance.com",
        timeout_sec: int = 10,
        retry_count: int = 3,
        retry_delay_sec: float = 1.0,
        recv_window: int = 5000,
        http_pool_maxsize: int = 64,
        proxies: Optional[Dict[str, str]] = None,
    ):
        if not api_key or not api_secret:
            raise ValueError("Binance API key/secret is required")

        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.retry_count = max(1, retry_count)
        self.retry_delay_sec = max(0.1, retry_delay_sec)
        self.recv_window = recv_window

        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.api_key})
        pool_size = max(10, int(http_pool_maxsize))
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        if proxies:
            self.session.proxies = proxies

        self._symbol_rules_cache: Dict[str, SymbolRules] = {}

    def _is_retriable_error(self, err: BinanceAPIError) -> bool:
        if err.http_status in self.RETRIABLE_HTTP_STATUS:
            return True
        try:
            code = int(err.code)
        except (TypeError, ValueError):
            return False
        return code in self.RETRIABLE_ERROR_CODES

    @staticmethod
    def _safe_error_code(err: BinanceAPIError) -> Optional[int]:
        try:
            return int(err.code)
        except (TypeError, ValueError):
            return None

    def _is_order_not_found_error(self, err: BinanceAPIError) -> bool:
        code = self._safe_error_code(err)
        return code in self.ORDER_NOT_FOUND_CODES

    @staticmethod
    def _is_algo_endpoint_unavailable_error(err: BinanceAPIError) -> bool:
        if err.http_status == 404:
            return True
        text = str(err.message or "").lower()
        return "algoorder" in text and "not found" in text

    def _normalize_params(self, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        normalized: Dict[str, Any] = {}
        if not params:
            return normalized
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, bool):
                normalized[key] = "true" if value else "false"
            else:
                normalized[key] = value
        return normalized

    def _sign_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        signed = dict(params)
        signed["timestamp"] = int(time.time() * 1000)
        signed["recvWindow"] = self.recv_window
        query = urlencode(signed, doseq=True)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        signed["signature"] = signature
        return signed

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
    ) -> Any:
        method = method.upper()
        payload = self._normalize_params(params)
        if signed:
            payload = self._sign_params(payload)

        url = f"{self.base_url}{path}"

        for attempt in range(1, self.retry_count + 1):
            try:
                if method in {"GET", "DELETE"}:
                    response = self.session.request(
                        method,
                        url,
                        params=payload,
                        timeout=self.timeout_sec,
                    )
                else:
                    response = self.session.request(
                        method,
                        url,
                        data=payload,
                        timeout=self.timeout_sec,
                    )
            except requests.RequestException as exc:
                if attempt >= self.retry_count:
                    raise BinanceAPIError("NETWORK", str(exc)) from exc
                sleep_sec = self.retry_delay_sec * (2 ** (attempt - 1))
                LOGGER.warning("Network error for %s %s: %s. Retry in %.2fs", method, path, exc, sleep_sec)
                time.sleep(sleep_sec)
                continue

            if response.status_code < 400:
                if not response.text:
                    return {}
                try:
                    return response.json()
                except ValueError:
                    return response.text

            code: Any = response.status_code
            msg = response.text
            try:
                err_data = response.json()
                code = err_data.get("code", code)
                msg = err_data.get("msg", msg)
            except ValueError:
                pass

            err = BinanceAPIError(code=code, message=msg, http_status=response.status_code)
            if attempt < self.retry_count and self._is_retriable_error(err):
                sleep_sec = self.retry_delay_sec * (2 ** (attempt - 1))
                LOGGER.warning(
                    "Retriable API error for %s %s: %s. Retry in %.2fs",
                    method,
                    path,
                    err,
                    sleep_sec,
                )
                time.sleep(sleep_sec)
                continue
            raise err

        raise BinanceAPIError("UNKNOWN", f"Unexpected failure for {method} {path}")

    def get_exchange_info(self) -> Dict[str, Any]:
        return self._request("GET", "/fapi/v1/exchangeInfo")

    def list_usdt_perpetual_symbols(self) -> List[str]:
        data = self.get_exchange_info()
        return [
            item["symbol"]
            for item in data.get("symbols", [])
            if item.get("contractType") == "PERPETUAL" and item.get("quoteAsset") == "USDT"
        ]

    def get_24hr_ticker_data(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/fapi/v1/ticker/24hr")

    def get_symbol_price(self, symbol: str) -> float:
        data = self._request("GET", "/fapi/v1/ticker/price", params={"symbol": symbol})
        return float(data["price"])

    def get_klines(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        end_time: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[List[Any]]:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
        }
        if end_time is not None:
            params["endTime"] = end_time
        if limit is not None:
            params["limit"] = limit
        return self._request("GET", "/fapi/v1/klines", params=params)

    def get_balance(self) -> List[Dict[str, Any]]:
        return self._request("GET", "/fapi/v2/balance", signed=True)

    def get_available_balance(self, asset: str = "USDT") -> float:
        balances = self.get_balance()
        for item in balances:
            if item.get("asset") == asset:
                return float(item.get("availableBalance", "0"))
        return 0.0

    def get_position_risk(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params = {"symbol": symbol} if symbol else None
        data = self._request("GET", "/fapi/v2/positionRisk", params=params, signed=True)
        if isinstance(data, dict):
            return [data]
        return data

    def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> Dict[str, Any]:
        try:
            return self._request(
                "POST",
                "/fapi/v1/marginType",
                params={"symbol": symbol, "marginType": margin_type},
                signed=True,
            )
        except BinanceAPIError as exc:
            if int(exc.code) in {-4046, -4048}:  # Already set / unchanged
                return {"code": exc.code, "msg": exc.message}
            raise

    def set_leverage(self, symbol: str, leverage: int) -> Dict[str, Any]:
        return self._request(
            "POST",
            "/fapi/v1/leverage",
            params={"symbol": symbol, "leverage": leverage},
            signed=True,
        )

    def create_order(self, **params: Any) -> Dict[str, Any]:
        order_type = str(params.get("type", "") or "").upper()
        if order_type in self.CONDITIONAL_ORDER_TYPES:
            algo_params = self._to_algo_order_params(params)
            try:
                return self.create_algo_order(**algo_params)
            except BinanceAPIError as exc:
                if not self._is_algo_endpoint_unavailable_error(exc):
                    raise
                LOGGER.warning(
                    "Algo order endpoint unavailable, fallback to legacy order endpoint: %s",
                    exc,
                )
        return self._request("POST", "/fapi/v1/order", params=params, signed=True)

    def cancel_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        try:
            return self._request("DELETE", "/fapi/v1/order", params=params, signed=True)
        except BinanceAPIError as exc:
            if not self._is_order_not_found_error(exc):
                raise
            return self.cancel_algo_order(
                algo_id=order_id,
                client_algo_id=orig_client_order_id,
            )

    def get_order(
        self,
        symbol: str,
        order_id: Optional[int] = None,
        orig_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        try:
            return self._request("GET", "/fapi/v1/order", params=params, signed=True)
        except BinanceAPIError as exc:
            if not self._is_order_not_found_error(exc):
                raise
            return self.get_algo_order(
                algo_id=order_id,
                client_algo_id=orig_client_order_id,
            )

    def cancel_all_open_orders(self, symbol: str) -> Dict[str, Any]:
        return self._request("DELETE", "/fapi/v1/allOpenOrders", params={"symbol": symbol}, signed=True)

    def create_algo_order(self, **params: Any) -> Dict[str, Any]:
        result = self._request("POST", "/fapi/v1/algoOrder", params=params, signed=True)
        if isinstance(result, dict):
            return self._normalize_algo_order_payload(result)
        return result

    def get_algo_order(
        self,
        algo_id: Optional[int] = None,
        client_algo_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if algo_id is not None:
            params["algoId"] = algo_id
        if client_algo_id:
            params["clientAlgoId"] = client_algo_id
        result = self._request("GET", "/fapi/v1/algoOrder", params=params, signed=True)
        if isinstance(result, dict):
            return self._normalize_algo_order_payload(result)
        return result

    def cancel_algo_order(
        self,
        algo_id: Optional[int] = None,
        client_algo_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        if algo_id is not None:
            params["algoId"] = algo_id
        if client_algo_id:
            params["clientAlgoId"] = client_algo_id
        result = self._request("DELETE", "/fapi/v1/algoOrder", params=params, signed=True)
        if isinstance(result, dict):
            return self._normalize_algo_order_payload(result)
        return result

    @staticmethod
    def _map_algo_status(raw_status: Any, actual_order_id: Any = None) -> Optional[str]:
        status = str(raw_status or "").upper()
        if not status:
            return None
        if status in {"NEW", "PENDING"}:
            return "NEW"
        if status in {"CANCELED", "CANCELLED"}:
            return "CANCELED"
        if status in {"EXPIRED", "FAILED", "REJECTED"}:
            return "EXPIRED"
        if status in {"TRIGGERED", "FINISHED", "FILLED", "SUCCESS"}:
            return "FILLED"
        if actual_order_id:
            return "FILLED"
        return status

    def _normalize_algo_order_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(payload)
        algo_id = payload.get("algoId")
        client_algo_id = payload.get("clientAlgoId")
        if "orderId" not in normalized and algo_id is not None:
            normalized["orderId"] = algo_id
        if "clientOrderId" not in normalized and client_algo_id is not None:
            normalized["clientOrderId"] = client_algo_id
        if "type" not in normalized and payload.get("orderType"):
            normalized["type"] = payload.get("orderType")
        if "status" not in normalized and "algoStatus" in payload:
            normalized["status"] = self._map_algo_status(
                payload.get("algoStatus"),
                actual_order_id=payload.get("actualOrderId"),
            )
        if "stopPrice" not in normalized and payload.get("triggerPrice") is not None:
            normalized["stopPrice"] = payload.get("triggerPrice")
        if "origQty" not in normalized and payload.get("quantity") is not None:
            normalized["origQty"] = payload.get("quantity")
        return normalized

    def _to_algo_order_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        order_type = str(params.get("type", "") or "").upper()
        algo_params: Dict[str, Any] = {
            "algoType": "CONDITIONAL",
            "type": order_type,
            "symbol": params.get("symbol"),
            "side": params.get("side"),
        }

        direct_fields = [
            "positionSide",
            "timeInForce",
            "quantity",
            "price",
            "workingType",
            "priceProtect",
            "reduceOnly",
            "closePosition",
            "priceMatch",
            "selfTradePreventionMode",
            "goodTillDate",
            "activatePrice",
            "callbackRate",
            "newOrderRespType",
        ]
        for field in direct_fields:
            if field in params:
                algo_params[field] = params[field]

        if "stopPrice" in params:
            algo_params["triggerPrice"] = params["stopPrice"]
        if "newClientOrderId" in params:
            algo_params["clientAlgoId"] = params["newClientOrderId"]

        if params.get("closePosition"):
            algo_params.pop("quantity", None)
            algo_params.pop("reduceOnly", None)

        return algo_params

    def get_symbol_rules(self, refresh: bool = False) -> Dict[str, SymbolRules]:
        if self._symbol_rules_cache and not refresh:
            return self._symbol_rules_cache

        exchange_info = self.get_exchange_info()
        rules: Dict[str, SymbolRules] = {}

        for symbol_data in exchange_info.get("symbols", []):
            symbol = symbol_data.get("symbol")
            if not symbol:
                continue

            filters = {flt.get("filterType"): flt for flt in symbol_data.get("filters", [])}
            price_filter = filters.get("PRICE_FILTER", {})
            lot_filter = filters.get("LOT_SIZE", {})
            min_notional_filter = filters.get("MIN_NOTIONAL") or filters.get("NOTIONAL") or {}

            tick_size = float(price_filter.get("tickSize", "0.01"))
            step_size = float(lot_filter.get("stepSize", "0.001"))
            min_qty = float(lot_filter.get("minQty", "0"))
            min_notional = float(
                min_notional_filter.get("notional")
                or min_notional_filter.get("minNotional")
                or 0.0
            )

            rules[symbol] = SymbolRules(
                symbol=symbol,
                tick_size=tick_size,
                step_size=step_size,
                min_qty=min_qty,
                min_notional=min_notional,
            )

        self._symbol_rules_cache = rules
        return rules

    def ensure_isolated_and_leverage(self, symbol: str, leverage: int) -> None:
        self.set_margin_type(symbol, "ISOLATED")
        self.set_leverage(symbol, leverage)

    def normalize_order_qty(self, symbol: str, notional: float, price: float) -> float:
        rules = self.get_symbol_rules().get(symbol)
        if not rules or price <= 0:
            return 0.0

        raw_qty = notional / price
        qty = floor_to_step(raw_qty, rules.step_size)
        if qty < rules.min_qty:
            return 0.0
        if rules.min_notional > 0 and qty * price < rules.min_notional:
            return 0.0
        return qty

    def normalize_trigger_price(self, symbol: str, price: float, round_up: bool = False) -> float:
        rules = self.get_symbol_rules().get(symbol)
        if not rules:
            return price
        if round_up:
            return ceil_to_step(price, rules.tick_size)
        return floor_to_step(price, rules.tick_size)

    def format_order_qty(self, symbol: str, qty: float) -> str:
        rules = self.get_symbol_rules().get(symbol)
        if not rules:
            text = _format_by_step(qty, 1e-8)
            return text
        normalized_qty = floor_to_step(qty, rules.step_size)
        return _format_by_step(normalized_qty, rules.step_size)

    def format_trigger_price(self, symbol: str, price: float, round_up: bool = False) -> str:
        rules = self.get_symbol_rules().get(symbol)
        if not rules:
            return _format_by_step(price, 1e-8)
        normalized_price = self.normalize_trigger_price(symbol, price, round_up=round_up)
        return _format_by_step(normalized_price, rules.tick_size)
