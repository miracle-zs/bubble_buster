import configparser
import logging
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter

# Configure logging for standalone usage.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BINANCE_API_BASE = "https://fapi.binance.com"
SERVERCHAN_API = "https://sctapi.ftqq.com/{SERVERCHAN_SENDKEY}.send"
DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.ini')


def _load_legacy_settings(config_path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Loads legacy [Settings] from config.ini in a best-effort way.

    This keeps the original script backward-compatible while allowing safe imports
    from other modules even when config.ini does not exist.
    """
    parser = configparser.ConfigParser()
    parser.read(config_path)

    section = parser['Settings'] if parser.has_section('Settings') else {}

    def _get_str(name: str, default: str = "") -> str:
        value = section.get(name, default) if isinstance(section, dict) else section.get(name, fallback=default)
        return value.strip() if isinstance(value, str) else default

    def _get_float(name: str, default: float = 0.0) -> float:
        raw = _get_str(name, str(default))
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    http_proxy = _get_str('HTTP_PROXY', "")
    https_proxy = _get_str('HTTPS_PROXY', "")

    return {
        'SERVERCHAN_SENDKEY': _get_str('SERVERCHAN_SENDKEY', ""),
        'VOLUME_THRESHOLD': _get_float('VOLUME_THRESHOLD', 0.0),
        'HTTP_PROXY': http_proxy,
        'HTTPS_PROXY': https_proxy,
        'PROXIES': {
            'http': http_proxy,
            'https': https_proxy,
        } if http_proxy and https_proxy else None,
    }


LEGACY_SETTINGS = _load_legacy_settings()
SERVERCHAN_SENDKEY = LEGACY_SETTINGS['SERVERCHAN_SENDKEY']
VOLUME_THRESHOLD = LEGACY_SETTINGS['VOLUME_THRESHOLD']
PROXIES = LEGACY_SETTINGS['PROXIES']


class ApiWeightLimiter:
    """Thread-safe weight limiter using a rolling 60-second window."""

    def __init__(self, max_weight_per_minute: int, min_request_interval_ms: int = 0):
        self.max_weight_per_minute = max(1, int(max_weight_per_minute))
        self.min_request_interval_sec = max(0.0, float(min_request_interval_ms) / 1000.0)
        self._events: deque = deque()
        self._weight_sum = 0
        self._last_request_ts = 0.0
        self._lock = threading.Lock()

    def _prune(self, now_mono: float) -> None:
        cutoff = now_mono - 60.0
        while self._events and self._events[0][0] <= cutoff:
            _, weight = self._events.popleft()
            self._weight_sum -= int(weight)

    def acquire(self, weight: int = 1) -> None:
        req_weight = max(1, int(weight))
        while True:
            sleep_for = 0.0
            with self._lock:
                now_mono = time.monotonic()
                self._prune(now_mono)

                if self.min_request_interval_sec > 0:
                    interval_wait = (self._last_request_ts + self.min_request_interval_sec) - now_mono
                    if interval_wait > sleep_for:
                        sleep_for = interval_wait

                if self._weight_sum + req_weight > self.max_weight_per_minute and self._events:
                    oldest_ts, _ = self._events[0]
                    quota_wait = (oldest_ts + 60.0) - now_mono
                    if quota_wait > sleep_for:
                        sleep_for = quota_wait

                if sleep_for <= 0:
                    now_mono = time.monotonic()
                    self._prune(now_mono)
                    self._events.append((now_mono, req_weight))
                    self._weight_sum += req_weight
                    self._last_request_ts = now_mono
                    return

            time.sleep(min(max(sleep_for, 0.01), 1.0))


def create_session(proxies: Optional[Dict[str, str]] = None, pool_maxsize: int = 64) -> requests.Session:
    session = requests.Session()
    pool_size = max(10, int(pool_maxsize))
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if proxies:
        session.proxies = proxies
    return session


# Create a shared session for standalone script execution.
SESSION = create_session(PROXIES)


def retry(
    retries: int = 3,
    delay: float = 1,
    backoff: float = 2,
    default_return_value=None,
):
    """Retry decorator with exponential backoff for transient HTTP calls."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _retries, _delay = retries, delay
            while _retries > 1:
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    _retries -= 1
                    logging.warning(
                        "Error in %s with args %s: %s. Retrying in %ss... (%s/%s)",
                        func.__name__,
                        args,
                        e,
                        _delay,
                        retries - _retries,
                        retries,
                    )
                    time.sleep(_delay)
                    _delay *= backoff

            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                logging.error("Final attempt for %s failed with args %s: %s", func.__name__, args, e)
                if default_return_value is not None:
                    return default_return_value

                if "info" in func.__name__ or "ticker" in func.__name__ or "klines" in func.__name__:
                    return []
                return None

        return wrapper

    return decorator


@retry(default_return_value=[])
def get_exchange_info(
    session: Optional[requests.Session] = None,
    base_url: str = BINANCE_API_BASE,
    rate_limiter: Optional[ApiWeightLimiter] = None,
    request_weight: int = 1,
) -> List[str]:
    """Fetches all USDT perpetual futures symbols."""
    if rate_limiter:
        rate_limiter.acquire(weight=request_weight)
    endpoint = f"{base_url}/fapi/v1/exchangeInfo"
    response = (session or SESSION).get(endpoint, timeout=10)
    response.raise_for_status()
    data = response.json()
    symbols = [
        item['symbol']
        for item in data['symbols']
        if item['contractType'] == 'PERPETUAL' and item['quoteAsset'] == 'USDT'
    ]
    return symbols


@retry(default_return_value=[])
def get_24hr_ticker_data(
    session: Optional[requests.Session] = None,
    base_url: str = BINANCE_API_BASE,
    rate_limiter: Optional[ApiWeightLimiter] = None,
    request_weight: int = 40,
) -> List[Dict[str, Any]]:
    """Fetches 24hr ticker data for all futures symbols."""
    if rate_limiter:
        rate_limiter.acquire(weight=request_weight)
    endpoint = f"{base_url}/fapi/v1/ticker/24hr"
    response = (session or SESSION).get(endpoint, timeout=10)
    response.raise_for_status()
    return response.json()


@retry(default_return_value=[])
def get_klines_data(
    symbol: str,
    interval: str,
    startTime: int,
    endTime: Optional[int] = None,
    limit: Optional[int] = None,
    session: Optional[requests.Session] = None,
    base_url: str = BINANCE_API_BASE,
    rate_limiter: Optional[ApiWeightLimiter] = None,
    request_weight: int = 1,
) -> List[List[Any]]:
    """Fetches klines data for a given symbol and interval."""
    if rate_limiter:
        rate_limiter.acquire(weight=request_weight)
    endpoint = f"{base_url}/fapi/v1/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': startTime,
    }
    if endTime is not None:
        params['endTime'] = endTime
    if limit is not None:
        params['limit'] = int(limit)

    response = (session or SESSION).get(endpoint, params=params, timeout=10)
    response.raise_for_status()
    return response.json()


def get_utc_midnight_timestamp(now_utc: Optional[datetime] = None) -> int:
    """Returns today's UTC midnight timestamp in milliseconds."""
    now_utc = now_utc or datetime.now(timezone.utc)
    midnight_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight_utc.timestamp() * 1000)


def get_open_price_at_midnight(
    symbol: str,
    midnight_utc_timestamp: int,
    session: Optional[requests.Session] = None,
    base_url: str = BINANCE_API_BASE,
    rate_limiter: Optional[ApiWeightLimiter] = None,
) -> Optional[float]:
    """Fetches the 1h kline open price at UTC midnight for a symbol."""
    # Fast path: for an exact 1h candle boundary, one kline is sufficient.
    klines = get_klines_data(
        symbol,
        '1h',
        midnight_utc_timestamp,
        limit=1,
        session=session,
        base_url=base_url,
        rate_limiter=rate_limiter,
    )

    if klines and klines[0][0] == midnight_utc_timestamp:
        return float(klines[0][1])

    logging.warning("Exact midnight kline not found for %s. Expanding search window.", symbol)
    start_time_wider = midnight_utc_timestamp - (60 * 60 * 1000)
    end_time_wider = midnight_utc_timestamp + (60 * 60 * 1000)
    klines_wider = get_klines_data(
        symbol,
        '1h',
        start_time_wider,
        end_time_wider,
        session=session,
        base_url=base_url,
        rate_limiter=rate_limiter,
    )

    if not klines_wider:
        logging.warning("Could not fetch 1-hour klines for %s around UTC midnight.", symbol)
        return None

    for kline in klines_wider:
        if kline[0] == midnight_utc_timestamp:
            return float(kline[1])

    logging.warning("No 1-hour kline found exactly at UTC midnight for %s.", symbol)
    return None


def calculate_daily_percentage_change(current_price: float, midnight_open_price: Optional[float]) -> float:
    """Calculates daily percentage change from UTC midnight open."""
    if midnight_open_price and midnight_open_price != 0:
        return (current_price / midnight_open_price - 1) * 100
    return 0.0


def build_top_gainers(
    top_n: int = 10,
    volume_threshold: Optional[float] = None,
    session: Optional[requests.Session] = None,
    base_url: str = BINANCE_API_BASE,
    max_workers: int = 24,
    weight_limit_per_minute: int = 1000,
    min_request_interval_ms: int = 20,
) -> List[Dict[str, float]]:
    """Builds top gainers ranked by UTC-midnight-based percentage change."""
    started_at = time.perf_counter()
    threshold = VOLUME_THRESHOLD if volume_threshold is None else volume_threshold
    logging.info(
        "Top10 ranking started: top_n=%s volume_threshold=%.4f workers=%s weight_limit/min=%s min_interval_ms=%s",
        top_n,
        float(threshold),
        max_workers,
        weight_limit_per_minute,
        min_request_interval_ms,
    )
    weight_limiter = ApiWeightLimiter(
        max_weight_per_minute=max(100, int(weight_limit_per_minute)),
        min_request_interval_ms=max(0, int(min_request_interval_ms)),
    )

    stage_started = time.perf_counter()
    symbols = get_exchange_info(
        session=session,
        base_url=base_url,
        rate_limiter=weight_limiter,
        request_weight=1,
    )
    if not symbols:
        logging.warning("Top10 ranking aborted: no USDT perpetual symbols found")
        return []
    logging.info(
        "Top10 ranking stage done: exchange_info symbols=%s elapsed=%.2fs",
        len(symbols),
        time.perf_counter() - stage_started,
    )

    stage_started = time.perf_counter()
    ticker_data = get_24hr_ticker_data(
        session=session,
        base_url=base_url,
        rate_limiter=weight_limiter,
        request_weight=40,
    )
    if not ticker_data:
        logging.warning("Top10 ranking aborted: no 24h ticker data")
        return []
    logging.info(
        "Top10 ranking stage done: ticker_24hr rows=%s elapsed=%.2fs",
        len(ticker_data),
        time.perf_counter() - stage_started,
    )

    ticker_map = {item['symbol']: item for item in ticker_data}
    midnight_utc_timestamp = get_utc_midnight_timestamp()

    candidates: List[Dict[str, float]] = []
    for symbol in symbols:
        ticker = ticker_map.get(symbol)
        if ticker is None:
            continue

        current_price = float(ticker['lastPrice'])
        quote_volume = float(ticker['quoteVolume'])
        if quote_volume < threshold:
            continue
        candidates.append(
            {
                'symbol': symbol,
                'current_price': current_price,
                'volume': quote_volume,
            }
        )

    if not candidates:
        logging.warning(
            "Top10 ranking aborted: no candidates after volume filter threshold=%.4f",
            float(threshold),
        )
        return []
    logging.info(
        "Top10 ranking candidates ready: total=%s (from symbols=%s, threshold=%.4f)",
        len(candidates),
        len(symbols),
        float(threshold),
    )

    workers = max(1, min(int(max_workers or 1), len(candidates)))
    logging.info(
        "Top10 ranking kline stage started: candidates=%s workers=%s",
        len(candidates),
        workers,
    )

    midnight_open_map: Dict[str, Optional[float]] = {}
    progress_total = len(candidates)
    progress_step = max(1, progress_total // 10)
    progress_count = 0
    missing_open_count = 0
    stage_started = time.perf_counter()
    if workers == 1:
        for item in candidates:
            symbol = str(item['symbol'])
            midnight_open = get_open_price_at_midnight(
                symbol,
                midnight_utc_timestamp,
                session=session,
                base_url=base_url,
                rate_limiter=weight_limiter,
            )
            midnight_open_map[symbol] = midnight_open
            progress_count += 1
            if midnight_open is None:
                missing_open_count += 1
            if progress_count % progress_step == 0 or progress_count == progress_total:
                progress_pct = (progress_count * 100.0) / max(1, progress_total)
                logging.info(
                    "Top10 ranking kline progress: %s/%s (%.1f%%), missing_open=%s",
                    progress_count,
                    progress_total,
                    progress_pct,
                    missing_open_count,
                )
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_symbol = {
                executor.submit(
                    get_open_price_at_midnight,
                    str(item['symbol']),
                    midnight_utc_timestamp,
                    session,
                    base_url,
                    weight_limiter,
                ): str(item['symbol'])
                for item in candidates
            }
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    midnight_open = future.result()
                    midnight_open_map[symbol] = midnight_open
                    if midnight_open is None:
                        missing_open_count += 1
                except Exception as exc:  # noqa: BLE001
                    logging.warning("Failed to load midnight open for %s: %s", symbol, exc)
                    midnight_open_map[symbol] = None
                    missing_open_count += 1
                progress_count += 1
                if progress_count % progress_step == 0 or progress_count == progress_total:
                    progress_pct = (progress_count * 100.0) / max(1, progress_total)
                    logging.info(
                        "Top10 ranking kline progress: %s/%s (%.1f%%), missing_open=%s",
                        progress_count,
                        progress_total,
                        progress_pct,
                        missing_open_count,
                    )
    logging.info(
        "Top10 ranking stage done: kline_midnight elapsed=%.2fs missing_open=%s/%s",
        time.perf_counter() - stage_started,
        missing_open_count,
        progress_total,
    )

    leaderboard = []
    for item in candidates:
        symbol = str(item['symbol'])
        current_price = float(item['current_price'])
        quote_volume = float(item['volume'])
        midnight_open_price = midnight_open_map.get(symbol)
        if midnight_open_price is None:
            continue

        percentage_change = calculate_daily_percentage_change(current_price, midnight_open_price)
        leaderboard.append(
            {
                'symbol': symbol,
                'change': percentage_change,
                'volume': quote_volume,
                'current_price': current_price,
                'midnight_open_price': midnight_open_price,
            }
        )

    leaderboard.sort(key=lambda x: x['change'], reverse=True)
    top_list = leaderboard[:top_n]
    if top_list:
        preview = ", ".join(
            f"{idx + 1}.{entry['symbol']} {float(entry['change']):.2f}%"
            for idx, entry in enumerate(top_list[:10])
        )
    else:
        preview = "(empty)"
    logging.info(
        "Top10 ranking completed: selected=%s valid=%s total_elapsed=%.2fs top=%s",
        len(top_list),
        len(leaderboard),
        time.perf_counter() - started_at,
        preview,
    )
    return top_list


def send_server_chan_notification(
    title: str,
    content: str,
    send_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> None:
    """Sends a notification via Server酱."""
    final_send_key = (send_key or SERVERCHAN_SENDKEY or "").strip()
    if not final_send_key or final_send_key == "YOUR_SERVERCHAN_SENDKEY":
        logging.warning("Server酱 SENDKEY is not configured. Skipping notification.")
        return

    url = SERVERCHAN_API.format(SERVERCHAN_SENDKEY=final_send_key)
    data = {
        "title": title,
        "desp": content,
    }

    try:
        response = (session or SESSION).post(url, data=data, timeout=10)
        response.raise_for_status()
        logging.info("Server酱 notification sent. Response: %s", response.json())
    except requests.exceptions.RequestException as e:
        logging.error("Error sending Server酱 notification: %s", e)


def format_top10_markdown(top_10_gainers: List[Dict[str, float]]) -> str:
    now_beijing_time = datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')
    content = f"### 币安合约市场实时涨幅榜\n\n**更新时间:** {now_beijing_time}\n\n"
    content += "| 排名 | 币种 | 日涨幅 | 24h成交额 |\n"
    content += "|:---:|:---:|:---:|:---:|\n"

    for i, entry in enumerate(top_10_gainers):
        symbol = entry['symbol']
        change = f"{entry['change']:.2f}%"
        volume = f"{int(entry['volume'] / 1_000_000)}M"
        content += f"| {i + 1} | {symbol} | {change} | {volume} |\n"

    return content


def main() -> None:
    logging.info("Script started: Fetching Top 10 Gainers.")
    top_10_gainers = build_top_gainers(top_n=10)
    if not top_10_gainers:
        logging.info("No leaderboard generated.")
        return

    title = "【币安合约市场涨幅榜 Top 10】"
    content = format_top10_markdown(top_10_gainers)

    logging.info("--- Sending Top 10 Gainers Notification ---")
    logging.info(content)
    send_server_chan_notification(title, content)

    logging.info("Script finished.")


if __name__ == "__main__":
    main()
