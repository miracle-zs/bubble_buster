"""只读账户交易统计获取模块。

为 readonly 模式账户从 Binance API 获取交易统计数据。
"""
import logging
import threading
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from infra.binance_futures_client import BinanceFuturesClient
from infra.protocols import TradeStatsStoreProtocol

LOGGER = logging.getLogger(__name__)

_USER_TRADES_MAX_WINDOW_MS = 7 * 24 * 60 * 60 * 1000 - 1
_USER_TRADES_TIME_PADDING_MS = 1000


@dataclass(frozen=True)
class TradeStats:
    """交易统计数据，total_trades 按已完成平仓订单计数。"""
    total_realized_pnl: float
    total_trades: int
    win_count: int
    loss_count: int
    win_rate_pct: float
    gross_profit: float
    gross_loss: float
    profit_factor: Optional[float]
    avg_win: float
    avg_loss: float
    last_updated_utc: str
    # Keep exchange gross realized PnL separate from the net figure shown on
    # the readonly dashboard card.
    net_realized_pnl: float = 0.0
    commission_usdt: float = 0.0
    funding_fee_usdt: float = 0.0


class TradeStatsFetcher:
    """从 Binance API 获取交易统计。"""

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: TradeStatsStoreProtocol,
        overlap_minutes: int = 20,
    ):
        if store is None:
            raise ValueError("store is required for TradeStatsFetcher")
        self.client = client
        self.store = store
        self.overlap_minutes = min(30, max(10, int(overlap_minutes)))
        self._cache: Dict[str, Optional[TradeStats]] = {}
        self._cache_lock = threading.RLock()
        self._inflight: Dict[str, threading.Event] = {}

    @staticmethod
    def _cache_key(account_id: str, lookback_days: int) -> str:
        return f"{account_id}:{lookback_days}"

    def get_cached_stats(
        self,
        *,
        account_id: str,
        lookback_days: int = 30,
    ) -> Optional[TradeStats]:
        """Return the latest cached value without making a network request."""
        cache_key = self._cache_key(account_id, lookback_days)
        with self._cache_lock:
            return self._cache.get(cache_key)

    def refresh_stats(
        self,
        *,
        account_id: str,
        lookback_days: int = 30,
    ) -> Optional[TradeStats]:
        """Refresh a cache entry, sharing one in-flight API call per key."""
        cache_key = self._cache_key(account_id, lookback_days)
        with self._cache_lock:
            refresh_event = self._inflight.get(cache_key)
            previous_stats = self._cache.get(cache_key)
            if refresh_event is None:
                refresh_event = threading.Event()
                self._inflight[cache_key] = refresh_event
                is_owner = True
            else:
                is_owner = False

        if not is_owner:
            refresh_event.wait()
            with self._cache_lock:
                return self._cache.get(cache_key)

        stats: Optional[TradeStats] = None
        try:
            self._sync_incremental(lookback_days=lookback_days)
            stats = self._calculate_stats_from_store(lookback_days=lookback_days)
            return stats
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch trade stats for account=%s: %s", account_id, exc)
            # Keep the last good snapshot available to the homepage after a
            # transient Binance error such as -1003.
            stats = previous_stats
            return stats
        finally:
            with self._cache_lock:
                self._cache[cache_key] = stats
                self._inflight.pop(cache_key, None)
                refresh_event.set()

    def _fetch_user_trades(
        self,
        *,
        symbol_windows: Dict[str, tuple[int, int]],
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Fetch user trades around realized income events in 7-day windows."""
        trades: List[Dict[str, Any]] = []
        seen_keys = set()
        page_limit = max(1, min(1000, int(limit)))

        for symbol, (symbol_start_ms, symbol_end_ms) in sorted(symbol_windows.items()):
            window_start_ms = symbol_start_ms
            while window_start_ms <= symbol_end_ms:
                window_end_ms = min(window_start_ms + _USER_TRADES_MAX_WINDOW_MS, symbol_end_ms)
                page_records = self.client.get_user_trades(
                    symbol=symbol,
                    start_time=window_start_ms,
                    end_time=window_end_ms,
                    limit=page_limit,
                )

                page_count = 0
                while page_records:
                    page_count += 1
                    reached_window_end = False
                    for record in page_records:
                        raw_time = record.get("time")
                        try:
                            trade_time_ms = int(raw_time) if raw_time is not None else None
                        except (TypeError, ValueError):
                            trade_time_ms = None
                        if trade_time_ms is not None and trade_time_ms > window_end_ms:
                            reached_window_end = True
                            break

                        key = (
                            symbol,
                            record.get("id"),
                            record.get("orderId"),
                            record.get("time"),
                            record.get("realizedPnl"),
                        )
                        if key in seen_keys:
                            continue
                        seen_keys.add(key)
                        trades.append(record)

                    if reached_window_end or len(page_records) < page_limit:
                        break
                    if page_count >= 100:
                        LOGGER.warning(
                            "User trade history pagination reached max_pages for symbol=%s window_start=%s; "
                            "stats may be partial",
                            symbol,
                            window_start_ms,
                        )
                        break

                    trade_ids = []
                    for record in page_records:
                        try:
                            trade_ids.append(int(record["id"]))
                        except (KeyError, TypeError, ValueError):
                            continue
                    if not trade_ids:
                        LOGGER.warning(
                            "User trade history page has no usable trade id for symbol=%s window_start=%s; "
                            "stats may be partial",
                            symbol,
                            window_start_ms,
                        )
                        break
                    next_from_id = max(trade_ids) + 1
                    page_records = self.client.get_user_trades(
                        symbol=symbol,
                        from_id=next_from_id,
                        limit=page_limit,
                    )

                window_start_ms = window_end_ms + 1

        return trades

    def _sync_incremental(self, *, lookback_days: int) -> Dict[str, int]:
        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        lookback_ms = max(1, int(lookback_days)) * 24 * 60 * 60 * 1000
        overlap_ms = self.overlap_minutes * 60 * 1000
        state = self.store.get_cursor_state("readonly_trade_stats_cursor_v2") or {}
        bootstrapped = bool(state.get("bootstrapped"))
        try:
            cursor_ms = int(state.get("income_cursor_ms") or 0)
        except (TypeError, ValueError):
            cursor_ms = 0
        draining_full_page = bool(state.get("income_draining_full_page"))
        if bootstrapped and draining_full_page:
            try:
                start_ms = int(state.get("income_drain_start_ms") or cursor_ms or 0)
                request_end_ms = int(state.get("income_drain_end_ms") or now_ms)
                request_page = max(1, int(state.get("income_drain_page") or 1))
            except (TypeError, ValueError):
                start_ms = max(0, cursor_ms - overlap_ms) if cursor_ms > 0 else max(0, now_ms - lookback_ms)
                request_end_ms = now_ms
                request_page = 1
                draining_full_page = False
        elif bootstrapped and cursor_ms > 0:
            start_ms = max(0, cursor_ms - overlap_ms)
            request_end_ms = now_ms
            request_page = 1
        else:
            start_ms = max(0, now_ms - lookback_ms)
            request_end_ms = now_ms
            request_page = 1

        all_rows: List[Dict[str, Any]] = []
        income_requests = 0
        background_scope = getattr(self.client, "background_requests", None)
        scope = background_scope() if callable(background_scope) else nullcontext()
        with scope:
            if bootstrapped:
                all_rows = self.client.get_income_history(
                    symbol=None,
                    income_type=None,
                    start_time=start_ms,
                    end_time=request_end_ms,
                    page=request_page,
                    limit=1000,
                )
                income_requests = 1
            else:
                for page in range(1, 101):
                    rows = self.client.get_income_history(
                        symbol=None,
                        income_type=None,
                        start_time=start_ms,
                        end_time=request_end_ms,
                        page=page,
                        limit=1000,
                    )
                    income_requests += 1
                    if not rows:
                        break
                    all_rows.extend(rows)
                    if len(rows) < 1000:
                        break
                else:
                    raise RuntimeError("readonly income backfill exceeded 100 pages")

            inserted_rows = self.store.add_binance_income_records(all_rows)
            new_realized = [
                row
                for row in inserted_rows
                if str(row.get("incomeType") or "").strip().upper() == "REALIZED_PNL"
                and str(row.get("symbol") or "").strip()
            ]
            symbol_windows: Dict[str, tuple[int, int]] = {}
            for row in new_realized:
                symbol = str(row.get("symbol") or "").strip().upper()
                try:
                    event_ms = int(row.get("time") or 0)
                except (TypeError, ValueError):
                    continue
                if event_ms <= 0:
                    continue
                window = (
                    max(start_ms, event_ms - _USER_TRADES_TIME_PADDING_MS),
                    min(request_end_ms, event_ms + _USER_TRADES_TIME_PADDING_MS),
                )
                previous = symbol_windows.get(symbol)
                symbol_windows[symbol] = (
                    min(previous[0], window[0]),
                    max(previous[1], window[1]),
                ) if previous is not None else window

            trade_requests_before = len(getattr(self.client, "trade_calls", []))
            user_trades = self._fetch_user_trades(symbol_windows=symbol_windows) if symbol_windows else []
            trades_inserted = self.store.add_binance_user_trades(user_trades)
            trade_requests_after = len(getattr(self.client, "trade_calls", []))

        complete_interval = (not all_rows) or len(all_rows) < 1000 or not bootstrapped
        next_cursor_ms = request_end_ms if complete_interval else cursor_ms
        self.store.save_cursor_state(
            "readonly_trade_stats_cursor_v2",
            {
                "bootstrapped": True,
                "income_cursor_ms": int(next_cursor_ms),
                "income_draining_full_page": not complete_interval,
                "income_drain_start_ms": int(start_ms) if not complete_interval else None,
                "income_drain_end_ms": int(request_end_ms) if not complete_interval else None,
                "income_drain_page": int(request_page + 1) if not complete_interval else None,
                "last_request_start_ms": int(start_ms),
                "last_request_end_ms": int(request_end_ms),
                "last_request_page": int(request_page),
                "last_income_rows": len(all_rows),
                "last_new_realized_rows": len(new_realized),
                "updated_at_utc": now.replace(microsecond=0).isoformat(),
            },
        )
        self.store.prune_binance_trade_ledger(max(0, now_ms - lookback_ms))
        return {
            "income_inserted": len(inserted_rows),
            "trades_inserted": int(trades_inserted),
            "income_requests": income_requests,
            "trade_requests": max(0, trade_requests_after - trade_requests_before),
        }

    def _calculate_stats_from_store(self, *, lookback_days: int) -> TradeStats:
        end_time = datetime.now(timezone.utc)
        start_time_ms = int((end_time - timedelta(days=max(1, lookback_days))).timestamp() * 1000)
        income_rows = self.store.load_binance_income_records(start_time_ms)
        trade_rows = self.store.load_binance_user_trades(start_time_ms)

        realized_rows = [row for row in income_rows if str(row.get("income_type") or "").upper() == "REALIZED_PNL"]
        commission_usdt = sum(
            float(row.get("income") or 0.0)
            for row in income_rows
            if str(row.get("income_type") or "").upper() == "COMMISSION"
        )
        funding_fee_usdt = sum(
            float(row.get("income") or 0.0)
            for row in income_rows
            if str(row.get("income_type") or "").upper() == "FUNDING_FEE"
        )

        order_by_trade = {
            (str(row.get("symbol") or "").upper(), str(row.get("trade_id") or "")): str(row.get("order_id") or "")
            for row in trade_rows
            if row.get("trade_id") is not None and row.get("order_id") is not None
        }
        order_pnl: Dict[tuple[str, str], float] = {}
        for row in realized_rows:
            symbol = str(row.get("symbol") or "").upper()
            trade_id = str(row.get("trade_id") or "")
            order_id = order_by_trade.get((symbol, trade_id))
            key = (symbol, order_id) if order_id else (symbol, f"income:{row.get('unique_key')}")
            order_pnl[key] = order_pnl.get(key, 0.0) + float(row.get("income") or 0.0)

        pnl_values = list(order_pnl.values())
        gross_profit = sum(value for value in pnl_values if value > 0)
        gross_loss = sum(abs(value) for value in pnl_values if value < 0)
        win_count = sum(1 for value in pnl_values if value > 0)
        loss_count = sum(1 for value in pnl_values if value < 0)
        total_trades = len(pnl_values)
        total_realized_pnl = sum(pnl_values)
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
        return TradeStats(
            total_realized_pnl=round(total_realized_pnl, 8),
            total_trades=total_trades,
            win_count=win_count,
            loss_count=loss_count,
            win_rate_pct=round((win_count / total_trades * 100.0) if total_trades else 0.0, 4),
            gross_profit=round(gross_profit, 8),
            gross_loss=round(gross_loss, 8),
            profit_factor=round(profit_factor, 4) if profit_factor is not None else None,
            avg_win=round(gross_profit / win_count, 8) if win_count else 0.0,
            avg_loss=round(gross_loss / loss_count, 8) if loss_count else 0.0,
            last_updated_utc=end_time.replace(microsecond=0).isoformat(),
            net_realized_pnl=round(total_realized_pnl + commission_usdt + funding_fee_usdt, 8),
            commission_usdt=round(commission_usdt, 8),
            funding_fee_usdt=round(funding_fee_usdt, 8),
        )
