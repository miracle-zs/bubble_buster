"""只读账户交易统计获取模块。

为 readonly 模式账户从 Binance API 获取交易统计数据。
"""
import logging
import threading
import time
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from core.state_store import StateStore
from infra.binance_futures_client import BinanceFuturesClient

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
        cache_ttl_sec: int = 300,
        store: Optional[StateStore] = None,
        overlap_minutes: int = 20,
    ):
        self.client = client
        self.cache_ttl_sec = max(60, int(cache_ttl_sec))
        self.store = store
        self.overlap_minutes = min(30, max(10, int(overlap_minutes)))
        self._cache: Dict[str, tuple[float, Optional[TradeStats]]] = {}
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
            cached = self._cache.get(cache_key)
            return cached[1] if cached is not None else None

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
            previous = self._cache.get(cache_key)
            previous_stats = previous[1] if previous is not None else None
            if refresh_event is None:
                refresh_event = threading.Event()
                self._inflight[cache_key] = refresh_event
                is_owner = True
            else:
                is_owner = False

        if not is_owner:
            refresh_event.wait()
            with self._cache_lock:
                cached = self._cache.get(cache_key)
                return cached[1] if cached is not None else None

        stats: Optional[TradeStats] = None
        try:
            if self.store is None:
                # Compatibility adapter for callers that have not supplied the
                # local ledger. Production readonly runtimes always supply it.
                stats = self._fetch_stats_from_api(lookback_days)
            else:
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
                self._cache[cache_key] = (time.time(), stats)
                self._inflight.pop(cache_key, None)
                refresh_event.set()

    def fetch_stats(
        self,
        account_id: str,
        lookback_days: int = 30,
    ) -> Optional[TradeStats]:
        """获取交易统计。

        Args:
            account_id: 账户ID（用于缓存）
            lookback_days: 回溯天数

        Returns:
            TradeStats 或 None（如果获取失败）
        """
        now = time.time()

        cache_key = self._cache_key(account_id, lookback_days)
        with self._cache_lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                cached_at, cached_stats = cached
                if now - cached_at < self.cache_ttl_sec:
                    return cached_stats

        return self.refresh_stats(account_id=account_id, lookback_days=lookback_days)

    def _fetch_income_records(
        self,
        *,
        income_type: str,
        start_time_ms: int,
        end_time_ms: int,
        limit: int = 1000,
        max_pages: int = 100,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        seen_keys = set()
        page_limit = max(1, min(1000, int(limit)))
        page_count = max(1, int(max_pages))
        for page in range(1, page_count + 1):
            page_records = self.client.get_income_history(
                symbol=None,
                income_type=income_type,
                start_time=start_time_ms,
                end_time=end_time_ms,
                page=page,
                limit=page_limit,
            )
            if not page_records:
                break

            for record in page_records:
                key = (
                    record.get("incomeType"),
                    record.get("tranId"),
                    record.get("tradeId"),
                    record.get("symbol"),
                    record.get("time"),
                    record.get("income"),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                records.append(record)

            if len(page_records) < page_limit:
                break
            if page == page_count:
                LOGGER.warning(
                    "Income history pagination reached max_pages=%s for income_type=%s; stats may be partial",
                    page_count,
                    income_type,
                )
        return records

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

    @staticmethod
    def _sum_income(records: List[Dict[str, Any]]) -> float:
        total = 0.0
        for record in records:
            try:
                total += float(record.get("income") or 0.0)
            except (TypeError, ValueError):
                continue
        return total

    @staticmethod
    def _aggregate_completed_order_pnl(
        income_records: List[Dict[str, Any]],
        user_trades: List[Dict[str, Any]],
    ) -> Dict[tuple[str, str], float]:
        income_by_trade: Dict[tuple[str, str], float] = {}
        for record in income_records:
            symbol = str(record.get("symbol") or "").strip().upper()
            trade_id = str(record.get("tradeId") or "").strip()
            if not symbol or not trade_id:
                continue
            try:
                income_by_trade[(symbol, trade_id)] = float(record.get("income") or 0.0)
            except (TypeError, ValueError):
                continue

        if not income_by_trade:
            return {}

        order_pnl: Dict[tuple[str, str], float] = {}
        matched_trade_keys = set()
        for trade in user_trades:
            symbol = str(trade.get("symbol") or "").strip().upper()
            raw_trade_id = trade.get("id")
            raw_order_id = trade.get("orderId")
            trade_id = str(raw_trade_id).strip() if raw_trade_id is not None else ""
            order_id = str(raw_order_id).strip() if raw_order_id is not None else ""
            trade_key = (symbol, trade_id)
            if not symbol or not trade_id or not order_id or trade_key not in income_by_trade:
                continue
            order_key = (symbol, order_id)
            order_pnl[order_key] = order_pnl.get(order_key, 0.0) + income_by_trade[trade_key]
            matched_trade_keys.add(trade_key)

        if len(matched_trade_keys) == len(income_by_trade):
            return order_pnl

        # Older or unusual API responses may not expose a matching trade id.
        # Keep the stats usable by falling back to the realized PnL returned by
        # userTrades, still grouped by orderId.
        fallback_order_pnl: Dict[tuple[str, str], float] = {}
        for trade in user_trades:
            symbol = str(trade.get("symbol") or "").strip().upper()
            raw_order_id = trade.get("orderId")
            order_id = str(raw_order_id).strip() if raw_order_id is not None else ""
            if not symbol or not order_id:
                continue
            try:
                realized_pnl = float(trade.get("realizedPnl") or 0.0)
            except (TypeError, ValueError):
                continue
            if realized_pnl == 0.0:
                continue
            order_key = (symbol, order_id)
            fallback_order_pnl[order_key] = fallback_order_pnl.get(order_key, 0.0) + realized_pnl

        if fallback_order_pnl:
            LOGGER.warning(
                "User trade/income id matching incomplete: matched=%s income_records=%s; "
                "using userTrades realizedPnl grouped by order",
                len(matched_trade_keys),
                len(income_by_trade),
            )
            return fallback_order_pnl
        return order_pnl

    def _fetch_stats_from_api(self, lookback_days: int) -> Optional[TradeStats]:
        """从 API 获取统计数据。"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=max(1, lookback_days))
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)

        # Binance returns realized PnL, commissions, and funding fees as
        # separate income types. The readonly card uses their sum as net PnL.
        income_records = self._fetch_income_records(
            income_type="REALIZED_PNL",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=1000,
        )
        commission_records = self._fetch_income_records(
            income_type="COMMISSION",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=1000,
        )
        funding_fee_records = self._fetch_income_records(
            income_type="FUNDING_FEE",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            limit=1000,
        )
        commission_usdt = self._sum_income(commission_records)
        funding_fee_usdt = self._sum_income(funding_fee_records)

        if not income_records:
            net_realized_pnl = commission_usdt + funding_fee_usdt
            return TradeStats(
                total_realized_pnl=0.0,
                total_trades=0,
                win_count=0,
                loss_count=0,
                win_rate_pct=0.0,
                gross_profit=0.0,
                gross_loss=0.0,
                profit_factor=None,
                avg_win=0.0,
                avg_loss=0.0,
                last_updated_utc=end_time.replace(microsecond=0).isoformat(),
                net_realized_pnl=round(net_realized_pnl, 8),
                commission_usdt=round(commission_usdt, 8),
                funding_fee_usdt=round(funding_fee_usdt, 8),
            )

        symbol_windows: Dict[str, tuple[int, int]] = {}
        for record in income_records:
            symbol = str(record.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            raw_time = record.get("time")
            try:
                income_time_ms = int(raw_time) if raw_time is not None else None
            except (TypeError, ValueError):
                income_time_ms = None
            if income_time_ms is None:
                income_window = (start_time_ms, end_time_ms)
            else:
                income_window = (
                    max(
                        start_time_ms,
                        min(income_time_ms - _USER_TRADES_TIME_PADDING_MS, end_time_ms),
                    ),
                    min(
                        end_time_ms,
                        max(income_time_ms + _USER_TRADES_TIME_PADDING_MS, start_time_ms),
                    ),
                )
            previous_window = symbol_windows.get(symbol)
            if previous_window is None:
                symbol_windows[symbol] = income_window
            else:
                symbol_windows[symbol] = (
                    min(previous_window[0], income_window[0]),
                    max(previous_window[1], income_window[1]),
                )
        user_trades = self._fetch_user_trades(
            symbol_windows=symbol_windows,
        )
        completed_order_pnl = self._aggregate_completed_order_pnl(income_records, user_trades)

        if completed_order_pnl:
            pnl_values = list(completed_order_pnl.values())
        else:
            # Preserve the previous income-row behavior only when the account
            # trade endpoint returns no usable order information.
            pnl_values = []
            for record in income_records:
                income_str = record.get("income")
                if income_str is None:
                    continue
                try:
                    pnl_values.append(float(income_str))
                except (TypeError, ValueError):
                    continue

        # 计算统计数据
        total_realized_pnl = 0.0
        win_count = 0
        loss_count = 0
        gross_profit = 0.0
        gross_loss = 0.0

        for income in pnl_values:
            total_realized_pnl += income
            if income > 0:
                win_count += 1
                gross_profit += income
            elif income < 0:
                loss_count += 1
                gross_loss += abs(income)

        total_trades = len(pnl_values)
        win_rate_pct = (win_count / total_trades * 100.0) if total_trades > 0 else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
        avg_win = gross_profit / win_count if win_count > 0 else 0.0
        avg_loss = gross_loss / loss_count if loss_count > 0 else 0.0
        net_realized_pnl = total_realized_pnl + commission_usdt + funding_fee_usdt

        return TradeStats(
            total_realized_pnl=round(total_realized_pnl, 8),
            total_trades=total_trades,
            win_count=win_count,
            loss_count=loss_count,
            win_rate_pct=round(win_rate_pct, 4),
            gross_profit=round(gross_profit, 8),
            gross_loss=round(gross_loss, 8),
            profit_factor=round(profit_factor, 4) if profit_factor is not None else None,
            avg_win=round(avg_win, 8),
            avg_loss=round(avg_loss, 8),
            last_updated_utc=end_time.replace(microsecond=0).isoformat(),
            net_realized_pnl=round(net_realized_pnl, 8),
            commission_usdt=round(commission_usdt, 8),
            funding_fee_usdt=round(funding_fee_usdt, 8),
        )

    def _sync_incremental(self, *, lookback_days: int) -> Dict[str, int]:
        if self.store is None:
            return {"income_inserted": 0, "trades_inserted": 0, "income_requests": 0, "trade_requests": 0}

        now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        lookback_ms = max(1, int(lookback_days)) * 24 * 60 * 60 * 1000
        overlap_ms = self.overlap_minutes * 60 * 1000
        state = self.store.get_lock_state("readonly_trade_stats_cursor_v2") or {}
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
        self.store.set_lock_state(
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
        if self.store is None:
            raise RuntimeError("local trade stats store is unavailable")
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

    def clear_cache(self, account_id: Optional[str] = None) -> None:
        """清除缓存。"""
        if account_id is None:
            self._cache.clear()
            return
        keys_to_remove = [k for k in self._cache if k.startswith(f"{account_id}:")]
        for key in keys_to_remove:
            self._cache.pop(key, None)
