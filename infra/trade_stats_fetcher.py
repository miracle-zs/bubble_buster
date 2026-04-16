"""只读账户交易统计获取模块。

为 readonly 模式账户从 Binance API 获取交易统计数据。
"""
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from infra.binance_futures_client import BinanceFuturesClient

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TradeStats:
    """交易统计数据。"""
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


class TradeStatsFetcher:
    """从 Binance API 获取交易统计。"""

    def __init__(
        self,
        client: BinanceFuturesClient,
        cache_ttl_sec: int = 300,
    ):
        self.client = client
        self.cache_ttl_sec = max(60, int(cache_ttl_sec))
        self._cache: Dict[str, tuple[float, Optional[TradeStats]]] = {}

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
        cache_key = f"{account_id}:{lookback_days}"
        now = time.time()

        # 检查缓存
        if cache_key in self._cache:
            cached_at, cached_stats = self._cache[cache_key]
            if now - cached_at < self.cache_ttl_sec and cached_stats is not None:
                return cached_stats

        try:
            stats = self._fetch_stats_from_api(lookback_days)
            if stats is not None:
                self._cache[cache_key] = (now, stats)
            return stats
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch trade stats for account=%s: %s", account_id, exc)
            return None

    def _fetch_stats_from_api(self, lookback_days: int) -> Optional[TradeStats]:
        """从 API 获取统计数据。"""
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=max(1, lookback_days))

        # 获取 REALIZED_PNL 收益记录
        income_records = self.client.get_income_history(
            symbol=None,
            income_type="REALIZED_PNL",
            start_time=int(start_time.timestamp() * 1000),
            end_time=int(end_time.timestamp() * 1000),
            limit=1000,
        )

        if not income_records:
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
            )

        # 计算统计数据
        total_realized_pnl = 0.0
        win_count = 0
        loss_count = 0
        gross_profit = 0.0
        gross_loss = 0.0

        for record in income_records:
            income_str = record.get("income")
            if income_str is None:
                continue
            try:
                income = float(income_str)
            except (TypeError, ValueError):
                continue

            total_realized_pnl += income
            if income > 0:
                win_count += 1
                gross_profit += income
            elif income < 0:
                loss_count += 1
                gross_loss += abs(income)

        total_trades = win_count + loss_count
        win_rate_pct = (win_count / total_trades * 100.0) if total_trades > 0 else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else None
        avg_win = gross_profit / win_count if win_count > 0 else 0.0
        avg_loss = gross_loss / loss_count if loss_count > 0 else 0.0

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
        )

    def clear_cache(self, account_id: Optional[str] = None) -> None:
        """清除缓存。"""
        if account_id is None:
            self._cache.clear()
            return
        keys_to_remove = [k for k in self._cache if k.startswith(f"{account_id}:")]
        for key in keys_to_remove:
            self._cache.pop(key, None)
