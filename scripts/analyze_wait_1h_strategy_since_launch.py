#!/usr/bin/env python3
"""Mine live trades created after the 1h bearish-entry strategy launch."""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sqlite3
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Optional
from zoneinfo import ZoneInfo


BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"
LOCAL_TZ = ZoneInfo("Asia/Shanghai")
HOUR_MS = 3_600_000


@dataclass(frozen=True)
class PositionRow:
    account_id: str
    trade_day: str
    manual_recovery: bool
    signal_time: datetime
    symbol: str
    entry_time: datetime
    entry_price: float
    close_time: datetime
    close_reason: str
    exit_price: float
    return_pct: float
    realized_pnl: float
    commission: float


@dataclass(frozen=True)
class Candle:
    open_ms: int
    close_ms: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float

    @property
    def bullish(self) -> bool:
        return self.close > self.open


@dataclass
class SignalRow:
    trade_day: str
    symbol: str
    manual_recovery: bool
    accounts: int
    signal_time_utc: str
    entry_time_utc: str
    entry_hour_local: int
    wait_hours: float
    entry_price: float
    close_time_utc: str
    close_reason: str
    actual_return_pct: float
    actual_realized_pnl_usdt: float
    actual_commission_usdt: float
    bearish_body_pct: float
    bearish_range_pct: float
    bearish_close_location: float
    bearish_volume_ratio_24h: float
    pre_signal_return_6h_pct: float
    pre_signal_return_24h_pct: float
    pre_signal_range_24h_pct: float
    prior_symbol_count: int
    prior_symbol_mean_return_pct: float
    prior_symbol_last_return_pct: float
    days_since_prior_symbol: int
    mfe_pct: float
    mae_pct: float
    tp_8_return_pct: float
    tp_10_return_pct: float
    tp_12_return_pct: float
    tp_15_return_pct: float
    tp_18_return_pct: float
    tp_19_return_pct: float
    tp_20_return_pct: float
    be_after_5_return_pct: float
    be_after_8_return_pct: float
    lock_2_after_8_return_pct: float
    lock_3_after_10_return_pct: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--start-utc", default="2026-06-29T16:00:00+00:00")
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--output-summary", required=True)
    return parser.parse_args()


def parse_dt(raw: str) -> datetime:
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def percentile(values: Iterable[float], q: float) -> Optional[float]:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    index = (len(ordered) - 1) * q
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def weighted_exit_price(row: sqlite3.Row) -> Optional[float]:
    qty = float(row["exit_qty"] or 0.0)
    notional = float(row["exit_notional"] or 0.0)
    if qty <= 0 or notional <= 0:
        return None
    return notional / qty


def load_positions(conn: sqlite3.Connection, start_utc: str) -> list[PositionRow]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        WITH exit_fills AS (
            SELECT
                position_id,
                SUM(CASE WHEN side = 'BUY' THEN executed_qty ELSE 0 END) AS exit_qty,
                SUM(CASE WHEN side = 'BUY' THEN executed_qty * avg_price ELSE 0 END) AS exit_notional,
                SUM(CASE WHEN side = 'BUY' THEN COALESCE(realized_pnl, 0) ELSE 0 END) AS realized_pnl,
                SUM(COALESCE(commission, 0)) AS commission
            FROM fills
            WHERE position_id IS NOT NULL
            GROUP BY position_id
        )
        SELECT
            r.account_id,
            r.trade_day_utc,
            r.started_at_utc,
            p.symbol,
            p.opened_at_utc,
            p.entry_price,
            p.closed_at_utc,
            p.close_reason,
            ef.exit_qty,
            ef.exit_notional,
            ef.realized_pnl,
            ef.commission
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        JOIN exit_fills ef ON ef.position_id = p.id
        WHERE p.side = 'SHORT'
          AND p.status LIKE 'CLOSED%'
          AND p.opened_at_utc >= ?
          AND p.closed_at_utc IS NOT NULL
          AND p.entry_price > 0
        ORDER BY r.trade_day_utc, p.symbol, r.account_id
        """,
        (start_utc,),
    ).fetchall()
    out: list[PositionRow] = []
    for row in rows:
        exit_price = weighted_exit_price(row)
        if exit_price is None:
            continue
        entry_price = float(row["entry_price"])
        raw_trade_day = str(row["trade_day_utc"])
        out.append(
            PositionRow(
                account_id=str(row["account_id"]),
                trade_day=raw_trade_day[:10],
                manual_recovery="manual-recover" in raw_trade_day.lower(),
                signal_time=parse_dt(str(row["started_at_utc"])),
                symbol=str(row["symbol"]).upper(),
                entry_time=parse_dt(str(row["opened_at_utc"])),
                entry_price=entry_price,
                close_time=parse_dt(str(row["closed_at_utc"])),
                close_reason=str(row["close_reason"] or ""),
                exit_price=exit_price,
                return_pct=(entry_price - exit_price) / entry_price * 100.0,
                realized_pnl=float(row["realized_pnl"] or 0.0),
                commission=float(row["commission"] or 0.0),
            )
        )
    return out


def median_dt(values: Iterable[datetime]) -> datetime:
    return datetime.fromtimestamp(median(value.timestamp() for value in values), tz=timezone.utc)


def mode_text(values: Iterable[str]) -> str:
    counts = Counter(values)
    return counts.most_common(1)[0][0] if counts else ""


def candle_from_raw(row: list[object]) -> Candle:
    return Candle(
        open_ms=int(row[0]),
        open=float(row[1]),
        high=float(row[2]),
        low=float(row[3]),
        close=float(row[4]),
        close_ms=int(row[6]),
        quote_volume=float(row[7]),
    )


def fetch_klines(symbol: str, start_ms: int, end_ms: int) -> list[list[object]]:
    rows: list[list[object]] = []
    cursor = start_ms
    while cursor <= end_ms:
        query = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "interval": "1h",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1500,
            }
        )
        last_error: Optional[Exception] = None
        payload: Optional[list[list[object]]] = None
        for attempt in range(5):
            try:
                with urllib.request.urlopen(f"{BINANCE_KLINES_URL}?{query}", timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time.sleep(0.5 * (attempt + 1))
        if payload is None:
            raise RuntimeError(f"failed to load {symbol}: {last_error}")
        if not payload:
            break
        rows.extend(payload)
        next_cursor = int(payload[-1][0]) + HOUR_MS
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(payload) < 1500:
            break
    return rows


def load_candles(
    symbol: str,
    start: datetime,
    end: datetime,
    cache_dir: Path,
) -> list[Candle]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{symbol}.json"
    if path.exists() and path.stat().st_size > 0:
        payload = json.loads(path.read_text(encoding="utf-8"))
    else:
        payload = fetch_klines(
            symbol,
            int(start.timestamp() * 1000),
            int(end.timestamp() * 1000),
        )
        path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        time.sleep(0.03)
    return sorted((candle_from_raw(row) for row in payload), key=lambda candle: candle.open_ms)


def last_index_at_or_before(candles: list[Candle], timestamp: datetime) -> Optional[int]:
    target_ms = int(timestamp.timestamp() * 1000)
    result: Optional[int] = None
    for index, candle in enumerate(candles):
        if candle.close_ms <= target_ms:
            result = index
        else:
            break
    return result


def safe_return(start: float, end: float) -> float:
    return (end - start) / start * 100.0 if start > 0 else 0.0


def feature_values(
    candles: list[Candle],
    signal_time: datetime,
    entry_time: datetime,
    entry_price: float,
    close_time: datetime,
) -> dict[str, float]:
    entry_index = last_index_at_or_before(candles, entry_time)
    signal_index = last_index_at_or_before(candles, signal_time)
    if entry_index is None or signal_index is None:
        raise ValueError("missing entry or signal candle")
    entry_candle = candles[entry_index]
    previous_24 = candles[max(0, entry_index - 24) : entry_index]
    volume_reference = median(candle.quote_volume for candle in previous_24) if previous_24 else 0.0
    signal_6_index = max(0, signal_index - 6)
    signal_24_index = max(0, signal_index - 24)
    signal_window = candles[signal_24_index : signal_index + 1]
    range_high = max(candle.high for candle in signal_window)
    range_low = min(candle.low for candle in signal_window)
    candle_range = entry_candle.high - entry_candle.low

    close_ms = int(close_time.timestamp() * 1000)
    path = [
        candle
        for candle in candles
        if candle.open_ms >= int(entry_time.timestamp() * 1000) - 60_000
        and candle.close_ms <= close_ms
    ]
    min_low = min((candle.low for candle in path), default=entry_price)
    max_high = max((candle.high for candle in path), default=entry_price)
    return {
        "bearish_body_pct": max(0.0, safe_return(entry_candle.open, entry_candle.close) * -1.0),
        "bearish_range_pct": (candle_range / entry_candle.open * 100.0) if entry_candle.open > 0 else 0.0,
        "bearish_close_location": (
            (entry_candle.close - entry_candle.low) / candle_range if candle_range > 0 else 0.5
        ),
        "bearish_volume_ratio_24h": (
            entry_candle.quote_volume / volume_reference if volume_reference > 0 else 1.0
        ),
        "pre_signal_return_6h_pct": safe_return(candles[signal_6_index].close, candles[signal_index].close),
        "pre_signal_return_24h_pct": safe_return(candles[signal_24_index].close, candles[signal_index].close),
        "pre_signal_range_24h_pct": (
            (range_high - range_low) / candles[signal_24_index].close * 100.0
            if candles[signal_24_index].close > 0
            else 0.0
        ),
        "mfe_pct": max(0.0, (entry_price - min_low) / entry_price * 100.0),
        "mae_pct": max(0.0, (max_high - entry_price) / entry_price * 100.0),
    }


def path_candles(
    candles: list[Candle],
    entry_time: datetime,
    close_time: datetime,
) -> list[Candle]:
    entry_ms = int(entry_time.timestamp() * 1000)
    close_ms = int(close_time.timestamp() * 1000)
    return [
        candle
        for candle in candles
        if candle.open_ms >= entry_ms - 60_000 and candle.close_ms <= close_ms
    ]


def tp_counterfactual(
    candles: list[Candle],
    entry_price: float,
    actual_return: float,
    threshold_pct: float,
) -> float:
    threshold_price = entry_price * (1.0 - threshold_pct / 100.0)
    eligible = False
    for candle in candles:
        if candle.low <= threshold_price:
            eligible = True
        if eligible and candle.bullish:
            return (entry_price - candle.close) / entry_price * 100.0
    return actual_return


def trailing_counterfactual(
    candles: list[Candle],
    entry_price: float,
    actual_return: float,
    trigger_pct: float,
    lock_pct: float,
) -> float:
    trigger_price = entry_price * (1.0 - trigger_pct / 100.0)
    stop_price = entry_price * (1.0 - lock_pct / 100.0)
    active = False
    for candle in candles:
        if active and candle.high >= stop_price:
            return lock_pct
        if candle.low <= trigger_price:
            if candle.close >= stop_price:
                return (entry_price - candle.close) / entry_price * 100.0
            active = True
    return actual_return


def build_signals(
    positions: list[PositionRow],
    cache_dir: Path,
) -> list[SignalRow]:
    groups: dict[tuple[str, str], list[PositionRow]] = defaultdict(list)
    by_symbol: dict[str, list[PositionRow]] = defaultdict(list)
    for position in positions:
        groups[(position.trade_day, position.symbol)].append(position)
        by_symbol[position.symbol].append(position)

    candles_by_symbol: dict[str, list[Candle]] = {}
    for index, (symbol, symbol_positions) in enumerate(sorted(by_symbol.items()), start=1):
        start = min(position.signal_time for position in symbol_positions)
        end = max(position.close_time for position in symbol_positions)
        candles_by_symbol[symbol] = load_candles(
            symbol,
            datetime.fromtimestamp(start.timestamp() - 30 * 3600, tz=timezone.utc),
            datetime.fromtimestamp(end.timestamp() + 2 * 3600, tz=timezone.utc),
            cache_dir,
        )
        if index % 20 == 0 or index == len(by_symbol):
            print(f"market data {index}/{len(by_symbol)}", flush=True)

    signals: list[SignalRow] = []
    for (trade_day, symbol), rows in sorted(groups.items()):
        entry_time = median_dt(row.entry_time for row in rows)
        close_time = median_dt(row.close_time for row in rows)
        signal_time = min(row.signal_time for row in rows)
        entry_price = median(row.entry_price for row in rows)
        actual_return = median(row.return_pct for row in rows)
        candles = candles_by_symbol[symbol]
        try:
            features = feature_values(candles, signal_time, entry_time, entry_price, close_time)
        except ValueError:
            continue
        path = path_candles(candles, entry_time, close_time)
        tp_returns = {
            threshold: tp_counterfactual(path, entry_price, actual_return, threshold)
            for threshold in (8, 10, 12, 15, 18, 19, 20)
        }
        signals.append(
            SignalRow(
                trade_day=trade_day,
                symbol=symbol,
                manual_recovery=all(row.manual_recovery for row in rows),
                accounts=len(rows),
                signal_time_utc=iso_utc(signal_time),
                entry_time_utc=iso_utc(entry_time),
                entry_hour_local=entry_time.astimezone(LOCAL_TZ).hour,
                wait_hours=(entry_time - signal_time).total_seconds() / 3600.0,
                entry_price=entry_price,
                close_time_utc=iso_utc(close_time),
                close_reason=mode_text(row.close_reason for row in rows),
                actual_return_pct=actual_return,
                actual_realized_pnl_usdt=sum(row.realized_pnl for row in rows),
                actual_commission_usdt=sum(row.commission for row in rows),
                tp_8_return_pct=tp_returns[8],
                tp_10_return_pct=tp_returns[10],
                tp_12_return_pct=tp_returns[12],
                tp_15_return_pct=tp_returns[15],
                tp_18_return_pct=tp_returns[18],
                tp_19_return_pct=tp_returns[19],
                tp_20_return_pct=tp_returns[20],
                be_after_5_return_pct=trailing_counterfactual(path, entry_price, actual_return, 5, 0),
                be_after_8_return_pct=trailing_counterfactual(path, entry_price, actual_return, 8, 0),
                lock_2_after_8_return_pct=trailing_counterfactual(path, entry_price, actual_return, 8, 2),
                lock_3_after_10_return_pct=trailing_counterfactual(path, entry_price, actual_return, 10, 3),
                prior_symbol_count=0,
                prior_symbol_mean_return_pct=0.0,
                prior_symbol_last_return_pct=0.0,
                days_since_prior_symbol=999,
                **features,
            )
        )
    symbol_history: dict[str, list[SignalRow]] = defaultdict(list)
    for signal in signals:
        history = symbol_history[signal.symbol]
        if history:
            signal.prior_symbol_count = len(history)
            signal.prior_symbol_mean_return_pct = mean(row.actual_return_pct for row in history)
            signal.prior_symbol_last_return_pct = history[-1].actual_return_pct
            signal.days_since_prior_symbol = (
                datetime.fromisoformat(signal.trade_day).date()
                - datetime.fromisoformat(history[-1].trade_day).date()
            ).days
        history.append(signal)
    return signals


def metric(
    rows: list[SignalRow],
    value_field: str = "actual_return_pct",
    denominator: Optional[int] = None,
) -> dict[str, object]:
    values = [float(getattr(row, value_field)) for row in rows]
    total_denominator = denominator if denominator is not None else len(values)
    by_day: dict[str, list[float]] = defaultdict(list)
    for row, value in zip(rows, values):
        by_day[row.trade_day].append(value)
    daily_slot_returns = [
        sum(day_values) / 10.0
        for _, day_values in sorted(by_day.items())
    ]
    return {
        "signals": len(values),
        "coverage_pct": len(values) / total_denominator * 100.0 if total_denominator else 0.0,
        "sum_slot_return_pct": sum(values),
        "fixed_slot_mean_return_pct": sum(values) / total_denominator if total_denominator else 0.0,
        "active_mean_return_pct": mean(values) if values else None,
        "median_return_pct": median(values) if values else None,
        "win_rate_pct": sum(value > 0 for value in values) / len(values) * 100.0 if values else None,
        "tp20_rate_pct": (
            sum(row.close_reason == "HOURLY_EXCHANGE_TAKE_PROFIT" for row in rows) / len(rows) * 100.0
            if rows
            else None
        ),
        "p05_return_pct": percentile(values, 0.05),
        "p95_return_pct": percentile(values, 0.95),
        "mean_daily_portfolio_return_pct_at_10_slots": (
            mean(daily_slot_returns) if daily_slot_returns else None
        ),
        "worst_daily_portfolio_return_pct_at_10_slots": (
            min(daily_slot_returns) if daily_slot_returns else None
        ),
    }


def split_rows(rows: list[SignalRow]) -> tuple[list[SignalRow], list[SignalRow], str]:
    days = sorted({row.trade_day for row in rows})
    split_index = max(1, int(len(days) * 0.6))
    split_day = days[split_index]
    return (
        [row for row in rows if row.trade_day < split_day],
        [row for row in rows if row.trade_day >= split_day],
        split_day,
    )


def evaluate_filter(
    rows: list[SignalRow],
    predicate,
    total: int,
) -> dict[str, object]:
    return metric([row for row in rows if predicate(row)], denominator=total)


def policy_metric(
    rows: list[SignalRow],
    predicate,
    value_field: str,
) -> dict[str, object]:
    selected_values = [
        float(getattr(row, value_field))
        for row in rows
        if predicate(row)
    ]
    values = [
        float(getattr(row, value_field)) if predicate(row) else 0.0
        for row in rows
    ]
    by_day: dict[str, list[float]] = defaultdict(list)
    for row, value in zip(rows, values):
        by_day[row.trade_day].append(value)
    daily = [sum(day_values) / 10.0 for _, day_values in sorted(by_day.items())]
    return {
        "signals": len(selected_values),
        "coverage_pct": len(selected_values) / len(rows) * 100.0 if rows else 0.0,
        "sum_slot_return_pct": sum(values),
        "fixed_slot_mean_return_pct": mean(values) if values else None,
        "active_mean_return_pct": mean(selected_values) if selected_values else None,
        "median_return_pct": median(selected_values) if selected_values else None,
        "win_rate_pct": (
            sum(value > 0 for value in selected_values) / len(selected_values) * 100.0
            if selected_values
            else None
        ),
        "p05_return_pct": percentile(selected_values, 0.05),
        "p95_return_pct": percentile(selected_values, 0.95),
        "mean_daily_portfolio_return_pct_at_10_slots": mean(daily) if daily else None,
        "worst_daily_portfolio_return_pct_at_10_slots": min(daily) if daily else None,
    }


def bootstrap_policy_delta(
    rows: list[SignalRow],
    predicate,
    value_field: str,
    iterations: int = 20_000,
) -> dict[str, object]:
    baseline_by_day: dict[str, list[float]] = defaultdict(list)
    candidate_by_day: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        baseline_by_day[row.trade_day].append(row.actual_return_pct)
        candidate_by_day[row.trade_day].append(
            float(getattr(row, value_field)) if predicate(row) else 0.0
        )
    days = sorted(baseline_by_day)
    paired_deltas = [
        sum(candidate_by_day[day]) / 10.0 - sum(baseline_by_day[day]) / 10.0
        for day in days
    ]
    rng = random.Random(20260724)
    bootstrap_means = []
    for _ in range(iterations):
        bootstrap_means.append(
            mean(paired_deltas[rng.randrange(len(paired_deltas))] for _ in paired_deltas)
        )
    return {
        "days": len(days),
        "mean_daily_delta_pct": mean(paired_deltas),
        "median_daily_delta_pct": median(paired_deltas),
        "improved_days": sum(value > 0 for value in paired_deltas),
        "worse_days": sum(value < 0 for value in paired_deltas),
        "flat_days": sum(value == 0 for value in paired_deltas),
        "bootstrap_p025_mean_daily_delta_pct": percentile(bootstrap_means, 0.025),
        "bootstrap_p975_mean_daily_delta_pct": percentile(bootstrap_means, 0.975),
        "bootstrap_probability_positive_pct": (
            sum(value > 0 for value in bootstrap_means) / len(bootstrap_means) * 100.0
        ),
    }


def summarize(positions: list[PositionRow], signals: list[SignalRow]) -> dict[str, object]:
    automated_signals = [row for row in signals if not row.manual_recovery]
    manual_signals = [row for row in signals if row.manual_recovery]
    train, test, split_day = split_rows(automated_signals)
    filters = {
        "entry_before_12": lambda row: row.entry_hour_local < 12,
        "entry_before_11": lambda row: row.entry_hour_local < 11,
        "entry_before_10": lambda row: row.entry_hour_local < 10,
        "bearish_body_ge_0_5": lambda row: row.bearish_body_pct >= 0.5,
        "bearish_close_bottom_half": lambda row: row.bearish_close_location <= 0.5,
        "volume_ratio_ge_1": lambda row: row.bearish_volume_ratio_24h >= 1.0,
        "before_12_and_body_ge_0_5": (
            lambda row: row.entry_hour_local < 12 and row.bearish_body_pct >= 0.5
        ),
        "skip_recent_symbol_loss_7d": (
            lambda row: not (
                row.prior_symbol_count > 0
                and row.days_since_prior_symbol <= 7
                and row.prior_symbol_last_return_pct < 0
            )
        ),
        "skip_negative_symbol_mean_after_2": (
            lambda row: row.prior_symbol_count < 2 or row.prior_symbol_mean_return_pct >= 0
        ),
    }
    account_rows: dict[str, list[PositionRow]] = defaultdict(list)
    for position in positions:
        account_rows[position.account_id].append(position)
    by_account = {}
    for account_id, rows in sorted(account_rows.items()):
        by_account[account_id] = {
            "positions_with_exchange_exit": len(rows),
            "gross_realized_pnl_usdt": sum(row.realized_pnl for row in rows),
            "linked_commission_usdt": sum(row.commission for row in rows),
            "net_linked_pnl_usdt": sum(row.realized_pnl - row.commission for row in rows),
            "mean_position_return_pct": mean(row.return_pct for row in rows),
        }

    policies = {
        "entry_before_12": (lambda row: row.entry_hour_local < 12, "actual_return_pct"),
        "entry_before_10": (lambda row: row.entry_hour_local < 10, "actual_return_pct"),
        "tp10_all": (lambda row: True, "tp_10_return_pct"),
        "tp18_all": (lambda row: True, "tp_18_return_pct"),
        "tp19_all": (lambda row: True, "tp_19_return_pct"),
        "entry_before_12_tp10": (
            lambda row: row.entry_hour_local < 12,
            "tp_10_return_pct",
        ),
        "entry_before_10_tp10": (
            lambda row: row.entry_hour_local < 10,
            "tp_10_return_pct",
        ),
        "volume_ge_1": (
            lambda row: row.bearish_volume_ratio_24h >= 1.0,
            "actual_return_pct",
        ),
        "volume_ge_1_tp10": (
            lambda row: row.bearish_volume_ratio_24h >= 1.0,
            "tp_10_return_pct",
        ),
        "entry_before_12_volume_ge_1_tp10": (
            lambda row: row.entry_hour_local < 12 and row.bearish_volume_ratio_24h >= 1.0,
            "tp_10_return_pct",
        ),
        "lock_3_after_10_all": (lambda row: True, "lock_3_after_10_return_pct"),
        "entry_before_12_lock_3_after_10": (
            lambda row: row.entry_hour_local < 12,
            "lock_3_after_10_return_pct",
        ),
        "skip_recent_symbol_loss_7d": (
            lambda row: not (
                row.prior_symbol_count > 0
                and row.days_since_prior_symbol <= 7
                and row.prior_symbol_last_return_pct < 0
            ),
            "actual_return_pct",
        ),
        "skip_recent_symbol_loss_7d_lock_3_after_10": (
            lambda row: not (
                row.prior_symbol_count > 0
                and row.days_since_prior_symbol <= 7
                and row.prior_symbol_last_return_pct < 0
            ),
            "lock_3_after_10_return_pct",
        ),
        "skip_negative_symbol_mean_after_2": (
            lambda row: row.prior_symbol_count < 2 or row.prior_symbol_mean_return_pct >= 0,
            "actual_return_pct",
        ),
    }

    return {
        "sample": {
            "positions_with_exchange_exit": len(positions),
            "unique_signal_days": len({row.trade_day for row in signals}),
            "unique_symbol_day_signals": len(signals),
            "automated_symbol_day_signals": len(automated_signals),
            "manual_recovery_symbol_day_signals": len(manual_signals),
            "first_trade_day": min(row.trade_day for row in signals),
            "last_trade_day": max(row.trade_day for row in signals),
            "train_test_split_day": split_day,
        },
        "actual": {
            "overall": metric(automated_signals),
            "train": metric(train),
            "test": metric(test),
            "manual_recovery": metric(manual_signals),
        },
        "by_account": by_account,
        "by_entry_hour": {
            str(hour): metric([row for row in automated_signals if row.entry_hour_local == hour])
            for hour in sorted({row.entry_hour_local for row in automated_signals})
        },
        "filters": {
            name: {
                "overall": evaluate_filter(
                    automated_signals,
                    predicate,
                    len(automated_signals),
                ),
                "train": evaluate_filter(train, predicate, len(train)),
                "test": evaluate_filter(test, predicate, len(test)),
            }
            for name, predicate in filters.items()
        },
        "exit_counterfactuals": {
            field: {
                "overall": metric(automated_signals, field),
                "train": metric(train, field),
                "test": metric(test, field),
            }
            for field in (
                "tp_8_return_pct",
                "tp_10_return_pct",
                "tp_12_return_pct",
                "tp_15_return_pct",
                "tp_18_return_pct",
                "tp_19_return_pct",
                "tp_20_return_pct",
                "be_after_5_return_pct",
                "be_after_8_return_pct",
                "lock_2_after_8_return_pct",
                "lock_3_after_10_return_pct",
            )
        },
        "policies": {
            name: {
                "overall": policy_metric(automated_signals, predicate, field),
                "train": policy_metric(train, predicate, field),
                "test": policy_metric(test, predicate, field),
                "paired_day_bootstrap": bootstrap_policy_delta(
                    automated_signals,
                    predicate,
                    field,
                ),
            }
            for name, (predicate, field) in policies.items()
        },
        "feature_quartiles": {
            feature: [
                {
                    "low": low,
                    "high": high,
                    **metric(
                        [
                            row
                            for row in automated_signals
                            if low <= float(getattr(row, feature)) <= high
                        ]
                    ),
                }
                for low, high in zip(
                    [
                        float("-inf"),
                        percentile((getattr(row, feature) for row in automated_signals), 0.25),
                        percentile((getattr(row, feature) for row in automated_signals), 0.50),
                        percentile((getattr(row, feature) for row in automated_signals), 0.75),
                    ],
                    [
                        percentile((getattr(row, feature) for row in automated_signals), 0.25),
                        percentile((getattr(row, feature) for row in automated_signals), 0.50),
                        percentile((getattr(row, feature) for row in automated_signals), 0.75),
                        float("inf"),
                    ],
                )
            ]
            for feature in (
                "wait_hours",
                "bearish_body_pct",
                "bearish_close_location",
                "bearish_volume_ratio_24h",
                "pre_signal_return_6h_pct",
                "pre_signal_return_24h_pct",
                "pre_signal_range_24h_pct",
                "mfe_pct",
                "mae_pct",
            )
        },
    }


def write_csv(path: Path, rows: list[SignalRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(rows[0]).keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


def main() -> None:
    args = parse_args()
    connection = sqlite3.connect(args.db)
    try:
        positions = load_positions(connection, args.start_utc)
    finally:
        connection.close()
    print(f"positions with exchange exit={len(positions)}", flush=True)
    signals = build_signals(positions, Path(args.cache_dir))
    if not signals:
        raise RuntimeError("no analyzable signals")
    summary = summarize(positions, signals)
    write_csv(Path(args.output_csv), signals)
    summary_path = Path(args.output_summary)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary["sample"], ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
