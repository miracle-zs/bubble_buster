#!/usr/bin/env python3
"""Backtest an additional 1h EMA5/EMA10 entry gate on local strategy data.

The daily symbols are taken from the recorded acc01 runs.  Two counterfactuals
are reported:

1. gate_at_actual_entry: keep the historical entry timestamp and skip a trade
   when its last closed bearish candle is not below both EMAs;
2. wait_for_qualifying_candle: keep the same daily symbols and wait from the
   recorded run start until the first bearish 1h candle whose close is below
   both EMA5 and EMA10.  The historical exit event is kept for an
   event-preserving entry-price counterfactual.

This is intentionally local-only.  It does not query Binance or any server.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Optional
from zoneinfo import ZoneInfo


UTC = timezone.utc
LOCAL_TZ = ZoneInfo("Asia/Shanghai")
HOUR_MS = 60 * 60 * 1000
QUARTER_MS = 15 * 60 * 1000


@dataclass(frozen=True)
class Candle:
    open_ms: int
    close_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float = 0.0

    @property
    def open_dt(self) -> datetime:
        return datetime.fromtimestamp(self.open_ms / 1000, tz=UTC)

    @property
    def close_dt(self) -> datetime:
        return datetime.fromtimestamp(self.close_ms / 1000, tz=UTC)


@dataclass
class Sample:
    position_id: int
    run_id: str
    trade_day: str
    symbol: str
    qty: float
    actual_entry_price: float
    actual_opened_at: datetime
    actual_closed_at: datetime
    actual_exit_price: Optional[float]
    actual_close_reason: str
    gross_pnl: float
    commission: float
    run_started_at: datetime
    ordinal: int = 0

    @property
    def net_pnl(self) -> float:
        return self.gross_pnl - self.commission


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso_day(value: str) -> Optional[str]:
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed.isoformat() if parsed.isoformat() == value else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--entry-1h-cache", required=True)
    parser.add_argument("--market-15m", action="append", required=True)
    parser.add_argument("--account", default="acc01")
    parser.add_argument("--start-day", default="2026-06-29")
    parser.add_argument("--end-day", default="2026-08-11")
    parser.add_argument("--max-wait-hours", type=float, default=16.0)
    parser.add_argument("--close-grace-sec", type=float, default=1.0)
    parser.add_argument("--output-csv", default="reports/ema5_ema10_entry_filter_positions_20260814.csv")
    parser.add_argument("--output-summary", default="reports/ema5_ema10_entry_filter_summary_20260814.json")
    return parser.parse_args()


def safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_samples(conn: sqlite3.Connection, account: str, start_day: str, end_day: str) -> list[Sample]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT
            p.id AS position_id,
            p.run_id,
            r.trade_day_utc,
            r.started_at_utc,
            p.symbol,
            p.qty,
            p.entry_price,
            p.opened_at_utc,
            p.closed_at_utc,
            p.close_reason,
            COALESCE(SUM(f.realized_pnl), 0.0) AS gross_pnl,
            COALESCE(SUM(f.commission), 0.0) AS commission,
            COALESCE(
                SUM(CASE WHEN f.side = 'BUY' THEN f.executed_qty ELSE 0.0 END),
                0.0
            ) AS exit_qty,
            COALESCE(
                SUM(CASE WHEN f.side = 'BUY' THEN f.executed_qty * f.avg_price ELSE 0.0 END),
                0.0
            ) AS exit_notional
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        LEFT JOIN fills f ON f.position_id = p.id
        WHERE r.account_id = ?
          AND p.side = 'SHORT'
          AND p.closed_at_utc IS NOT NULL
        GROUP BY p.id
        ORDER BY r.trade_day_utc, r.started_at_utc, p.opened_at_utc, p.id
        """,
        (account,),
    ).fetchall()

    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        day = iso_day(str(row["trade_day_utc"]))
        if day is None or not (start_day <= day <= end_day):
            continue
        grouped[str(row["run_id"])].append(row)

    samples: list[Sample] = []
    for run_rows in grouped.values():
        for ordinal, row in enumerate(sorted(run_rows, key=lambda item: (str(item["opened_at_utc"]), int(item["position_id"])) )):
            exit_qty = safe_float(row["exit_qty"])
            exit_notional = safe_float(row["exit_notional"])
            exit_price = exit_notional / exit_qty if exit_qty > 0 and exit_notional > 0 else None
            samples.append(
                Sample(
                    position_id=int(row["position_id"]),
                    run_id=str(row["run_id"]),
                    trade_day=str(row["trade_day_utc"]),
                    symbol=str(row["symbol"]).upper(),
                    qty=safe_float(row["qty"]),
                    actual_entry_price=safe_float(row["entry_price"]),
                    actual_opened_at=parse_dt(str(row["opened_at_utc"])),
                    actual_closed_at=parse_dt(str(row["closed_at_utc"])),
                    actual_exit_price=exit_price,
                    actual_close_reason=str(row["close_reason"] or ""),
                    gross_pnl=safe_float(row["gross_pnl"]),
                    commission=safe_float(row["commission"]),
                    run_started_at=parse_dt(str(row["started_at_utc"])),
                    ordinal=ordinal,
                )
            )
    return samples


def load_1h_cache(cache_dir: Path) -> dict[str, dict[int, Candle]]:
    out: dict[str, dict[int, Candle]] = defaultdict(dict)
    paths = sorted(cache_dir.glob("*.json"))
    for path in paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        symbol = path.stem.upper()
        for row in payload:
            if len(row) < 7:
                continue
            try:
                open_ms = int(row[0])
                out[symbol][open_ms] = Candle(
                    open_ms=open_ms,
                    close_ms=int(row[6]),
                    open_price=float(row[1]),
                    high_price=float(row[2]),
                    low_price=float(row[3]),
                    close_price=float(row[4]),
                    volume=float(row[5]),
                )
            except (TypeError, ValueError):
                continue
    return out


def aggregate_15m_file(path: Path) -> dict[str, dict[int, Candle]]:
    buckets: dict[tuple[str, int], list[tuple[int, float, float, float, float, float]]] = defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                symbol = str(row["symbol"]).upper()
                open_ms = int(parse_dt(str(row["candle_start"])).timestamp() * 1000)
                bucket_ms = (open_ms // HOUR_MS) * HOUR_MS
                buckets[(symbol, bucket_ms)].append(
                    (
                        open_ms,
                        float(row["open_price"]),
                        float(row["high_price"]),
                        float(row["low_price"]),
                        float(row["close_price"]),
                        float(row.get("volume") or 0.0),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue

    out: dict[str, dict[int, Candle]] = defaultdict(dict)
    for (symbol, bucket_ms), values in buckets.items():
        values.sort(key=lambda value: value[0])
        if len(values) < 4:
            continue
        expected = [bucket_ms + index * QUARTER_MS for index in range(4)]
        if [value[0] for value in values[:4]] != expected:
            continue
        out[symbol][bucket_ms] = Candle(
            open_ms=bucket_ms,
            close_ms=bucket_ms + HOUR_MS - 1,
            open_price=values[0][1],
            high_price=max(value[2] for value in values[:4]),
            low_price=min(value[3] for value in values[:4]),
            close_price=values[3][4],
            volume=sum(value[5] for value in values[:4]),
        )
    return out


def load_market_candles(cache_dir: Path, market_files: list[Path]) -> dict[str, list[Candle]]:
    merged = load_1h_cache(cache_dir)
    for index, path in enumerate(market_files):
        current = aggregate_15m_file(path)
        for symbol, candles in current.items():
            merged.setdefault(symbol, {})
            # Earlier files are preferred in overlapping periods.  The first
            # local archive covers 2026-07-25 through 2026-08-10; later files
            # fill its tail without replacing it.
            for open_ms, candle in candles.items():
                if open_ms not in merged[symbol] or index > 0 and open_ms >= 1783785600000:
                    if open_ms not in merged[symbol] or index > 0:
                        merged[symbol][open_ms] = candle
    return {symbol: [candles[key] for key in sorted(candles)] for symbol, candles in merged.items()}


def ema_series(candles: list[Candle], period: int) -> list[Optional[float]]:
    alpha = 2.0 / (period + 1.0)
    result: list[Optional[float]] = [None] * len(candles)
    if len(candles) < period:
        return result
    seed = mean(candle.close_price for candle in candles[:period])
    result[period - 1] = seed
    previous = seed
    for index in range(period, len(candles)):
        previous = alpha * candles[index].close_price + (1.0 - alpha) * previous
        result[index] = previous
    return result


def build_indicators(candles: list[Candle]) -> dict[int, tuple[Optional[float], Optional[float]]]:
    ema5 = ema_series(candles, 5)
    ema10 = ema_series(candles, 10)
    return {
        candle.open_ms: (ema5[index], ema10[index])
        for index, candle in enumerate(candles)
    }


def contiguous_before(candles: list[Candle], index: int, count: int) -> bool:
    if index < count - 1:
        return False
    start = index - count + 1
    return all(
        candles[position].open_ms - candles[position - 1].open_ms == HOUR_MS
        for position in range(start + 1, index + 1)
    )


def candle_index_by_close(candles: list[Candle], close_ms: int) -> Optional[int]:
    result = None
    for index, candle in enumerate(candles):
        if candle.close_ms <= close_ms:
            result = index
        else:
            break
    return result


def actual_signal_candle(
    candles: list[Candle],
    opened_at: datetime,
    preclose_sec: float = 10.0,
) -> Optional[int]:
    # Production has two paths: a preclose check roughly 10 seconds before
    # the hour ends, and a final-close path a few seconds after the hour.  For
    # a preclose fill, the candle containing the fill is the signal candle;
    # for a final-close fill, the last fully closed candle is the signal.
    opened_ms = int(opened_at.timestamp() * 1000)
    hour_open_ms = (opened_ms // HOUR_MS) * HOUR_MS
    for index, candle in enumerate(candles):
        if candle.open_ms != hour_open_ms:
            continue
        seconds_to_close = (candle.close_ms - opened_ms) / 1000.0
        if 0.0 <= seconds_to_close <= preclose_sec + 2.0:
            return index
        break
    return candle_index_by_close(candles, opened_ms)


def gate_for_index(
    candles: list[Candle],
    indicators: dict[int, tuple[Optional[float], Optional[float]]],
    index: Optional[int],
) -> tuple[str, bool, Optional[float], Optional[float]]:
    if index is None:
        return "NO_SIGNAL_CANDLE", False, None, None
    candle = candles[index]
    ema5, ema10 = indicators.get(candle.open_ms, (None, None))
    if ema5 is None or ema10 is None or not contiguous_before(candles, index, 10):
        return "INSUFFICIENT_EMA_WARMUP", False, ema5, ema10
    if candle.close_price >= candle.open_price:
        return "NOT_BEARISH", False, ema5, ema10
    passed = candle.close_price < ema5 and candle.close_price < ema10
    return ("PASS" if passed else "BEARISH_ABOVE_EMA"), passed, ema5, ema10


def local_midnight_deadline(signal_time: datetime, max_wait_hours: float) -> datetime:
    local = signal_time.astimezone(LOCAL_TZ)
    next_day = (local + timedelta(days=1)).date()
    midnight_local = datetime(next_day.year, next_day.month, next_day.day, tzinfo=LOCAL_TZ)
    return min(signal_time + timedelta(hours=max_wait_hours), midnight_local.astimezone(UTC))


def first_qualifying_candle(
    candles: list[Candle],
    indicators: dict[int, tuple[Optional[float], Optional[float]]],
    signal_time: datetime,
    max_wait_hours: float,
    grace_sec: float,
    require_ema: bool,
) -> tuple[str, Optional[datetime], Optional[float], Optional[int]]:
    deadline = local_midnight_deadline(signal_time, max_wait_hours)
    start_ms = int(signal_time.timestamp() * 1000)
    deadline_ms = int(deadline.timestamp() * 1000)
    for index, candle in enumerate(candles):
        if candle.close_ms < start_ms:
            continue
        if candle.close_ms > deadline_ms:
            break
        if candle.close_price >= candle.open_price:
            continue
        if require_ema:
            status, passed, _ema5, _ema10 = gate_for_index(candles, indicators, index)
            if status != "PASS" or not passed:
                continue
        entry_time = candle.close_dt + timedelta(seconds=grace_sec)
        return "PASS", entry_time, candle.close_price, index
    return "NO_QUALIFYING_CANDLE", None, None, None


def forward_mfe(candles: list[Candle], entry_time: datetime, end_time: datetime) -> Optional[float]:
    entry_index = candle_index_by_close(candles, int(entry_time.timestamp() * 1000))
    if entry_index is None:
        return None
    lows = [
        candle.low_price
        for candle in candles[entry_index:]
        if candle.close_ms > int(entry_time.timestamp() * 1000)
        and candle.close_ms <= int(end_time.timestamp() * 1000)
    ]
    if not lows:
        return None
    entry_price = candles[entry_index].close_price
    return max(0.0, (entry_price - min(lows)) / entry_price * 100.0) if entry_price > 0 else None


def summarize_rows(rows: list[dict], pnl_key: str) -> dict:
    selected = [row for row in rows if row[pnl_key] is not None]
    pnls = [float(row[pnl_key]) for row in selected]
    returns = [float(row["counterfactual_return_pct"]) for row in selected if row["counterfactual_return_pct"] is not None]
    return {
        "positions": len(rows),
        "traded": len(selected),
        "skipped": len(rows) - len(selected),
        "gross_pnl_usdt": round(sum(float(row["counterfactual_gross_pnl"]) for row in selected), 4),
        "net_pnl_usdt": round(sum(pnls), 4),
        "mean_return_pct": round(mean(returns), 4) if returns else None,
        "median_return_pct": round(median(returns), 4) if returns else None,
        "win_rate": round(sum(value > 0 for value in returns) / len(returns), 4) if returns else None,
        "stop_loss_trades": sum(
            row["counterfactual_close_reason"] in {"STOP_LOSS_FILLED", "PORTFOLIO_EQUITY_LOSS_CUT"}
            for row in selected
        ),
        "mfe_median_pct": round(median([row["mfe_pct"] for row in selected if row["mfe_pct"] is not None]), 4)
        if any(row["mfe_pct"] is not None for row in selected)
        else None,
    }


def daily_stats(rows: list[dict], pnl_key: str) -> dict:
    by_day: dict[str, float] = defaultdict(float)
    for row in rows:
        if row[pnl_key] is not None:
            by_day[str(row["trade_day"])] += float(row[pnl_key])
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for day in sorted(by_day):
        cumulative += by_day[day]
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "days": len(by_day),
        "daily_pnl": {day: round(value, 4) for day, value in sorted(by_day.items())},
        "max_cohort_drawdown_usdt": round(max_drawdown, 4),
        "positive_days": sum(value > 0 for value in by_day.values()),
        "negative_days": sum(value < 0 for value in by_day.values()),
    }


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        samples = load_samples(conn, args.account, args.start_day, args.end_day)
    finally:
        conn.close()
    candles_by_symbol = load_market_candles(Path(args.entry_1h_cache), [Path(value) for value in args.market_15m])
    indicators_by_symbol = {
        symbol: build_indicators(candles)
        for symbol, candles in candles_by_symbol.items()
    }

    sample_rows: list[dict] = []
    coverage = defaultdict(int)
    for sample in samples:
        candles = candles_by_symbol.get(sample.symbol, [])
        indicators = indicators_by_symbol.get(sample.symbol, {})
        actual_index = actual_signal_candle(candles, sample.actual_opened_at)
        actual_status, actual_pass, actual_ema5, actual_ema10 = gate_for_index(candles, indicators, actual_index)
        signal_time = sample.run_started_at + timedelta(seconds=sample.ordinal)
        baseline_status, _baseline_pass, _b5, _b10 = first_qualifying_candle(
            candles, indicators, signal_time, args.max_wait_hours, args.close_grace_sec, False
        )
        wait_status, wait_time, wait_price, wait_index = first_qualifying_candle(
            candles, indicators, signal_time, args.max_wait_hours, args.close_grace_sec, True
        )
        if actual_status == "PASS":
            coverage["actual_ema_pass"] += 1
        elif actual_status == "BEARISH_ABOVE_EMA":
            coverage["actual_ema_fail"] += 1
        else:
            coverage[actual_status.lower()] += 1
        if wait_time is not None:
            coverage["wait_qualifying"] += 1
        else:
            coverage["wait_no_qualifying"] += 1
        if baseline_status == "PASS":
            coverage["baseline_bearish_found"] += 1
        else:
            coverage["baseline_no_bearish"] += 1

        actual_exit = sample.actual_exit_price
        actual_gross = sample.gross_pnl if actual_exit is None else (sample.actual_entry_price - actual_exit) * sample.qty
        actual_net = actual_gross - sample.commission
        gate_gross = actual_gross if actual_pass else None
        gate_net = actual_net if actual_pass else None
        gate_return = ((sample.actual_entry_price - actual_exit) / sample.actual_entry_price * 100.0) if actual_pass and actual_exit else None

        wait_is_trade = wait_time is not None and actual_exit is not None and wait_time < sample.actual_closed_at
        wait_gross = (wait_price - actual_exit) * sample.qty if wait_is_trade and wait_price is not None else None
        wait_net = wait_gross - sample.commission if wait_gross is not None else None
        wait_return = (wait_price - actual_exit) / wait_price * 100.0 if wait_is_trade and wait_price else None
        mfe_end = sample.actual_closed_at if sample.actual_closed_at > sample.actual_opened_at else sample.actual_opened_at + timedelta(hours=47.5)
        mfe = forward_mfe(candles, sample.actual_opened_at, mfe_end)
        sample_rows.append(
            {
                "position_id": sample.position_id,
                "run_id": sample.run_id,
                "trade_day": sample.trade_day,
                "symbol": sample.symbol,
                "actual_opened_at_utc": sample.actual_opened_at.isoformat(),
                "actual_closed_at_utc": sample.actual_closed_at.isoformat(),
                "actual_close_reason": sample.actual_close_reason,
                "actual_entry_price": sample.actual_entry_price,
                "actual_exit_price": actual_exit,
                "actual_gross_pnl": round(actual_gross, 8),
                "actual_net_pnl": round(actual_net, 8),
                "signal_candle_status_at_actual": actual_status,
                "actual_signal_ema5": actual_ema5,
                "actual_signal_ema10": actual_ema10,
                "actual_signal_pass": actual_pass,
                "baseline_bearish_status": baseline_status,
                "ema_wait_status": wait_status,
                "ema_wait_entry_time_utc": wait_time.isoformat() if wait_time else None,
                "ema_wait_entry_price": wait_price,
                "ema_wait_delay_hours_from_run_start": ((wait_time - sample.run_started_at).total_seconds() / 3600.0) if wait_time else None,
                "ema_wait_before_actual_exit": wait_is_trade,
                "counterfactual_gross_pnl": round(gate_gross, 8) if gate_gross is not None else None,
                "counterfactual_net_pnl": round(gate_net, 8) if gate_net is not None else None,
                "counterfactual_return_pct": round(gate_return, 8) if gate_return is not None else None,
                "counterfactual_close_reason": sample.actual_close_reason if gate_gross is not None else None,
                "wait_counterfactual_gross_pnl": round(wait_gross, 8) if wait_gross is not None else None,
                "wait_counterfactual_net_pnl": round(wait_net, 8) if wait_net is not None else None,
                "wait_counterfactual_return_pct": round(wait_return, 8) if wait_return is not None else None,
                "wait_counterfactual_close_reason": sample.actual_close_reason if wait_gross is not None else None,
                "mfe_pct": round(mfe, 8) if mfe is not None else None,
            }
        )

    # Rename fields into the shape consumed by summarize_rows for both variants.
    actual_variant = [
        {
            **row,
            "counterfactual_gross_pnl": row["actual_gross_pnl"],
            "counterfactual_net_pnl": row["actual_net_pnl"],
            "counterfactual_return_pct": (
                (row["actual_entry_price"] - row["actual_exit_price"]) / row["actual_entry_price"] * 100.0
                if row["actual_exit_price"] is not None
                else None
            ),
            "counterfactual_close_reason": row["actual_close_reason"],
        }
        for row in sample_rows
    ]
    gate_variant = sample_rows
    wait_variant = [
        {
            **row,
            "counterfactual_gross_pnl": row["wait_counterfactual_gross_pnl"],
            "counterfactual_net_pnl": row["wait_counterfactual_net_pnl"],
            "counterfactual_return_pct": row["wait_counterfactual_return_pct"],
            "counterfactual_close_reason": row["wait_counterfactual_close_reason"],
        }
        for row in sample_rows
    ]

    by_status: dict[str, list[dict]] = defaultdict(list)
    for row in actual_variant:
        by_status[str(row["signal_candle_status_at_actual"])].append(row)

    summary = {
        "scope": {
            "account": args.account,
            "start_day": args.start_day,
            "end_day": args.end_day,
            "positions": len(samples),
            "trade_days": sorted({sample.trade_day for sample in samples}),
            "market_files": args.market_15m,
            "ema_definition": "EMA on closed UTC 1h candle closes; signal requires bearish close and close < EMA5 and close < EMA10",
            "wait_definition": "same recorded daily symbol, from run start, max 16h and before next local midnight",
            "exit_definition": "historical exit event and price retained for entry-price counterfactual; not a full re-simulation of exit orders",
        },
        "coverage": dict(sorted(coverage.items())),
        "actual_baseline": summarize_rows(actual_variant, "actual_net_pnl"),
        "gate_at_actual_entry": summarize_rows(gate_variant, "counterfactual_net_pnl"),
        "wait_for_qualifying_candle": summarize_rows(wait_variant, "counterfactual_net_pnl"),
        "daily_actual": daily_stats(actual_variant, "actual_net_pnl"),
        "daily_gate_at_actual_entry": daily_stats(gate_variant, "counterfactual_net_pnl"),
        "daily_wait_for_qualifying_candle": daily_stats(wait_variant, "counterfactual_net_pnl"),
        "actual_signal_status_performance": {
            status: summarize_rows(group, "actual_net_pnl")
            for status, group in sorted(by_status.items())
        },
        "ema_wait_entry_timing": {
            "traded_after_actual_open": sum(
                row["wait_counterfactual_net_pnl"] is not None and row["ema_wait_entry_time_utc"] is not None
                and parse_dt(row["ema_wait_entry_time_utc"]) > parse_dt(row["actual_opened_at_utc"])
                for row in sample_rows
            ),
            "traded_before_or_at_actual_open": sum(
                row["wait_counterfactual_net_pnl"] is not None and row["ema_wait_entry_time_utc"] is not None
                and parse_dt(row["ema_wait_entry_time_utc"]) <= parse_dt(row["actual_opened_at_utc"])
                for row in sample_rows
            ),
            "delay_hours": [
                round(row["ema_wait_delay_hours_from_run_start"], 4)
                for row in sample_rows
                if row["wait_counterfactual_net_pnl"] is not None and row["ema_wait_delay_hours_from_run_start"] is not None
            ],
        },
    }
    if summary["ema_wait_entry_timing"]["delay_hours"]:
        delays = summary["ema_wait_entry_timing"]["delay_hours"]
        summary["ema_wait_entry_timing"]["delay_median_hours"] = round(median(delays), 4)
        summary["ema_wait_entry_timing"]["delay_p90_hours"] = round(sorted(delays)[max(0, math.ceil(len(delays) * 0.9) - 1)], 4)

    output_csv = Path(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = list(sample_rows[0].keys()) if sample_rows else []
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample_rows)
    output_summary = Path(args.output_summary)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    output_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"wrote {output_csv}")
    print(f"wrote {output_summary}")


if __name__ == "__main__":
    main()
