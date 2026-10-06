#!/usr/bin/env python3
"""Mine 08:00-anchored price-path features for daily short signals."""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sqlite3
import time as time_module
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Optional
from zoneinfo import ZoneInfo


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
HOUR_MS = 3_600_000
LAUNCH_DAY = date(2026, 6, 30)
VALIDATION_DAY = date(2026, 5, 1)
ENTRY_FEATURES = (
    "entry_vs_8_pct",
    "pre_entry_peak_gain_pct",
    "pre_entry_drawdown_from_peak_pct",
    "pre_entry_retrace_ratio_pct",
    "wait_from_8_hours",
)
PATH_FEATURES = (
    "peak_gain_24h_pct",
    "trough_return_24h_pct",
    "final_return_24h_pct",
    "peak_to_final_drawdown_pct",
    "peak_retrace_ratio_pct",
    "peak_hour_offset",
)
NEXT_8_FEATURES = (
    "peak_gain_24h_pct",
    "final_return_24h_pct",
    "peak_to_final_drawdown_pct",
    "peak_retrace_ratio_pct",
    "peak_hour_offset",
    "next_8_short_return_pct",
)
NOON_FEATURES = (
    "return_8_to_noon_pct",
    "peak_gain_to_noon_pct",
    "noon_drawdown_from_peak_pct",
    "noon_retrace_ratio_pct",
    "noon_short_return_from_entry_pct",
)
BINANCE_KLINES_URL = "https://fapi.binance.com/fapi/v1/klines"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--prelaunch-replay", required=True)
    parser.add_argument("--live-signals", required=True)
    parser.add_argument("--cache-dir", required=True)
    parser.add_argument("--output-csv", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--fetch-missing", action="store_true")
    parser.add_argument("--request-delay-sec", type=float, default=0.15)
    return parser.parse_args()


def parse_dt(raw: str) -> datetime:
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def percentile(values: Iterable[float], q: float) -> Optional[float]:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))
    if not ordered:
        return None
    index = (len(ordered) - 1) * q
    low = math.floor(index)
    high = math.ceil(index)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def rank_values(values: list[float]) -> list[float]:
    ordered = sorted(enumerate(values), key=lambda pair: pair[1])
    ranks = [0.0] * len(values)
    index = 0
    while index < len(ordered):
        end = index + 1
        while end < len(ordered) and ordered[end][1] == ordered[index][1]:
            end += 1
        rank = (index + end - 1) / 2.0
        for position in range(index, end):
            ranks[ordered[position][0]] = rank
        index = end
    return ranks


def pearson(left: list[float], right: list[float]) -> Optional[float]:
    if len(left) < 3 or len(left) != len(right):
        return None
    left_mean = mean(left)
    right_mean = mean(right)
    numerator = sum((x - left_mean) * (y - right_mean) for x, y in zip(left, right))
    left_var = sum((x - left_mean) ** 2 for x in left)
    right_var = sum((y - right_mean) ** 2 for y in right)
    if left_var <= 0 or right_var <= 0:
        return None
    return numerator / math.sqrt(left_var * right_var)


def spearman(rows: list[dict[str, object]], feature: str) -> Optional[float]:
    pairs = [
        (float(row[feature]), float(row["strategy_return_pct"]))
        for row in rows
        if row.get(feature) is not None
    ]
    if len(pairs) < 3:
        return None
    return pearson(
        rank_values([pair[0] for pair in pairs]),
        rank_values([pair[1] for pair in pairs]),
    )


def load_position_sessions(db_path: Path) -> dict[int, dict[str, object]]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT p.id, p.symbol, r.trade_day_utc, r.started_at_utc
            FROM positions p
            JOIN runs r ON r.run_id = p.run_id
            """
        ).fetchall()
    finally:
        conn.close()
    result: dict[int, dict[str, object]] = {}
    for row in rows:
        started = parse_dt(str(row["started_at_utc"]))
        result[int(row["id"])] = {
            "symbol": str(row["symbol"]).upper(),
            "session_day": started.astimezone(LOCAL_TZ).date(),
            "manual_recovery": "manual-recover" in str(row["trade_day_utc"]).lower(),
        }
    return result


def load_prelaunch_signals(
    replay_path: Path,
    sessions: dict[int, dict[str, object]],
) -> list[dict[str, object]]:
    grouped: dict[tuple[date, str], list[dict[str, object]]] = defaultdict(list)
    with replay_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            session = sessions.get(int(row["position_id"]))
            if not session or bool(session["manual_recovery"]):
                continue
            session_day = session["session_day"]
            if not isinstance(session_day, date) or session_day >= LAUNCH_DAY:
                continue
            if str(row.get("replay_entered") or "") != "1":
                continue
            if not str(row.get("replay_return_pct") or "").strip():
                continue
            grouped[(session_day, str(row["symbol"]).upper())].append(row)

    signals: list[dict[str, object]] = []
    for (session_day, symbol), rows in sorted(grouped.items()):
        entry_times = [parse_dt(row["replay_entry_time_utc"]) for row in rows]
        exit_times = [
            parse_dt(row["replay_exit_time_utc"])
            for row in rows
            if str(row.get("replay_exit_time_utc") or "").strip()
        ]
        signals.append(
            {
                "session_day": session_day,
                "symbol": symbol,
                "period": "pre_train" if session_day < VALIDATION_DAY else "pre_validation",
                "accounts": len(rows),
                "entry_time": datetime.fromtimestamp(
                    median(value.timestamp() for value in entry_times), tz=timezone.utc
                ),
                "entry_price": median(float(row["replay_entry_price"]) for row in rows),
                "exit_time": (
                    datetime.fromtimestamp(
                        median(value.timestamp() for value in exit_times), tz=timezone.utc
                    )
                    if exit_times
                    else None
                ),
                "strategy_return_pct": median(float(row["replay_return_pct"]) for row in rows),
                "source": "prelaunch_18pct_replay",
            }
        )
    return signals


def load_live_signals(path: Path) -> list[dict[str, object]]:
    signals: list[dict[str, object]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("manual_recovery") or "").strip().lower() == "true":
                continue
            signal_time = parse_dt(row["signal_time_utc"])
            session_day = signal_time.astimezone(LOCAL_TZ).date()
            if session_day < LAUNCH_DAY:
                continue
            signals.append(
                {
                    "session_day": session_day,
                    "symbol": str(row["symbol"]).upper(),
                    "period": "live_validation",
                    "accounts": int(row["accounts"]),
                    "entry_time": parse_dt(row["entry_time_utc"]),
                    "entry_price": float(row["entry_price"]),
                    "exit_time": parse_dt(row["close_time_utc"]),
                    "strategy_return_pct": float(row["tp_18_return_pct"]),
                    "source": "live_18pct_counterfactual",
                }
            )
    return signals


def load_candles(cache_dir: Path, symbol: str) -> list[dict[str, float]]:
    path = cache_dir / f"{symbol}.json"
    if not path.exists():
        return []
    raw_rows = json.loads(path.read_text(encoding="utf-8"))
    return [
        {
            "open_ms": int(row[0]),
            "close_ms": int(row[6]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
        }
        for row in raw_rows
    ]


def fetch_klines(
    symbol: str,
    start_ms: int,
    end_ms: int,
    request_delay_sec: float,
) -> list[list[object]]:
    rows: list[list[object]] = []
    cursor = start_ms
    while cursor < end_ms:
        query = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "interval": "1h",
                "startTime": cursor,
                "endTime": end_ms - 1,
                "limit": 1000,
            }
        )
        last_error: Optional[Exception] = None
        payload: object = None
        for attempt in range(5):
            try:
                with urllib.request.urlopen(f"{BINANCE_KLINES_URL}?{query}", timeout=30) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                last_error = None
                break
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                time_module.sleep(1.0 * (attempt + 1))
        if last_error is not None:
            raise RuntimeError(f"failed to fetch {symbol}: {last_error}") from last_error
        if not isinstance(payload, list):
            raise RuntimeError(f"unexpected Binance response for {symbol}: {payload}")
        batch = [row for row in payload if isinstance(row, list) and len(row) >= 7]
        if not batch:
            break
        rows.extend(batch)
        next_cursor = int(batch[-1][0]) + HOUR_MS
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        time_module.sleep(max(0.0, request_delay_sec))
    return rows


def merge_missing_cache(
    cache_dir: Path,
    signals: list[dict[str, object]],
    request_delay_sec: float,
) -> dict[str, int]:
    required_by_symbol: dict[str, list[int]] = defaultdict(list)
    for signal in signals:
        session_day = signal["session_day"]
        if not isinstance(session_day, date):
            continue
        start_local = datetime.combine(session_day, time(hour=8), tzinfo=LOCAL_TZ)
        required_by_symbol[str(signal["symbol"])].append(
            int(start_local.astimezone(timezone.utc).timestamp() * 1000)
        )

    cache_dir.mkdir(parents=True, exist_ok=True)
    fetched_requests = 0
    fetched_rows = 0
    for index, (symbol, required_starts) in enumerate(sorted(required_by_symbol.items()), start=1):
        path = cache_dir / f"{symbol}.json"
        existing_rows = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
        existing_by_open = {
            int(row[0]): row
            for row in existing_rows
            if isinstance(row, list) and len(row) >= 7
        }
        missing_starts = sorted(
            {
                start
                for start in required_starts
                if any(start + offset * HOUR_MS not in existing_by_open for offset in range(24))
            }
        )
        if not missing_starts:
            continue

        ranges: list[list[int]] = []
        for start in missing_starts:
            if not ranges or start - ranges[-1][1] > 7 * 24 * HOUR_MS:
                ranges.append([start, start + 24 * HOUR_MS])
            else:
                ranges[-1][1] = max(ranges[-1][1], start + 24 * HOUR_MS)

        for start_ms, end_ms in ranges:
            fetched = fetch_klines(symbol, start_ms, end_ms, request_delay_sec)
            fetched_requests += math.ceil(max(1, len(fetched)) / 1000)
            fetched_rows += len(fetched)
            for row in fetched:
                existing_by_open[int(row[0])] = row
        path.write_text(
            json.dumps(
                [existing_by_open[key] for key in sorted(existing_by_open)],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            encoding="utf-8",
        )
        if index % 20 == 0 or index == len(required_by_symbol):
            print(
                f"market cache {index}/{len(required_by_symbol)} "
                f"requests={fetched_requests} rows={fetched_rows}",
                flush=True,
            )
    return {"requests": fetched_requests, "rows": fetched_rows}


def ratio(numerator: float, denominator: float) -> Optional[float]:
    if abs(denominator) <= 1e-12:
        return None
    return numerator / denominator


def anchored_features(
    signal: dict[str, object],
    candles: list[dict[str, float]],
) -> Optional[dict[str, object]]:
    session_day = signal["session_day"]
    if not isinstance(session_day, date):
        return None
    start_local = datetime.combine(session_day, time(hour=8), tzinfo=LOCAL_TZ)
    start_ms = int(start_local.astimezone(timezone.utc).timestamp() * 1000)
    end_ms = start_ms + 24 * HOUR_MS
    day_rows = [row for row in candles if start_ms <= int(row["open_ms"]) < end_ms]
    if not day_rows or int(day_rows[0]["open_ms"]) != start_ms:
        return None

    base_price = float(day_rows[0]["open"])
    entry_time = signal["entry_time"]
    if not isinstance(entry_time, datetime):
        return None
    entry_ms = int(entry_time.timestamp() * 1000)
    known_rows = [
        row for row in day_rows if int(row["close_ms"]) <= entry_ms + 1_000
    ]
    entry_price = float(signal["entry_price"])
    pre_high = max([base_price] + [float(row["high"]) for row in known_rows])
    pre_low = min([base_price] + [float(row["low"]) for row in known_rows])
    pre_peak_gain = (pre_high / base_price - 1.0) * 100.0
    pre_drawdown = (pre_high - entry_price) / pre_high * 100.0
    pre_retrace = ratio(pre_high - entry_price, pre_high - base_price)

    result: dict[str, object] = {
        **signal,
        "base_8_price": base_price,
        "entry_vs_8_pct": (entry_price / base_price - 1.0) * 100.0,
        "pre_entry_peak_gain_pct": pre_peak_gain,
        "pre_entry_trough_return_pct": (pre_low / base_price - 1.0) * 100.0,
        "pre_entry_drawdown_from_peak_pct": pre_drawdown,
        "pre_entry_retrace_ratio_pct": None if pre_retrace is None else pre_retrace * 100.0,
        "wait_from_8_hours": max(0.0, (entry_time - start_local.astimezone(timezone.utc)).total_seconds() / 3600),
        "full_24h_coverage": len(day_rows) == 24,
    }

    if len(day_rows) == 24:
        peak_index = max(range(len(day_rows)), key=lambda index: float(day_rows[index]["high"]))
        peak_price = float(day_rows[peak_index]["high"])
        trough_price = min(float(row["low"]) for row in day_rows)
        final_price = float(day_rows[-1]["close"])
        peak_gain = (peak_price / base_price - 1.0) * 100.0
        peak_retrace = ratio(peak_price - final_price, peak_price - base_price)
        noon_rows = day_rows[:4]
        noon_price = float(noon_rows[-1]["close"])
        noon_peak = max(float(row["high"]) for row in noon_rows)
        noon_retrace = ratio(noon_peak - noon_price, noon_peak - base_price)
        result.update(
            {
                "peak_gain_24h_pct": peak_gain,
                "trough_return_24h_pct": (trough_price / base_price - 1.0) * 100.0,
                "final_return_24h_pct": (final_price / base_price - 1.0) * 100.0,
                "peak_to_final_drawdown_pct": (peak_price - final_price) / peak_price * 100.0,
                "peak_retrace_ratio_pct": None if peak_retrace is None else peak_retrace * 100.0,
                "peak_hour_offset": peak_index,
                "next_8_short_return_pct": (entry_price - final_price) / entry_price * 100.0,
                "return_8_to_noon_pct": (noon_price / base_price - 1.0) * 100.0,
                "peak_gain_to_noon_pct": (noon_peak / base_price - 1.0) * 100.0,
                "noon_drawdown_from_peak_pct": (noon_peak - noon_price) / noon_peak * 100.0,
                "noon_retrace_ratio_pct": (
                    None if noon_retrace is None else noon_retrace * 100.0
                ),
                "noon_short_return_from_entry_pct": (
                    (entry_price - noon_price) / entry_price * 100.0
                ),
            }
        )
        effective_exit_time = signal.get("exit_time")
        if str(signal.get("source")) == "live_18pct_counterfactual":
            eligible = False
            for candle in candles:
                if int(candle["open_ms"]) < entry_ms - 60_000:
                    continue
                if effective_exit_time is not None and isinstance(effective_exit_time, datetime):
                    if int(candle["close_ms"]) > int(effective_exit_time.timestamp() * 1000):
                        break
                if float(candle["low"]) <= entry_price * 0.82:
                    eligible = True
                if eligible and float(candle["close"]) > float(candle["open"]):
                    effective_exit_time = datetime.fromtimestamp(
                        int(candle["close_ms"]) / 1000.0, tz=timezone.utc
                    )
                    break
        result["effective_exit_time"] = effective_exit_time
        noon_time = start_local.replace(hour=12).astimezone(timezone.utc)
        result["open_at_noon"] = (
            entry_time <= noon_time
            and (
                effective_exit_time is None
                or (
                    isinstance(effective_exit_time, datetime)
                    and effective_exit_time > noon_time
                )
            )
        )
        result["open_at_next_8"] = (
            effective_exit_time is None
            or (
                isinstance(effective_exit_time, datetime)
                and effective_exit_time > datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
            )
        )
    else:
        for feature in (*PATH_FEATURES, *NOON_FEATURES, "next_8_short_return_pct"):
            result[feature] = None
        result["effective_exit_time"] = signal.get("exit_time")
        result["open_at_noon"] = None
        result["open_at_next_8"] = None
    return result


def period_summary(rows: list[dict[str, object]]) -> dict[str, object]:
    values = [float(row["strategy_return_pct"]) for row in rows]
    days = sorted({str(row["session_day"]) for row in rows})
    return {
        "signals": len(rows),
        "days": len(days),
        "mean_return_pct": mean(values) if values else None,
        "median_return_pct": median(values) if values else None,
        "win_rate_pct": mean([value > 0 for value in values]) * 100.0 if values else None,
        "full_24h_coverage_pct": (
            mean([bool(row["full_24h_coverage"]) for row in rows]) * 100.0 if rows else None
        ),
    }


def quantile_bins(
    rows: list[dict[str, object]],
    feature: str,
    cutoffs: list[float],
) -> list[dict[str, object]]:
    boundaries = [-math.inf, *cutoffs, math.inf]
    result: list[dict[str, object]] = []
    for index in range(len(boundaries) - 1):
        selected = [
            row
            for row in rows
            if row.get(feature) is not None
            and boundaries[index] < float(row[feature]) <= boundaries[index + 1]
        ]
        values = [float(row["strategy_return_pct"]) for row in selected]
        result.append(
            {
                "lower_exclusive": None if math.isinf(boundaries[index]) else boundaries[index],
                "upper_inclusive": None if math.isinf(boundaries[index + 1]) else boundaries[index + 1],
                "signals": len(selected),
                "mean_return_pct": mean(values) if values else None,
                "win_rate_pct": mean([value > 0 for value in values]) * 100.0 if values else None,
            }
        )
    return result


def rule_keeps(row: dict[str, object], feature: str, operator: str, threshold: float) -> bool:
    value = row.get(feature)
    if value is None:
        return True
    if operator == ">=":
        return float(value) >= threshold
    return float(value) <= threshold


def evaluate_rule(
    rows: list[dict[str, object]],
    feature: str,
    operator: str,
    threshold: float,
) -> dict[str, object]:
    if not rows:
        return {
            "signals": 0,
            "kept": 0,
            "coverage_pct": None,
            "active_mean_return_pct": None,
            "fixed_slot_mean_return_pct": None,
            "baseline_mean_return_pct": None,
            "delta_fixed_slot_mean_pct": None,
        }
    kept = [row for row in rows if rule_keeps(row, feature, operator, threshold)]
    baseline_values = [float(row["strategy_return_pct"]) for row in rows]
    kept_values = [float(row["strategy_return_pct"]) for row in kept]
    fixed_mean = sum(kept_values) / len(rows)
    baseline_mean = mean(baseline_values)
    return {
        "signals": len(rows),
        "kept": len(kept),
        "coverage_pct": len(kept) / len(rows) * 100.0,
        "active_mean_return_pct": mean(kept_values) if kept_values else None,
        "fixed_slot_mean_return_pct": fixed_mean,
        "baseline_mean_return_pct": baseline_mean,
        "delta_fixed_slot_mean_pct": fixed_mean - baseline_mean,
    }


def bootstrap_daily_delta(
    rows: list[dict[str, object]],
    feature: str,
    operator: str,
    threshold: float,
    iterations: int = 20_000,
) -> dict[str, object]:
    by_day: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_day[str(row["session_day"])].append(row)
    daily_deltas = []
    for day_rows in by_day.values():
        skipped_return = sum(
            float(row["strategy_return_pct"])
            for row in day_rows
            if not rule_keeps(row, feature, operator, threshold)
        )
        daily_deltas.append(-skipped_return / 10.0)
    if not daily_deltas:
        return {"days": 0}
    rng = random.Random(20260725)
    samples = [
        mean(rng.choice(daily_deltas) for _ in daily_deltas)
        for _ in range(iterations)
    ]
    return {
        "days": len(daily_deltas),
        "mean_daily_delta_pct": mean(daily_deltas),
        "positive_probability_pct": mean([value > 0 for value in samples]) * 100.0,
        "p025_pct": percentile(samples, 0.025),
        "p975_pct": percentile(samples, 0.975),
    }


def next_8_rule_triggers(
    row: dict[str, object],
    feature: str,
    operator: str,
    threshold: float,
) -> bool:
    if not bool(row.get("open_at_next_8")) or row.get(feature) is None:
        return False
    if operator == ">=":
        return float(row[feature]) >= threshold
    return float(row[feature]) <= threshold


def evaluate_next_8_rule(
    rows: list[dict[str, object]],
    feature: str,
    operator: str,
    threshold: float,
) -> dict[str, object]:
    baseline = [float(row["strategy_return_pct"]) for row in rows]
    hybrid = [
        (
            float(row["next_8_short_return_pct"])
            if next_8_rule_triggers(row, feature, operator, threshold)
            else float(row["strategy_return_pct"])
        )
        for row in rows
    ]
    triggered = sum(next_8_rule_triggers(row, feature, operator, threshold) for row in rows)
    return {
        "signals": len(rows),
        "open_at_next_8": sum(bool(row.get("open_at_next_8")) for row in rows),
        "triggered": triggered,
        "triggered_pct": triggered / len(rows) * 100.0 if rows else None,
        "baseline_mean_return_pct": mean(baseline) if baseline else None,
        "hybrid_mean_return_pct": mean(hybrid) if hybrid else None,
        "delta_mean_return_pct": mean(hybrid) - mean(baseline) if baseline else None,
    }


def bootstrap_next_8_delta(
    rows: list[dict[str, object]],
    feature: str,
    operator: str,
    threshold: float,
    iterations: int = 20_000,
) -> dict[str, object]:
    by_day: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        delta = 0.0
        if next_8_rule_triggers(row, feature, operator, threshold):
            delta = float(row["next_8_short_return_pct"]) - float(row["strategy_return_pct"])
        by_day[str(row["session_day"])].append(delta)
    daily_deltas = [sum(values) / 10.0 for values in by_day.values()]
    if not daily_deltas:
        return {"days": 0}
    rng = random.Random(20260725)
    samples = [
        mean(rng.choice(daily_deltas) for _ in daily_deltas)
        for _ in range(iterations)
    ]
    return {
        "days": len(daily_deltas),
        "mean_daily_delta_pct": mean(daily_deltas),
        "positive_probability_pct": mean([value > 0 for value in samples]) * 100.0,
        "p025_pct": percentile(samples, 0.025),
        "p975_pct": percentile(samples, 0.975),
    }


def build_candidates(
    period_rows: dict[str, list[dict[str, object]]],
) -> list[dict[str, object]]:
    train_rows = period_rows["pre_train"]
    candidates: list[dict[str, object]] = []
    for feature in ENTRY_FEATURES:
        values = [float(row[feature]) for row in train_rows if row.get(feature) is not None]
        thresholds = sorted(
            {
                round(value, 6)
                for q in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
                if (value := percentile(values, q)) is not None
            }
        )
        for threshold in thresholds:
            for operator in ("<=", ">="):
                evaluations = {
                    period: evaluate_rule(rows, feature, operator, threshold)
                    for period, rows in period_rows.items()
                }
                train = evaluations["pre_train"]
                pre_validation = evaluations["pre_validation"]
                live = evaluations["live_validation"]
                if (
                    float(train["coverage_pct"] or 0.0) < 40.0
                    or int(pre_validation["kept"]) < 20
                    or int(live["kept"]) < 20
                ):
                    continue
                deltas = [
                    float(evaluations[period]["delta_fixed_slot_mean_pct"] or 0.0)
                    for period in ("pre_train", "pre_validation", "live_validation")
                ]
                candidates.append(
                    {
                        "feature": feature,
                        "operator": operator,
                        "threshold": threshold,
                        "evaluations": evaluations,
                        "positive_periods": sum(delta > 0 for delta in deltas),
                        "minimum_period_delta_pct": min(deltas),
                        "mean_period_delta_pct": mean(deltas),
                    }
                )
    return sorted(
        candidates,
        key=lambda row: (
            int(row["positive_periods"]),
            float(row["minimum_period_delta_pct"]),
            float(row["mean_period_delta_pct"]),
        ),
        reverse=True,
    )


def build_next_8_candidates(
    period_rows: dict[str, list[dict[str, object]]],
) -> list[dict[str, object]]:
    train_rows = [
        row for row in period_rows["pre_train"] if bool(row.get("open_at_next_8"))
    ]
    candidates: list[dict[str, object]] = []
    for feature in NEXT_8_FEATURES:
        values = [float(row[feature]) for row in train_rows if row.get(feature) is not None]
        thresholds = sorted(
            {
                round(value, 6)
                for q in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
                if (value := percentile(values, q)) is not None
            }
        )
        for threshold in thresholds:
            for operator in ("<=", ">="):
                evaluations = {
                    period: evaluate_next_8_rule(rows, feature, operator, threshold)
                    for period, rows in period_rows.items()
                }
                if (
                    int(evaluations["pre_train"]["triggered"]) < 20
                    or int(evaluations["pre_validation"]["triggered"]) < 10
                    or int(evaluations["live_validation"]["triggered"]) < 5
                ):
                    continue
                deltas = [
                    float(evaluations[period]["delta_mean_return_pct"] or 0.0)
                    for period in ("pre_train", "pre_validation", "live_validation")
                ]
                candidates.append(
                    {
                        "feature": feature,
                        "operator": operator,
                        "threshold": threshold,
                        "evaluations": evaluations,
                        "positive_periods": sum(delta > 0 for delta in deltas),
                        "minimum_period_delta_pct": min(deltas),
                        "mean_period_delta_pct": mean(deltas),
                    }
                )
    return sorted(
        candidates,
        key=lambda row: (
            int(row["positive_periods"]),
            float(row["minimum_period_delta_pct"]),
            float(row["mean_period_delta_pct"]),
        ),
        reverse=True,
    )


def checkpoint_rule_triggers(
    row: dict[str, object],
    feature: str,
    operator: str,
    threshold: float,
    open_field: str,
) -> bool:
    if not bool(row.get(open_field)) or row.get(feature) is None:
        return False
    if operator == ">=":
        return float(row[feature]) >= threshold
    return float(row[feature]) <= threshold


def evaluate_checkpoint_rule(
    rows: list[dict[str, object]],
    feature: str,
    operator: str,
    threshold: float,
    open_field: str,
    exit_return_field: str,
) -> dict[str, object]:
    baseline = [float(row["strategy_return_pct"]) for row in rows]
    hybrid = []
    triggered = 0
    for row in rows:
        if checkpoint_rule_triggers(row, feature, operator, threshold, open_field):
            hybrid.append(float(row[exit_return_field]))
            triggered += 1
        else:
            hybrid.append(float(row["strategy_return_pct"]))
    return {
        "signals": len(rows),
        "open_at_checkpoint": sum(bool(row.get(open_field)) for row in rows),
        "triggered": triggered,
        "triggered_pct": triggered / len(rows) * 100.0 if rows else None,
        "baseline_mean_return_pct": mean(baseline) if baseline else None,
        "hybrid_mean_return_pct": mean(hybrid) if hybrid else None,
        "delta_mean_return_pct": mean(hybrid) - mean(baseline) if baseline else None,
    }


def bootstrap_checkpoint_delta(
    rows: list[dict[str, object]],
    feature: str,
    operator: str,
    threshold: float,
    open_field: str,
    exit_return_field: str,
    iterations: int = 20_000,
) -> dict[str, object]:
    by_day: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        delta = 0.0
        if checkpoint_rule_triggers(row, feature, operator, threshold, open_field):
            delta = float(row[exit_return_field]) - float(row["strategy_return_pct"])
        by_day[str(row["session_day"])].append(delta)
    daily_deltas = [sum(values) / 10.0 for values in by_day.values()]
    if not daily_deltas:
        return {"days": 0}
    rng = random.Random(20260725)
    samples = [
        mean(rng.choice(daily_deltas) for _ in daily_deltas)
        for _ in range(iterations)
    ]
    return {
        "days": len(daily_deltas),
        "mean_daily_delta_pct": mean(daily_deltas),
        "positive_probability_pct": mean([value > 0 for value in samples]) * 100.0,
        "p025_pct": percentile(samples, 0.025),
        "p975_pct": percentile(samples, 0.975),
    }


def build_checkpoint_candidates(
    period_rows: dict[str, list[dict[str, object]]],
    features: tuple[str, ...],
    open_field: str,
    exit_return_field: str,
) -> list[dict[str, object]]:
    train_rows = [
        row for row in period_rows["pre_train"] if bool(row.get(open_field))
    ]
    candidates: list[dict[str, object]] = []
    for feature in features:
        values = [float(row[feature]) for row in train_rows if row.get(feature) is not None]
        thresholds = sorted(
            {
                round(value, 6)
                for q in (0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
                if (value := percentile(values, q)) is not None
            }
        )
        for threshold in thresholds:
            for operator in ("<=", ">="):
                evaluations = {
                    period: evaluate_checkpoint_rule(
                        rows,
                        feature,
                        operator,
                        threshold,
                        open_field,
                        exit_return_field,
                    )
                    for period, rows in period_rows.items()
                }
                if (
                    int(evaluations["pre_train"]["triggered"]) < 20
                    or int(evaluations["pre_validation"]["triggered"]) < 10
                    or int(evaluations["live_validation"]["triggered"]) < 5
                ):
                    continue
                deltas = [
                    float(evaluations[period]["delta_mean_return_pct"] or 0.0)
                    for period in ("pre_train", "pre_validation", "live_validation")
                ]
                candidates.append(
                    {
                        "feature": feature,
                        "operator": operator,
                        "threshold": threshold,
                        "evaluations": evaluations,
                        "positive_periods": sum(delta > 0 for delta in deltas),
                        "minimum_period_delta_pct": min(deltas),
                        "mean_period_delta_pct": mean(deltas),
                    }
                )
    return sorted(
        candidates,
        key=lambda row: (
            int(row["positive_periods"]),
            float(row["minimum_period_delta_pct"]),
            float(row["mean_period_delta_pct"]),
        ),
        reverse=True,
    )


def csv_value(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    sessions = load_position_sessions(db_path)
    signals = load_prelaunch_signals(Path(args.prelaunch_replay), sessions)
    signals.extend(load_live_signals(Path(args.live_signals)))

    cache_dir = Path(args.cache_dir)
    fetch_summary = {"requests": 0, "rows": 0}
    if args.fetch_missing:
        fetch_summary = merge_missing_cache(
            cache_dir,
            signals,
            request_delay_sec=max(0.0, float(args.request_delay_sec)),
        )
    candles_by_symbol: dict[str, list[dict[str, float]]] = {}
    feature_rows: list[dict[str, object]] = []
    missing: list[dict[str, str]] = []
    for signal in signals:
        symbol = str(signal["symbol"])
        candles = candles_by_symbol.setdefault(symbol, load_candles(cache_dir, symbol))
        row = anchored_features(signal, candles)
        if row is None:
            missing.append({"session_day": str(signal["session_day"]), "symbol": symbol})
            continue
        feature_rows.append(row)

    period_rows = {
        period: [row for row in feature_rows if row["period"] == period]
        for period in ("pre_train", "pre_validation", "live_validation")
    }
    train_rows = period_rows["pre_train"]
    cutoffs_by_feature: dict[str, list[float]] = {}
    for feature in (*ENTRY_FEATURES, *PATH_FEATURES):
        values = [float(row[feature]) for row in train_rows if row.get(feature) is not None]
        cutoffs_by_feature[feature] = [
            value
            for q in (0.25, 0.5, 0.75)
            if (value := percentile(values, q)) is not None
        ]

    correlations = {
        feature: {
            period: spearman(rows, feature)
            for period, rows in period_rows.items()
        }
        for feature in (*ENTRY_FEATURES, *PATH_FEATURES)
    }
    bins = {
        feature: {
            period: quantile_bins(rows, feature, cutoffs_by_feature[feature])
            for period, rows in period_rows.items()
        }
        for feature in (*ENTRY_FEATURES, *PATH_FEATURES)
    }
    candidates = build_candidates(period_rows)
    for candidate in candidates[:10]:
        candidate["bootstrap"] = {
            period: bootstrap_daily_delta(
                rows,
                str(candidate["feature"]),
                str(candidate["operator"]),
                float(candidate["threshold"]),
            )
            for period, rows in period_rows.items()
        }
    next_8_candidates = build_next_8_candidates(period_rows)
    for candidate in next_8_candidates[:10]:
        candidate["bootstrap"] = {
            period: bootstrap_next_8_delta(
                rows,
                str(candidate["feature"]),
                str(candidate["operator"]),
                float(candidate["threshold"]),
            )
            for period, rows in period_rows.items()
        }
    noon_candidates = build_checkpoint_candidates(
        period_rows,
        features=NOON_FEATURES,
        open_field="open_at_noon",
        exit_return_field="noon_short_return_from_entry_pct",
    )
    for candidate in noon_candidates[:10]:
        candidate["bootstrap"] = {
            period: bootstrap_checkpoint_delta(
                rows,
                str(candidate["feature"]),
                str(candidate["operator"]),
                float(candidate["threshold"]),
                open_field="open_at_noon",
                exit_return_field="noon_short_return_from_entry_pct",
            )
            for period, rows in period_rows.items()
        }

    summary = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "definitions": {
            "anchor": "Asia/Shanghai 08:00",
            "full_window": "[08:00, next day 08:00)",
            "strategy_return": "short price return under the 18% trigger replay/counterfactual",
            "entry_features": "known by simulated/actual entry time",
            "path_features": "descriptive only; unavailable at entry time",
            "fixed_slot_mean": "skipped signals contribute zero return",
        },
        "input_signals": len(signals),
        "feature_rows": len(feature_rows),
        "market_fetch": fetch_summary,
        "missing_anchor_rows": missing,
        "periods": {
            period: period_summary(rows)
            for period, rows in period_rows.items()
        },
        "spearman_correlations": correlations,
        "quartile_cutoffs_from_pre_train": cutoffs_by_feature,
        "quartile_bins": bins,
        "entry_filter_candidates": candidates[:20],
        "noon_exit_candidates": noon_candidates[:20],
        "next_8_exit_candidates": next_8_candidates[:20],
    }

    output_csv = Path(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "session_day",
        "symbol",
        "period",
        "source",
        "accounts",
        "entry_time",
        "entry_price",
        "exit_time",
        "strategy_return_pct",
        "base_8_price",
        "entry_vs_8_pct",
        "pre_entry_peak_gain_pct",
        "pre_entry_trough_return_pct",
        "pre_entry_drawdown_from_peak_pct",
        "pre_entry_retrace_ratio_pct",
        "wait_from_8_hours",
        "full_24h_coverage",
        *NOON_FEATURES,
        "open_at_noon",
        *PATH_FEATURES,
        "next_8_short_return_pct",
        "effective_exit_time",
        "open_at_next_8",
    ]
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in feature_rows:
            writer.writerow({key: csv_value(row.get(key)) for key in fieldnames})

    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "signals": len(signals),
                "feature_rows": len(feature_rows),
                "missing": len(missing),
                "periods": summary["periods"],
                "top_candidates": candidates[:5],
            },
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
    )


if __name__ == "__main__":
    main()
