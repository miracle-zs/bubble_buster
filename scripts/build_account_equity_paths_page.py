#!/usr/bin/env python3
"""Build a static 08:00-to-08:00 account equity path explorer."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import median

try:
    from scripts.analyze_account_equity_8am import (
        ACCOUNTS,
        build_paths,
        load_cashflows,
        load_points,
        path_metrics,
    )
except ModuleNotFoundError:
    from analyze_account_equity_8am import (  # type: ignore[no-redef]
        ACCOUNTS,
        build_paths,
        load_cashflows,
        load_points,
        path_metrics,
    )


CATEGORY_LABELS = {
    "up": "一直涨",
    "down": "一直跌",
    "down_up": "先跌后涨",
    "up_down": "先涨后跌",
}
CATEGORY_ORDER = ("up", "down", "down_up", "up_down")
SAMPLE_INTERVALS = 96
STOP_THRESHOLDS = (-6, -8, -10, -12, -14, -16, -18)
STRATEGY_LAUNCH = date(2026, 6, 30)


def basis_points(value: float | None) -> int | None:
    if value is None or not math.isfinite(value):
        return None
    return int(round(value * 10000))


def quantile(values: list[int], probability: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * weight)


def classify_returns(returns: list[float]) -> str:
    """Classify the broad shape using medians of four quarters.

    This is deliberately descriptive rather than a trading rule. Quarter
    medians reduce the effect of one noisy minute snapshot.
    """
    if len(returns) < 8:
        return "up" if returns[-1] >= returns[0] else "down"
    quarter = max(1, len(returns) // 4)
    quarters = [
        returns[index * quarter : (index + 1) * quarter]
        for index in range(4)
    ]
    deltas = [median(quarters[1]) - median(quarters[0]), median(quarters[3]) - median(quarters[2])]
    epsilon = 0.001
    first, second = deltas
    if first <= -epsilon and second >= epsilon:
        return "down_up"
    if first >= epsilon and second <= -epsilon:
        return "up_down"
    if first >= -epsilon and second >= -epsilon:
        return "up"
    if first <= epsilon and second <= epsilon:
        return "down"
    if first < 0 and second >= 0:
        return "down_up"
    if first >= 0 and second < 0:
        return "up_down"
    return "up" if returns[-1] >= returns[0] else "down"


def sample_returns(path, sample_intervals: int = SAMPLE_INTERVALS) -> list[int]:
    """Linearly sample a raw path to a compact, fixed 15-minute grid."""
    timestamps = [point.timestamp for point in path.points]
    returns = path.returns()
    start = timestamps[0]
    duration = timestamps[-1] - start
    result: list[int] = []
    cursor = 0
    for index in range(sample_intervals + 1):
        target = start + duration * index / sample_intervals
        while cursor + 1 < len(timestamps) and timestamps[cursor + 1] <= target:
            cursor += 1
        if cursor + 1 >= len(timestamps):
            value = returns[-1]
        elif timestamps[cursor] == target:
            value = returns[cursor]
        else:
            left_time = timestamps[cursor]
            right_time = timestamps[cursor + 1]
            span = (right_time - left_time).total_seconds()
            ratio = (
                (target - left_time).total_seconds() / span
                if span > 0
                else 0.0
            )
            value = returns[cursor] + (returns[cursor + 1] - returns[cursor]) * ratio
        result.append(basis_points(value) or 0)
    return result


def local_clock(iso_timestamp: str) -> str:
    value = datetime.fromisoformat(iso_timestamp)
    return value.strftime("%H:%M")


def make_day_row(path) -> dict[str, object]:
    metrics = path_metrics(path)
    returns = path.returns()
    category = classify_returns(returns)
    return {
        "d": path.session_date.isoformat(),
        "c": category,
        "r": sample_returns(path),
        "end": basis_points(float(metrics["end_return"])) or 0,
        "peak": basis_points(float(metrics["peak_return"])) or 0,
        "trough": basis_points(float(metrics["trough_return"])) or 0,
        "dd": basis_points(float(metrics["max_intraday_drawdown"])) or 0,
        "giveback": basis_points(float(metrics["giveback_return"])) or 0,
        "peakTime": local_clock(str(metrics["peak_time_local"])),
        "troughTime": local_clock(str(metrics["trough_time_local"])),
        "startEquity": round(float(metrics["start_equity"]), 4),
        "endEquity": round(float(metrics["end_equity"]), 4),
        "snapshots": int(metrics["snapshot_count"]),
    }


def stats_for(rows: list[dict[str, object]]) -> dict[str, object]:
    counts = Counter(str(row["c"]) for row in rows)
    end_values = [int(row["end"]) for row in rows]
    down_up_values = [int(row["trough"]) for row in rows if row["c"] == "down_up"]
    return {
        "count": len(rows),
        "positiveEndRate": (
            sum(value > 0 for value in end_values) / len(end_values)
            if end_values
            else None
        ),
        "meanEnd": round(sum(end_values) / len(end_values)) if end_values else None,
        "medianEnd": quantile(end_values, 0.5),
        "categories": {category: counts.get(category, 0) for category in CATEGORY_ORDER},
        "downUp": {
            "count": len(down_up_values),
            "worst": min(down_up_values) if down_up_values else None,
            "p05": quantile(down_up_values, 0.05),
            "p10": quantile(down_up_values, 0.10),
            "p50": quantile(down_up_values, 0.50),
            "p90": quantile(down_up_values, 0.90),
            "best": max(down_up_values) if down_up_values else None,
        },
    }


def stop_thresholds_for(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for threshold in STOP_THRESHOLDS:
        threshold_bps = threshold * 100
        rates: dict[str, float | None] = {}
        for category in CATEGORY_ORDER:
            category_rows = [row for row in rows if row["c"] == category]
            rates[category] = (
                sum(int(row["trough"]) <= threshold_bps for row in category_rows)
                / len(category_rows)
                if category_rows
                else None
            )
        rates["all"] = (
            sum(int(row["trough"]) <= threshold_bps for row in rows) / len(rows)
            if rows
            else None
        )
        result.append({"threshold": threshold, "rates": rates})
    return result


def median_path(rows: list[dict[str, object]]) -> list[int] | None:
    if not rows:
        return None
    length = len(rows[0]["r"])
    return [
        round(median(int(row["r"][index]) for row in rows))
        for index in range(length)
    ]


def build_payload(paths, missing: list[dict[str, str]], start: date, end: date) -> dict[str, object]:
    rows_by_account: dict[str, list[dict[str, object]]] = defaultdict(list)
    for path in paths:
        rows_by_account[path.account_id].append(make_day_row(path))
    for rows in rows_by_account.values():
        rows.sort(key=lambda row: str(row["d"]))

    accounts: dict[str, object] = {}
    all_rows: list[dict[str, object]] = []
    for account_id in ACCOUNTS:
        rows = rows_by_account.get(account_id, [])
        all_rows.extend(rows)
        medians = {
            "all": median_path(rows),
            **{
                category: median_path([row for row in rows if row["c"] == category])
                for category in CATEGORY_ORDER
            },
        }
        accounts[account_id] = {
            "days": rows,
            "stats": stats_for(rows),
            "stopThresholds": stop_thresholds_for(rows),
            "medians": medians,
        }

    values = [int(value) for row in all_rows for value in row["r"]]
    domain = {
        "min": min(values) if values else -100,
        "max": max(values) if values else 100,
    }
    if domain["min"] == domain["max"]:
        domain["min"] -= 100
        domain["max"] += 100
    overall_medians = {
        "all": median_path(all_rows),
        **{
            category: median_path([row for row in all_rows if row["c"] == category])
            for category in CATEGORY_ORDER
        },
    }
    return {
        "period": {"start": start.isoformat(), "end": end.isoformat()},
        "scope": {
            "strategyLaunch": STRATEGY_LAUNCH.isoformat(),
            "label": (
                "仅统计等待第一根 1h 阴线策略启用后的完整交易日"
                if start >= STRATEGY_LAUNCH
                else "包含策略启用前后的完整交易日"
            ),
        },
        "sample": {
            "source": "wallet_snapshots",
            "rawInterval": "60s snapshots; extrema use every snapshot",
            "chartInterval": "15m display grid",
            "intervals": SAMPLE_INTERVALS,
        },
        "missing": missing,
        "domain": domain,
        "accounts": accounts,
        "overall": {
            "stats": stats_for(all_rows),
            "stopThresholds": stop_thresholds_for(all_rows),
            "medians": overall_medians,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--summary-output",
        type=Path,
        help="Optional JSON output containing the same statistics as the page.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.end < args.start:
        raise SystemExit("--end must be on or after --start")
    conn = sqlite3.connect(f"file:{args.db.resolve()}?mode=ro", uri=True)
    try:
        points = load_points(conn, args.start, args.end)
        cashflows = load_cashflows(conn)
    finally:
        conn.close()
    paths, missing = build_paths(points, cashflows, args.start, args.end)
    if not paths:
        raise SystemExit("No complete account-day paths found")
    payload = build_payload(paths, missing, args.start, args.end)
    template_path = Path(__file__).with_name("account_equity_paths_8am_template.html")
    template = template_path.read_text(encoding="utf-8")
    encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    encoded = encoded.replace("</", "<\\/")
    html = template.replace("__ACCOUNT_EQUITY_PATHS_DATA__", encoded)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(html, encoding="utf-8")
    if args.summary_output:
        args.summary_output.parent.mkdir(parents=True, exist_ok=True)
        args.summary_output.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
    total = sum(len(account["days"]) for account in payload["accounts"].values())
    print(f"wrote {args.output} ({total} account-days, {len(missing)} missing)")
    if args.summary_output:
        print(f"wrote {args.summary_output}")


if __name__ == "__main__":
    main()
