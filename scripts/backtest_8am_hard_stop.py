#!/usr/bin/env python3
"""Backtest a first-crossing account equity stop on complete 08:00 sessions."""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import mean, median

try:
    from scripts.analyze_account_equity_8am import (
        ACCOUNTS,
        build_paths,
        compound,
        load_cashflows,
        load_points,
        max_compound_drawdown,
    )
except ModuleNotFoundError:
    from analyze_account_equity_8am import (  # type: ignore[no-redef]
        ACCOUNTS,
        build_paths,
        compound,
        load_cashflows,
        load_points,
        max_compound_drawdown,
    )


def first_crossing(path, threshold: float, exit_cost: float) -> tuple[float, str | None]:
    """Return the first snapshot return at or below threshold, not the later trough."""
    for point, value in zip(path.points, path.returns()):
        if value <= threshold:
            return value - exit_cost, point.timestamp.isoformat()
    return path.returns()[-1], None


def summarize(values: list[float]) -> dict[str, object]:
    return {
        "days": len(values),
        "compound": compound(values),
        "mean": mean(values) if values else None,
        "median": median(values) if values else None,
        "positive": sum(value > 0 for value in values),
        "negative": sum(value < 0 for value in values),
        "maxDrawdown": max_compound_drawdown(values),
        "worst": min(values) if values else None,
        "best": max(values) if values else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--threshold", type=float, default=-3.5)
    parser.add_argument(
        "--exit-cost",
        type=float,
        default=0.0006,
        help="Additional account-equity cost on a stop exit, as a decimal.",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(f"file:{args.db.resolve()}?mode=ro", uri=True)
    try:
        points = load_points(conn, args.start, args.end)
        cashflows = load_cashflows(conn)
    finally:
        conn.close()

    paths, missing = build_paths(points, cashflows, args.start, args.end)
    if not paths:
        raise SystemExit("No complete account-day paths found")

    rows: list[dict[str, object]] = []
    by_account: dict[str, list[dict[str, object]]] = defaultdict(list)
    by_day: dict[date, list[dict[str, object]]] = defaultdict(list)
    for path in paths:
        actual = path.returns()[-1]
        raw_stop, raw_time = first_crossing(path, args.threshold / 100.0, 0.0)
        cost_stop, cost_time = first_crossing(path, args.threshold / 100.0, args.exit_cost)
        row = {
            "account": path.account_id,
            "sessionDate": path.session_date.isoformat(),
            "actual": actual,
            "stopRaw": raw_stop,
            "stopCost": cost_stop,
            "deltaRaw": raw_stop - actual,
            "deltaCost": cost_stop - actual,
            "triggered": raw_time is not None,
            "triggerTimeUtc": raw_time,
            "triggerTimeCostUtc": cost_time,
            "trough": min(path.returns()),
        }
        rows.append(row)
        by_account[path.account_id].append(row)
        by_day[path.session_date].append(row)

    complete_days = {
        day: sorted(day_rows, key=lambda row: str(row["account"]))
        for day, day_rows in by_day.items()
        if len(day_rows) == len(ACCOUNTS)
    }
    daily = []
    for day in sorted(complete_days):
        day_rows = complete_days[day]
        actual = mean(float(row["actual"]) for row in day_rows)
        stop_raw = mean(float(row["stopRaw"]) for row in day_rows)
        stop_cost = mean(float(row["stopCost"]) for row in day_rows)
        daily.append(
            {
                "date": day.isoformat(),
                "actual": actual,
                "stopRaw": stop_raw,
                "stopCost": stop_cost,
                "deltaRaw": stop_raw - actual,
                "deltaCost": stop_cost - actual,
                "triggeredAccounts": sum(bool(row["triggered"]) for row in day_rows),
            }
        )

    accounts = {}
    for account in ACCOUNTS:
        account_rows = sorted(by_account.get(account, []), key=lambda row: str(row["sessionDate"]))
        actual = [float(row["actual"]) for row in account_rows]
        stop_raw = [float(row["stopRaw"]) for row in account_rows]
        stop_cost = [float(row["stopCost"]) for row in account_rows]
        accounts[account] = {
            "accountDays": len(account_rows),
            "triggered": sum(bool(row["triggered"]) for row in account_rows),
            "actual": summarize(actual),
            "stopRaw": summarize(stop_raw),
            "stopCost": summarize(stop_cost),
            "deltaRawCompound": compound(stop_raw) - compound(actual),
            "deltaCostCompound": compound(stop_cost) - compound(actual),
        }

    actual_daily = [float(row["actual"]) for row in daily]
    stop_raw_daily = [float(row["stopRaw"]) for row in daily]
    stop_cost_daily = [float(row["stopCost"]) for row in daily]
    negative_actual = [row for row in rows if float(row["actual"]) <= 0]
    positive_actual = [row for row in rows if float(row["actual"]) > 0]
    triggered_negative = [row for row in negative_actual if row["triggered"]]
    triggered_positive = [row for row in positive_actual if row["triggered"]]
    payload = {
        "period": {"start": args.start.isoformat(), "end": args.end.isoformat()},
        "thresholdPct": args.threshold,
        "exitCostPct": args.exit_cost * 100.0,
        "accountDays": len(rows),
        "completeDays": len(daily),
        "missing": missing,
        "overall": {
            "actual": summarize(actual_daily),
            "stopRaw": summarize(stop_raw_daily),
            "stopCost": summarize(stop_cost_daily),
            "deltaRawCompound": compound(stop_raw_daily) - compound(actual_daily),
            "deltaCostCompound": compound(stop_cost_daily) - compound(actual_daily),
        },
        "classification": {
            "actualNegativeRows": len(negative_actual),
            "actualPositiveRows": len(positive_actual),
            "negativeRowsTriggered": len(triggered_negative),
            "positiveRowsTriggered": len(triggered_positive),
            "negativeRecall": len(triggered_negative) / len(negative_actual)
            if negative_actual
            else None,
            "positiveHarmRate": len(triggered_positive) / len(positive_actual)
            if positive_actual
            else None,
        },
        "accounts": accounts,
        "daily": daily,
        "rows": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        f"wrote {args.output} ({len(rows)} account-days, {len(daily)} complete days, "
        f"{sum(bool(row['triggered']) for row in rows)} triggers)"
    )


if __name__ == "__main__":
    main()
