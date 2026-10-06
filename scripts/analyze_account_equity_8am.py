#!/usr/bin/env python3
"""Analyze account-level equity paths from 08:00 to the next 08:00."""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import math
import random
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
ACCOUNTS = ("acc01", "acc02", "acc03", "acc04")
DEFAULT_START = date(2026, 6, 30)
DEFAULT_END = date(2026, 7, 23)


@dataclass(frozen=True)
class Point:
    timestamp: datetime
    equity: float


@dataclass(frozen=True)
class DayPath:
    account_id: str
    session_date: date
    points: tuple[Point, ...]

    @property
    def start_equity(self) -> float:
        return self.points[0].equity

    @property
    def end_equity(self) -> float:
        return self.points[-1].equity

    def returns(self) -> list[float]:
        return [(point.equity / self.start_equity) - 1.0 for point in self.points]


def parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def session_boundary(day: date) -> datetime:
    return datetime.combine(day, time(8, 0), SHANGHAI).astimezone(UTC)


def load_cashflows(conn: sqlite3.Connection) -> dict[str, list[tuple[datetime, float]]]:
    rows = conn.execute(
        """
        SELECT account_id, event_time_utc, SUM(amount)
        FROM (
            SELECT DISTINCT account_id, unique_key, event_time_utc, amount
            FROM cashflow_events
            WHERE account_id IN ('acc01', 'acc02', 'acc03', 'acc04')
        )
        GROUP BY account_id, event_time_utc
        ORDER BY account_id, event_time_utc
        """
    ).fetchall()
    result: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
    for account_id, event_time, amount in rows:
        result[str(account_id)].append((parse_iso(str(event_time)), float(amount)))
    return result


def load_points(
    conn: sqlite3.Connection,
    start_day: date,
    end_day: date,
) -> dict[str, list[Point]]:
    query_start = session_boundary(start_day) - timedelta(minutes=5)
    query_end = session_boundary(end_day + timedelta(days=1)) + timedelta(minutes=5)
    placeholders = ",".join("?" for _ in ACCOUNTS)
    rows = conn.execute(
        f"""
        SELECT account_id, captured_at_utc, balance_usdt
        FROM wallet_snapshots
        WHERE account_id IN ({placeholders})
          AND captured_at_utc >= ?
          AND captured_at_utc <= ?
        ORDER BY account_id, captured_at_utc, id
        """,
        (*ACCOUNTS, query_start.isoformat(), query_end.isoformat()),
    ).fetchall()
    result: dict[str, list[Point]] = defaultdict(list)
    for account_id, captured_at, equity in rows:
        result[str(account_id)].append(Point(parse_iso(str(captured_at)), float(equity)))
    return result


def load_entry_completion_times(
    conn: sqlite3.Connection,
) -> dict[tuple[str, date], datetime]:
    rows = conn.execute(
        """
        SELECT account_id, started_at_utc, completed_at_utc
        FROM runs
        WHERE account_id IN ('acc01', 'acc02', 'acc03', 'acc04')
          AND completed_at_utc IS NOT NULL
          AND trade_day_utc NOT LIKE '%manual%'
        ORDER BY started_at_utc
        """
    ).fetchall()
    result: dict[tuple[str, date], datetime] = {}
    for account_id, started_at, completed_at in rows:
        session_day = parse_iso(str(started_at)).astimezone(SHANGHAI).date()
        result[(str(account_id), session_day)] = parse_iso(str(completed_at))
    return result


def cumulative_cashflow(
    events: Sequence[tuple[datetime, float]],
    timestamp: datetime,
) -> float:
    total = 0.0
    for event_time, amount in events:
        if event_time > timestamp:
            break
        total += amount
    return total


def nearest_index(
    timestamps: Sequence[datetime],
    target: datetime,
    tolerance: timedelta,
) -> int | None:
    position = bisect.bisect_left(timestamps, target)
    candidates = [index for index in (position - 1, position) if 0 <= index < len(timestamps)]
    if not candidates:
        return None
    index = min(candidates, key=lambda candidate: abs(timestamps[candidate] - target))
    if abs(timestamps[index] - target) > tolerance:
        return None
    return index


def build_paths(
    points_by_account: dict[str, list[Point]],
    cashflows_by_account: dict[str, list[tuple[datetime, float]]],
    start_day: date,
    end_day: date,
) -> tuple[list[DayPath], list[dict[str, str]]]:
    paths: list[DayPath] = []
    missing: list[dict[str, str]] = []
    tolerance = timedelta(minutes=3)
    day = start_day
    while day <= end_day:
        start = session_boundary(day)
        end = start + timedelta(days=1)
        for account_id in ACCOUNTS:
            raw_points = points_by_account.get(account_id, [])
            timestamps = [point.timestamp for point in raw_points]
            start_index = nearest_index(timestamps, start, tolerance)
            end_index = nearest_index(timestamps, end, tolerance)
            if start_index is None or end_index is None or end_index <= start_index:
                missing.append({"account_id": account_id, "session_date": day.isoformat()})
                continue
            session_points = raw_points[start_index : end_index + 1]
            start_cashflow = cumulative_cashflow(cashflows_by_account.get(account_id, []), start)
            adjusted = tuple(
                Point(
                    point.timestamp,
                    point.equity
                    - (
                        cumulative_cashflow(cashflows_by_account.get(account_id, []), point.timestamp)
                        - start_cashflow
                    ),
                )
                for point in session_points
            )
            paths.append(DayPath(account_id, day, adjusted))
        day += timedelta(days=1)
    return paths, missing


def path_metrics(path: DayPath) -> dict[str, object]:
    returns = path.returns()
    peak_index = max(range(len(returns)), key=returns.__getitem__)
    trough_index = min(range(len(returns)), key=returns.__getitem__)
    running_peak = -math.inf
    max_drawdown = 0.0
    for value in returns:
        running_peak = max(running_peak, value)
        max_drawdown = min(max_drawdown, value - running_peak)
    peak_return = returns[peak_index]
    end_return = returns[-1]
    giveback = peak_return - end_return
    giveback_ratio = giveback / peak_return if peak_return > 0 else None
    return {
        "session_date": path.session_date.isoformat(),
        "account_id": path.account_id,
        "start_equity": path.start_equity,
        "end_equity": path.end_equity,
        "end_pnl_usdt": path.end_equity - path.start_equity,
        "end_return": end_return,
        "peak_equity": path.points[peak_index].equity,
        "peak_time_local": path.points[peak_index].timestamp.astimezone(SHANGHAI).isoformat(),
        "peak_pnl_usdt": path.points[peak_index].equity - path.start_equity,
        "peak_return": peak_return,
        "trough_equity": path.points[trough_index].equity,
        "trough_time_local": path.points[trough_index].timestamp.astimezone(SHANGHAI).isoformat(),
        "trough_return": returns[trough_index],
        "giveback_usdt": path.points[peak_index].equity - path.end_equity,
        "giveback_return": giveback,
        "giveback_ratio": giveback_ratio,
        "max_intraday_drawdown": max_drawdown,
        "peak_positive_end_negative": peak_return > 0 and end_return < 0,
        "snapshot_count": len(path.points),
    }


def simulate_fixed(
    path: DayPath,
    threshold: float,
    exit_cost: float,
    not_before_hour: int | None = None,
) -> tuple[float, str | None]:
    not_before = (
        datetime.combine(path.session_date, time(not_before_hour, 0), SHANGHAI).astimezone(UTC)
        if not_before_hour is not None
        else None
    )
    for point, value in zip(path.points, path.returns()):
        if not_before is not None and point.timestamp < not_before:
            continue
        if value >= threshold:
            return value - exit_cost, point.timestamp.astimezone(SHANGHAI).isoformat()
    return path.returns()[-1], None


def simulate_trailing(
    path: DayPath,
    arm: float,
    giveback_fraction: float,
    exit_cost: float,
    not_before_hour: int | None = None,
    not_before_at: datetime | None = None,
) -> tuple[float, str | None]:
    not_before = not_before_at or (
        datetime.combine(path.session_date, time(not_before_hour, 0), SHANGHAI).astimezone(UTC)
        if not_before_hour is not None
        else None
    )
    peak = -math.inf
    armed = False
    for point, value in zip(path.points, path.returns()):
        peak = max(peak, value)
        armed = armed or peak >= arm
        if not_before is not None and point.timestamp < not_before:
            continue
        if armed and value <= peak * (1.0 - giveback_fraction):
            return value - exit_cost, point.timestamp.astimezone(SHANGHAI).isoformat()
    return path.returns()[-1], None


def simulate_after_entry_completion(
    path: DayPath,
    arm: float,
    giveback_fraction: float,
    exit_cost: float,
    completion_times: dict[tuple[str, date], datetime],
) -> tuple[float, str | None]:
    completed_at = completion_times.get((path.account_id, path.session_date))
    if completed_at is None:
        return path.returns()[-1], None
    return simulate_trailing(
        path,
        arm,
        giveback_fraction,
        exit_cost,
        not_before_at=completed_at,
    )


def compound(returns: Iterable[float]) -> float:
    value = 1.0
    for daily_return in returns:
        value *= 1.0 + daily_return
    return value - 1.0


def max_compound_drawdown(returns: Iterable[float]) -> float:
    value = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for daily_return in returns:
        value *= 1.0 + daily_return
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, (value / peak) - 1.0)
    return max_drawdown


def aggregate_daily(
    paths: Sequence[DayPath],
    rule,
) -> dict[date, float]:
    by_day: dict[date, list[float]] = defaultdict(list)
    for path in paths:
        by_day[path.session_date].append(rule(path))
    return {day: mean(values) for day, values in by_day.items() if len(values) == len(ACCOUNTS)}


def summarize_returns(daily: dict[date, float]) -> dict[str, object]:
    ordered = [daily[day] for day in sorted(daily)]
    return {
        "days": len(ordered),
        "compounded_return": compound(ordered),
        "mean_daily_return": mean(ordered) if ordered else None,
        "median_daily_return": median(ordered) if ordered else None,
        "positive_days": sum(value > 0 for value in ordered),
        "negative_days": sum(value < 0 for value in ordered),
        "max_drawdown": max_compound_drawdown(ordered),
        "worst_day": min(ordered) if ordered else None,
        "best_day": max(ordered) if ordered else None,
    }


def selected_rule_outcome(
    path: DayPath,
    evaluation: dict[str, object],
    exit_cost: float,
    completion_times: dict[tuple[str, date], datetime],
) -> tuple[float, str | None]:
    if evaluation["rule"] == "fixed_take_profit":
        return simulate_fixed(
            path,
            float(evaluation["threshold_pct"]) / 100.0,
            exit_cost,
            int(evaluation["not_before_hour"]) if "not_before_hour" in evaluation else None,
        )
    if evaluation["rule"] in {"profit_trailing", "profit_trailing_gated"}:
        return simulate_trailing(
            path,
            float(evaluation["arm_pct"]) / 100.0,
            float(evaluation["giveback_pct"]) / 100.0,
            exit_cost,
            int(evaluation["not_before_hour"]) if "not_before_hour" in evaluation else None,
        )
    if evaluation["rule"] == "profit_trailing_after_entry":
        return simulate_after_entry_completion(
            path,
            float(evaluation["arm_pct"]) / 100.0,
            float(evaluation["giveback_pct"]) / 100.0,
            exit_cost,
            completion_times,
        )
    raise ValueError(f"Unsupported selected rule: {evaluation['rule']}")


def paired_bootstrap_delta(
    actual_daily: dict[date, float],
    candidate_daily: dict[date, float],
    iterations: int = 10_000,
) -> dict[str, float]:
    days = sorted(set(actual_daily) & set(candidate_daily))
    rng = random.Random(20260725)
    deltas: list[float] = []
    for _ in range(iterations):
        sampled = [days[rng.randrange(len(days))] for _ in days]
        actual = compound(actual_daily[day] for day in sampled)
        candidate = compound(candidate_daily[day] for day in sampled)
        deltas.append(candidate - actual)
    deltas.sort()
    return {
        "iterations": float(iterations),
        "mean_delta": mean(deltas),
        "p_improves": sum(value > 0 for value in deltas) / iterations,
        "ci95_low": deltas[math.floor(iterations * 0.025)],
        "ci95_high": deltas[math.floor(iterations * 0.975)],
    }


def evaluate_rules(
    paths: Sequence[DayPath],
    train_days: set[date],
    test_days: set[date],
    exit_cost: float,
    completion_times: dict[tuple[str, date], datetime],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    actual = aggregate_daily(paths, lambda path: path.returns()[-1])
    oracle = aggregate_daily(paths, lambda path: max(path.returns()) - exit_cost)
    candidates: list[tuple[str, dict[str, float], object]] = [
        ("actual_next_08", {}, lambda path: path.returns()[-1]),
        ("oracle_peak", {}, lambda path: max(path.returns()) - exit_cost),
    ]
    for threshold_pct in (
        0.2,
        0.25,
        0.3,
        0.4,
        0.5,
        0.75,
        1.0,
        1.25,
        1.5,
        2.0,
        2.5,
        3.0,
        4.0,
        5.0,
        6.0,
        7.0,
        8.0,
    ):
        threshold = threshold_pct / 100.0
        candidates.append(
            (
                "fixed_take_profit",
                {"threshold_pct": threshold_pct},
                lambda path, threshold=threshold: simulate_fixed(path, threshold, exit_cost)[0],
            )
        )
    for arm_pct in (0.2, 0.25, 0.3, 0.4, 0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 2.5, 3.0):
        for giveback_pct in (20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0):
            arm = arm_pct / 100.0
            giveback = giveback_pct / 100.0
            candidates.append(
                (
                    "profit_trailing",
                    {"arm_pct": arm_pct, "giveback_pct": giveback_pct},
                    lambda path, arm=arm, giveback=giveback: simulate_trailing(
                        path, arm, giveback, exit_cost
                    )[0],
                )
            )
    for not_before_hour in (12, 14, 15, 16, 17, 18, 20):
        for arm_pct in (0.25, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0):
            for giveback_pct in (30.0, 40.0, 50.0, 60.0, 70.0, 90.0, 100.0):
                arm = arm_pct / 100.0
                giveback = giveback_pct / 100.0
                candidates.append(
                    (
                        "profit_trailing_gated",
                        {
                            "arm_pct": arm_pct,
                            "giveback_pct": giveback_pct,
                            "not_before_hour": not_before_hour,
                        },
                        lambda path, arm=arm, giveback=giveback, hour=not_before_hour: simulate_trailing(
                            path,
                            arm,
                            giveback,
                            exit_cost,
                            hour,
                        )[0],
                    )
                )
    for arm_pct in (0.25, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0):
        for giveback_pct in (30.0, 40.0, 50.0, 60.0, 70.0, 90.0, 100.0):
            arm = arm_pct / 100.0
            giveback = giveback_pct / 100.0
            candidates.append(
                (
                    "profit_trailing_after_entry",
                    {"arm_pct": arm_pct, "giveback_pct": giveback_pct},
                    lambda path, arm=arm, giveback=giveback: simulate_after_entry_completion(
                        path,
                        arm,
                        giveback,
                        exit_cost,
                        completion_times,
                    )[0],
                )
            )
    for name, params, rule in candidates:
        daily = aggregate_daily(paths, rule)
        train = {day: value for day, value in daily.items() if day in train_days}
        test = {day: value for day, value in daily.items() if day in test_days}
        full = summarize_returns(daily)
        train_summary = summarize_returns(train)
        test_summary = summarize_returns(test)
        rows.append(
            {
                "rule": name,
                **params,
                "full": full,
                "train": train_summary,
                "test": test_summary,
                "full_delta_vs_actual": float(full["compounded_return"])
                - compound(actual[day] for day in sorted(actual)),
                "full_delta_vs_oracle": float(full["compounded_return"])
                - compound(oracle[day] for day in sorted(oracle)),
            }
        )
    return rows


def pct(value: float | None) -> str:
    return "--" if value is None else f"{value * 100:.3f}%"


def money(value: float) -> str:
    return f"{value:+.2f}"


def write_daily_csv(path: Path, rows: Sequence[dict[str, object]]) -> None:
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_report(
    path: Path,
    paths: Sequence[DayPath],
    daily_rows: Sequence[dict[str, object]],
    evaluations: Sequence[dict[str, object]],
    start_day: date,
    end_day: date,
    exit_cost: float,
    missing: Sequence[dict[str, str]],
    completion_times: dict[tuple[str, date], datetime],
) -> None:
    actual = next(row for row in evaluations if row["rule"] == "actual_next_08")
    oracle = next(row for row in evaluations if row["rule"] == "oracle_peak")
    actionable = [row for row in evaluations if row["rule"] not in {"actual_next_08", "oracle_peak"}]
    best_train = max(actionable, key=lambda row: float(row["train"]["compounded_return"]))
    best_test = max(actionable, key=lambda row: float(row["test"]["compounded_return"]))
    variant_specs = [
        ("激进早停", "profit_trailing", 0.3, 50.0, None),
        ("17:00后保护", "profit_trailing_gated", 0.3, 40.0, 17),
        ("开单完成后保护", "profit_trailing_after_entry", 0.3, 40.0, None),
        ("回本线保护", "profit_trailing", 0.3, 100.0, None),
    ]
    variants = []
    for label, rule_name, arm_pct, giveback_pct, hour in variant_specs:
        evaluation = next(
            row
            for row in evaluations
            if row["rule"] == rule_name
            and row.get("arm_pct") == arm_pct
            and row.get("giveback_pct") == giveback_pct
            and (hour is None or row.get("not_before_hour") == hour)
        )
        variants.append((label, evaluation))
    peak_to_loss = [row for row in daily_rows if row["peak_positive_end_negative"]]
    biggest_givebacks = sorted(
        daily_rows,
        key=lambda row: float(row["giveback_return"]),
        reverse=True,
    )[:12]
    actual_daily = aggregate_daily(paths, lambda item: item.returns()[-1])
    selected_daily = aggregate_daily(
        paths,
        lambda item: selected_rule_outcome(item, best_train, exit_cost, completion_times)[0],
    )
    bootstrap = paired_bootstrap_delta(actual_daily, selected_daily)
    variant_bootstraps = []
    for label, evaluation in variants:
        variant_daily = aggregate_daily(
            paths,
            lambda item, evaluation=evaluation: selected_rule_outcome(
                item,
                evaluation,
                exit_cost,
                completion_times,
            )[0],
        )
        variant_bootstraps.append(
            (label, evaluation, paired_bootstrap_delta(actual_daily, variant_daily))
        )
    daily_comparison = [
        {
            "day": day,
            "actual": actual_daily[day],
            "selected": selected_daily[day],
            "delta": selected_daily[day] - actual_daily[day],
        }
        for day in sorted(actual_daily)
    ]
    account_comparison = []
    for account_id in ACCOUNTS:
        account_paths = [item for item in paths if item.account_id == account_id]
        actual_returns = [item.returns()[-1] for item in account_paths]
        selected_returns = [
            selected_rule_outcome(item, best_train, exit_cost, completion_times)[0]
            for item in account_paths
        ]
        account_comparison.append(
            {
                "account_id": account_id,
                "actual": compound(actual_returns),
                "selected": compound(selected_returns),
                "actual_dd": max_compound_drawdown(actual_returns),
                "selected_dd": max_compound_drawdown(selected_returns),
            }
        )
    unique_peak_to_loss_days = len({str(row["session_date"]) for row in peak_to_loss})
    lines = [
        "# 08:00 账户权益路径与组合止盈研究",
        "",
        f"- 样本：{start_day.isoformat()} 08:00 至 {(end_day + timedelta(days=1)).isoformat()} 08:00",
        f"- 账号：{', '.join(ACCOUNTS)}；完整账号日：{len(daily_rows)}",
        f"- 组合平仓成本假设：权益的 {exit_cost * 100:.3f}%（约等于 60% 资金、2x 名义敞口按 0.05% taker 平仓）",
        f"- 缺失账号日：{len(missing)}",
        "",
        "## 核心事实",
        "",
        f"- 实际持有到次日 08:00 的等权复利收益：{pct(actual['full']['compounded_return'])}，最大回撤 {pct(actual['full']['max_drawdown'])}。",
        f"- 每日最高权益全部可成交的事后上限：{pct(oracle['full']['compounded_return'])}。这使用未来信息，不能直接上线。",
        f"- “日内曾盈利、次日 08:00 变亏损”共有 {len(peak_to_loss)} 个账号日、{unique_peak_to_loss_days} 个交易日，占 {len(peak_to_loss) / len(daily_rows):.1%}。",
        "",
        "## 参数外推检查",
        "",
        "前 2/3 日期用于选参，后 1/3 日期只用于检验。四个账号同一天高度相关，统计时先按日期取账号等权平均。",
        "",
        f"- 训练段最优可执行规则：`{best_train['rule']}` {json.dumps({key: value for key, value in best_train.items() if key.endswith('_pct') or key == 'not_before_hour'}, ensure_ascii=False)}",
        f"- 训练段复利 {pct(best_train['train']['compounded_return'])}，测试段复利 {pct(best_train['test']['compounded_return'])}，全段复利 {pct(best_train['full']['compounded_return'])}，最大回撤 {pct(best_train['full']['max_drawdown'])}。",
        f"- 对照实际：训练段 {pct(actual['train']['compounded_return'])}，测试段 {pct(actual['test']['compounded_return'])}，全段 {pct(actual['full']['compounded_return'])}，最大回撤 {pct(actual['full']['max_drawdown'])}。",
        f"- 按日期配对 bootstrap：候选相对实际复利增量均值 {pct(bootstrap['mean_delta'])}，95% 区间 {pct(bootstrap['ci95_low'])} 至 {pct(bootstrap['ci95_high'])}，改善概率 {bootstrap['p_improves']:.1%}。",
        f"- 测试段事后最优规则：`{best_test['rule']}` {json.dumps({key: value for key, value in best_test.items() if key.endswith('_pct') or key == 'not_before_hour'}, ensure_ascii=False)}；仅用于观察稳定性，不能据此选参。",
        "",
        "## 执行方案对比",
        "",
        "| 方案 | 规则 | 训练复利 | 测试复利 | 全段复利 | 最大回撤 | bootstrap改善概率 |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for label, evaluation, variant_bootstrap in variant_bootstraps:
        params = {
            key: value
            for key, value in evaluation.items()
            if key.endswith("_pct") or key == "not_before_hour"
        }
        lines.append(
            f"| {label} | `{evaluation['rule']}` {json.dumps(params, ensure_ascii=False)} | "
            f"{pct(evaluation['train']['compounded_return'])} | "
            f"{pct(evaluation['test']['compounded_return'])} | "
            f"{pct(evaluation['full']['compounded_return'])} | "
            f"{pct(evaluation['full']['max_drawdown'])} | "
            f"{variant_bootstrap['p_improves']:.1%} |"
        )
    lines.extend(
        [
        "",
        "## 分账号复利",
        "",
        "| 账号 | 实际复利 | 候选复利 | 实际最大回撤 | 候选最大回撤 |",
        "|---|---:|---:|---:|---:|",
        ]
    )
    for row in account_comparison:
        lines.append(
            f"| {row['account_id']} | {pct(row['actual'])} | {pct(row['selected'])} | "
            f"{pct(row['actual_dd'])} | {pct(row['selected_dd'])} |"
        )
    lines.extend(
        [
            "",
            "## 候选规则逐日影响",
            "",
            "| 日期 | 实际次日08收益 | 候选收益 | 增减 |",
            "|---|---:|---:|---:|",
        ]
    )
    for row in sorted(daily_comparison, key=lambda item: float(item["delta"]), reverse=True):
        lines.append(
            f"| {row['day'].isoformat()} | {pct(row['actual'])} | {pct(row['selected'])} | {pct(row['delta'])} |"
        )
    lines.extend(
        [
            "",
        "## 日内盈利拖成亏损的实例",
        "",
        "| 日期 | 账号 | 08:00权益 | 日内最高盈利 | 最高时间 | 次日08:00盈亏 | 回吐 |",
        "|---|---|---:|---:|---|---:|---:|",
        ]
    )
    for row in sorted(
        peak_to_loss,
        key=lambda item: float(item["giveback_return"]),
        reverse=True,
    )[:20]:
        peak_time = datetime.fromisoformat(str(row["peak_time_local"])).strftime("%m-%d %H:%M")
        lines.append(
            f"| {row['session_date']} | {row['account_id']} | {float(row['start_equity']):.2f} | "
            f"{money(float(row['peak_pnl_usdt']))} ({pct(float(row['peak_return']))}) | "
            f"{peak_time} | {money(float(row['end_pnl_usdt']))} ({pct(float(row['end_return']))}) | "
            f"{money(float(row['giveback_usdt']))} |"
        )
    lines.extend(
        [
            "",
            "## 最大盈利回吐",
            "",
            "| 日期 | 账号 | 最高盈利 | 次日08:00盈亏 | 回吐比例 |",
            "|---|---|---:|---:|---:|",
        ]
    )
    for row in biggest_givebacks:
        lines.append(
            f"| {row['session_date']} | {row['account_id']} | {pct(float(row['peak_return']))} | "
            f"{pct(float(row['end_return']))} | {pct(float(row['giveback_ratio'])) if row['giveback_ratio'] is not None else '--'} |"
        )
    lines.extend(
        [
            "",
            "## 解释边界",
            "",
            "- 逐分钟权益包含未实现盈亏，适合判断账户整体止盈触发点。",
            "- 每日规则回放假设触发后全部平仓，次日 08:00 再按实际日收益比例继续复利；它不是逐订单重建，改变平仓后也会改变后续真实持仓。",
            "- 固定止盈不会解决“尚未达到阈值便转亏”的日期；利润回撤止盈只有在达到启动阈值后才生效。",
            "- 样本日期仍少，参数是否上线应以测试段方向一致、相邻参数表现平滑为最低要求。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--end", type=date.fromisoformat, default=DEFAULT_END)
    parser.add_argument("--exit-cost-pct", type=float, default=0.06)
    parser.add_argument("--output-dir", type=Path, default=Path("reports"))
    parser.add_argument("--tag", default="20260725")
    args = parser.parse_args()

    conn = sqlite3.connect(f"file:{args.db.resolve()}?mode=ro", uri=True)
    try:
        points = load_points(conn, args.start, args.end)
        cashflows = load_cashflows(conn)
        completion_times = load_entry_completion_times(conn)
    finally:
        conn.close()
    paths, missing = build_paths(points, cashflows, args.start, args.end)
    if not paths:
        raise SystemExit("No complete account-day paths found")
    daily_rows = [path_metrics(path) for path in paths]
    dates = sorted({path.session_date for path in paths})
    split_index = max(1, math.floor(len(dates) * 2 / 3))
    train_days = set(dates[:split_index])
    test_days = set(dates[split_index:])
    evaluations = evaluate_rules(
        paths,
        train_days,
        test_days,
        args.exit_cost_pct / 100.0,
        completion_times,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    daily_path = args.output_dir / f"account_equity_8am_daily_{args.tag}.csv"
    json_path = args.output_dir / f"account_equity_8am_analysis_{args.tag}.json"
    report_path = args.output_dir / f"account_equity_8am_mining_report_{args.tag}.md"
    write_daily_csv(daily_path, daily_rows)
    payload = {
        "period": {"start": args.start.isoformat(), "end": args.end.isoformat()},
        "exit_cost_pct": args.exit_cost_pct,
        "train_dates": [day.isoformat() for day in sorted(train_days)],
        "test_dates": [day.isoformat() for day in sorted(test_days)],
        "missing": missing,
        "evaluations": evaluations,
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    write_report(
        report_path,
        paths,
        daily_rows,
        evaluations,
        args.start,
        args.end,
        args.exit_cost_pct / 100.0,
        missing,
        completion_times,
    )
    print(f"wrote {daily_path}")
    print(f"wrote {json_path}")
    print(f"wrote {report_path}")


if __name__ == "__main__":
    main()
