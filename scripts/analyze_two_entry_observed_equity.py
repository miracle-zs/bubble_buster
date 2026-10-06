#!/usr/bin/env python3
"""Compare the deployed two-entry allocation with a one-entry counterfactual.

The current service records wallet balances and actual exit fills, but it does
not record a historical mark-to-market equity series.  This analysis therefore
uses the server wallet snapshots as the observed two-entry curve and applies a
counterfactual adjustment to the same observed exit path:

* current: use the actual initial and ``t10s-add`` entries;
* original: enter the inferred full target at the initial entry price, skip
  the later entry, and scale each observed exit by the actual position
  fraction closed.

The result is an entry-allocation attribution, not a claim that the original
strategy would have produced identical exit decisions.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from replay_independent_strategy_portfolio import (  # noqa: E402
    SHANGHAI,
    clean_for_json,
    iso,
    open_db,
    parse_dt,
)


UTC = timezone.utc
ENTRY_RATIO = 0.50
FEE_RATE = 0.0005
INDEPENDENT_MODE = "after_bullish_bearish_independent"
GRID = timedelta(minutes=15)


@dataclass(frozen=True)
class Plan:
    plan_id: str
    position_id: int
    run_id: str
    run_started_at: datetime
    symbol: str
    initial_time: datetime
    initial_price: float
    initial_qty: float
    full_target: float
    original_qty: float


@dataclass(frozen=True)
class TradeEvent:
    time: datetime
    kind: str
    client_order_id: str
    position_id: int | None
    symbol: str
    qty: float
    price: float
    commission: float | None
    realized_pnl: float | None
    plan_id: str | None = None
    signal_hour: datetime | None = None


def positive(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def finite(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def payload_float(payload: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        value = finite(payload.get(key))
        if value is not None:
            return value
    return None


def parse_payload(raw: object) -> dict[str, Any]:
    try:
        value = json.loads(str(raw or "{}"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def event_time(payload: dict[str, Any], fallback: object) -> datetime:
    audit = payload.get("entry_audit")
    audit = audit if isinstance(audit, dict) else {}
    return parse_dt(str(audit.get("filled_at_utc") or fallback))


def load_entry_events(conn: sqlite3.Connection, account: str) -> list[TradeEvent]:
    rows = conn.execute(
        """
        SELECT id, position_id, symbol, client_order_id, event_time_utc, raw_json
        FROM order_events
        WHERE account_id = ?
          AND status = 'FILLED'
          AND (client_order_id LIKE 't10s-ent-%' OR client_order_id LIKE 't10s-add-%')
        ORDER BY event_time_utc ASC, id ASC
        """,
        (account,),
    ).fetchall()
    grouped: dict[str, list[tuple[sqlite3.Row, dict[str, Any]]]] = defaultdict(list)
    for row in rows:
        client_id = str(row["client_order_id"] or "")
        grouped[client_id].append((row, parse_payload(row["raw_json"])))

    events: list[TradeEvent] = []
    for client_id, items in grouped.items():
        if not client_id:
            continue
        # Prefer the payload with the audit/fill price; the other row is often
        # the order-status update with the same client id and no fill fields.
        chosen: tuple[sqlite3.Row, dict[str, Any]] | None = None
        ordered_items = sorted(items, key=lambda item: (item[0]["position_id"] is None, item[0]["event_time_utc"], item[0]["id"]))
        for row, payload in ordered_items:
            audit = payload.get("entry_audit")
            audit = audit if isinstance(audit, dict) else {}
            price = positive(audit.get("fill_price")) or payload_float(payload, "avgPrice", "ap")
            qty = positive(payload.get("executedQty")) or positive(payload.get("z"))
            qty = qty or positive(payload.get("origQty")) or positive(payload.get("q")) or positive(row["qty"])
            if price is not None and qty is not None:
                chosen = (row, payload)
                break
        if chosen is None:
            continue
        row, payload = chosen
        audit = payload.get("entry_audit")
        audit = audit if isinstance(audit, dict) else {}
        price = positive(audit.get("fill_price")) or payload_float(payload, "avgPrice", "ap")
        qty = positive(payload.get("executedQty")) or positive(payload.get("z"))
        qty = qty or positive(payload.get("origQty")) or positive(payload.get("q")) or positive(row["qty"])
        if price is None or qty is None:
            continue
        position_id = int(row["position_id"]) if row["position_id"] is not None else None
        commission = payload_float(payload, "commission", "n")
        signal_hour_raw = audit.get("signal_hour_open_utc")
        signal_hour = parse_dt(str(signal_hour_raw)) if signal_hour_raw else None
        events.append(
            TradeEvent(
                time=event_time(payload, row["event_time_utc"]),
                kind="initial" if client_id.startswith("t10s-ent-") else "add",
                client_order_id=client_id,
                position_id=position_id,
                symbol=str(row["symbol"] or "").strip().upper(),
                qty=qty,
                price=price,
                commission=commission,
                realized_pnl=None,
                signal_hour=signal_hour,
            )
        )
    events.sort(key=lambda item: (item.time, item.symbol, item.client_order_id))
    return events


def load_exit_events(conn: sqlite3.Connection, account: str) -> list[TradeEvent]:
    rows = conn.execute(
        """
        SELECT
            f.id, f.position_id, f.symbol, f.event_time_utc,
            f.executed_qty, f.avg_price, f.realized_pnl, f.commission,
            oe.client_order_id, oe.raw_json
        FROM fills f
        JOIN order_events oe ON oe.id = f.order_event_id
        WHERE oe.account_id = ?
          AND f.reduce_only = 1
          AND f.executed_qty > 0
          AND oe.client_order_id LIKE 't10s-%'
          AND oe.client_order_id NOT LIKE 't10s-ent-%'
          AND oe.client_order_id NOT LIKE 't10s-add-%'
        ORDER BY f.event_time_utc ASC, f.id ASC
        """,
        (account,),
    ).fetchall()
    grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
    for row in rows:
        client_id = str(row["client_order_id"] or "")
        grouped[client_id].append(row)

    events: list[TradeEvent] = []
    for client_id, items in grouped.items():
        if not client_id:
            continue
        chosen: sqlite3.Row | None = None
        chosen_payload: dict[str, Any] = {}
        for row in sorted(items, key=lambda item: (item["position_id"] is None, item["event_time_utc"], item["id"])):
            payload = parse_payload(row["raw_json"])
            price = positive(row["avg_price"]) or payload_float(payload, "avgPrice", "ap")
            if price is not None:
                chosen = row
                chosen_payload = payload
                break
        if chosen is None:
            chosen = max(items, key=lambda row: float(row["executed_qty"] or 0.0))
            chosen_payload = parse_payload(chosen["raw_json"])
        price = positive(chosen["avg_price"]) or payload_float(chosen_payload, "avgPrice", "ap")
        qty = positive(chosen["executed_qty"]) or positive(chosen_payload.get("executedQty"))
        qty = qty or positive(chosen_payload.get("z"))
        if price is None or qty is None:
            continue
        position_id = int(chosen["position_id"]) if chosen["position_id"] is not None else None
        realized = finite(chosen["realized_pnl"])
        if realized is None:
            realized = payload_float(chosen_payload, "realizedPnl", "rp")
        commission = finite(chosen["commission"])
        if commission is None:
            commission = payload_float(chosen_payload, "commission", "n")
        events.append(
            TradeEvent(
                time=parse_dt(str(chosen["event_time_utc"])),
                kind="exit",
                client_order_id=client_id,
                position_id=position_id,
                symbol=str(chosen["symbol"] or "").strip().upper(),
                qty=qty,
                price=price,
                commission=commission,
                realized_pnl=realized,
            )
        )
    events.sort(key=lambda item: (item.time, item.symbol, item.client_order_id))
    return events


def load_rollout_plans(
    conn: sqlite3.Connection,
    account: str,
    entry_events: list[TradeEvent],
) -> tuple[list[Plan], datetime]:
    run = conn.execute(
        """
        SELECT run_id, started_at_utc
        FROM runs
        WHERE account_id = ? AND message LIKE '%after_bullish_bearish_independent%'
        ORDER BY started_at_utc ASC
        LIMIT 1
        """,
        (account,),
    ).fetchone()
    if run is None:
        raise RuntimeError(f"no independent rollout run found for {account}")
    rollout_start = parse_dt(str(run["started_at_utc"]))

    rows = conn.execute(
        """
        SELECT p.id, p.run_id, p.symbol, r.started_at_utc, r.message
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        WHERE r.account_id = ?
          AND p.side = 'SHORT'
          AND p.qty > 0
          AND p.entry_price > 0
          AND r.message LIKE '%after_bullish_bearish_independent%'
        """,
        (account,),
    ).fetchall()
    by_position = {int(row["id"]): row for row in rows}
    plans: list[Plan] = []
    seen: set[int] = set()
    for event in entry_events:
        if event.kind != "initial" or event.position_id is None or event.position_id in seen:
            continue
        row = by_position.get(event.position_id)
        if row is None:
            continue
        run_started = parse_dt(str(row["started_at_utc"]))
        if run_started < rollout_start:
            continue
        full_target = event.qty * event.price / ENTRY_RATIO
        plans.append(
            Plan(
                plan_id=f"p{event.position_id}",
                position_id=event.position_id,
                run_id=str(row["run_id"]),
                run_started_at=run_started,
                symbol=str(row["symbol"] or "").strip().upper(),
                initial_time=event.time,
                initial_price=event.price,
                initial_qty=event.qty,
                full_target=full_target,
                original_qty=full_target / event.price,
            )
        )
        seen.add(event.position_id)
    plans.sort(key=lambda item: (item.initial_time, item.symbol, item.plan_id))
    return plans, rollout_start


def assign_plans(plans: list[Plan], events: list[TradeEvent]) -> list[TradeEvent]:
    by_position = {plan.position_id: plan.plan_id for plan in plans}
    result: list[TradeEvent] = []
    for event in events:
        plan_id = by_position.get(event.position_id or -1)
        if plan_id is None and event.kind == "add":
            candidates = [
                plan
                for plan in plans
                if plan.symbol == event.symbol
                and plan.initial_time <= event.time
                and plan.run_started_at <= (event.signal_hour or event.time)
                and (event.signal_hour or event.time) <= plan.run_started_at + timedelta(hours=ENTRY_WAIT_HOURS_FOR_MATCH)
            ]
            if not candidates:
                candidates = [
                    plan for plan in plans
                    if plan.symbol == event.symbol and plan.initial_time <= event.time
                ]
            if candidates:
                plan_id = max(candidates, key=lambda plan: plan.initial_time).plan_id
        result.append(
            TradeEvent(
                time=event.time,
                kind=event.kind,
                client_order_id=event.client_order_id,
                position_id=event.position_id,
                symbol=event.symbol,
                qty=event.qty,
                price=event.price,
                commission=event.commission,
                realized_pnl=event.realized_pnl,
                plan_id=plan_id,
                signal_hour=event.signal_hour,
            )
        )
    return result


ENTRY_WAIT_HOURS_FOR_MATCH = 20.0


def load_wallet_series(
    conn: sqlite3.Connection,
    account: str,
    start: datetime,
    end: datetime,
) -> tuple[float, datetime, list[tuple[datetime, float]]]:
    row = conn.execute(
        """
        SELECT balance_usdt, captured_at_utc
        FROM wallet_snapshots
        WHERE account_id = ? AND captured_at_utc >= ?
        ORDER BY captured_at_utc ASC, id ASC
        LIMIT 1
        """,
        (account, iso(start - timedelta(minutes=2))),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"no wallet snapshot near rollout for {account}")
    baseline = float(row["balance_usdt"])
    baseline_time = parse_dt(str(row["captured_at_utc"]))
    rows = conn.execute(
        """
        SELECT captured_at_utc, balance_usdt
        FROM wallet_snapshots
        WHERE account_id = ?
          AND captured_at_utc >= ?
          AND captured_at_utc <= ?
        ORDER BY captured_at_utc ASC, id ASC
        """,
        (account, iso(baseline_time), iso(end)),
    ).fetchall()
    samples = [(parse_dt(str(item["captured_at_utc"])), float(item["balance_usdt"])) for item in rows]
    if not samples or samples[0][0] > baseline_time:
        samples.insert(0, (baseline_time, baseline))
    return baseline, baseline_time, samples


def wallet_at(samples: list[tuple[datetime, float]], timestamp: datetime, fallback: float) -> float:
    value = fallback
    for sample_time, sample_value in samples:
        if sample_time > timestamp:
            break
        value = sample_value
    return value


def actual_delta(event: TradeEvent, active_qty: float, avg_entry: float) -> tuple[float, float]:
    qty = min(max(0.0, event.qty), max(0.0, active_qty)) if event.kind == "exit" else event.qty
    if event.kind == "entry":
        fee = event.commission if event.commission is not None else event.qty * event.price * FEE_RATE
        return -fee, qty
    gross = event.realized_pnl
    if gross is None:
        gross = (avg_entry - event.price) * qty
    fee = event.commission if event.commission is not None else qty * event.price * FEE_RATE
    return gross - fee, -qty


def build_adjustments(
    plans: list[Plan],
    events: list[TradeEvent],
    start: datetime,
    end: datetime,
) -> tuple[list[tuple[datetime, float]], dict[str, object]]:
    by_plan = {plan.plan_id: plan for plan in plans}
    state_actual: dict[str, dict[str, float]] = defaultdict(lambda: {"qty": 0.0, "cost": 0.0})
    state_original: dict[str, float] = defaultdict(float)
    adjustments: list[tuple[datetime, float]] = []
    stats = Counter()
    for event in sorted(events, key=lambda item: (item.time, item.kind != "exit", item.symbol, item.client_order_id)):
        if event.time < start or event.time > end:
            continue
        plan = by_plan.get(event.plan_id or "")
        if plan is None:
            continue
        actual = state_actual[plan.plan_id]
        if event.kind == "initial":
            initial_actual_delta, _ = actual_delta_for_entry(event)
            original_fee = plan.original_qty * plan.initial_price * FEE_RATE
            actual["qty"] += event.qty
            actual["cost"] += event.qty * event.price
            state_original[plan.plan_id] += plan.original_qty
            adjustments.append((event.time, -original_fee - initial_actual_delta))
            stats["initial_events"] += 1
            continue
        if event.kind == "add":
            fee = event.commission if event.commission is not None else event.qty * event.price * FEE_RATE
            actual["qty"] += event.qty
            actual["cost"] += event.qty * event.price
            adjustments.append((event.time, fee))
            stats["add_events"] += 1
            continue
        if event.kind != "exit":
            continue
        active_qty = actual["qty"]
        if active_qty <= 1e-12:
            stats["exit_without_active_entry"] += 1
            continue
        exit_qty = min(event.qty, active_qty)
        avg_entry = actual["cost"] / active_qty if active_qty > 0 else event.price
        actual_pnl, _ = actual_delta_for_exit(event, exit_qty, avg_entry)
        fraction = exit_qty / active_qty if active_qty > 0 else 0.0
        original_exit_qty = state_original[plan.plan_id] * fraction
        original_pnl = (plan.initial_price - event.price) * original_exit_qty
        original_fee = original_exit_qty * event.price * FEE_RATE
        adjustments.append((event.time, (original_pnl - original_fee) - actual_pnl))
        actual["qty"] -= exit_qty
        actual["cost"] -= avg_entry * exit_qty
        state_original[plan.plan_id] = max(0.0, state_original[plan.plan_id] - original_exit_qty)
        stats["exit_events"] += 1
    adjustments.sort(key=lambda item: item[0])
    return adjustments, dict(stats)


def actual_delta_for_entry(event: TradeEvent) -> tuple[float, float]:
    fee = event.commission if event.commission is not None else event.qty * event.price * FEE_RATE
    return -fee, event.qty


def actual_delta_for_exit(event: TradeEvent, qty: float, avg_entry: float) -> tuple[float, float]:
    gross = event.realized_pnl
    if gross is None:
        gross = (avg_entry - event.price) * qty
    fee = event.commission if event.commission is not None else qty * event.price * FEE_RATE
    return gross - fee, -qty


def grid_times(start: datetime, end: datetime) -> list[datetime]:
    current = start
    values: list[datetime] = []
    while current <= end:
        values.append(current)
        current += GRID
    if values[-1] != end:
        values.append(end)
    return values


def curve_for_account(
    baseline: float,
    baseline_time: datetime,
    samples: list[tuple[datetime, float]],
    adjustments: list[tuple[datetime, float]],
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    points: list[dict[str, object]] = []
    adjustment_sum = 0.0
    index = 0
    for timestamp in grid_times(start, end):
        while index < len(adjustments) and adjustments[index][0] <= timestamp:
            adjustment_sum += adjustments[index][1]
            index += 1
        actual = wallet_at(samples, timestamp, baseline)
        counterfactual = actual + adjustment_sum
        points.append(
            {
                "t": iso(timestamp),
                "hours": round((timestamp - start).total_seconds() / 3600.0, 4),
                "actual": round(actual, 8),
                "original": round(counterfactual, 8),
                "delta": round(counterfactual - actual, 8),
            }
        )
    actual_values = [float(point["actual"]) for point in points]
    original_values = [float(point["original"]) for point in points]
    return_pct = lambda values: (values[-1] / values[0] - 1.0) * 100.0 if values[0] else None
    def max_drawdown(values: list[float]) -> float:
        peak = -math.inf
        drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            if peak > 0:
                drawdown = min(drawdown, value / peak - 1.0)
        return drawdown * 100.0
    metrics = {
        "baseline_time_utc": iso(baseline_time),
        "baseline_balance": baseline,
        "actual_final": actual_values[-1],
        "original_final": original_values[-1],
        "actual_return_pct": return_pct(actual_values),
        "original_return_pct": return_pct(original_values),
        "delta_final_usdt": original_values[-1] - actual_values[-1],
        "delta_final_pct_points": (return_pct(original_values) or 0.0) - (return_pct(actual_values) or 0.0),
        "actual_max_drawdown_pct": max_drawdown(actual_values),
        "original_max_drawdown_pct": max_drawdown(original_values),
    }
    return points, metrics


def main() -> int:
    parser = argparse.ArgumentParser(description="Observed-exit entry allocation comparison")
    parser.add_argument("--db", default="state.db")
    parser.add_argument("--end-utc", default="2026-09-05T00:00:00+00:00")
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--accounts", default="acc01,acc02,acc03,acc04")
    args = parser.parse_args()
    end = parse_dt(args.end_utc)
    conn = open_db(Path(args.db))
    accounts: dict[str, object] = {}
    try:
        for account in [item.strip() for item in args.accounts.split(",") if item.strip()]:
            entry_events = load_entry_events(conn, account)
            plans, rollout_start = load_rollout_plans(conn, account, entry_events)
            all_events = assign_plans(plans, entry_events + load_exit_events(conn, account))
            baseline, baseline_time, wallet_samples = load_wallet_series(conn, account, rollout_start, end)
            adjustments, event_stats = build_adjustments(plans, all_events, rollout_start, end)
            period_events = [event for event in all_events if rollout_start <= event.time <= end]
            points, metrics = curve_for_account(
                baseline,
                baseline_time,
                wallet_samples,
                adjustments,
                rollout_start,
                end,
            )
            current_end = wallet_samples[-1][1] if wallet_samples else baseline
            computed_end = baseline + sum(adjustment for _time, adjustment in adjustments)
            accounts[account] = {
                "meta": {
                    "account": account,
                    "rollout_start_utc": iso(rollout_start),
                    "rollout_start_local": rollout_start.astimezone(SHANGHAI).isoformat(timespec="seconds"),
                    "end_utc": iso(end),
                    "end_local": end.astimezone(SHANGHAI).isoformat(timespec="seconds"),
                    "plan_count": sum(1 for plan in plans if rollout_start <= plan.initial_time <= end),
                    "entry_event_count": sum(1 for event in period_events if event.kind in {"initial", "add"}),
                    "exit_event_count": sum(1 for event in period_events if event.kind == "exit"),
                    "mapped_event_count": sum(1 for event in period_events if event.plan_id),
                    "adjustment_event_count": len(adjustments),
                    "event_stats": event_stats,
                    "wallet_snapshot_count": len(wallet_samples),
                    "wallet_end_balance": current_end,
                    "counterfactual_adjustment_sum": computed_end - baseline,
                    "validation_note": "wallet snapshots remain authoritative for the current two-entry curve; the original curve applies only the allocation adjustment.",
                },
                "metrics": metrics,
                "points": points,
            }
            print(
                f"{account}: plans={len(plans)} snapshots={len(wallet_samples)} "
                f"actual={metrics['actual_final']:.4f} original={metrics['original_final']:.4f} "
                f"delta={metrics['delta_final_usdt']:.4f}",
                flush=True,
            )
    finally:
        conn.close()

    if not accounts:
        raise RuntimeError("no accounts analyzed")
    starts = [parse_dt(str(item["meta"]["rollout_start_utc"])) for item in accounts.values()]
    common_start = max(starts)
    aggregate_points: list[dict[str, object]] = []
    for timestamp in grid_times(common_start, end):
        actual_indexes: list[float] = []
        original_indexes: list[float] = []
        for account_data in accounts.values():
            points = account_data["points"]
            candidate = points[0]
            for point in points:
                if parse_dt(str(point["t"])) <= timestamp:
                    candidate = point
                else:
                    break
            anchor_actual = points[0]
            for point in points:
                if parse_dt(str(point["t"])) <= common_start:
                    anchor_actual = point
                else:
                    break
            anchor_original = anchor_actual
            if float(anchor_actual["actual"]):
                actual_indexes.append(float(candidate["actual"]) / float(anchor_actual["actual"]) * 100.0)
                original_indexes.append(float(candidate["original"]) / float(anchor_original["original"]) * 100.0)
        aggregate_points.append(
            {
                "t": iso(timestamp),
                "actual_index": round(sum(actual_indexes) / len(actual_indexes), 6) if actual_indexes else None,
                "original_index": round(sum(original_indexes) / len(original_indexes), 6) if original_indexes else None,
                "accounts": len(actual_indexes),
            }
        )

    payload = {
        "meta": {
            "title": "一次满仓 vs 两次建仓·真实平仓路径反事实",
            "timezone": "Asia/Shanghai",
            "common_start_utc": iso(common_start),
            "common_start_local": common_start.astimezone(SHANGHAI).isoformat(timespec="seconds"),
            "end_utc": iso(end),
            "end_local": end.astimezone(SHANGHAI).isoformat(timespec="seconds"),
            "entry_definition": "现行两次建仓=首笔50%+阳线后阴线追加50%；原策略=首根阴线一次性投入按首笔50%反推的完整目标金额。",
            "exit_definition": "两条曲线沿用服务器记录的实际平仓时点/价格；原策略按实际仓位关闭比例缩放平仓数量。",
            "curve_definition": "现行线使用服务器wallet_snapshots；原策略线=现行钱包余额+累计反事实入场/平仓差额，因此是已实现钱包权益归因，不含历史未实现浮盈亏。",
            "assumptions": [
                "完整目标金额按当前生产配置首笔比例50%从t10s-ent实际成交反推。",
                "手续费按双边0.05%估算；若服务器成交记录提供手续费则优先使用记录值。",
                "这是同一实际平仓路径下的入场分配比较，不等同于重新运行原策略后会触发完全相同的止损/止盈。",
            ],
            "accounts": sorted(accounts),
        },
        "aggregate": {"points": aggregate_points},
        "accounts": accounts,
    }
    output = Path(args.output_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(clean_for_json(payload), ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
