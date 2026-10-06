#!/usr/bin/env python3
"""Compare equal-sized signal-independent bullish-then-bearish segments.

This is an analysis-only replay.  It deliberately reuses the independent
portfolio replay engine, but generates an arbitrary number of tranches:

* tranche 1: first bearish hourly candle;
* every later tranche: bullish hourly candle, then a later bearish candle;
* every tranche has 1/x of the candidate's target notional;
* later signals are allowed to reopen after an earlier position was closed.

The production strategy is not changed by this script.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean, median

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from replay_independent_strategy_portfolio import (  # noqa: E402
    ENTRY_PRECLOSE_SECONDS,
    ENTRY_WAIT_HOURS,
    EntryOrder,
    MarketData,
    PORTFOLIO_LOSS_PCT,
    SHANGHAI,
    StrategyReplay,
    clean_for_json,
    first_signal,
    floor_hour,
    iso,
    load_candidates,
    load_start_equity,
    local_boundary,
    open_db,
    parse_dt,
    signal_independent_orders,
)


def protection_for_entry(
    candidate: object,
    signal_candle: object,
    entry_time: datetime,
    entry_price: float,
    market: MarketData,
    preclose: bool = False,
) -> tuple[float | None, datetime | None]:
    """Match the existing replay's initial-entry structure protection."""
    if signal_candle is None:
        return None, None
    symbol = str(candidate.symbol)
    highs: list[float] = []
    for offset in (2, 1):
        candle = market.hour(symbol, signal_candle.close_time - timedelta(hours=offset))
        if candle is not None:
            highs.append(candle.high_price)
    if preclose and highs:
        return max(highs), signal_candle.close_time
    if entry_time.astimezone(SHANGHAI).hour >= 12:
        post_close_high = market.range_extreme(
            symbol,
            signal_candle.close_time,
            entry_time,
            "high",
        )
        values = highs + [entry_price]
        if post_close_high is not None:
            values.append(post_close_high)
        return (max(values), entry_time) if values else (None, None)
    return None, None


def build_segment_orders(
    candidates: list[object],
    market: MarketData,
    start: datetime,
    end: datetime,
    segments: int,
) -> list[EntryOrder]:
    """Build first-bearish + repeated bullish-then-bearish orders."""
    if segments < 1:
        raise ValueError("segments must be >= 1")

    orders: list[EntryOrder] = []
    ratio = 1.0 / float(segments)
    for candidate in candidates:
        base = floor_hour(candidate.run_started_at)
        deadline = base + timedelta(hours=ENTRY_WAIT_HOURS)
        candles = market.hourly(candidate.symbol)
        first_time, first_price, first_candle = first_signal(
            candles,
            base,
            deadline,
            "first_bearish",
        )
        if first_time is None or first_price is None or first_candle is None:
            continue
        if first_time < start or first_time >= end:
            continue

        structure_stop, structure_active_at = protection_for_entry(
            candidate,
            first_candle,
            first_time,
            first_price,
            market,
            preclose=True,
        )
        orders.append(
            EntryOrder(
                plan_id=candidate.candidate_id,
                candidate_id=candidate.candidate_id,
                run_id=candidate.run_id,
                session_date=candidate.session_date,
                symbol=candidate.symbol,
                stage="FIRST",
                entry_time=first_time,
                entry_price=first_price,
                target_notional=candidate.target_notional * ratio,
                qty=candidate.target_notional * ratio / first_price,
                liq_ratio=candidate.liq_ratio,
                actual_sl_price=candidate.actual_sl_price,
                structure_stop=structure_stop,
                structure_active_at=structure_active_at,
                signal_time=first_time,
                signal_kind="first_bearish",
            )
        )

        if segments == 1:
            continue

        phase = "WAIT_BULLISH"
        tranche = 2
        for candle in candles:
            if candle.open_time <= first_candle.open_time:
                continue
            if candle.close_time > deadline + timedelta(seconds=1):
                break
            if phase == "WAIT_BULLISH":
                if candle.close_price > candle.open_price:
                    phase = "WAIT_BEARISH"
                continue
            if candle.close_price >= candle.open_price:
                continue

            # StrategyReplay accepts SECOND for all later tranches.  The
            # signal_kind still records the exact tranche number for audit.
            orders.append(
                EntryOrder(
                    plan_id=candidate.candidate_id,
                    candidate_id=candidate.candidate_id,
                    run_id=candidate.run_id,
                    session_date=candidate.session_date,
                    symbol=candidate.symbol,
                    stage="SECOND",
                    entry_time=candle.close_time,
                    entry_price=candle.close_price,
                    target_notional=candidate.target_notional * ratio,
                    qty=candidate.target_notional * ratio / candle.close_price,
                    liq_ratio=candidate.liq_ratio,
                    actual_sl_price=candidate.actual_sl_price,
                    structure_stop=None,
                    structure_active_at=None,
                    signal_time=candle.close_time,
                    signal_kind=f"bullish_then_bearish_{tranche}",
                )
            )
            tranche += 1
            if tranche > segments:
                break
            phase = "WAIT_BULLISH"

    orders.sort(key=lambda item: (item.entry_time, item.symbol, item.stage, item.candidate_id, item.signal_kind))
    return orders


def event_stats(result: dict[str, object], orders: list[EntryOrder], segments: int) -> dict[str, object]:
    events = [event for event in result.get("events", []) if isinstance(event, dict)]
    entries = [event for event in events if event.get("type") == "entry"]
    exits = [event for event in events if event.get("type") == "exit"]
    first_entries = [event for event in entries if event.get("stage") == "FIRST"]
    follow_entries = [event for event in entries if event.get("stage") != "FIRST"]
    planned_by_plan = Counter(order.plan_id for order in orders)
    filled_by_plan = Counter(str(event.get("plan_id") or "") for event in entries)
    planned_plans = len(planned_by_plan)
    completed_plans = sum(1 for plan_id in planned_by_plan if filled_by_plan[plan_id] >= segments)
    multi_plans = sum(1 for plan_id in planned_by_plan if filled_by_plan[plan_id] >= 2)

    # A later signal is a true independent re-entry when the symbol was no
    # longer active at that event time.  Simultaneous exits are processed
    # before entries by the replay loop, so time sorting keeps that behavior.
    active_symbols: set[str] = set()
    independent_reentries = 0
    max_open_symbols = 0
    for event in sorted(events, key=lambda item: parse_dt(str(item.get("time") or ""))):
        symbol = str(event.get("symbol") or "")
        if event.get("type") == "exit":
            active_symbols.discard(symbol)
        elif event.get("type") == "entry":
            if event.get("stage") != "FIRST" and symbol not in active_symbols:
                independent_reentries += 1
            active_symbols.add(symbol)
            max_open_symbols = max(max_open_symbols, len(active_symbols))

    exit_reasons = Counter(str(event.get("reason") or "UNKNOWN") for event in exits)
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    chart_metrics = result.get("chart_metrics") if isinstance(result.get("chart_metrics"), dict) else {}
    mean_tranches = mean(filled_by_plan.get(plan_id, 0) for plan_id in planned_by_plan) if planned_plans else 0.0
    return {
        "segments": segments,
        "planned_plans": planned_plans,
        "planned_orders": len(orders),
        "planned_followup_orders": len(orders) - len([order for order in orders if order.stage == "FIRST"]),
        "filled_entries": len(entries),
        "filled_first_entries": len(first_entries),
        "filled_followup_entries": len(follow_entries),
        "entry_fill_rate_pct": (len(entries) / len(orders) * 100.0) if orders else 0.0,
        "followup_fill_rate_pct": (
            len(follow_entries)
            / (len(orders) - len([order for order in orders if order.stage == "FIRST"]))
            * 100.0
            if len(orders) > len(first_entries)
            else 0.0
        ),
        "plans_with_second_plus": multi_plans,
        "plans_with_second_plus_pct": (multi_plans / planned_plans * 100.0) if planned_plans else 0.0,
        "plans_fully_filled": completed_plans,
        "plans_fully_filled_pct": (completed_plans / planned_plans * 100.0) if planned_plans else 0.0,
        "mean_filled_tranches": mean_tranches,
        "median_filled_tranches": median(filled_by_plan.get(plan_id, 0) for plan_id in planned_by_plan)
        if planned_plans
        else 0.0,
        "independent_reentries": independent_reentries,
        "max_open_symbols_from_events": max_open_symbols,
        "exit_count": len(exits),
        "portfolio_stop_count": len(result.get("portfolio_events", [])),
        "exit_reasons": dict(sorted(exit_reasons.items())),
        "final_equity": metrics.get("final_equity"),
        "pnl": metrics.get("pnl"),
        "return_pct": metrics.get("return_pct"),
        "max_drawdown_pct": metrics.get("max_drawdown_pct"),
        "peak_equity": metrics.get("peak_equity"),
        "trough_equity": metrics.get("trough_equity"),
        "chart_final_equity": chart_metrics.get("final_equity"),
        "chart_return_pct": chart_metrics.get("return_pct"),
        "chart_max_drawdown_pct": chart_metrics.get("max_drawdown_pct"),
    }


def write_summary_csv(path: Path, summaries: list[dict[str, object]]) -> None:
    fields = [
        "segments",
        "planned_plans",
        "planned_orders",
        "planned_followup_orders",
        "filled_entries",
        "filled_first_entries",
        "filled_followup_entries",
        "entry_fill_rate_pct",
        "followup_fill_rate_pct",
        "plans_with_second_plus",
        "plans_with_second_plus_pct",
        "plans_fully_filled",
        "plans_fully_filled_pct",
        "mean_filled_tranches",
        "median_filled_tranches",
        "independent_reentries",
        "max_open_symbols_from_events",
        "exit_count",
        "portfolio_stop_count",
        "final_equity",
        "pnl",
        "return_pct",
        "max_drawdown_pct",
        "peak_equity",
        "trough_equity",
        "chart_final_equity",
        "chart_return_pct",
        "chart_max_drawdown_pct",
        "exit_reasons",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for summary in summaries:
            row = dict(summary)
            row["exit_reasons"] = json.dumps(row.get("exit_reasons", {}), ensure_ascii=False, separators=(",", ":"))
            writer.writerow({field: row.get(field) for field in fields})


def write_daily_csv(path: Path, series: dict[str, dict[str, object]], days: list[date]) -> None:
    fields = [
        "date",
        "segments",
        "baseline_equity",
        "threshold_equity",
        "carried_count",
        "carried_notional",
        "new_first",
        "new_second",
        "portfolio_stop",
        "portfolio_stop_time",
        "hourly_tp",
        "individual_stop",
        "daily_loss_cut",
        "noon_protection",
        "morning_protection",
        "structure_stop",
        "max_hold",
        "end_equity",
        "next_day_active_count",
        "next_day_active_notional",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for day in days:
            day_text = day.isoformat()
            for series_item in series.values():
                segments = series_item["segments"]
                rows = series_item["result"].get("daily", [])
                source = next((row for row in rows if row.get("date") == day_text), {})
                writer.writerow({"date": day_text, "segments": segments, **{field: source.get(field) for field in fields if field not in {"date", "segments"}}})


def parse_segments(raw: str) -> list[int]:
    values: list[int] = []
    for item in raw.split(","):
        value = int(item.strip())
        if value < 1 or value > 20:
            raise ValueError("segments must be between 1 and 20")
        if value not in values:
            values.append(value)
    if not values:
        raise ValueError("at least one segment value is required")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Optimize equal-sized signal-independent bullish-then-bearish segments")
    parser.add_argument("--db", default="remote_data/state.db")
    parser.add_argument("--market-cache-dir", default="remote_artifacts/independent_replay_market_1h_20260902")
    parser.add_argument("--output-json", default="reports/bullbear-segment-optimization-20260902.json")
    parser.add_argument("--output-csv", default="reports/bullbear-segment-optimization-20260902.csv")
    parser.add_argument("--daily-csv", default="reports/bullbear-segment-optimization-daily-20260902.csv")
    parser.add_argument("--start-day", default="2026-08-11")
    parser.add_argument("--end-day", default="2026-08-31")
    parser.add_argument("--account", default="acc04")
    parser.add_argument("--segments", default="1,2,3,4,5,6,7,8")
    parser.add_argument("--workers", type=int, default=12)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    segments_values = parse_segments(args.segments)
    chart_start_day = date.fromisoformat(args.start_day)
    chart_end_day = date.fromisoformat(args.end_day)
    chart_start = local_boundary(chart_start_day)
    chart_end = local_boundary(chart_end_day + timedelta(days=1))
    simulation_start = chart_start - timedelta(days=1)

    db_path = Path(args.db)
    conn = open_db(db_path)
    try:
        start_equity, snapshot_time = load_start_equity(conn, args.account, simulation_start)
        candidates, seed, symbols = load_candidates(conn, args.account, simulation_start, chart_end)
    finally:
        conn.close()

    print(
        f"loaded account={args.account} candidates={len(candidates)} seed={len(seed)} symbols={len(symbols)} "
        f"start_equity={start_equity:.8f} snapshot={iso(snapshot_time)}",
        flush=True,
    )
    market = MarketData(Path(args.market_cache_dir), simulation_start, chart_end, symbols, workers=args.workers)
    market.load()
    coverage = market.coverage()
    print(
        f"market 15m={coverage['symbols_with_15m']} 1h={coverage['symbols_with_1h']} "
        f"missing={len(market.missing_symbols)} failed_files={len(market.failed_files)}",
        flush=True,
    )

    results: dict[str, dict[str, object]] = {}
    summaries: list[dict[str, object]] = []
    series: dict[str, dict[str, object]] = {}
    for segments in segments_values:
        raw_orders = build_segment_orders(candidates, market, simulation_start, chart_end, segments)
        orders = signal_independent_orders(raw_orders, market)
        name = f"bullbear_{segments}"
        replay = StrategyReplay(
            name=name,
            label=f"先阳后阴·信号独立开仓（{segments}段）",
            start=simulation_start,
            end=chart_end,
            start_equity=start_equity,
            market=market,
            orders=orders,
            seed=seed,
            allow_signal_independent_scale_ins=True,
        )
        result = replay.run()
        chart_points = [
            point
            for point in result["points"]
            if chart_start <= parse_dt(str(point["t"])) <= chart_end
        ]
        result["chart_metrics"] = StrategyReplay._metrics(chart_points)
        result["segments"] = segments
        result["order_count"] = len(orders)
        results[name] = result
        summary = event_stats(result, orders, segments)
        summaries.append(summary)
        series[name] = {"segments": segments, "summary": summary, "result": result}
        print(
            f"x={segments}: final={float(summary['final_equity']):.4f} "
            f"return={float(summary['return_pct']):.3f}% "
            f"mdd={float(summary['max_drawdown_pct']):.3f}% "
            f"portfolio_stops={summary['portfolio_stop_count']} "
            f"filled={summary['filled_entries']}/{summary['planned_orders']} "
            f"reentries={summary['independent_reentries']}",
            flush=True,
        )

    summaries.sort(key=lambda item: int(item["segments"]))
    compact_series: dict[str, object] = {}
    for name, item in series.items():
        result = item["result"]
        compact_series[name] = {
            "segments": item["segments"],
            "label": result["label"],
            "summary": item["summary"],
            "points": result["points"],
            "daily": result["daily"],
            "portfolio_events": result["portfolio_events"],
        }
    payload = {
        "meta": {
            "title": "先阳后阴 x 段·信号独立开仓寻优",
            "account": args.account,
            "timezone": "Asia/Shanghai",
            "segments_definition": "x 为总段数，每段目标名义金额为候选实际名义金额的 1/x；首段为首根阴线，后续每段需要阳线后再出现后续阴线。",
            "signal_independence": "后续信号到达时，即使此前同 symbol 仓位已退出，也重新开独立仓位；若仍有仓位则按交易所单 symbol 净仓追加并重算保护。",
            "chart_start_local": f"{args.start_day}T08:00:00+08:00",
            "chart_end_local": f"{(chart_end_day + timedelta(days=1)).isoformat()}T08:00:00+08:00",
            "simulation_start_utc": iso(simulation_start),
            "simulation_end_utc": iso(chart_end),
            "starting_wallet_snapshot_utc": iso(snapshot_time),
            "starting_equity": start_equity,
            "portfolio_loss_cut_pct": PORTFOLIO_LOSS_PCT,
            "market_coverage": coverage,
            "seed_count": len(seed),
            "candidate_count": len(candidates),
            "notes": [
                "每个 x 独立维护持仓、逐仓保护、小时止盈、组合止损和跨日仓位。",
                "候选区间为 2026-08-11 08:00 至 2026-09-01 08:00（北京时间），起始前一天的实际持仓作为共同 warm-start。",
                "仍沿用当前 replay 的 15 分钟权益快照近似、18% 小时止盈、47.5 小时最长持仓和 3.5% 组合止损。",
                "这是样本内寻优，不等于未来最优；正式采用前应做滚动/留出区间验证。",
            ],
        },
        "summary": summaries,
        "series": compact_series,
    }
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(clean_for_json(payload), ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    write_summary_csv(Path(args.output_csv), summaries)
    write_daily_csv(Path(args.daily_csv), series, [chart_start_day + timedelta(days=index) for index in range((chart_end_day - chart_start_day).days + 1)])
    print(f"wrote {output_json}", flush=True)
    print(f"wrote {args.output_csv}", flush=True)
    print(f"wrote {args.daily_csv}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
