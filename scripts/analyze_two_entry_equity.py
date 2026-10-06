#!/usr/bin/env python3
"""Replay the deployed independent-entry runs as one-entry vs two-entry scenarios.

This is an analysis-only helper.  The production database stores the first
tranche and a later ``t10s-add`` order on the same logical position row, so a
position-row-only loader would silently halve the notional in the comparison.
This loader identifies the initial ``t10s-ent`` event, infers the full target
from its 50% fill, and ignores independent scale-in rows as new candidates.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from optimize_bullbear_segments import build_segment_orders  # noqa: E402
from replay_independent_strategy_portfolio import (  # noqa: E402
    Candidate,
    MarketData,
    StrategyReplay,
    SHANGHAI,
    UTC,
    clean_for_json,
    iso,
    open_db,
    parse_dt,
    signal_independent_orders,
)


ENTRY_RATIO = 0.50
INDEPENDENT_MODE = "after_bullish_bearish_independent"


@dataclass(frozen=True)
class InitialFill:
    position_id: int
    event_time: datetime
    price: float
    qty: float


def _positive_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _payload_float(payload: dict[str, object], *keys: str) -> float | None:
    for key in keys:
        value = _positive_float(payload.get(key))
        if value is not None:
            return value
    return None


def load_initial_fills(conn: sqlite3.Connection, account: str) -> dict[int, InitialFill]:
    rows = conn.execute(
        """
        SELECT position_id, event_time_utc, raw_json
        FROM order_events
        WHERE account_id = ?
          AND position_id IS NOT NULL
          AND client_order_id LIKE 't10s-ent-%'
          AND status = 'FILLED'
        ORDER BY event_time_utc ASC, id ASC
        """,
        (account,),
    ).fetchall()
    fills: dict[int, InitialFill] = {}
    for row in rows:
        position_id = int(row["position_id"])
        if position_id in fills:
            continue
        try:
            payload = json.loads(str(row["raw_json"] or "{}"))
        except json.JSONDecodeError:
            continue
        audit = payload.get("entry_audit")
        audit = audit if isinstance(audit, dict) else {}
        price = _positive_float(audit.get("fill_price"))
        price = price or _payload_float(payload, "avgPrice", "ap")
        qty = _payload_float(payload, "executedQty", "z", "origQty", "q")
        if price is None or qty is None:
            continue
        filled_at = audit.get("filled_at_utc")
        event_time = parse_dt(str(filled_at or row["event_time_utc"]))
        fills[position_id] = InitialFill(
            position_id=position_id,
            event_time=event_time,
            price=price,
            qty=qty,
        )
    return fills


def _candidate_from_row(row: sqlite3.Row, *, qty: float, price: float, entry_time: datetime, target: float) -> Candidate:
    liq_price = _positive_float(row["liq_price_open"])
    liq_ratio = liq_price / price if liq_price is not None and price > 0 else None
    actual_sl = _positive_float(row["sl_price"])
    run_started = parse_dt(str(row["started_at_utc"]))
    return Candidate(
        candidate_id=f"p{int(row['id'])}",
        run_id=str(row["run_id"]),
        run_started_at=run_started,
        session_date=run_started.astimezone(SHANGHAI).date(),
        symbol=str(row["symbol"]).strip().upper(),
        target_notional=float(target),
        actual_qty=float(qty),
        actual_entry_price=float(price),
        actual_entry_time=entry_time,
        liq_ratio=liq_ratio,
        actual_sl_price=actual_sl,
        actual_close_reason=str(row["close_reason"] or ""),
    )


def load_plan_candidates(
    conn: sqlite3.Connection,
    account: str,
    start: datetime,
    end: datetime,
) -> tuple[list[Candidate], list[Candidate], set[str], dict[str, int]]:
    """Load active warm-start positions and initial plans for the rollout.

    The requested chart windows begin before the first independent run for
    each account.  Warm-start positions are therefore the positions already
    active at the rollout boundary; new plans are only initial entries from
    independent-mode runs.
    """
    initial_fills = load_initial_fills(conn, account)
    rows = conn.execute(
        """
        SELECT
            p.id, p.run_id, p.symbol, p.qty, p.entry_price,
            p.liq_price_open, p.sl_price, p.opened_at_utc,
            p.closed_at_utc, p.close_reason, p.status,
            r.started_at_utc, r.message
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        WHERE r.account_id = ?
          AND p.side = 'SHORT'
          AND p.qty > 0
          AND p.entry_price > 0
          AND p.opened_at_utc < ?
        ORDER BY p.opened_at_utc ASC, p.id ASC
        """,
        (account, iso(end)),
    ).fetchall()

    # Only one position row per symbol should be active at the rollout
    # boundary.  Choosing the latest opened row handles signal-independent
    # rows without inventing an overlapping position.
    active_by_symbol: dict[str, sqlite3.Row] = {}
    for row in rows:
        opened_at = parse_dt(str(row["opened_at_utc"]))
        closed_raw = row["closed_at_utc"]
        closed_at = parse_dt(str(closed_raw)) if closed_raw else None
        if opened_at >= start or (closed_at is not None and closed_at <= start):
            continue
        symbol = str(row["symbol"]).strip().upper()
        current = active_by_symbol.get(symbol)
        if current is None or parse_dt(str(current["opened_at_utc"])) < opened_at:
            active_by_symbol[symbol] = row

    seed: list[Candidate] = []
    for row in active_by_symbol.values():
        qty = _positive_float(row["qty"])
        price = _positive_float(row["entry_price"])
        if qty is None or price is None:
            continue
        opened_at = parse_dt(str(row["opened_at_utc"]))
        seed.append(
            _candidate_from_row(
                row,
                qty=qty,
                price=price,
                entry_time=opened_at,
                target=qty * price,
            )
        )

    candidates: list[Candidate] = []
    skipped_no_initial_fill = 0
    skipped_non_independent = 0
    for row in rows:
        run_message = str(row["message"] or "")
        if INDEPENDENT_MODE not in run_message:
            skipped_non_independent += 1
            continue
        initial = initial_fills.get(int(row["id"]))
        if initial is None:
            skipped_no_initial_fill += 1
            continue
        if not start <= initial.event_time < end:
            continue
        # Production config uses a 50% first tranche.  Reconstruct the
        # logical full target from the actual first fill, including any
        # exchange-side quantity shrink, so both counterfactuals have the
        # same realised capital scale.
        full_target = initial.qty * initial.price / ENTRY_RATIO
        candidates.append(
            _candidate_from_row(
                row,
                qty=initial.qty,
                price=initial.price,
                entry_time=initial.event_time,
                target=full_target,
            )
        )

    candidates.sort(key=lambda item: (item.actual_entry_time, item.symbol, item.candidate_id))
    seed.sort(key=lambda item: (item.actual_entry_time, item.symbol, item.candidate_id))
    symbols = {item.symbol for item in seed + candidates}
    diagnostics = {
        "initial_fill_rows": len(initial_fills),
        "seed_positions": len(seed),
        "rollout_plans": len(candidates),
        "skipped_no_initial_fill": skipped_no_initial_fill,
        "non_independent_position_rows": skipped_non_independent,
    }
    return candidates, seed, symbols, diagnostics


def safe_summary(result: dict[str, object], orders: list[object], segments: int) -> dict[str, object]:
    events = [item for item in result.get("events", []) if isinstance(item, dict)]
    entries = [item for item in events if item.get("type") == "entry"]
    exits = [item for item in events if item.get("type") == "exit"]
    first_entries = [item for item in entries if item.get("stage") == "FIRST"]
    follow_entries = [item for item in entries if item.get("stage") != "FIRST"]
    planned_orders = len(orders)
    planned_followup = sum(1 for order in orders if getattr(order, "stage", "") != "FIRST")
    planned_plans = len({getattr(order, "plan_id", "") for order in orders})
    filled_by_plan = Counter(str(item.get("plan_id") or "") for item in entries)
    full_plans = sum(1 for plan_id in filled_by_plan if filled_by_plan[plan_id] >= segments)
    multi_plans = sum(1 for plan_id in filled_by_plan if filled_by_plan[plan_id] >= 2)
    active_symbols: set[str] = set()
    independent_reentries = 0
    for event in sorted(events, key=lambda item: parse_dt(str(item.get("time") or ""))):
        symbol = str(event.get("symbol") or "")
        if event.get("type") == "exit":
            active_symbols.discard(symbol)
        elif event.get("type") == "entry":
            if event.get("stage") != "FIRST" and symbol not in active_symbols:
                independent_reentries += 1
            active_symbols.add(symbol)
    reasons = Counter(str(item.get("reason") or "UNKNOWN") for item in exits)
    metrics = result.get("metrics") if isinstance(result.get("metrics"), dict) else {}
    chart_metrics = result.get("chart_metrics") if isinstance(result.get("chart_metrics"), dict) else {}
    filled_count = len(entries)
    return {
        "segments": segments,
        "planned_plans": planned_plans,
        "planned_orders": planned_orders,
        "planned_followup_orders": planned_followup,
        "filled_entries": filled_count,
        "filled_first_entries": len(first_entries),
        "filled_followup_entries": len(follow_entries),
        "entry_fill_rate_pct": filled_count / planned_orders * 100.0 if planned_orders else 0.0,
        "followup_fill_rate_pct": len(follow_entries) / planned_followup * 100.0 if planned_followup else 0.0,
        "plans_with_second_plus": multi_plans,
        "plans_with_second_plus_pct": multi_plans / planned_plans * 100.0 if planned_plans else 0.0,
        "plans_fully_filled": full_plans,
        "plans_fully_filled_pct": full_plans / planned_plans * 100.0 if planned_plans else 0.0,
        "independent_reentries": independent_reentries,
        "exit_count": len(exits),
        "portfolio_stop_count": len(result.get("portfolio_events", [])),
        "exit_reasons": dict(sorted(reasons.items())),
        "initial_equity": metrics.get("initial_equity"),
        "final_equity": metrics.get("final_equity"),
        "pnl": metrics.get("pnl"),
        "return_pct": metrics.get("return_pct"),
        "max_drawdown_pct": metrics.get("max_drawdown_pct"),
        "chart_initial_equity": chart_metrics.get("initial_equity"),
        "chart_final_equity": chart_metrics.get("final_equity"),
        "chart_pnl": chart_metrics.get("pnl"),
        "chart_return_pct": chart_metrics.get("return_pct"),
        "chart_max_drawdown_pct": chart_metrics.get("max_drawdown_pct"),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare one full entry with two independent 50% entries")
    parser.add_argument("--db", default="state.db")
    parser.add_argument("--market-cache-dir", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True)
    parser.add_argument("--workers", type=int, default=12)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = parse_dt(args.start_utc)
    end = parse_dt(args.end_utc)
    simulation_start = start - timedelta(days=1)

    conn = open_db(Path(args.db))
    try:
        start_equity_row = conn.execute(
            """
            SELECT balance_usdt, captured_at_utc
            FROM wallet_snapshots
            WHERE account_id = ? AND captured_at_utc >= ?
            ORDER BY captured_at_utc ASC, id ASC
            LIMIT 1
            """,
            (args.account, iso(simulation_start)),
        ).fetchone()
        if start_equity_row is None:
            raise RuntimeError(f"no wallet snapshot after {iso(simulation_start)} for {args.account}")
        start_equity = float(start_equity_row["balance_usdt"])
        candidates, seed, symbols, diagnostics = load_plan_candidates(conn, args.account, start, end)
    finally:
        conn.close()

    print(
        f"loaded account={args.account} plans={len(candidates)} seed={len(seed)} symbols={len(symbols)} "
        f"start_equity={start_equity:.8f} snapshot={start_equity_row['captured_at_utc']}",
        flush=True,
    )
    market = MarketData(Path(args.market_cache_dir), simulation_start, end, symbols, workers=args.workers)
    market.load()
    coverage = market.coverage()
    print(
        f"market 15m={coverage['symbols_with_15m']} 1h={coverage['symbols_with_1h']} "
        f"missing={len(market.missing_symbols)} failed_files={len(market.failed_files)}",
        flush=True,
    )

    series: dict[str, object] = {}
    for segments, label in ((1, "原策略·一次满仓"), (2, "两次建仓·50%+50%")):
        raw_orders = build_segment_orders(candidates, market, simulation_start, end, segments)
        orders = signal_independent_orders(raw_orders, market)
        replay = StrategyReplay(
            name=f"segments_{segments}",
            label=label,
            start=simulation_start,
            end=end,
            start_equity=start_equity,
            market=market,
            orders=orders,
            seed=seed,
            allow_signal_independent_scale_ins=True,
        )
        result = replay.run()
        chart_points = [
            point for point in result["points"]
            if start <= parse_dt(str(point["t"])) <= end
        ]
        result["chart_metrics"] = StrategyReplay._metrics(chart_points)
        summary = safe_summary(result, orders, segments)
        series[f"segments_{segments}"] = {
            "segments": segments,
            "label": label,
            "summary": summary,
            "points": chart_points,
            "daily": result.get("daily", []),
            "portfolio_events": result.get("portfolio_events", []),
        }
        print(
            f"x={segments}: chart_final={float(summary['chart_final_equity']):.4f} "
            f"chart_return={float(summary['chart_return_pct']):.3f}% "
            f"chart_mdd={float(summary['chart_max_drawdown_pct']):.3f}% "
            f"planned={summary['planned_orders']} filled={summary['filled_entries']} "
            f"reentries={summary['independent_reentries']}",
            flush=True,
        )

    payload = {
        "meta": {
            "title": "原策略一次满仓 vs 两次建仓权益曲线",
            "account": args.account,
            "timezone": "Asia/Shanghai",
            "start_utc": iso(start),
            "end_utc": iso(end),
            "start_local": start.astimezone(SHANGHAI).isoformat(timespec="seconds"),
            "end_local": end.astimezone(SHANGHAI).isoformat(timespec="seconds"),
            "simulation_start_utc": iso(simulation_start),
            "starting_wallet_snapshot_utc": str(start_equity_row["captured_at_utc"]),
            "starting_equity": start_equity,
            "entry_definition": "一次满仓=首根阴线一次投入完整目标名义金额；两次建仓=首根阴线50%，阳线后下一根阴线再投入50%。",
            "target_reconstruction": "当前生产库的t10s-ent首笔实际成交按50%反推逻辑完整目标；t10s-add独立仓位行不作为新的候选计划。",
            "assumptions": [
                "沿用回放引擎的逐仓结构保护、小时止盈、日内保护、47.5小时最长持仓、3.5%组合止损和双边0.05%手续费。",
                "行情使用Binance Vision 15分钟K线；信号按可获得的完整小时收盘处理。",
                "这是按当时独立建仓候选与历史行情重放的反事实比较，不是交易所直接记录的另一条真实权益曲线。",
            ],
            "loader_diagnostics": diagnostics,
            "market_coverage": coverage,
        },
        "series": series,
    }
    output = Path(args.output_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(clean_for_json(payload), ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"wrote {output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
