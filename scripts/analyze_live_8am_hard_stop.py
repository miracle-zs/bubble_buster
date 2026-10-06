#!/usr/bin/env python3
"""Compare a live 08:00 equity path with a first-crossing hard stop."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


UTC = timezone.utc
SHANGHAI = ZoneInfo("Asia/Shanghai")
ACCOUNTS = ("acc01", "acc02", "acc03", "acc04")


def parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--threshold", type=float, default=-3.5)
    parser.add_argument("--exit-cost", type=float, default=0.0006)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    boundary = datetime.combine(args.date, time(8, 0), SHANGHAI).astimezone(UTC)
    conn = sqlite3.connect(f"file:{args.db.resolve()}?mode=ro", uri=True)
    try:
        cashflows: dict[str, list[tuple[datetime, float]]] = {account: [] for account in ACCOUNTS}
        for account, event_time, amount in conn.execute(
            """
            SELECT account_id, event_time_utc, SUM(amount)
            FROM (
                SELECT DISTINCT account_id, unique_key, event_time_utc, amount
                FROM cashflow_events
                WHERE account_id IN ('acc01', 'acc02', 'acc03', 'acc04')
            )
            WHERE event_time_utc >= ?
            GROUP BY account_id, event_time_utc
            ORDER BY account_id, event_time_utc
            """,
            (boundary.isoformat(),),
        ):
            cashflows[str(account)].append((parse_iso(str(event_time)), float(amount)))

        rows = []
        for account in ACCOUNTS:
            snapshots = conn.execute(
                """
                SELECT captured_at_utc, balance_usdt
                FROM wallet_snapshots
                WHERE account_id = ? AND captured_at_utc >= ?
                ORDER BY captured_at_utc, id
                """,
                (account, boundary.isoformat()),
            ).fetchall()
            if not snapshots:
                rows.append({"account": account, "error": "no snapshots"})
                continue
            start_time = parse_iso(str(snapshots[0][0]))
            start_equity = float(snapshots[0][1])

            adjusted = []
            for captured_at, equity in snapshots:
                timestamp = parse_iso(str(captured_at))
                cashflow = sum(
                    amount for event_time, amount in cashflows[account] if event_time <= timestamp
                )
                adjusted_equity = float(equity) - cashflow
                adjusted.append((timestamp, adjusted_equity, adjusted_equity / start_equity - 1.0))

            first_crossing = next(
                (point for point in adjusted if point[2] <= args.threshold / 100.0),
                None,
            )
            latest = adjusted[-1]
            trough = min(adjusted, key=lambda point: point[2])
            stop_raw = first_crossing[2] if first_crossing else latest[2]
            stop_cost = stop_raw - args.exit_cost if first_crossing else latest[2]
            rows.append(
                {
                    "account": account,
                    "startTime": start_time.astimezone(SHANGHAI).isoformat(),
                    "latestTime": latest[0].astimezone(SHANGHAI).isoformat(),
                    "startEquity": start_equity,
                    "latestEquity": latest[1],
                    "currentReturn": latest[2],
                    "troughReturn": trough[2],
                    "troughTime": trough[0].astimezone(SHANGHAI).isoformat(),
                    "triggered": first_crossing is not None,
                    "triggerTime": first_crossing[0].astimezone(SHANGHAI).isoformat()
                    if first_crossing
                    else None,
                    "triggerReturn": first_crossing[2] if first_crossing else None,
                    "stopRaw": stop_raw,
                    "stopCost": stop_cost,
                    "savedRawPctPoints": stop_raw - latest[2] if first_crossing else 0.0,
                    "savedCostPctPoints": stop_cost - latest[2] if first_crossing else 0.0,
                    "cashflow": sum(amount for _, amount in cashflows[account]),
                    "snapshots": len(adjusted),
                }
            )
    finally:
        conn.close()

    payload = {
        "sessionDate": args.date.isoformat(),
        "thresholdPct": args.threshold,
        "exitCostPct": args.exit_cost * 100.0,
        "rows": rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.output}")


if __name__ == "__main__":
    main()
