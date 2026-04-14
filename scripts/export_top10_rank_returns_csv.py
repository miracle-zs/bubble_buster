import argparse
import csv
import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


LOCAL_TZ = ZoneInfo("Asia/Shanghai")
RANKING_PATTERN = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+-\s+INFO\s+-\s+root\s+-\s+Top10 ranking completed: .* top=(?P<top>.+)$"
)
TOP_ITEM_PATTERN = re.compile(r"(?P<rank>\d+)\.(?P<symbol>.+?)\s+(?P<pct>-?\d+(?:\.\d+)?)%")


@dataclass(frozen=True)
class RankedItem:
    rank: int
    symbol: str
    pct_change: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export per-account daily top10 short rank returns to CSV."
    )
    parser.add_argument("--db", default="state.db", help="SQLite database path")
    parser.add_argument("--log-dir", default="logs", help="Strategy log directory")
    parser.add_argument(
        "--output",
        default="reports/top10_short_rank_returns.csv",
        help="CSV output path",
    )
    return parser.parse_args()


def iter_log_files(log_dir: Path) -> Iterable[Path]:
    files = sorted(
        path
        for path in log_dir.glob("strategy.log*")
        if path.is_file() and not path.name.endswith(".downloading")
    )
    return files


def parse_rankings(log_dir: Path) -> Dict[str, List[RankedItem]]:
    rankings: Dict[str, List[RankedItem]] = {}
    for path in iter_log_files(log_dir):
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                match = RANKING_PATTERN.match(line.strip())
                if not match:
                    continue
                local_dt = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S")
                local_date = local_dt.date().isoformat()
                items: List[RankedItem] = []
                for item_text in match.group("top").split(", "):
                    item_match = TOP_ITEM_PATTERN.fullmatch(item_text.strip())
                    if not item_match:
                        continue
                    items.append(
                        RankedItem(
                            rank=int(item_match.group("rank")),
                            symbol=item_match.group("symbol").strip().upper(),
                            pct_change=float(item_match.group("pct")),
                        )
                    )
                if items:
                    rankings[local_date] = sorted(items, key=lambda item: item.rank)
    return rankings


def load_runs(conn: sqlite3.Connection) -> Dict[Tuple[str, str], sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT run_id, account_id, trade_day_utc, started_at_utc, status, message
        FROM runs
        ORDER BY started_at_utc ASC
        """
    ).fetchall()
    mapping: Dict[Tuple[str, str], sqlite3.Row] = {}
    for row in rows:
        started_local = datetime.fromisoformat(row["started_at_utc"]).astimezone(LOCAL_TZ)
        mapping[(row["account_id"], started_local.date().isoformat())] = row
    return mapping


def load_positions(conn: sqlite3.Connection) -> Dict[Tuple[str, str], sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT id, run_id, symbol, side, qty, entry_price, opened_at_utc, closed_at_utc, status, close_reason
        FROM positions
        """
    ).fetchall()
    return {(row["run_id"], row["symbol"].upper()): row for row in rows}


def load_fill_stats(conn: sqlite3.Connection) -> Dict[int, Dict[str, Optional[float]]]:
    rows = conn.execute(
        """
        SELECT
            position_id,
            SUM(CASE WHEN side = 'SELL' THEN executed_qty ELSE 0 END) AS sell_qty,
            SUM(CASE WHEN side = 'SELL' THEN executed_qty * avg_price ELSE 0 END) AS sell_notional,
            SUM(CASE WHEN side = 'BUY' THEN executed_qty ELSE 0 END) AS buy_qty,
            SUM(CASE WHEN side = 'BUY' THEN executed_qty * avg_price ELSE 0 END) AS buy_notional,
            MIN(CASE WHEN side = 'SELL' THEN event_time_utc END) AS first_sell_time_utc,
            MAX(CASE WHEN side = 'BUY' THEN event_time_utc END) AS last_buy_time_utc
        FROM fills
        WHERE position_id IS NOT NULL
        GROUP BY position_id
        """
    ).fetchall()
    stats: Dict[int, Dict[str, Optional[float]]] = {}
    for row in rows:
        sell_qty = float(row["sell_qty"] or 0.0)
        buy_qty = float(row["buy_qty"] or 0.0)
        sell_avg = (float(row["sell_notional"]) / sell_qty) if sell_qty > 0 else None
        buy_avg = (float(row["buy_notional"]) / buy_qty) if buy_qty > 0 else None
        stats[int(row["position_id"])] = {
            "sell_qty": sell_qty,
            "sell_avg_price": sell_avg,
            "buy_qty": buy_qty,
            "buy_avg_price": buy_avg,
            "first_sell_time_utc": row["first_sell_time_utc"],
            "last_buy_time_utc": row["last_buy_time_utc"],
        }
    return stats


def calc_short_return_pct(entry_price: Optional[float], exit_price: Optional[float]) -> Optional[float]:
    if entry_price is None or exit_price is None or entry_price == 0:
        return None
    return (entry_price - exit_price) / entry_price * 100.0


def build_rows(
    rankings: Dict[str, List[RankedItem]],
    runs_by_account_day: Dict[Tuple[str, str], sqlite3.Row],
    positions_by_run_symbol: Dict[Tuple[str, str], sqlite3.Row],
    fill_stats_by_position: Dict[int, Dict[str, Optional[float]]],
) -> List[Dict[str, object]]:
    runs_by_local_date: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    for (account_id, local_date), run in runs_by_account_day.items():
        _ = account_id
        runs_by_local_date[local_date].append(run)
    rows: List[Dict[str, object]] = []

    for ranking_local_date in sorted(rankings.keys()):
        ranked_items = rankings[ranking_local_date][:10]
        daily_runs = sorted(
            runs_by_local_date.get(ranking_local_date, []),
            key=lambda item: str(item["account_id"]),
        )
        for run in daily_runs:
            account_id = str(run["account_id"])
            for item in ranked_items:
                row: Dict[str, object] = {
                    "ranking_local_date": ranking_local_date,
                    "account_id": account_id,
                    "rank": item.rank,
                    "symbol": item.symbol,
                    "rank_pct_change": item.pct_change,
                    "run_found": 1 if run else 0,
                    "run_id": "",
                    "trade_day_utc": "",
                    "started_at_utc": "",
                    "started_at_local": "",
                    "position_found": 0,
                    "position_id": "",
                    "position_status": "",
                    "close_reason": "",
                    "entry_price": "",
                    "entry_fill_avg_price": "",
                    "exit_fill_avg_price": "",
                    "opened_at_utc": "",
                    "closed_at_utc": "",
                    "return_pct": "",
                    "return_basis": "",
                }
                started_local = datetime.fromisoformat(run["started_at_utc"]).astimezone(LOCAL_TZ)
                row.update(
                    {
                        "run_id": run["run_id"],
                        "trade_day_utc": run["trade_day_utc"],
                        "started_at_utc": run["started_at_utc"],
                        "started_at_local": started_local.isoformat(),
                    }
                )
                position = positions_by_run_symbol.get((run["run_id"], item.symbol))
                if position:
                    row["position_found"] = 1
                    row["position_id"] = position["id"]
                    row["position_status"] = position["status"]
                    row["close_reason"] = position["close_reason"] or ""
                    row["entry_price"] = position["entry_price"]
                    row["opened_at_utc"] = position["opened_at_utc"]
                    row["closed_at_utc"] = position["closed_at_utc"] or ""

                    fill_stats = fill_stats_by_position.get(position["id"], {})
                    entry_fill_avg_price = fill_stats.get("sell_avg_price")
                    exit_fill_avg_price = fill_stats.get("buy_avg_price")
                    row["entry_fill_avg_price"] = entry_fill_avg_price or ""
                    row["exit_fill_avg_price"] = exit_fill_avg_price or ""

                    return_pct = calc_short_return_pct(
                        float(position["entry_price"]) if position["entry_price"] is not None else None,
                        float(exit_fill_avg_price) if exit_fill_avg_price is not None else None,
                    )
                    if return_pct is not None:
                        row["return_pct"] = return_pct
                        row["return_basis"] = "entry_to_exit_fill"
                    elif position["status"] == "OPEN":
                        row["return_basis"] = "open_position_no_exit_fill"
                    else:
                        row["return_basis"] = "missing_exit_fill"
                else:
                    row["return_basis"] = "rank_symbol_not_opened"
                rows.append(row)
    return rows


def write_csv(output_path: Path, rows: List[Dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ranking_local_date",
        "account_id",
        "rank",
        "symbol",
        "rank_pct_change",
        "run_found",
        "run_id",
        "trade_day_utc",
        "started_at_utc",
        "started_at_local",
        "position_found",
        "position_id",
        "position_status",
        "close_reason",
        "entry_price",
        "entry_fill_avg_price",
        "exit_fill_avg_price",
        "opened_at_utc",
        "closed_at_utc",
        "return_pct",
        "return_basis",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    log_dir = Path(args.log_dir)
    output_path = Path(args.output)

    rankings = parse_rankings(log_dir)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        runs_by_account_day = load_runs(conn)
        positions_by_run_symbol = load_positions(conn)
        fill_stats_by_position = load_fill_stats(conn)
    finally:
        conn.close()

    rows = build_rows(
        rankings=rankings,
        runs_by_account_day=runs_by_account_day,
        positions_by_run_symbol=positions_by_run_symbol,
        fill_stats_by_position=fill_stats_by_position,
    )
    write_csv(output_path, rows)
    print(f"wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
