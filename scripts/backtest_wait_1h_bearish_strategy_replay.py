import argparse
import csv
import json
import math
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


BINANCE_FAPI_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_VISION = "https://data.binance.vision/data/futures/um/monthly/klines"
LOCAL_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class PositionSample:
    position_id: int
    account_id: str
    symbol: str
    qty: float
    actual_entry_price: float
    opened_at_utc: datetime
    closed_at_utc: datetime
    close_reason: str
    actual_exit_fill_price: Optional[float]


@dataclass(frozen=True)
class Candle:
    open_time_ms: int
    close_time_ms: int
    open_price: float
    high_price: float
    low_price: float
    close_price: float

    @property
    def open_dt(self) -> datetime:
        return datetime.fromtimestamp(self.open_time_ms / 1000, tz=timezone.utc)

    @property
    def close_dt(self) -> datetime:
        return datetime.fromtimestamp(self.close_time_ms / 1000, tz=timezone.utc)


@dataclass(frozen=True)
class ReplayResult:
    entered: bool
    entry_time_utc: Optional[datetime]
    entry_price: Optional[float]
    exit_time_utc: Optional[datetime]
    exit_price: Optional[float]
    exit_reason: str
    pnl_usdt: Optional[float]
    return_pct: Optional[float]
    wait_hours: Optional[float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay delayed short entry: wait until first bearish closed 1h candle, then simulate exits by 1h OHLC."
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--cache-dir", default="remote_artifacts/market_data_cache/binance_um_1h_replay")
    parser.add_argument("--output-csv", default="reports/wait_1h_bearish_strategy_replay.csv")
    parser.add_argument("--output-summary", default="reports/wait_1h_bearish_strategy_replay_summary.json")
    parser.add_argument("--account", action="append", help="Limit to one or more accounts.")
    parser.add_argument("--tp-drop-pct", type=float, default=20.0)
    parser.add_argument("--max-hold-hours", type=float, default=47.5)
    parser.add_argument("--lookahead-hours", type=float, default=120.0)
    return parser.parse_args()


def parse_dt(raw: str) -> datetime:
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def short_return_pct(entry_price: float, exit_price: float) -> float:
    return (entry_price - exit_price) / entry_price * 100.0


def short_pnl(entry_price: float, exit_price: float, qty: float) -> float:
    return (entry_price - exit_price) * qty


def hour_floor(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def month_keys(start: datetime, end: datetime) -> Iterable[Tuple[int, int]]:
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            year += 1
            month = 1


def parse_kline(row: List[object]) -> Candle:
    return Candle(
        open_time_ms=int(row[0]),
        open_price=float(row[1]),
        high_price=float(row[2]),
        low_price=float(row[3]),
        close_price=float(row[4]),
        close_time_ms=int(row[6]),
    )


def load_positions(conn: sqlite3.Connection, accounts: Optional[set[str]]) -> List[PositionSample]:
    conn.row_factory = sqlite3.Row
    params: List[object] = []
    account_clause = ""
    if accounts:
        account_clause = "AND r.account_id IN (%s)" % ",".join("?" for _ in accounts)
        params.extend(sorted(accounts))
    rows = conn.execute(
        f"""
        WITH exit_fills AS (
            SELECT
                position_id,
                SUM(CASE WHEN side = 'BUY' THEN executed_qty ELSE 0 END) AS buy_qty,
                SUM(CASE WHEN side = 'BUY' THEN executed_qty * avg_price ELSE 0 END) AS buy_notional
            FROM fills
            WHERE position_id IS NOT NULL
            GROUP BY position_id
        )
        SELECT
            p.id,
            r.account_id,
            p.symbol,
            p.qty,
            p.entry_price,
            p.opened_at_utc,
            p.closed_at_utc,
            p.close_reason,
            ef.buy_qty,
            ef.buy_notional
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        LEFT JOIN exit_fills ef ON ef.position_id = p.id
        WHERE p.side = 'SHORT'
          AND p.status != 'OPEN'
          AND p.closed_at_utc IS NOT NULL
          AND p.entry_price > 0
          {account_clause}
        ORDER BY r.account_id ASC, p.opened_at_utc ASC, p.id ASC
        """,
        params,
    ).fetchall()
    samples: List[PositionSample] = []
    for row in rows:
        exit_fill = None
        if (
            row["buy_qty"] is not None
            and row["buy_notional"] is not None
            and float(row["buy_qty"]) > 0
        ):
            exit_fill = float(row["buy_notional"]) / float(row["buy_qty"])
        samples.append(
            PositionSample(
                position_id=int(row["id"]),
                account_id=str(row["account_id"]),
                symbol=str(row["symbol"]).upper(),
                qty=float(row["qty"]),
                actual_entry_price=float(row["entry_price"]),
                opened_at_utc=parse_dt(str(row["opened_at_utc"])),
                closed_at_utc=parse_dt(str(row["closed_at_utc"])),
                close_reason=str(row["close_reason"] or ""),
                actual_exit_fill_price=exit_fill,
            )
        )
    return samples


def load_account_equity_pnl(conn: sqlite3.Connection, accounts: Optional[set[str]]) -> Dict[str, Dict[str, object]]:
    conn.row_factory = sqlite3.Row
    params: List[object] = []
    account_clause = ""
    if accounts:
        account_clause = "WHERE account_id IN (%s)" % ",".join("?" for _ in accounts)
        params.extend(sorted(accounts))
    rows = conn.execute(
        f"""
        WITH bounds AS (
            SELECT account_id, MIN(captured_at_utc) AS first_t, MAX(captured_at_utc) AS last_t
            FROM wallet_snapshots
            {account_clause}
            GROUP BY account_id
        )
        SELECT
            b.account_id,
            b.first_t,
            b.last_t,
            (
                SELECT balance_usdt
                FROM wallet_snapshots w
                WHERE w.account_id = b.account_id
                ORDER BY captured_at_utc ASC, id ASC
                LIMIT 1
            ) AS first_balance,
            (
                SELECT balance_usdt
                FROM wallet_snapshots w
                WHERE w.account_id = b.account_id
                ORDER BY captured_at_utc DESC, id DESC
                LIMIT 1
            ) AS last_balance,
            COALESCE(
                (
                    SELECT SUM(amount)
                    FROM (
                        SELECT DISTINCT c.unique_key, c.amount
                        FROM cashflow_events c
                        WHERE c.account_id = b.account_id
                          AND c.event_time_utc > b.first_t
                          AND c.event_time_utc <= b.last_t
                    )
                ),
                0
            ) AS cashflow_in_window
        FROM bounds b
        """,
        params,
    ).fetchall()
    out: Dict[str, Dict[str, object]] = {}
    for row in rows:
        first_balance = float(row["first_balance"] or 0.0)
        last_balance = float(row["last_balance"] or 0.0)
        cashflow = float(row["cashflow_in_window"] or 0.0)
        out[str(row["account_id"])] = {
            "first_snapshot_utc": row["first_t"],
            "last_snapshot_utc": row["last_t"],
            "first_balance": first_balance,
            "last_balance": last_balance,
            "cashflow_in_window": cashflow,
            "actual_account_equity_pnl_usdt": last_balance - first_balance - cashflow,
        }
    return out


def fetch_api_klines(symbol: str, start_ms: int, end_ms: int) -> List[List[object]]:
    rows: List[List[object]] = []
    cursor = start_ms
    while cursor <= end_ms:
        params = urllib.parse.urlencode(
            {
                "symbol": symbol,
                "interval": "1h",
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1500,
            }
        )
        payload = None
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                with urllib.request.urlopen(f"{BINANCE_FAPI_KLINES}?{params}", timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                time.sleep(0.4 * (attempt + 1))
        if payload is None:
            raise RuntimeError(f"fapi failed for {symbol}: {last_exc}")
        if not payload:
            break
        rows.extend(payload)
        next_cursor = int(payload[-1][0]) + 3600_000
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(payload) < 1500:
            break
    return rows


def download_month_zip(symbol: str, year: int, month: int, cache_dir: Path) -> Optional[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}-1h-{year:04d}-{month:02d}.zip"
    path = cache_dir / symbol / name
    if path.exists() and path.stat().st_size > 0:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"{BINANCE_VISION}/{symbol}/1h/{name}"
    tmp = path.with_suffix(path.suffix + ".tmp")
    last_exc: Optional[Exception] = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp, open(tmp, "wb") as handle:
                while True:
                    data = resp.read(256 * 1024)
                    if not data:
                        break
                    handle.write(data)
            tmp.replace(path)
            return path
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                tmp.unlink(missing_ok=True)
                return None
            last_exc = exc
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        tmp.unlink(missing_ok=True)
        time.sleep(0.4 * (attempt + 1))
    print(f"monthly unavailable {symbol} {year:04d}-{month:02d}: {last_exc}", flush=True)
    return None


def load_monthly_klines(symbol: str, start: datetime, end: datetime, cache_dir: Path) -> List[List[object]]:
    rows: List[List[object]] = []
    for year, month in month_keys(start, end):
        zip_path = download_month_zip(symbol, year, month, cache_dir)
        if zip_path is None:
            continue
        with zipfile.ZipFile(zip_path) as zf:
            names = [name for name in zf.namelist() if name.endswith(".csv")]
            if not names:
                continue
            text = zf.read(names[0]).decode("utf-8").splitlines()
        for line in text:
            if not line or line.startswith("open_time"):
                continue
            parts = line.split(",")
            if len(parts) >= 7:
                rows.append(parts)
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    return [row for row in rows if start_ms <= int(row[0]) <= end_ms]


def load_symbol_candles(symbol: str, start: datetime, end: datetime, cache_dir: Path) -> List[Candle]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    start_h = hour_floor(start)
    end_h = hour_floor(end + timedelta(hours=1))
    start_ms = int(start_h.timestamp() * 1000)
    end_ms = int(end_h.timestamp() * 1000)
    cache_path = cache_dir / "api_ranges" / f"{symbol}-{start_ms}-{end_ms}.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    raw_rows: List[List[object]]
    if cache_path.exists() and cache_path.stat().st_size > 0:
        raw_rows = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        try:
            raw_rows = fetch_api_klines(symbol, start_ms, end_ms)
            cache_path.write_text(json.dumps(raw_rows), encoding="utf-8")
        except Exception as exc:  # noqa: BLE001
            print(f"api fallback {symbol}: {exc}", flush=True)
            raw_rows = load_monthly_klines(symbol, start_h, end_h, cache_dir / "monthly_zip")
            if raw_rows:
                cache_path.write_text(json.dumps(raw_rows), encoding="utf-8")
    candles = [parse_kline(row) for row in raw_rows]
    candles.sort(key=lambda c: c.open_time_ms)
    return candles


def build_candle_cache(samples: List[PositionSample], cache_dir: Path, lookahead_hours: float) -> Dict[str, List[Candle]]:
    ranges: Dict[str, Tuple[datetime, datetime]] = {}
    for sample in samples:
        start = sample.opened_at_utc - timedelta(hours=1)
        end = max(sample.closed_at_utc, sample.opened_at_utc + timedelta(hours=lookahead_hours))
        current = ranges.get(sample.symbol)
        if current is None:
            ranges[sample.symbol] = (start, end)
        else:
            ranges[sample.symbol] = (min(current[0], start), max(current[1], end))
    out: Dict[str, List[Candle]] = {}
    total = len(ranges)
    for idx, (symbol, (start, end)) in enumerate(sorted(ranges.items()), start=1):
        out[symbol] = load_symbol_candles(symbol, start, end, cache_dir)
        if idx % 50 == 0 or idx == total:
            print(f"loaded symbols {idx}/{total}", flush=True)
    return out


def candle_close_at_or_after(candles: List[Candle], target: datetime) -> Optional[float]:
    target_ms = int(target.timestamp() * 1000)
    for candle in candles:
        if candle.close_time_ms >= target_ms:
            return candle.close_price
    return None


def entry_candle(candles: List[Candle], opened_at: datetime) -> Optional[Candle]:
    ts = int(opened_at.timestamp() * 1000)
    for candle in candles:
        if candle.open_time_ms <= ts <= candle.close_time_ms:
            return candle
    for candle in candles:
        if candle.close_time_ms >= ts:
            return candle
    return None


def find_delayed_entry(sample: PositionSample, candles: List[Candle]) -> Tuple[Optional[datetime], Optional[float]]:
    start = entry_candle(candles, sample.opened_at_utc)
    if start is None:
        return None, None
    started = False
    for candle in candles:
        if candle.open_time_ms == start.open_time_ms:
            started = True
        if not started:
            continue
        if candle.close_price < candle.open_price:
            return candle.close_dt, candle.close_price
    return None, None


def local_noon_between(start: datetime, end: datetime) -> Iterable[datetime]:
    local_day = start.astimezone(LOCAL_TZ).date()
    end_day = end.astimezone(LOCAL_TZ).date()
    while local_day <= end_day:
        noon_local = datetime(local_day.year, local_day.month, local_day.day, 12, 0, tzinfo=LOCAL_TZ)
        noon_utc = noon_local.astimezone(timezone.utc)
        if start < noon_utc <= end:
            yield noon_utc
        local_day += timedelta(days=1)


def high_between(candles: List[Candle], start: datetime, end: datetime) -> Optional[float]:
    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    highs = [
        candle.high_price
        for candle in candles
        if candle.close_time_ms >= start_ms and candle.close_time_ms <= end_ms
    ]
    return max(highs) if highs else None


def replay_delayed(
    sample: PositionSample,
    candles: List[Candle],
    tp_drop_pct: float,
    max_hold_hours: float,
) -> ReplayResult:
    entry_time, entry_price = find_delayed_entry(sample, candles)
    if entry_time is None or entry_price is None:
        return ReplayResult(False, None, None, None, None, "NO_BEARISH_ENTRY", None, None, None)

    max_hold_time = entry_time + timedelta(hours=max_hold_hours)
    tp_threshold = entry_price * (1.0 - tp_drop_pct / 100.0)
    tp_eligible = False
    stop_price: Optional[float] = None
    next_noons = list(local_noon_between(entry_time, max_hold_time + timedelta(hours=24)))
    noon_idx = 0

    for candle in candles:
        if candle.close_dt <= entry_time:
            continue

        while noon_idx < len(next_noons) and next_noons[noon_idx] <= candle.open_dt:
            noon_time = next_noons[noon_idx]
            local_day_start = noon_time.astimezone(LOCAL_TZ).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
            ref_start = max(local_day_start, entry_time)
            ref_high = high_between(candles, ref_start, noon_time)
            if ref_high is not None:
                stop_price = ref_high if stop_price is None else min(stop_price, ref_high)
            noon_idx += 1

        if stop_price is not None and candle.high_price >= stop_price:
            return ReplayResult(
                True,
                entry_time,
                entry_price,
                candle.close_dt,
                stop_price,
                "NOON_PROTECTION_STOP",
                short_pnl(entry_price, stop_price, sample.qty),
                short_return_pct(entry_price, stop_price),
                (entry_time - sample.opened_at_utc).total_seconds() / 3600.0,
            )

        if candle.low_price <= tp_threshold:
            tp_eligible = True
        if tp_eligible and candle.close_price > candle.open_price:
            return ReplayResult(
                True,
                entry_time,
                entry_price,
                candle.close_dt,
                candle.close_price,
                "HOURLY_TP_BULLISH_CLOSE",
                short_pnl(entry_price, candle.close_price, sample.qty),
                short_return_pct(entry_price, candle.close_price),
                (entry_time - sample.opened_at_utc).total_seconds() / 3600.0,
            )

        if candle.close_dt >= max_hold_time:
            return ReplayResult(
                True,
                entry_time,
                entry_price,
                candle.close_dt,
                candle.close_price,
                "MAX_HOLD_EXCEEDED",
                short_pnl(entry_price, candle.close_price, sample.qty),
                short_return_pct(entry_price, candle.close_price),
                (entry_time - sample.opened_at_utc).total_seconds() / 3600.0,
            )

    return ReplayResult(True, entry_time, entry_price, None, None, "NO_EXIT_IN_DATA", None, None, (entry_time - sample.opened_at_utc).total_seconds() / 3600.0)


def percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (idx - lo)


def summarize(rows: List[Dict[str, object]], account_equity_pnl: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    by_account: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        by_account[str(row["account_id"])].append(row)

    def summarize_group(group: List[Dict[str, object]]) -> Dict[str, object]:
        comparable = [r for r in group if r["replay_pnl_usdt"] != "" and r["actual_position_pnl_est_usdt"] != ""]
        deltas = [float(r["delta_position_est_pnl_usdt"]) for r in comparable]
        ret_deltas = [float(r["delta_return_pct"]) for r in comparable]
        account_ids = sorted({str(r["account_id"]) for r in group})
        actual_account_equity_pnl = sum(
            float(account_equity_pnl.get(account_id, {}).get("actual_account_equity_pnl_usdt", 0.0))
            for account_id in account_ids
        )
        replay_pnl = sum(float(r["replay_pnl_usdt"]) for r in comparable)
        return {
            "positions": len(group),
            "comparable": len(comparable),
            "entered": sum(1 for r in group if r["replay_entered"] == "1"),
            "no_entry": sum(1 for r in group if r["replay_entered"] == "0"),
            "actual_account_equity_pnl_usdt": actual_account_equity_pnl,
            "actual_position_est_pnl_usdt": sum(float(r["actual_position_pnl_est_usdt"]) for r in comparable),
            "replay_pnl_usdt": replay_pnl,
            "delta_vs_actual_account_equity_pnl_usdt": replay_pnl - actual_account_equity_pnl,
            "delta_vs_position_est_pnl_usdt": sum(deltas) if deltas else 0.0,
            "mean_delta_return_pct": mean(ret_deltas) if ret_deltas else None,
            "median_delta_return_pct": median(ret_deltas) if ret_deltas else None,
            "delta_return_p25": percentile(ret_deltas, 0.25),
            "delta_return_p75": percentile(ret_deltas, 0.75),
            "improved": sum(1 for value in deltas if value > 0),
            "worse": sum(1 for value in deltas if value < 0),
            "flat": sum(1 for value in deltas if value == 0),
        }

    return {
        "overall": summarize_group(rows),
        "by_account": {account: summarize_group(group) for account, group in sorted(by_account.items())},
        "account_equity_pnl_source": account_equity_pnl,
    }


def main() -> None:
    args = parse_args()
    accounts = {a.strip() for a in args.account if a.strip()} if args.account else None
    conn = sqlite3.connect(args.db)
    try:
        samples = load_positions(conn, accounts)
        effective_accounts = accounts or {sample.account_id for sample in samples}
        account_equity_pnl = load_account_equity_pnl(conn, effective_accounts)
    finally:
        conn.close()
    print(f"loaded positions={len(samples)}", flush=True)
    candle_cache = build_candle_cache(samples, Path(args.cache_dir), args.lookahead_hours)

    rows: List[Dict[str, object]] = []
    for sample in samples:
        candles = candle_cache.get(sample.symbol, [])
        actual_exit_price = sample.actual_exit_fill_price or candle_close_at_or_after(candles, sample.closed_at_utc)
        actual_position_pnl_est = ""
        actual_return = ""
        if actual_exit_price is not None:
            actual_position_pnl_est = short_pnl(sample.actual_entry_price, actual_exit_price, sample.qty)
            actual_return = short_return_pct(sample.actual_entry_price, actual_exit_price)

        replay = replay_delayed(sample, candles, args.tp_drop_pct, args.max_hold_hours) if candles else ReplayResult(False, None, None, None, None, "NO_KLINES", None, None, None)
        delta_pnl = ""
        delta_return = ""
        if actual_position_pnl_est != "" and replay.pnl_usdt is not None and actual_return != "" and replay.return_pct is not None:
            delta_pnl = replay.pnl_usdt - float(actual_position_pnl_est)
            delta_return = replay.return_pct - float(actual_return)

        rows.append(
            {
                "position_id": sample.position_id,
                "account_id": sample.account_id,
                "symbol": sample.symbol,
                "qty": sample.qty,
                "actual_opened_at_utc": sample.opened_at_utc.isoformat(),
                "actual_closed_at_utc": sample.closed_at_utc.isoformat(),
                "actual_close_reason": sample.close_reason,
                "actual_entry_price": sample.actual_entry_price,
                "actual_exit_price": actual_exit_price if actual_exit_price is not None else "",
                "actual_exit_price_basis": "fill" if sample.actual_exit_fill_price is not None else ("hour_close" if actual_exit_price is not None else ""),
                "actual_position_pnl_est_usdt": actual_position_pnl_est,
                "actual_return_pct": actual_return,
                "replay_entered": "1" if replay.entered else "0",
                "replay_entry_time_utc": replay.entry_time_utc.isoformat() if replay.entry_time_utc else "",
                "replay_entry_price": replay.entry_price if replay.entry_price is not None else "",
                "replay_exit_time_utc": replay.exit_time_utc.isoformat() if replay.exit_time_utc else "",
                "replay_exit_price": replay.exit_price if replay.exit_price is not None else "",
                "replay_exit_reason": replay.exit_reason,
                "replay_wait_hours": replay.wait_hours if replay.wait_hours is not None else "",
                "replay_pnl_usdt": replay.pnl_usdt if replay.pnl_usdt is not None else "",
                "replay_return_pct": replay.return_pct if replay.return_pct is not None else "",
                "delta_position_est_pnl_usdt": delta_pnl,
                "delta_return_pct": delta_return,
            }
        )

    output_csv = Path(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    summary = summarize(rows, account_equity_pnl)
    output_summary = Path(args.output_summary)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    output_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    print(f"wrote {output_csv}", flush=True)
    print(f"wrote {output_summary}", flush=True)


if __name__ == "__main__":
    main()
