import argparse
import csv
import json
import math
import sqlite3
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Dict, Iterable, List, Optional, Tuple


BINANCE_VISION = "https://data.binance.vision/data/futures/um/monthly/klines"
BINANCE_FAPI_KLINES = "https://fapi.binance.com/fapi/v1/klines"


@dataclass(frozen=True)
class PositionSample:
    position_id: int
    account_id: str
    symbol: str
    qty: float
    entry_price: float
    opened_at_utc: datetime
    closed_at_utc: datetime
    close_reason: str
    entry_fill_avg_price: Optional[float]
    exit_fill_avg_price: Optional[float]


@dataclass(frozen=True)
class Candle:
    open_time_ms: int
    close_time_ms: int
    open_price: float
    close_price: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backtest delaying morning short entries until the first closed bearish 1h candle."
    )
    parser.add_argument("--db", required=True, help="SQLite state.db path")
    parser.add_argument(
        "--cache-dir",
        default="remote_artifacts/market_data_cache/binance_um_1h",
        help="Kline cache directory",
    )
    parser.add_argument(
        "--output-csv",
        default="reports/wait_1h_bearish_entry_backtest.csv",
        help="Per-position output CSV",
    )
    parser.add_argument(
        "--output-summary",
        default="reports/wait_1h_bearish_entry_summary.json",
        help="Summary JSON output",
    )
    parser.add_argument(
        "--max-wait-hours",
        type=float,
        default=24.0,
        help="Skip if no bearish 1h close appears within this many hours after signal",
    )
    parser.add_argument(
        "--sample-mode",
        choices=("fills", "all_positions"),
        default="fills",
        help="fills requires entry/exit fills; all_positions uses position entry and candle-close exit approximation",
    )
    return parser.parse_args()


def parse_dt(raw: str) -> datetime:
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def month_keys(start: datetime, end: datetime) -> Iterable[Tuple[int, int]]:
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield year, month
        month += 1
        if month > 12:
            month = 1
            year += 1


def hour_floor_ms(value: datetime) -> int:
    floored = value.replace(minute=0, second=0, microsecond=0)
    return int(floored.timestamp() * 1000)


def short_return_pct(entry_price: float, exit_price: float) -> float:
    return (entry_price - exit_price) / entry_price * 100.0


def load_samples(conn: sqlite3.Connection, sample_mode: str) -> List[PositionSample]:
    conn.row_factory = sqlite3.Row
    if sample_mode == "fills":
        rows = conn.execute(
            """
            WITH fill_stats AS (
                SELECT
                    position_id,
                    SUM(CASE WHEN side = 'SELL' THEN executed_qty ELSE 0 END) AS sell_qty,
                    SUM(CASE WHEN side = 'SELL' THEN executed_qty * avg_price ELSE 0 END) AS sell_notional,
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
                fs.sell_qty,
                fs.sell_notional,
                fs.buy_qty,
                fs.buy_notional
            FROM positions p
            JOIN runs r ON r.run_id = p.run_id
            JOIN fill_stats fs ON fs.position_id = p.id
            WHERE p.side = 'SHORT'
              AND p.status != 'OPEN'
              AND p.closed_at_utc IS NOT NULL
              AND fs.sell_qty > 0
              AND fs.buy_qty > 0
            ORDER BY p.opened_at_utc ASC, p.id ASC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                p.id,
                r.account_id,
                p.symbol,
                p.qty,
                p.entry_price,
                p.opened_at_utc,
                p.closed_at_utc,
                p.close_reason,
                NULL AS sell_qty,
                NULL AS sell_notional,
                NULL AS buy_qty,
                NULL AS buy_notional
            FROM positions p
            JOIN runs r ON r.run_id = p.run_id
            WHERE p.side = 'SHORT'
              AND p.status != 'OPEN'
              AND p.closed_at_utc IS NOT NULL
              AND p.entry_price > 0
            ORDER BY p.opened_at_utc ASC, p.id ASC
            """
        ).fetchall()
    samples: List[PositionSample] = []
    for row in rows:
        sell_avg = (
            float(row["sell_notional"]) / float(row["sell_qty"])
            if row["sell_qty"] is not None and float(row["sell_qty"]) > 0
            else None
        )
        buy_avg = (
            float(row["buy_notional"]) / float(row["buy_qty"])
            if row["buy_qty"] is not None and float(row["buy_qty"]) > 0
            else None
        )
        samples.append(
            PositionSample(
                position_id=int(row["id"]),
                account_id=str(row["account_id"]),
                symbol=str(row["symbol"]).upper(),
                qty=float(row["qty"]),
                entry_price=float(row["entry_price"]),
                opened_at_utc=parse_dt(str(row["opened_at_utc"])),
                closed_at_utc=parse_dt(str(row["closed_at_utc"])),
                close_reason=str(row["close_reason"] or ""),
                entry_fill_avg_price=sell_avg,
                exit_fill_avg_price=buy_avg,
            )
        )
    return samples


def download_month_zip(symbol: str, year: int, month: int, cache_dir: Path) -> Optional[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    name = f"{symbol}-1h-{year:04d}-{month:02d}.zip"
    path = cache_dir / symbol / name
    if path.exists() and path.stat().st_size > 0:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    url = f"{BINANCE_VISION}/{symbol}/1h/{name}"
    tmp = path.with_suffix(path.suffix + ".tmp")
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=30) as resp, open(tmp, "wb") as handle:
                while True:
                    data = resp.read(1024 * 256)
                    if not data:
                        break
                    handle.write(data)
            tmp.replace(path)
            return path
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                tmp.unlink(missing_ok=True)
                return None
            last_exc: Exception = exc
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        tmp.unlink(missing_ok=True)
    print(f"monthly kline unavailable symbol={symbol} month={year:04d}-{month:02d}: {last_exc}", flush=True)
    return None


def load_candles_for_month(symbol: str, year: int, month: int, cache_dir: Path) -> List[Candle]:
    zip_path = download_month_zip(symbol, year, month, cache_dir)
    if zip_path is None:
        return []
    candles: List[Candle] = []
    with zipfile.ZipFile(zip_path) as zf:
        names = [name for name in zf.namelist() if name.endswith(".csv")]
        if not names:
            return []
        with zf.open(names[0]) as raw:
            text = raw.read().decode("utf-8").splitlines()
    for line in text:
        if not line or line.startswith("open_time"):
            continue
        parts = line.split(",")
        if len(parts) < 7:
            continue
        try:
            candles.append(
                Candle(
                    open_time_ms=int(parts[0]),
                    open_price=float(parts[1]),
                    close_price=float(parts[4]),
                    close_time_ms=int(parts[6]),
                )
            )
        except ValueError:
            continue
    return candles


def parse_api_kline(row: List[object]) -> Candle:
    return Candle(
        open_time_ms=int(row[0]),
        open_price=float(row[1]),
        close_price=float(row[4]),
        close_time_ms=int(row[6]),
    )


def fetch_api_klines(symbol: str, start_ms: int, end_ms: int) -> List[Candle]:
    import urllib.parse

    rows: List[Candle] = []
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
        for attempt in range(3):
            try:
                with urllib.request.urlopen(f"{BINANCE_FAPI_KLINES}?{params}", timeout=30) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                break
            except Exception:
                if attempt == 2:
                    raise
        if payload is None:
            break
        if not payload:
            break
        chunk = [parse_api_kline(item) for item in payload]
        rows.extend(chunk)
        next_cursor = chunk[-1].open_time_ms + 3600_000
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(chunk) < 1500:
            break
    return rows


def load_candles_for_symbol_range(
    symbol: str,
    start: datetime,
    end: datetime,
    cache_dir: Path,
) -> List[Candle]:
    api_dir = cache_dir / "api_ranges"
    api_dir.mkdir(parents=True, exist_ok=True)
    start_ms = hour_floor_ms(start)
    end_ms = int(end.timestamp() * 1000)
    cache_path = api_dir / f"{symbol}-{start_ms}-{end_ms}.json"
    if cache_path.exists() and cache_path.stat().st_size > 0:
        return [parse_api_kline(item) for item in json.loads(cache_path.read_text(encoding="utf-8"))]
    try:
        candles = fetch_api_klines(symbol, start_ms, end_ms)
        raw_rows = [
            [
                c.open_time_ms,
                str(c.open_price),
                "",
                "",
                str(c.close_price),
                "",
                c.close_time_ms,
            ]
            for c in candles
        ]
        cache_path.write_text(json.dumps(raw_rows), encoding="utf-8")
        return candles
    except Exception as exc:
        print(f"api kline fallback symbol={symbol}: {exc}", flush=True)
        rows: List[Candle] = []
        monthly_dir = cache_dir / "monthly_zip"
        for year, month in month_keys(start, end):
            rows.extend(load_candles_for_month(symbol, year, month, monthly_dir))
        rows.sort(key=lambda c: c.open_time_ms)
        return [c for c in rows if start_ms <= c.open_time_ms <= end_ms]


def build_candle_cache(samples: List[PositionSample], cache_dir: Path) -> Dict[Tuple[str, int, int], List[Candle]]:
    by_symbol: Dict[str, Tuple[datetime, datetime]] = {}
    for sample in samples:
        current = by_symbol.get(sample.symbol)
        if current is None:
            by_symbol[sample.symbol] = (sample.opened_at_utc, sample.closed_at_utc)
        else:
            by_symbol[sample.symbol] = (
                min(current[0], sample.opened_at_utc),
                max(current[1], sample.closed_at_utc),
            )
    loaded: Dict[Tuple[str, int, int], List[Candle]] = {}
    total = len(by_symbol)
    for idx, (symbol, (start, end)) in enumerate(sorted(by_symbol.items()), start=1):
        symbol_rows = load_candles_for_symbol_range(symbol, start, end, cache_dir)
        for candle in symbol_rows:
            dt = datetime.fromtimestamp(candle.open_time_ms / 1000, tz=timezone.utc)
            loaded.setdefault((symbol, dt.year, dt.month), []).append(candle)
        if idx % 50 == 0 or idx == total:
            print(f"loaded symbols {idx}/{total}", flush=True)
    return loaded


def candles_for_sample(
    sample: PositionSample,
    loaded: Dict[Tuple[str, int, int], List[Candle]],
) -> List[Candle]:
    rows: List[Candle] = []
    for year, month in month_keys(sample.opened_at_utc, sample.closed_at_utc):
        rows.extend(loaded.get((sample.symbol, year, month), []))
    rows.sort(key=lambda c: c.open_time_ms)
    return rows


def first_bearish_close_after(
    sample: PositionSample,
    candles: List[Candle],
    max_wait_hours: float,
) -> Optional[Candle]:
    signal_ms = int(sample.opened_at_utc.timestamp() * 1000)
    latest_ms = min(
        int(sample.closed_at_utc.timestamp() * 1000),
        signal_ms + int(max_wait_hours * 3600 * 1000),
    )
    for candle in candles:
        if candle.close_time_ms < signal_ms:
            continue
        if candle.close_time_ms > latest_ms:
            return None
        if candle.close_price < candle.open_price:
            return candle
    return None


def candle_close_at_or_after(candles: List[Candle], target: datetime) -> Optional[float]:
    target_ms = int(target.timestamp() * 1000)
    for candle in candles:
        if candle.close_time_ms >= target_ms:
            return candle.close_price
    return None


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


def summarize(rows: List[Dict[str, object]]) -> Dict[str, object]:
    eligible = [row for row in rows if row["delayed_entry_found"]]
    skipped = [row for row in rows if not row["delayed_entry_found"]]
    baseline = [float(row["baseline_return_pct"]) for row in eligible]
    delayed = [float(row["delayed_return_pct"]) for row in eligible]
    deltas = [float(row["delta_return_pct"]) for row in eligible]
    all_baseline = [float(row["baseline_return_pct"]) for row in rows]
    notional_baseline = sum(float(row["baseline_pnl_est_usdt"]) for row in eligible)
    notional_delayed = sum(float(row["delayed_pnl_est_usdt"]) for row in eligible)
    return {
        "total_closed_positions_with_fills": len(rows),
        "delayed_entry_found": len(eligible),
        "skipped_no_bearish_before_exit_or_timeout": len(skipped),
        "baseline_mean_return_pct_all": mean(all_baseline) if all_baseline else None,
        "baseline_mean_return_pct_comparable": mean(baseline) if baseline else None,
        "delayed_mean_return_pct_comparable": mean(delayed) if delayed else None,
        "mean_delta_return_pct": mean(deltas) if deltas else None,
        "median_delta_return_pct": median(deltas) if deltas else None,
        "delta_p25_return_pct": percentile(deltas, 0.25),
        "delta_p75_return_pct": percentile(deltas, 0.75),
        "improved_count": sum(1 for value in deltas if value > 0),
        "worse_count": sum(1 for value in deltas if value < 0),
        "flat_count": sum(1 for value in deltas if value == 0),
        "baseline_pnl_est_usdt_comparable": notional_baseline,
        "delayed_pnl_est_usdt_comparable": notional_delayed,
        "delta_pnl_est_usdt_comparable": notional_delayed - notional_baseline,
        "avg_wait_hours": mean(float(row["wait_hours"]) for row in eligible) if eligible else None,
    }


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    cache_dir = Path(args.cache_dir)
    output_csv = Path(args.output_csv)
    output_summary = Path(args.output_summary)

    conn = sqlite3.connect(db_path)
    try:
        samples = load_samples(conn, args.sample_mode)
    finally:
        conn.close()
    print(f"loaded samples={len(samples)}")

    loaded = build_candle_cache(samples, cache_dir)

    rows: List[Dict[str, object]] = []
    for sample in samples:
        candles = candles_for_sample(sample, loaded)
        delayed_candle = first_bearish_close_after(sample, candles, args.max_wait_hours)
        baseline_entry_price = sample.entry_fill_avg_price or sample.entry_price
        exit_price = sample.exit_fill_avg_price
        if exit_price is None:
            exit_price = candle_close_at_or_after(candles, sample.closed_at_utc)
        if exit_price is None:
            continue
        baseline_return = short_return_pct(baseline_entry_price, exit_price)
        baseline_pnl = (baseline_entry_price - exit_price) * sample.qty
        row: Dict[str, object] = {
            "position_id": sample.position_id,
            "account_id": sample.account_id,
            "symbol": sample.symbol,
            "opened_at_utc": sample.opened_at_utc.isoformat(),
            "closed_at_utc": sample.closed_at_utc.isoformat(),
            "close_reason": sample.close_reason,
            "qty": sample.qty,
            "baseline_entry_price": baseline_entry_price,
            "exit_price": exit_price,
            "baseline_return_pct": baseline_return,
            "baseline_pnl_est_usdt": baseline_pnl,
            "delayed_entry_found": False,
            "delayed_entry_time_utc": "",
            "delayed_entry_price": "",
            "wait_hours": "",
            "delayed_return_pct": "",
            "delayed_pnl_est_usdt": "",
            "delta_return_pct": "",
            "delta_pnl_est_usdt": "",
        }
        if delayed_candle is not None:
            delayed_time = datetime.fromtimestamp(delayed_candle.close_time_ms / 1000, tz=timezone.utc)
            delayed_return = short_return_pct(delayed_candle.close_price, exit_price)
            delayed_pnl = (delayed_candle.close_price - exit_price) * sample.qty
            row.update(
                {
                    "delayed_entry_found": True,
                    "delayed_entry_time_utc": delayed_time.isoformat(),
                    "delayed_entry_price": delayed_candle.close_price,
                    "wait_hours": (delayed_time - sample.opened_at_utc).total_seconds() / 3600.0,
                    "delayed_return_pct": delayed_return,
                    "delayed_pnl_est_usdt": delayed_pnl,
                    "delta_return_pct": delayed_return - baseline_return,
                    "delta_pnl_est_usdt": delayed_pnl - baseline_pnl,
                }
            )
        rows.append(row)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    summary = summarize(rows)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    output_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"wrote {output_csv}")
    print(f"wrote {output_summary}")


if __name__ == "__main__":
    main()
