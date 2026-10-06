#!/usr/bin/env python3
"""Replay acc04 entry variants with independent portfolio exits.

The historical comparison that preceded this script kept the observed exit
timestamps fixed and only changed the entry path.  That is useful as an entry
attribution, but it is not an equity-curve backtest.  This replay owns the
positions for each strategy separately and recomputes:

* first / second entry timing;
* per-symbol protection stops;
* hourly exchange take-profit;
* 11:55 daily floating-loss cut;
* 12:00 noon protection and 07:55 morning protection;
* the 08:00 local daily portfolio loss stop;
* carried positions and the next day's entries.

Market data is read from Binance Vision daily archives.  The database is
opened read-only and is used for the observed acc04 symbol set, actual fills,
notionals, and the starting wallet snapshot.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Optional
from zoneinfo import ZoneInfo


UTC = timezone.utc
SHANGHAI = ZoneInfo("Asia/Shanghai")
VISION_ROOT = "https://data.binance.vision/data/futures/um/daily/klines"
INTERVAL_15M = timedelta(minutes=15)
INTERVAL_1H = timedelta(hours=1)
FEE_RATE = 0.0005
PORTFOLIO_LOSS_PCT = 3.5
HOURLY_TP_DROP_PCT = 18.0
MAX_HOLD_HOURS = 47.5
SL_LIQ_BUFFER_PCT = 1.0
ENTRY_WAIT_HOURS = 16.0
ENTRY_PRECLOSE_SECONDS = 10.0
MARKET_LOOKBACK_HOURS = 48.0


def parse_dt(value: str | None) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")


def local_boundary(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 8, 0, tzinfo=SHANGHAI).astimezone(UTC)


def local_day(timestamp: datetime) -> date:
    """Return the 08:00-to-08:00 trading session date in Shanghai time."""
    local = timestamp.astimezone(SHANGHAI)
    if local.hour < 8:
        return local.date() - timedelta(days=1)
    return local.date()


def floor_hour(timestamp: datetime) -> datetime:
    value = timestamp.astimezone(UTC)
    return value.replace(minute=0, second=0, microsecond=0)


def ceil_hour(timestamp: datetime) -> datetime:
    base = floor_hour(timestamp)
    return base if base == timestamp else base + INTERVAL_1H


def iter_days(start: datetime, end: datetime) -> Iterable[date]:
    current = start.astimezone(UTC).date()
    last = end.astimezone(UTC).date()
    while current <= last:
        yield current
        current += timedelta(days=1)


@dataclass(frozen=True)
class Bar:
    open_time: datetime
    close_time: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    interval: str


@dataclass(frozen=True)
class Candidate:
    candidate_id: str
    run_id: str
    run_started_at: datetime
    session_date: date
    symbol: str
    target_notional: float
    actual_qty: float
    actual_entry_price: float
    actual_entry_time: datetime
    liq_ratio: Optional[float]
    actual_sl_price: Optional[float]
    actual_close_reason: str


@dataclass(frozen=True)
class EntryOrder:
    plan_id: str
    candidate_id: str
    run_id: str
    session_date: date
    symbol: str
    stage: str
    entry_time: datetime
    entry_price: float
    target_notional: float
    qty: float
    liq_ratio: Optional[float]
    actual_sl_price: Optional[float]
    structure_stop: Optional[float]
    structure_active_at: Optional[datetime]
    signal_time: Optional[datetime]
    signal_kind: str


@dataclass
class Lot:
    qty: float
    entry_price: float
    base_price: float
    opened_at: datetime
    notional: float


@dataclass
class SimPosition:
    symbol: str
    lots: list[Lot]
    first_opened_at: datetime
    stop_price: float
    liq_ratio: Optional[float]
    structure_stop: Optional[float] = None
    structure_active_at: Optional[datetime] = None
    lowest_price_since_open: float = math.inf
    noon_cap: Optional[float] = None
    morning_cap: Optional[float] = None

    @property
    def qty(self) -> float:
        return sum(lot.qty for lot in self.lots)

    @property
    def notional(self) -> float:
        return sum(lot.notional for lot in self.lots)

    @property
    def avg_entry(self) -> float:
        qty = self.qty
        return sum(lot.qty * lot.entry_price for lot in self.lots) / qty if qty > 0 else 0.0

    @property
    def max_hold_at(self) -> datetime:
        return self.first_opened_at + timedelta(hours=MAX_HOLD_HOURS)


class MarketData:
    """15-minute bars with an hourly fallback for unavailable symbols."""

    def __init__(self, cache_dir: Path, start: datetime, end: datetime, symbols: set[str], workers: int = 12):
        self.cache_dir = cache_dir
        self.start = start.astimezone(UTC)
        self.end = end.astimezone(UTC)
        self.symbols = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        self.workers = max(1, workers)
        self.bars15: dict[str, dict[datetime, Bar]] = defaultdict(dict)
        self.bars1h: dict[str, dict[datetime, Bar]] = defaultdict(dict)
        self.requested_files = 0
        self.downloaded_files = 0
        self.failed_files: list[str] = []
        self.missing_symbols: set[str] = set()

    @staticmethod
    def _safe_symbol_path(symbol: str) -> str:
        return urllib.parse.quote(symbol, safe="")

    def _cache_path(self, symbol: str, interval: str, day: date) -> Path:
        return self.cache_dir / interval / symbol / f"{symbol}-{interval}-{day.isoformat()}.zip"

    def _url(self, symbol: str, interval: str, day: date) -> str:
        encoded = self._safe_symbol_path(symbol)
        filename = f"{encoded}-{interval}-{day.isoformat()}.zip"
        return f"{VISION_ROOT}/{encoded}/{interval}/{filename}"

    def _download(self, symbol: str, interval: str, day: date) -> tuple[str, str, date, Optional[bytes], Optional[str]]:
        path = self._cache_path(symbol, interval, day)
        if path.exists() and path.stat().st_size > 0:
            try:
                return symbol, interval, day, path.read_bytes(), None
            except OSError as exc:
                return symbol, interval, day, None, str(exc)

        path.parent.mkdir(parents=True, exist_ok=True)
        url = self._url(symbol, interval, day)
        last_error: Optional[str] = None
        for attempt in range(3):
            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "bubble-buster-independent-replay/1.0"},
                )
                with urllib.request.urlopen(request, timeout=45) as response:
                    payload = response.read()
                if payload:
                    temporary = path.with_suffix(".tmp")
                    temporary.write_bytes(payload)
                    temporary.replace(path)
                    return symbol, interval, day, payload, None
                last_error = "empty response"
            except urllib.error.HTTPError as exc:
                if exc.code == 404:
                    return symbol, interval, day, None, "404"
                last_error = f"HTTP {exc.code}"
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
            time.sleep(0.4 * (attempt + 1))
        return symbol, interval, day, None, last_error or "download failed"

    @staticmethod
    def _parse_zip(payload: bytes, interval: str) -> list[Bar]:
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not csv_names:
                return []
            raw = archive.read(csv_names[0]).decode("utf-8")
        bars: list[Bar] = []
        for line in raw.splitlines():
            if not line or line.lower().startswith("open_time"):
                continue
            parts = line.split(",")
            if len(parts) < 7:
                continue
            try:
                open_ms = int(float(parts[0]))
                open_price = float(parts[1])
                high_price = float(parts[2])
                low_price = float(parts[3])
                close_price = float(parts[4])
                close_ms = int(float(parts[6]))
            except (TypeError, ValueError):
                continue
            if min(open_price, high_price, low_price, close_price) <= 0:
                continue
            bars.append(
                Bar(
                    open_time=datetime.fromtimestamp(open_ms / 1000, tz=UTC),
                    close_time=datetime.fromtimestamp((close_ms + 1) / 1000, tz=UTC),
                    open_price=open_price,
                    high_price=high_price,
                    low_price=low_price,
                    close_price=close_price,
                    interval=interval,
                )
            )
        return bars

    def load(self) -> None:
        # Seed positions can have been opened during the previous run and
        # still be active at the replay start.  Keep enough history to derive
        # their first-signal structure stop and favourable excursion.
        days = list(iter_days(self.start - timedelta(hours=MARKET_LOOKBACK_HOURS), self.end))
        jobs = [(symbol, "15m", day) for symbol in sorted(self.symbols) for day in days]
        self.requested_files = len(jobs)
        results: list[tuple[str, str, date, Optional[bytes], Optional[str]]] = []
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = [executor.submit(self._download, *job) for job in jobs]
            for future in as_completed(futures):
                results.append(future.result())

        missing_15m = {(symbol, day) for symbol, _interval, day, payload, error in results if payload is None and error == "404"}
        fallback_jobs = [(symbol, "1h", day) for symbol, day in sorted(missing_15m)]
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            fallback_futures = [executor.submit(self._download, *job) for job in fallback_jobs]
            for future in as_completed(fallback_futures):
                results.append(future.result())
        self.downloaded_files = sum(payload is not None for _symbol, _interval, _day, payload, _error in results)

        for symbol, interval, day, payload, error in results:
            if payload is None:
                if error != "404":
                    self.failed_files.append(f"{symbol}:{interval}:{day.isoformat()}:{error}")
                continue
            try:
                bars = self._parse_zip(payload, interval)
            except (OSError, ValueError, zipfile.BadZipFile) as exc:
                self.failed_files.append(f"{symbol}:{interval}:{day.isoformat()}:{exc}")
                continue
            target = self.bars15 if interval == "15m" else self.bars1h
            for bar in bars:
                if self.start - timedelta(hours=MARKET_LOOKBACK_HOURS + 2) <= bar.open_time <= self.end:
                    target[symbol][bar.open_time] = bar
        for symbol in self.symbols:
            if not self.bars15.get(symbol) and not self.bars1h.get(symbol):
                self.missing_symbols.add(symbol)

        for symbol, bars in self.bars15.items():
            for hour in {floor_hour(open_time) for open_time in bars}:
                pieces = [bars.get(hour + INTERVAL_15M * index) for index in range(4)]
                if not all(pieces):
                    continue
                first = pieces[0]
                last = pieces[-1]
                self.bars1h[symbol][hour] = Bar(
                    open_time=hour,
                    close_time=hour + INTERVAL_1H,
                    open_price=first.open_price,
                    high_price=max(piece.high_price for piece in pieces),
                    low_price=min(piece.low_price for piece in pieces),
                    close_price=last.close_price,
                    interval="1h",
                )

    def hourly(self, symbol: str) -> list[Bar]:
        return sorted(self.bars1h.get(symbol, {}).values(), key=lambda bar: bar.open_time)

    def hour(self, symbol: str, hour: datetime) -> Optional[Bar]:
        return self.bars1h.get(symbol, {}).get(hour.astimezone(UTC).replace(minute=0, second=0, microsecond=0))

    def interval_bar(self, symbol: str, interval_start: datetime) -> Optional[Bar]:
        start = interval_start.astimezone(UTC).replace(second=0, microsecond=0)
        exact = self.bars15.get(symbol, {}).get(start)
        if exact is not None:
            return exact
        if start.minute == 0:
            return self.bars1h.get(symbol, {}).get(start)
        return None

    def initial_mark(self, symbol: str, timestamp: datetime) -> Optional[float]:
        timestamp = timestamp.astimezone(UTC)
        bar = self.interval_bar(symbol, timestamp)
        if bar is not None:
            return bar.open_price
        candidates = [
            bar
            for bar in self.bars15.get(symbol, {}).values()
            if bar.open_time <= timestamp
        ]
        candidates.sort(key=lambda bar: bar.open_time)
        if candidates:
            return candidates[-1].close_price
        hourly = [bar for bar in self.hourly(symbol) if bar.open_time <= timestamp]
        return hourly[-1].close_price if hourly else None

    def range_extreme(self, symbol: str, start: datetime, end: datetime, kind: str) -> Optional[float]:
        start = start.astimezone(UTC)
        end = end.astimezone(UTC)
        if end <= start:
            return None
        values: list[float] = []
        for bar in self.bars15.get(symbol, {}).values():
            if bar.open_time < end and bar.close_time > start:
                values.append(bar.low_price if kind == "low" else bar.high_price)
        if not values:
            for bar in self.bars1h.get(symbol, {}).values():
                if bar.open_time < end and bar.close_time > start:
                    values.append(bar.low_price if kind == "low" else bar.high_price)
        if not values:
            return None
        return min(values) if kind == "low" else max(values)

    def coverage(self) -> dict[str, object]:
        return {
            "symbols_requested": len(self.symbols),
            "symbols_with_15m": sum(bool(self.bars15.get(symbol)) for symbol in self.symbols),
            "symbols_with_1h": sum(bool(self.bars1h.get(symbol)) for symbol in self.symbols),
            "symbols_missing": sorted(self.missing_symbols),
            "failed_files": self.failed_files[:200],
            "requested_files": self.requested_files,
            "downloaded_files": self.downloaded_files,
            "market_cache_dir": str(self.cache_dir),
        }


def open_db(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.resolve()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def load_start_equity(conn: sqlite3.Connection, account: str, timestamp: datetime) -> tuple[float, datetime]:
    row = conn.execute(
        """
        SELECT balance_usdt, captured_at_utc
        FROM wallet_snapshots
        WHERE account_id = ? AND captured_at_utc >= ?
        ORDER BY captured_at_utc ASC, id ASC
        LIMIT 1
        """,
        (account, iso(timestamp)),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"no wallet snapshot after {iso(timestamp)} for {account}")
    return float(row["balance_usdt"]), parse_dt(str(row["captured_at_utc"]))


def load_candidates(
    conn: sqlite3.Connection,
    account: str,
    start: datetime,
    end: datetime,
) -> tuple[list[Candidate], list[Candidate], set[str]]:
    rows = conn.execute(
        """
        SELECT
            p.id,
            p.run_id,
            p.symbol,
            p.qty,
            p.entry_price,
            p.liq_price_open,
            p.sl_price,
            p.opened_at_utc,
            p.closed_at_utc,
            p.close_reason,
            r.started_at_utc
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        WHERE r.account_id = ?
          AND p.side = 'SHORT'
          AND p.qty > 0
          AND p.entry_price > 0
          AND r.started_at_utc < ?
          AND p.opened_at_utc < ?
          AND (p.closed_at_utc IS NULL OR p.closed_at_utc > ?)
        ORDER BY p.opened_at_utc ASC, p.id ASC
        """,
        (account, iso(end), iso(end), iso(start)),
    ).fetchall()
    seed: list[Candidate] = []
    for row in rows:
        entry = float(row["entry_price"])
        liq = float(row["liq_price_open"]) if row["liq_price_open"] else None
        liq_ratio = liq / entry if liq and entry > 0 else None
        candidate = Candidate(
            candidate_id=f"p{int(row['id'])}",
            run_id=str(row["run_id"]),
            run_started_at=parse_dt(str(row["started_at_utc"])),
            session_date=local_day(parse_dt(str(row["started_at_utc"]))),
            symbol=str(row["symbol"]).strip().upper(),
            target_notional=float(row["qty"]) * entry,
            actual_qty=float(row["qty"]),
            actual_entry_price=entry,
            actual_entry_time=parse_dt(str(row["opened_at_utc"])),
            liq_ratio=liq_ratio,
            actual_sl_price=float(row["sl_price"]) if row["sl_price"] else None,
            actual_close_reason=str(row["close_reason"] or ""),
        )
        if candidate.actual_entry_time < start:
            seed.append(candidate)

    all_rows = conn.execute(
        """
        SELECT
            p.id,
            p.run_id,
            p.symbol,
            p.qty,
            p.entry_price,
            p.liq_price_open,
            p.sl_price,
            p.opened_at_utc,
            p.closed_at_utc,
            p.close_reason,
            r.started_at_utc
        FROM positions p
        JOIN runs r ON r.run_id = p.run_id
        WHERE r.account_id = ?
          AND p.side = 'SHORT'
          AND p.qty > 0
          AND p.entry_price > 0
          AND r.started_at_utc >= ?
          AND r.started_at_utc < ?
          AND p.opened_at_utc < ?
        ORDER BY p.opened_at_utc ASC, p.id ASC
        """,
        (account, iso(start - timedelta(days=2)), iso(end), iso(end)),
    ).fetchall()
    candidates: list[Candidate] = []
    for row in all_rows:
        entry = float(row["entry_price"])
        liq = float(row["liq_price_open"]) if row["liq_price_open"] else None
        liq_ratio = liq / entry if liq and entry > 0 else None
        candidate = Candidate(
            candidate_id=f"p{int(row['id'])}",
            run_id=str(row["run_id"]),
            run_started_at=parse_dt(str(row["started_at_utc"])),
            session_date=local_day(parse_dt(str(row["started_at_utc"]))),
            symbol=str(row["symbol"]).strip().upper(),
            target_notional=float(row["qty"]) * entry,
            actual_qty=float(row["qty"]),
            actual_entry_price=entry,
            actual_entry_time=parse_dt(str(row["opened_at_utc"])),
            liq_ratio=liq_ratio,
            actual_sl_price=float(row["sl_price"]) if row["sl_price"] else None,
            actual_close_reason=str(row["close_reason"] or ""),
        )
        if start <= candidate.actual_entry_time < end:
            candidates.append(candidate)
    symbols = {candidate.symbol for candidate in seed + candidates}
    return candidates, seed, symbols


def first_signal(candles: list[Bar], base: datetime, deadline: datetime, kind: str) -> tuple[Optional[datetime], Optional[float], Optional[Bar]]:
    hour_start = floor_hour(base)
    eligible = [
        candle
        for candle in candles
        if candle.open_time >= hour_start
        and candle.close_time <= deadline + timedelta(seconds=1)
    ]
    if kind == "first_bearish":
        for candle in eligible:
            if candle.close_price < candle.open_price:
                entry_time = candle.close_time - timedelta(seconds=ENTRY_PRECLOSE_SECONDS)
                return entry_time, candle.close_price, candle
        return None, None, None
    raise ValueError(f"unknown signal kind: {kind}")


def build_orders(
    candidates: list[Candidate],
    market: MarketData,
    start: datetime,
    end: datetime,
) -> dict[str, list[EntryOrder]]:
    orders: dict[str, list[EntryOrder]] = {
        "actual": [],
        "second": [],
        "bullbear": [],
        "bullbear3": [],
    }

    def previous_highs(symbol: str, signal_candle: Optional[Bar]) -> list[float]:
        if signal_candle is None:
            return []
        highs: list[float] = []
        for offset in (2, 1):
            candle = market.hour(symbol, signal_candle.close_time - timedelta(hours=offset))
            if candle is not None:
                highs.append(candle.high_price)
        return highs

    def protection_for_entry(
        candidate: Candidate,
        signal_candle: Optional[Bar],
        entry_time: datetime,
        entry_price: float,
        preclose: bool = False,
    ) -> tuple[Optional[float], Optional[datetime]]:
        highs = previous_highs(candidate.symbol, signal_candle)
        if preclose and highs:
            return max(highs), signal_candle.close_time if signal_candle is not None else None
        if (
            signal_candle is not None
            and entry_time.astimezone(SHANGHAI).hour >= 12
        ):
            post_close_high = market.range_extreme(
                candidate.symbol,
                signal_candle.close_time,
                entry_time,
                "high",
            )
            values = highs + [entry_price]
            if post_close_high is not None:
                values.append(post_close_high)
            return max(values), entry_time
        return None, None

    for candidate in candidates:
        base = floor_hour(candidate.run_started_at)
        deadline = base + timedelta(hours=ENTRY_WAIT_HOURS)
        candles = market.hourly(candidate.symbol)
        first_time, first_price, first_candle = first_signal(candles, base, deadline, "first_bearish")

        if start <= candidate.actual_entry_time < end:
            actual_preclose = (
                first_candle is not None
                and candidate.actual_entry_time < first_candle.close_time
                and abs(
                    (
                        candidate.actual_entry_time
                        - (first_candle.close_time - timedelta(seconds=ENTRY_PRECLOSE_SECONDS))
                    ).total_seconds()
                )
                <= 15.0
            )
            actual_structure_stop, actual_structure_active_at = protection_for_entry(
                candidate,
                first_candle,
                candidate.actual_entry_time,
                candidate.actual_entry_price,
                preclose=actual_preclose,
            )
            orders["actual"].append(
                EntryOrder(
                    plan_id=candidate.candidate_id,
                    candidate_id=candidate.candidate_id,
                    run_id=candidate.run_id,
                    session_date=candidate.session_date,
                    symbol=candidate.symbol,
                    stage="FULL",
                    entry_time=candidate.actual_entry_time,
                    entry_price=candidate.actual_entry_price,
                    target_notional=candidate.target_notional,
                    qty=candidate.actual_qty,
                    liq_ratio=candidate.liq_ratio,
                    actual_sl_price=candidate.actual_sl_price,
                    structure_stop=actual_structure_stop,
                    structure_active_at=actual_structure_active_at,
                    signal_time=first_time,
                    signal_kind="observed_actual_fill",
                )
            )

        if first_time is None or first_price is None or first_candle is None:
            continue
        if first_time < start or first_time >= end:
            continue

        second_bearish: Optional[Bar] = None
        bullbear_second: Optional[Bar] = None
        for candle in candles:
            if candle.open_time <= first_candle.open_time or candle.close_time > deadline + timedelta(seconds=1):
                continue
            if second_bearish is None and candle.close_price < candle.open_price:
                second_bearish = candle
                break
        orders["second"].append(
            EntryOrder(
                plan_id=candidate.candidate_id,
                candidate_id=candidate.candidate_id,
                run_id=candidate.run_id,
                session_date=candidate.session_date,
                symbol=candidate.symbol,
                stage="FIRST",
                entry_time=first_time,
                entry_price=first_price,
                target_notional=candidate.target_notional * 0.50,
                qty=candidate.target_notional * 0.50 / first_price,
                liq_ratio=candidate.liq_ratio,
                actual_sl_price=candidate.actual_sl_price,
                structure_stop=protection_for_entry(
                    candidate,
                    first_candle,
                    first_time,
                    first_price,
                    preclose=True,
                )[0],
                structure_active_at=protection_for_entry(
                    candidate,
                    first_candle,
                    first_time,
                    first_price,
                    preclose=True,
                )[1],
                signal_time=first_candle.close_time,
                signal_kind="first_bearish",
            )
        )
        if second_bearish is not None:
            orders["second"].append(
                EntryOrder(
                    plan_id=candidate.candidate_id,
                    candidate_id=candidate.candidate_id,
                    run_id=candidate.run_id,
                    session_date=candidate.session_date,
                    symbol=candidate.symbol,
                    stage="SECOND",
                    entry_time=second_bearish.close_time,
                    entry_price=second_bearish.close_price,
                    target_notional=candidate.target_notional * 0.50,
                    qty=candidate.target_notional * 0.50 / second_bearish.close_price,
                    liq_ratio=candidate.liq_ratio,
                    actual_sl_price=candidate.actual_sl_price,
                    structure_stop=None,
                    structure_active_at=None,
                    signal_time=second_bearish.close_time,
                    signal_kind="second_bearish",
                )
            )

        phase = "WAIT_BULLISH"
        for candle in candles:
            if candle.open_time <= first_candle.open_time or candle.close_time > deadline + timedelta(seconds=1):
                continue
            if phase == "WAIT_BULLISH":
                if candle.close_price > candle.open_price:
                    phase = "WAIT_BEARISH"
                continue
            if candle.close_price < candle.open_price:
                bullbear_second = candle
                break
        orders["bullbear"].append(
            EntryOrder(
                plan_id=candidate.candidate_id,
                candidate_id=candidate.candidate_id,
                run_id=candidate.run_id,
                session_date=candidate.session_date,
                symbol=candidate.symbol,
                stage="FIRST",
                entry_time=first_time,
                entry_price=first_price,
                target_notional=candidate.target_notional * 0.50,
                qty=candidate.target_notional * 0.50 / first_price,
                liq_ratio=candidate.liq_ratio,
                actual_sl_price=candidate.actual_sl_price,
                structure_stop=protection_for_entry(
                    candidate,
                    first_candle,
                    first_time,
                    first_price,
                    preclose=True,
                )[0],
                structure_active_at=protection_for_entry(
                    candidate,
                    first_candle,
                    first_time,
                    first_price,
                    preclose=True,
                )[1],
                signal_time=first_candle.close_time,
                signal_kind="first_bearish",
            )
        )
        if bullbear_second is not None:
            orders["bullbear"].append(
                EntryOrder(
                    plan_id=candidate.candidate_id,
                    candidate_id=candidate.candidate_id,
                    run_id=candidate.run_id,
                    session_date=candidate.session_date,
                    symbol=candidate.symbol,
                    stage="SECOND",
                    entry_time=bullbear_second.close_time,
                    entry_price=bullbear_second.close_price,
                    target_notional=candidate.target_notional * 0.50,
                    qty=candidate.target_notional * 0.50 / bullbear_second.close_price,
                    liq_ratio=candidate.liq_ratio,
                    actual_sl_price=candidate.actual_sl_price,
                    structure_stop=None,
                    structure_active_at=None,
                    signal_time=bullbear_second.close_time,
                    signal_kind="bullish_then_bearish",
                )
            )

        # Three-tranche extension: first bearish, then bullish -> bearish,
        # then require another bullish -> bearish cycle for the final third.
        bullbear3_second: Optional[Bar] = None
        bullbear3_third: Optional[Bar] = None
        phase = "WAIT_BULLISH"
        for candle in candles:
            if candle.open_time <= first_candle.open_time or candle.close_time > deadline + timedelta(seconds=1):
                continue
            if phase == "WAIT_BULLISH":
                if candle.close_price > candle.open_price:
                    phase = "WAIT_BEARISH"
            elif candle.close_price < candle.open_price:
                if bullbear3_second is None:
                    bullbear3_second = candle
                    phase = "WAIT_BULLISH"
                else:
                    bullbear3_third = candle
                    break
        first_third = 1.0 / 3.0
        orders["bullbear3"].append(
            EntryOrder(
                plan_id=candidate.candidate_id,
                candidate_id=candidate.candidate_id,
                run_id=candidate.run_id,
                session_date=candidate.session_date,
                symbol=candidate.symbol,
                stage="FIRST",
                entry_time=first_time,
                entry_price=first_price,
                target_notional=candidate.target_notional * first_third,
                qty=candidate.target_notional * first_third / first_price,
                liq_ratio=candidate.liq_ratio,
                actual_sl_price=candidate.actual_sl_price,
                structure_stop=protection_for_entry(
                    candidate,
                    first_candle,
                    first_time,
                    first_price,
                    preclose=True,
                )[0],
                structure_active_at=protection_for_entry(
                    candidate,
                    first_candle,
                    first_time,
                    first_price,
                    preclose=True,
                )[1],
                signal_time=first_candle.close_time,
                signal_kind="first_bearish",
            )
        )
        if bullbear3_second is not None:
            orders["bullbear3"].append(
                EntryOrder(
                    plan_id=candidate.candidate_id,
                    candidate_id=candidate.candidate_id,
                    run_id=candidate.run_id,
                    session_date=candidate.session_date,
                    symbol=candidate.symbol,
                    stage="SECOND",
                    entry_time=bullbear3_second.close_time,
                    entry_price=bullbear3_second.close_price,
                    target_notional=candidate.target_notional * first_third,
                    qty=candidate.target_notional * first_third / bullbear3_second.close_price,
                    liq_ratio=candidate.liq_ratio,
                    actual_sl_price=candidate.actual_sl_price,
                    structure_stop=None,
                    structure_active_at=None,
                    signal_time=bullbear3_second.close_time,
                    signal_kind="bullish_then_bearish_second",
                )
            )
        if bullbear3_third is not None:
            orders["bullbear3"].append(
                EntryOrder(
                    plan_id=candidate.candidate_id,
                    candidate_id=candidate.candidate_id,
                    run_id=candidate.run_id,
                    session_date=candidate.session_date,
                    symbol=candidate.symbol,
                    stage="THIRD",
                    entry_time=bullbear3_third.close_time,
                    entry_price=bullbear3_third.close_price,
                    target_notional=candidate.target_notional * first_third,
                    qty=candidate.target_notional * first_third / bullbear3_third.close_price,
                    liq_ratio=candidate.liq_ratio,
                    actual_sl_price=candidate.actual_sl_price,
                    structure_stop=None,
                    structure_active_at=None,
                    signal_time=bullbear3_third.close_time,
                    signal_kind="bullish_then_bearish_third",
                )
            )
    for key in orders:
        orders[key].sort(key=lambda item: (item.entry_time, item.symbol, item.stage, item.candidate_id))
    return orders


def structure_stop(market: MarketData, symbol: str, entry_time: datetime, entry_price: float) -> Optional[float]:
    if entry_time.astimezone(SHANGHAI).hour < 12:
        return None
    signal_hour = floor_hour(entry_time)
    highs: list[float] = []
    for offset in (2, 1):
        candle = market.hour(symbol, signal_hour - timedelta(hours=offset))
        if candle is not None:
            highs.append(candle.high_price)
    highs.append(entry_price)
    return max(highs) if highs else None


def signal_independent_orders(orders: list[EntryOrder], market: MarketData) -> list[EntryOrder]:
    """Give every later signal the protection of a fresh independent entry."""
    independent: list[EntryOrder] = []
    for order in orders:
        if order.stage not in {"SECOND", "THIRD"}:
            independent.append(order)
            continue
        stop = structure_stop(market, order.symbol, order.entry_time, order.entry_price)
        independent.append(
            replace(
                order,
                structure_stop=stop,
                structure_active_at=order.entry_time if stop is not None else None,
            )
        )
    return independent


class StrategyReplay:
    def __init__(
        self,
        name: str,
        label: str,
        start: datetime,
        end: datetime,
        start_equity: float,
        market: MarketData,
        orders: list[EntryOrder],
        seed: list[Candidate],
        allow_signal_independent_scale_ins: bool = False,
    ):
        self.name = name
        self.label = label
        self.start = start
        self.end = end
        self.start_equity = start_equity
        self.market = market
        self.orders = orders
        self.allow_signal_independent_scale_ins = allow_signal_independent_scale_ins
        self.order_index = 0
        self.active: dict[str, SimPosition] = {}
        self.marks: dict[str, float] = {}
        self.cash_equity = start_equity
        self.events: list[dict[str, object]] = []
        self.portfolio_events: list[dict[str, object]] = []
        self.daily: dict[date, dict[str, object]] = {}
        self.plan_first_entered: dict[str, bool] = {}
        self.portfolio_loss_latched = False
        self.day_baseline = start_equity
        self.initial_seed_count = 0
        self.missing_mark_symbols: set[str] = set()
        self._initialize_seed(seed)

    def _initialize_seed(self, seed: list[Candidate]) -> None:
        for candidate in seed:
            mark = self.market.initial_mark(candidate.symbol, self.start)
            if mark is None:
                mark = candidate.actual_entry_price
                self.missing_mark_symbols.add(candidate.symbol)
            self.marks[candidate.symbol] = mark
            initial_stop = self._initial_stop(candidate.actual_entry_price, candidate.liq_ratio, candidate.actual_sl_price)
            seed_structure_stop: Optional[float] = None
            seed_structure_active_at: Optional[datetime] = None
            seed_deadline = floor_hour(candidate.run_started_at) + timedelta(hours=ENTRY_WAIT_HOURS)
            _seed_signal_time, _seed_signal_price, seed_candle = first_signal(
                self.market.hourly(candidate.symbol),
                floor_hour(candidate.run_started_at),
                seed_deadline,
                "first_bearish",
            )
            if (
                seed_candle is not None
                and candidate.actual_entry_time < seed_candle.close_time
                and abs(
                    (
                        candidate.actual_entry_time
                        - (seed_candle.close_time - timedelta(seconds=ENTRY_PRECLOSE_SECONDS))
                    ).total_seconds()
                )
                <= 15.0
            ):
                previous_highs = [
                    candle.high_price
                    for offset in (2, 1)
                    for candle in [
                        self.market.hour(candidate.symbol, seed_candle.close_time - timedelta(hours=offset))
                    ]
                    if candle is not None
                ]
                if previous_highs:
                    seed_structure_stop = max(previous_highs)
                    seed_structure_active_at = seed_candle.close_time
                    if seed_structure_active_at <= self.start:
                        initial_stop = min(initial_stop, seed_structure_stop)
            position = SimPosition(
                symbol=candidate.symbol,
                lots=[
                    Lot(
                        qty=candidate.actual_qty,
                        entry_price=candidate.actual_entry_price,
                        base_price=mark,
                        opened_at=candidate.actual_entry_time,
                        notional=candidate.target_notional,
                    )
                ],
                first_opened_at=candidate.actual_entry_time,
                stop_price=initial_stop,
                liq_ratio=candidate.liq_ratio,
                structure_stop=seed_structure_stop,
                structure_active_at=seed_structure_active_at,
            )
            seed_low = self.market.range_extreme(
                candidate.symbol,
                candidate.actual_entry_time,
                self.start,
                "low",
            )
            position.lowest_price_since_open = min(mark, seed_low) if seed_low is not None else mark
            self.active[candidate.symbol] = position
            self.initial_seed_count += 1

    @staticmethod
    def _initial_stop(entry_price: float, liq_ratio: Optional[float], actual_sl: Optional[float]) -> float:
        if liq_ratio and liq_ratio > 1:
            return entry_price * liq_ratio * (1.0 - SL_LIQ_BUFFER_PCT / 100.0)
        if actual_sl and actual_sl > entry_price:
            return actual_sl
        return entry_price * 1.50

    def _current_equity(self) -> float:
        equity = self.cash_equity
        for symbol, position in self.active.items():
            mark = self.marks.get(symbol, position.avg_entry)
            equity += sum((lot.base_price - mark) * lot.qty for lot in position.lots)
        return equity

    def _mark(self, symbol: str, fallback: float = 0.0) -> float:
        mark = self.marks.get(symbol)
        if mark is None:
            mark = self.market.initial_mark(symbol, self.start)
        if mark is None or mark <= 0:
            self.missing_mark_symbols.add(symbol)
            return fallback
        self.marks[symbol] = mark
        return mark

    def _record(self, event: dict[str, object]) -> None:
        event["strategy"] = self.name
        self.events.append(event)

    def _add_order(self, order: EntryOrder, timestamp: datetime) -> None:
        symbol = order.symbol
        entry_timestamp = order.entry_time if order.entry_time <= timestamp else timestamp
        if order.stage == "FIRST":
            if self.plan_first_entered.get(order.plan_id):
                return
            if symbol in self.active:
                self._record(
                    {
                        "type": "entry_skip",
                        "time": iso(entry_timestamp),
                        "symbol": symbol,
                        "stage": order.stage,
                        "plan_id": order.plan_id,
                        "reason": "SYMBOL_ALREADY_ACTIVE",
                    }
                )
                self.plan_first_entered[order.plan_id] = False
                return
            self.plan_first_entered[order.plan_id] = True
        elif order.stage in {"SECOND", "THIRD"}:
            if not self.plan_first_entered.get(order.plan_id, False):
                self._record(
                    {
                        "type": "entry_skip",
                        "time": iso(entry_timestamp),
                        "symbol": symbol,
                        "stage": order.stage,
                        "plan_id": order.plan_id,
                        "reason": "FIRST_TRANCHE_NOT_OPEN",
                    }
                )
                return
            if symbol not in self.active and not self.allow_signal_independent_scale_ins:
                self._record(
                    {
                        "type": "entry_skip",
                        "time": iso(entry_timestamp),
                        "symbol": symbol,
                        "stage": order.stage,
                        "plan_id": order.plan_id,
                        "reason": "POSITION_CLOSED_BEFORE_SECOND",
                    }
                )
                return
        elif symbol in self.active:
            self._record(
                {
                    "type": "entry_skip",
                    "time": iso(timestamp),
                    "symbol": symbol,
                    "stage": order.stage,
                    "plan_id": order.plan_id,
                    "reason": "SYMBOL_ALREADY_ACTIVE",
                }
            )
            return

        if order.entry_price <= 0 or order.qty <= 0:
            return
        fee = order.entry_price * order.qty * FEE_RATE
        self.cash_equity -= fee
        if symbol in self.active:
            position = self.active[symbol]
            position.lots.append(
                Lot(
                    qty=order.qty,
                    entry_price=order.entry_price,
                    base_price=order.entry_price,
                    opened_at=entry_timestamp,
                    notional=order.target_notional,
                )
            )
            if order.structure_stop is not None:
                if position.structure_stop is None:
                    position.structure_stop = order.structure_stop
                    position.structure_active_at = order.structure_active_at
                else:
                    position.structure_stop = min(position.structure_stop, order.structure_stop)
            recomputed_initial = self._initial_stop(
                position.avg_entry,
                position.liq_ratio,
                order.actual_sl_price,
            )
            position.stop_price = min(position.stop_price, recomputed_initial)
        else:
            stop = self._initial_stop(order.entry_price, order.liq_ratio, order.actual_sl_price)
            candidate_stop = order.structure_stop
            if candidate_stop is not None and (
                order.structure_active_at is None or order.structure_active_at <= entry_timestamp
            ):
                stop = min(stop, candidate_stop)
            position = SimPosition(
                symbol=symbol,
                lots=[
                    Lot(
                        qty=order.qty,
                        entry_price=order.entry_price,
                        base_price=order.entry_price,
                        opened_at=entry_timestamp,
                        notional=order.target_notional,
                    )
                ],
                first_opened_at=entry_timestamp,
                stop_price=stop,
                liq_ratio=order.liq_ratio,
                structure_stop=candidate_stop,
                structure_active_at=order.structure_active_at,
                lowest_price_since_open=order.entry_price,
            )
            self.active[symbol] = position
        # A symbol may have been traded earlier in the replay, leaving a
        # stale mark after that position was closed.  Re-anchor a newly opened
        # position to the current completed 15-minute bar rather than letting
        # an old price create artificial PnL at the entry boundary.
        current_bar = self.market.interval_bar(symbol, timestamp - INTERVAL_15M)
        if current_bar is not None:
            self.marks[symbol] = current_bar.close_price
        self._record(
            {
                "type": "entry",
                "time": iso(entry_timestamp),
                "symbol": symbol,
                "stage": order.stage,
                "plan_id": order.plan_id,
                "price": order.entry_price,
                "qty": order.qty,
                "notional": order.target_notional,
                "signal_time": iso(order.signal_time) if order.signal_time else None,
                "signal_kind": order.signal_kind,
            }
        )

    def _activate_structure_stops(self, timestamp: datetime) -> None:
        for symbol, position in list(self.active.items()):
            if (
                position.structure_stop is None
                or position.structure_active_at is None
                or timestamp < position.structure_active_at
            ):
                continue
            position.stop_price = min(position.stop_price, position.structure_stop)
            mark = self._mark(symbol, position.avg_entry)
            if mark > position.stop_price:
                self._close(symbol, mark, timestamp, "ENTRY_STRUCTURE_STOP")

    def _close(self, symbol: str, price: float, timestamp: datetime, reason: str) -> None:
        position = self.active.pop(symbol, None)
        if position is None:
            return
        qty = position.qty
        gross_entry_pnl = sum((lot.entry_price - price) * lot.qty for lot in position.lots)
        equity_delta = sum((lot.base_price - price) * lot.qty for lot in position.lots)
        exit_fee = price * qty * FEE_RATE
        self.cash_equity += equity_delta - exit_fee
        self._record(
            {
                "type": "exit",
                "time": iso(timestamp),
                "symbol": symbol,
                "reason": reason,
                "price": price,
                "qty": qty,
                "avg_entry": position.avg_entry,
                "gross_pnl": gross_entry_pnl,
                "equity_delta": equity_delta - exit_fee,
                "hold_hours": (timestamp - position.first_opened_at).total_seconds() / 3600.0,
            }
        )

    def _update_market(self, timestamp: datetime) -> dict[str, Bar]:
        interval_start = timestamp - INTERVAL_15M
        bars: dict[str, Bar] = {}
        for symbol in list(self.active):
            bar = self.market.interval_bar(symbol, interval_start)
            if bar is not None:
                bars[symbol] = bar
                self.marks[symbol] = bar.close_price
                position = self.active.get(symbol)
                if position is not None and bar.close_time > position.first_opened_at:
                    position.lowest_price_since_open = min(position.lowest_price_since_open, bar.low_price)
        return bars

    def _apply_individual_stops(self, timestamp: datetime, bars: dict[str, Bar]) -> None:
        for symbol, bar in list(bars.items()):
            position = self.active.get(symbol)
            if position is None:
                continue
            if bar.high_price >= position.stop_price:
                self._close(symbol, position.stop_price, timestamp, "INDIVIDUAL_STOP")

    def _apply_morning_protection(self, timestamp: datetime) -> None:
        check_time = timestamp
        hour_start = check_time - timedelta(hours=1)
        for symbol, position in list(self.active.items()):
            if (check_time - position.first_opened_at).total_seconds() < 6 * 3600:
                continue
            ref = self.market.range_extreme(symbol, hour_start, check_time, "low")
            if ref is None:
                continue
            position.morning_cap = ref if position.morning_cap is None else min(position.morning_cap, ref)
            position.stop_price = min(position.stop_price, position.morning_cap)
            mark = self._mark(symbol, position.avg_entry)
            if mark > position.stop_price:
                self._close(symbol, mark, timestamp, "MORNING_PROTECTION_IMMEDIATE")

    def _noon_window(self, position: SimPosition, timestamp: datetime) -> tuple[datetime, datetime]:
        local = timestamp.astimezone(SHANGHAI)
        day_start = local_boundary(local.date())
        noon = datetime(local.year, local.month, local.day, 12, 0, tzinfo=SHANGHAI).astimezone(UTC)
        opened = position.first_opened_at
        if opened >= noon:
            return noon, noon
        if opened < day_start:
            return noon - timedelta(hours=2), noon
        entry_hour = floor_hour(opened)
        return entry_hour - timedelta(hours=2), noon

    def _apply_noon_protection(self, timestamp: datetime) -> None:
        for symbol, position in list(self.active.items()):
            start, end = self._noon_window(position, timestamp)
            ref = self.market.range_extreme(symbol, start, end, "low")
            if ref is None:
                continue
            position.noon_cap = ref if position.noon_cap is None else min(position.noon_cap, ref)
            position.stop_price = min(position.stop_price, position.noon_cap)
            mark = self._mark(symbol, position.avg_entry)
            if mark > position.stop_price:
                self._close(symbol, mark, timestamp, "NOON_PROTECTION_IMMEDIATE")

    def _apply_daily_loss_cut(self, timestamp: datetime) -> None:
        for symbol, position in list(self.active.items()):
            mark = self._mark(symbol, position.avg_entry)
            if mark > position.avg_entry:
                self._close(symbol, mark, timestamp, "DAILY_FLOATING_LOSS_CUT")

    def _apply_hourly_tp(self, timestamp: datetime) -> None:
        hour = floor_hour(timestamp - timedelta(seconds=1))
        for symbol, position in list(self.active.items()):
            candle = self.market.hour(symbol, hour)
            if candle is None or candle.close_price <= candle.open_price:
                continue
            threshold = position.avg_entry * (1.0 - HOURLY_TP_DROP_PCT / 100.0)
            if position.lowest_price_since_open <= threshold:
                mark = self._mark(symbol, position.avg_entry)
                self._close(symbol, mark, timestamp, "HOURLY_EXCHANGE_TAKE_PROFIT")

    def _apply_max_hold(self, timestamp: datetime) -> None:
        for symbol, position in list(self.active.items()):
            if timestamp >= position.max_hold_at:
                mark = self._mark(symbol, position.avg_entry)
                self._close(symbol, mark, timestamp, "MAX_HOLD_EXCEEDED")

    def _apply_portfolio_stop(self, timestamp: datetime) -> None:
        if self.portfolio_loss_latched:
            return
        threshold = self.day_baseline * (1.0 - PORTFOLIO_LOSS_PCT / 100.0)
        current = self._current_equity()
        if current > threshold:
            return
        self.portfolio_loss_latched = True
        event = {
            "time": iso(timestamp),
            "baseline_equity": self.day_baseline,
            "threshold_equity": threshold,
            "current_equity": current,
            "open_count": len(self.active),
            "symbols": sorted(self.active),
        }
        self.portfolio_events.append(event)
        for symbol in list(self.active):
            self._close(symbol, self._mark(symbol), timestamp, "PORTFOLIO_EQUITY_LOSS_CUT")

    def _ensure_daily_row(self, day: date) -> dict[str, object]:
        if day not in self.daily:
            self.daily[day] = {
                "date": day.isoformat(),
                "baseline_equity": None,
                "threshold_equity": None,
                "carried_count": 0,
                "carried_notional": 0.0,
                "new_first": 0,
                "new_second": 0,
                "new_third": 0,
                "new_full": 0,
                "entry_skips": 0,
                "portfolio_stop": False,
                "portfolio_stop_time": None,
                "portfolio_stop_current_equity": None,
                "hourly_tp": 0,
                "individual_stop": 0,
                "daily_loss_cut": 0,
                "noon_protection": 0,
                "morning_protection": 0,
                "structure_stop": 0,
                "max_hold": 0,
                "end_equity": None,
                "next_day_active_count": None,
                "next_day_active_notional": None,
            }
        return self.daily[day]

    def _set_boundary(self, timestamp: datetime, count_before_entries: bool = True) -> None:
        day = local_day(timestamp)
        row = self._ensure_daily_row(day)
        row["baseline_equity"] = self._current_equity()
        row["threshold_equity"] = self.day_baseline * (1.0 - PORTFOLIO_LOSS_PCT / 100.0)
        if count_before_entries:
            row["carried_count"] = len(self.active)
            row["carried_notional"] = sum(position.notional for position in self.active.values())

    def _count_event(self, event: dict[str, object]) -> None:
        day = local_day(parse_dt(str(event["time"])))
        row = self._ensure_daily_row(day)
        event_type = str(event.get("type") or "")
        if event_type == "entry":
            stage = str(event.get("stage") or "")
            row[
                {
                    "FIRST": "new_first",
                    "SECOND": "new_second",
                    "THIRD": "new_third",
                    "FULL": "new_full",
                }.get(stage, "new_full")
            ] += 1
        elif event_type == "entry_skip":
            row["entry_skips"] += 1
        elif event_type == "exit":
            reason = str(event.get("reason") or "")
            reason_key = {
                "HOURLY_EXCHANGE_TAKE_PROFIT": "hourly_tp",
                "INDIVIDUAL_STOP": "individual_stop",
                "DAILY_FLOATING_LOSS_CUT": "daily_loss_cut",
                "NOON_PROTECTION_IMMEDIATE": "noon_protection",
                "MORNING_PROTECTION_IMMEDIATE": "morning_protection",
                "ENTRY_STRUCTURE_STOP": "structure_stop",
                "MAX_HOLD_EXCEEDED": "max_hold",
            }.get(reason)
            if reason_key:
                row[reason_key] += 1

    def _process_orders(self, timestamp: datetime) -> None:
        while self.order_index < len(self.orders) and self.orders[self.order_index].entry_time <= timestamp:
            order = self.orders[self.order_index]
            self._add_order(order, timestamp)
            self.order_index += 1

    def run(self) -> dict[str, object]:
        self._process_orders(self.start)
        self._set_boundary(self.start)
        self.day_baseline = self._current_equity()
        self.portfolio_loss_latched = False
        points: list[dict[str, object]] = [
            {"t": iso(self.start), "equity": self._current_equity()}
        ]
        timestamp = self.start
        while timestamp < self.end:
            timestamp += INTERVAL_15M
            bars = self._update_market(timestamp)
            self._activate_structure_stops(timestamp)
            self._apply_individual_stops(timestamp, bars)

            local = timestamp.astimezone(SHANGHAI)
            # 07:55 checks are represented at the following 08:00 mark because
            # the available local archive is 15-minute OHLC.
            is_boundary = local.hour == 8 and local.minute == 0
            if is_boundary:
                self._apply_morning_protection(timestamp)

            self._process_orders(timestamp)
            self._activate_structure_stops(timestamp)

            if is_boundary:
                # Orders filled just before 08:00 (for example the observed
                # 07:59:51 fills) are already part of the next session's
                # carried state at the boundary.  Capture after processing
                # them so the daily table does not report a false zero.
                previous_row = self._ensure_daily_row(local.date() - timedelta(days=1))
                previous_row["next_day_active_count"] = len(self.active)
                previous_row["next_day_active_notional"] = sum(
                    position.notional for position in self.active.values()
                )
                row = self._ensure_daily_row(local.date())
                row["carried_count"] = len(self.active)
                row["carried_notional"] = sum(
                    position.notional for position in self.active.values()
                )
                self.day_baseline = self._current_equity()
                self.portfolio_loss_latched = False
                row["baseline_equity"] = self.day_baseline
                row["threshold_equity"] = self.day_baseline * (1.0 - PORTFOLIO_LOSS_PCT / 100.0)

            if local.hour == 12 and local.minute == 0:
                self._apply_daily_loss_cut(timestamp)
                self._apply_noon_protection(timestamp)
            if local.minute == 0:
                self._apply_hourly_tp(timestamp)
            self._apply_max_hold(timestamp)
            if not is_boundary:
                self._apply_portfolio_stop(timestamp)

            points.append({"t": iso(timestamp), "equity": self._current_equity()})

        # Finalize daily rows at the next 08:00 mark and attach event counts.
        for event in self.events:
            self._count_event(event)
        for event in self.portfolio_events:
            day = local_day(parse_dt(str(event["time"])))
            row = self._ensure_daily_row(day)
            row["portfolio_stop"] = True
            row["portfolio_stop_time"] = event["time"]
            row["portfolio_stop_current_equity"] = event["current_equity"]
        boundaries = sorted(self.daily)
        for index, day in enumerate(boundaries):
            row = self.daily[day]
            boundary_end = local_boundary(day + timedelta(days=1))
            row["end_equity"] = next(
                (
                    point["equity"]
                    for point in reversed(points)
                    if parse_dt(str(point["t"])) <= boundary_end
                ),
                points[-1]["equity"],
            )
            # The replay loop records the exact carried state immediately
            # before entries at every 08:00 boundary.  Do not overwrite that
            # state with the final replay state when preparing older rows.
            if row["next_day_active_count"] is None:
                active_at_end = self._active_at_timestamp(boundary_end)
                row["next_day_active_count"] = active_at_end["count"]
                row["next_day_active_notional"] = active_at_end["notional"]
        metrics = self._metrics(points)
        return {
            "name": self.name,
            "label": self.label,
            "points": points,
            "events": self.events,
            "portfolio_events": self.portfolio_events,
            "daily": [self.daily[day] for day in sorted(self.daily)],
            "metrics": metrics,
            "initial_seed_count": self.initial_seed_count,
            "missing_mark_symbols": sorted(self.missing_mark_symbols),
        }

    def _active_at_timestamp(self, timestamp: datetime) -> dict[str, float | int]:
        # The live state is at the end of the replay.  For daily reporting,
        # reconstructing a full position snapshot is unnecessary; next-day
        # counts are captured during the loop in the daily rows below.
        del timestamp
        return {
            "count": len(self.active),
            "notional": sum(position.notional for position in self.active.values()),
        }

    @staticmethod
    def _metrics(points: list[dict[str, object]]) -> dict[str, object]:
        values = [float(point["equity"]) for point in points]
        initial = values[0]
        final = values[-1]
        peak = -math.inf
        max_drawdown = 0.0
        for value in values:
            peak = max(peak, value)
            if peak > 0:
                max_drawdown = min(max_drawdown, value / peak - 1.0)
        return {
            "initial_equity": initial,
            "final_equity": final,
            "pnl": final - initial,
            "return_pct": (final / initial - 1.0) * 100.0 if initial else None,
            "max_drawdown_pct": max_drawdown * 100.0,
            "peak_equity": max(values),
            "trough_equity": min(values),
        }


def recompute_daily_next_day_positions(result: dict[str, object], start: datetime, end: datetime) -> None:
    """Fill each row's next-day active state from the event stream.

    The main replay loop intentionally keeps only the current active map.  A
    small event ledger is enough to reconstruct the number and nominal notional
    carried across each 08:00 boundary without changing the accounting path.
    """
    events = result.get("events")
    if not isinstance(events, list):
        return
    rows = result.get("daily")
    if not isinstance(rows, list):
        return
    for raw_row in rows:
        day = date.fromisoformat(str(raw_row["date"]))
        boundary = local_boundary(day + timedelta(days=1))
        active: dict[str, float] = {}
        for raw_event in events:
            if not isinstance(raw_event, dict):
                continue
            timestamp = parse_dt(str(raw_event.get("time")))
            if timestamp > boundary:
                continue
            symbol = str(raw_event.get("symbol") or "")
            if raw_event.get("type") == "entry":
                active[symbol] = active.get(symbol, 0.0) + float(raw_event.get("notional") or 0.0)
            elif raw_event.get("type") == "exit":
                active.pop(symbol, None)
        raw_row["next_day_active_count"] = len(active)
        raw_row["next_day_active_notional"] = sum(active.values())
    del start, end


def clean_for_json(value: object) -> object:
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return round(value, 8)
    if isinstance(value, dict):
        return {str(key): clean_for_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [clean_for_json(item) for item in value]
    return value


def write_daily_csv(path: Path, results: dict[str, dict[str, object]], days: list[date]) -> None:
    fieldnames = [
        "date",
        "strategy",
        "baseline_equity",
        "threshold_equity",
        "carried_count",
        "carried_notional",
        "new_full",
        "new_first",
        "new_second",
        "new_third",
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
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for day in days:
            for name, result in results.items():
                row = next((item for item in result["daily"] if item["date"] == day.isoformat()), {})
                writer.writerow(
                    {
                        "date": day.isoformat(),
                        "strategy": name,
                        **{key: row.get(key) for key in fieldnames if key not in {"date", "strategy"}},
                    }
                )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Independent acc04 strategy portfolio replay")
    parser.add_argument("--db", default="remote_data/state.db")
    parser.add_argument("--market-cache-dir", default="remote_artifacts/independent_replay_market_1h_20260902")
    parser.add_argument("--output-json", default="reports/acc04-independent-strategy-replay-20260902.json")
    parser.add_argument("--output-csv", default="reports/acc04-independent-strategy-replay-daily-20260902.csv")
    parser.add_argument("--start-day", default="2026-08-11")
    parser.add_argument("--end-day", default="2026-08-31")
    parser.add_argument("--account", default="acc04")
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument(
        "--signal-independent-scale-ins",
        action="store_true",
        help="also replay scale-in tranches as fresh entries after an earlier tranche has closed",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    chart_start_day = date.fromisoformat(args.start_day)
    chart_end_day = date.fromisoformat(args.end_day)
    chart_start = local_boundary(chart_start_day)
    chart_end = local_boundary(chart_end_day + timedelta(days=1))
    simulation_start = chart_start - timedelta(days=1)

    db_path = Path(args.db)
    conn = open_db(db_path)
    start_equity, snapshot_time = load_start_equity(conn, args.account, simulation_start)
    candidates, seed, symbols = load_candidates(conn, args.account, simulation_start, chart_end)
    print(
        f"loaded acc04 candidates={len(candidates)} seed={len(seed)} symbols={len(symbols)} "
        f"start_equity={start_equity:.8f} snapshot={iso(snapshot_time)}",
        flush=True,
    )

    market = MarketData(Path(args.market_cache_dir), simulation_start, chart_end, symbols, workers=args.workers)
    market.load()
    print(
        f"market 15m={market.coverage()['symbols_with_15m']} 1h={market.coverage()['symbols_with_1h']} "
        f"missing={len(market.missing_symbols)} files={market.downloaded_files}/{market.requested_files}",
        flush=True,
    )

    orders = build_orders(candidates, market, simulation_start, chart_end)
    replay_results: dict[str, dict[str, object]] = {}
    configs = [
        ("actual", "实际（独立重算）"),
        ("second", "第二次阴线（独立重算）"),
        ("bullbear", "先阳后阴（独立重算）"),
        ("bullbear3", "先阳后阴三段（独立重算）"),
    ]
    if args.signal_independent_scale_ins:
        orders["bullbear_reentry"] = signal_independent_orders(orders["bullbear"], market)
        orders["bullbear3_reentry"] = signal_independent_orders(orders["bullbear3"], market)
        configs.extend(
            [
                ("bullbear_reentry", "先阳后阴·信号独立开仓（两段）"),
                ("bullbear3_reentry", "先阳后阴·信号独立开仓（三段）"),
            ]
        )
    for name, label in configs:
        replay = StrategyReplay(
            name=name,
            label=label,
            start=simulation_start,
            end=chart_end,
            start_equity=start_equity,
            market=market,
            orders=orders[name],
            seed=seed,
            allow_signal_independent_scale_ins=name.endswith("_reentry"),
        )
        result = replay.run()
        chart_points = [
            point
            for point in result["points"]
            if chart_start <= parse_dt(str(point["t"])) <= chart_end
        ]
        result["chart_metrics"] = StrategyReplay._metrics(chart_points)
        replay_results[name] = result
        print(
            f"{name}: final={result['metrics']['final_equity']:.4f} "
            f"return={result['metrics']['return_pct']:.3f}% "
            f"portfolio_stops={len(result['portfolio_events'])} events={len(result['events'])}",
            flush=True,
        )

    days = [chart_start_day + timedelta(days=index) for index in range((chart_end_day - chart_start_day).days + 1)]
    payload = {
        "meta": {
            "title": "acc04 四套策略独立重算权益曲线",
            "account": args.account,
            "timezone": "Asia/Shanghai",
            "chart_start_local": f"{args.start_day}T08:00:00+08:00",
            "chart_end_local": f"{(chart_end_day + timedelta(days=1)).isoformat()}T08:00:00+08:00",
            "simulation_start_utc": iso(simulation_start),
            "simulation_end_utc": iso(chart_end),
            "starting_wallet_snapshot_utc": iso(snapshot_time),
            "starting_equity": start_equity,
            "db": str(db_path),
            "market_source": "Binance Vision daily futures UM 15m archives; 1h fallback when 15m archive unavailable",
            "portfolio_loss_cut_pct": PORTFOLIO_LOSS_PCT,
            "hourly_take_profit_drop_pct": HOURLY_TP_DROP_PCT,
            "max_hold_hours": MAX_HOLD_HOURS,
            "fee_rate_per_side": FEE_RATE,
            "notes": [
                "四套路径分别维护自己的持仓、止损、小时止盈、组合止损锁存和跨日仓位。",
                "三段路径按1/3、1/3、1/3分仓：首次阴线、首次阳后阴、再次阳后阴；若后续信号未在16小时内出现，则保留已开部分。",
                "组合止损按每15分钟收盘权益相对各自08:00基准计算；真实服务按约60秒钱包快照，时间可能有分钟级差异。",
                "07:55早盘保护和11:55浮亏止损在15分钟数据中分别用08:00和12:00附近价格近似。",
                "实际路径使用state.db中的实际成交时间、价格和数量；两套拆仓路径固定使用相同symbol和实际单笔目标名义金额的50%+50%。",
                "hourly_exchange_take_profit按18%有利跌幅后、上一根完整1h阳线在整点触发。",
                "当前实际配置fixed_take_profit_enabled=false，因此没有额外的固定20%止盈单。",
            ],
            "market_coverage": market.coverage(),
            "order_counts": {name: len(items) for name, items in orders.items()},
        },
        "series": replay_results,
        "days": [day.isoformat() for day in days],
    }
    output_json = Path(args.output_json)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(clean_for_json(payload), ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    write_daily_csv(Path(args.output_csv), replay_results, days)
    print(f"wrote {output_json}", flush=True)
    print(f"wrote {args.output_csv}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
