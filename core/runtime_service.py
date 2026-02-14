import logging
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServiceRuntimeConfig:
    timezone_name: str
    entry_hour: int
    entry_minute: int
    entry_misfire_grace_min: int
    entry_catchup_enabled: bool
    manager_interval_sec: int
    manager_max_catch_up_runs: int
    loop_sleep_sec: float
    run_manage_on_startup: bool


class StrategyRuntimeService:
    """In-process scheduler replacing external cron for entry/manage tasks."""

    def __init__(
        self,
        strategy,
        manager,
        cfg: ServiceRuntimeConfig,
        balance_sampler=None,
        now_monotonic: Optional[float] = None,
    ):
        self.strategy = strategy
        self.manager = manager
        self.balance_sampler = balance_sampler
        self.cfg = cfg

        try:
            self.timezone = ZoneInfo(cfg.timezone_name)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Invalid timezone=%s, fallback to UTC", cfg.timezone_name)
            self.timezone = ZoneInfo("UTC")

        now_monotonic = now_monotonic if now_monotonic is not None else time.monotonic()
        self._next_manage_monotonic = (
            now_monotonic
            if cfg.run_manage_on_startup
            else now_monotonic + cfg.manager_interval_sec
        )
        self._last_entry_local_date: Optional[date] = None
        self._last_entry_skipped_date: Optional[date] = None

    def _entry_schedule_for_day(self, day: date) -> datetime:
        return datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=self.cfg.entry_hour % 24,
            minute=self.cfg.entry_minute % 60,
            second=0,
            microsecond=0,
            tzinfo=self.timezone,
        )

    def _should_run_entry(self, now_local: datetime) -> bool:
        today = now_local.date()
        if self._last_entry_local_date == today:
            return False

        target = self._entry_schedule_for_day(today)
        if now_local < target:
            return False

        if not self.cfg.entry_catchup_enabled and now_local > target:
            if self._last_entry_skipped_date != today:
                LOGGER.warning(
                    "Entry missed scheduled time and catch-up disabled, skip for today: now=%s target=%s",
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                )
                self._last_entry_skipped_date = today
            self._last_entry_local_date = today
            return False

        grace = timedelta(minutes=max(0, self.cfg.entry_misfire_grace_min))
        if now_local - target > grace:
            if self._last_entry_skipped_date != today:
                LOGGER.warning(
                    "Entry missed beyond grace window, skip for today: now=%s target=%s grace_min=%s",
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                    self.cfg.entry_misfire_grace_min,
                )
                self._last_entry_skipped_date = today
            self._last_entry_local_date = today
            return False

        return True

    def _run_entry_if_due(self, now_local: datetime) -> None:
        if not self._should_run_entry(now_local):
            return

        result = self.strategy.run_entry()
        self._last_entry_local_date = now_local.date()
        LOGGER.info("service entry result: %s", result)

    def _run_manage_if_due(self, now_monotonic: float) -> None:
        if now_monotonic < self._next_manage_monotonic:
            return

        run_count = 0
        while now_monotonic >= self._next_manage_monotonic and run_count < max(1, self.cfg.manager_max_catch_up_runs):
            summary = self.manager.run_once()
            run_count += 1
            LOGGER.info("service manage summary: %s", summary)
            if self.balance_sampler is not None:
                try:
                    wallet_summary = self.balance_sampler.run_once()
                    LOGGER.info("service wallet snapshot: %s", wallet_summary)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("service wallet snapshot failed: %s", exc)
            self._next_manage_monotonic += self.cfg.manager_interval_sec

        if now_monotonic >= self._next_manage_monotonic:
            # Too much backlog; reset cadence to avoid a long catch-up burst.
            self._next_manage_monotonic = now_monotonic + self.cfg.manager_interval_sec
            LOGGER.warning(
                "Manage backlog truncated after %s catch-up runs; next run reset in %ss",
                run_count,
                self.cfg.manager_interval_sec,
            )

    def run_cycle(
        self,
        now_local: Optional[datetime] = None,
        now_monotonic: Optional[float] = None,
    ) -> None:
        local_dt = now_local or datetime.now(self.timezone)
        mono = now_monotonic if now_monotonic is not None else time.monotonic()
        self._run_entry_if_due(local_dt)
        self._run_manage_if_due(mono)

    def run_forever(self, stop_event: Optional[threading.Event] = None) -> None:
        stopper = stop_event or threading.Event()
        LOGGER.info(
            "runtime service started: tz=%s entry=%02d:%02d manage_interval=%ss grace=%smin catchup=%s",
            self.timezone.key,
            self.cfg.entry_hour,
            self.cfg.entry_minute,
            self.cfg.manager_interval_sec,
            self.cfg.entry_misfire_grace_min,
            "on" if self.cfg.entry_catchup_enabled else "off",
        )
        while not stopper.is_set():
            try:
                self.run_cycle()
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("runtime service cycle failed: %s", exc)
            stopper.wait(timeout=max(0.2, self.cfg.loop_sleep_sec))
        LOGGER.info("runtime service stopped")
