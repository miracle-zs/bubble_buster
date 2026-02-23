import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Dict, Optional
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ServiceRuntimeConfig:
    timezone_name: str
    entry_hour: int
    entry_minute: int
    entry_misfire_grace_min: int
    entry_catchup_enabled: bool
    daily_loss_cut_enabled: bool
    daily_loss_cut_hour: int
    daily_loss_cut_minute: int
    manager_interval_sec: int
    manager_max_catch_up_runs: int
    loop_sleep_sec: float
    run_manage_on_startup: bool
    max_account_workers: int = 1
    account_failure_threshold: int = 3
    account_cooldown_cycles: int = 2
    account_task_timeout_sec: float = 30.0


@dataclass
class AccountRuntimeState:
    failures: int = 0
    tripped_until_cycle: int = 0


class StrategyRuntimeService:
    """In-process scheduler replacing external cron for entry/manage tasks."""

    def __init__(
        self,
        strategy,
        manager,
        cfg: ServiceRuntimeConfig,
        balance_sampler=None,
        now_monotonic: Optional[float] = None,
        account_runtimes: Optional[Dict[str, Dict[str, object]]] = None,
        max_account_workers: Optional[int] = None,
    ):
        self.strategy = strategy
        self.manager = manager
        self.balance_sampler = balance_sampler
        self.cfg = cfg
        self.max_account_workers = max(
            1,
            max_account_workers
            if max_account_workers is not None
            else getattr(cfg, "max_account_workers", 1),
        )
        self.account_runtimes = account_runtimes or {
            "default": {
                "strategy": strategy,
                "manager": manager,
                "balance_sampler": balance_sampler,
            }
        }
        self.account_states: Dict[str, AccountRuntimeState] = {
            aid: AccountRuntimeState() for aid in self.account_runtimes
        }
        self.cycle_no = 0

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
        self._last_loss_cut_local_date: Optional[date] = None
        self._last_loss_cut_skipped_date: Optional[date] = None

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

    def _loss_cut_schedule_for_day(self, day: date) -> datetime:
        return datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=self.cfg.daily_loss_cut_hour % 24,
            minute=self.cfg.daily_loss_cut_minute % 60,
            second=0,
            microsecond=0,
            tzinfo=self.timezone,
        )

    def _run_daily_loss_cut_if_due(self, now_local: datetime) -> None:
        if not self.cfg.daily_loss_cut_enabled:
            return
        today = now_local.date()
        if self._last_loss_cut_local_date == today:
            return
        target = self._loss_cut_schedule_for_day(today)
        if now_local < target:
            return
        window_end = target + timedelta(minutes=1)
        if now_local >= window_end:
            if self._last_loss_cut_skipped_date != today:
                LOGGER.warning(
                    "Daily loss-cut missed strict window, skip for today: now=%s target=%s window=1min",
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                )
                self._last_loss_cut_skipped_date = today
            self._last_loss_cut_local_date = today
            return

        self._last_loss_cut_local_date = today
        if not hasattr(self.manager, "run_daily_loss_cut"):
            LOGGER.warning("manager has no run_daily_loss_cut method, skip daily loss cut")
            return
        result = self.manager.run_daily_loss_cut()
        LOGGER.info("service daily loss-cut result: %s", result)

    def _run_manage_if_due(self, now_monotonic: float) -> None:
        if now_monotonic < self._next_manage_monotonic:
            return

        run_count = 0
        while now_monotonic >= self._next_manage_monotonic and run_count < max(1, self.cfg.manager_max_catch_up_runs):
            summary = self.run_manage_tick()
            run_count += 1
            LOGGER.info("service manage summary: %s", summary)
            self._next_manage_monotonic += self.cfg.manager_interval_sec

        if now_monotonic >= self._next_manage_monotonic:
            # Too much backlog; reset cadence to avoid a long catch-up burst.
            self._next_manage_monotonic = now_monotonic + self.cfg.manager_interval_sec
            LOGGER.warning(
                "Manage backlog truncated after %s catch-up runs; next run reset in %ss",
                run_count,
                self.cfg.manager_interval_sec,
            )

    def _run_manage_for_account(self, account_id: str) -> Dict[str, object]:
        ctx = self.account_runtimes[account_id]
        manager = ctx["manager"]
        strategy = ctx.get("strategy")
        balance_sampler = ctx.get("balance_sampler")

        summary = manager.run_once()  # type: ignore[attr-defined]
        if balance_sampler is not None:
            try:
                wallet_summary = balance_sampler.run_once()  # type: ignore[attr-defined]
                LOGGER.info("service wallet snapshot account=%s: %s", account_id, wallet_summary)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("service wallet snapshot failed account=%s: %s", account_id, exc)

        if strategy is not None and hasattr(strategy, "run_equity_recovery_take_profit"):
            try:
                result = strategy.run_equity_recovery_take_profit()  # type: ignore[attr-defined]
                if isinstance(result, dict) and result.get("status") in {"TRIGGERED", "PARTIAL"}:
                    LOGGER.info("service equity recovery take-profit account=%s result: %s", account_id, result)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("service equity recovery take-profit failed account=%s: %s", account_id, exc)

        return {"account_id": account_id, "summary": summary}

    def _record_account_failure(self, account_id: str, error_text: str) -> None:
        state = self.account_states.setdefault(account_id, AccountRuntimeState())
        state.failures += 1
        threshold = max(1, int(self.cfg.account_failure_threshold))
        if state.failures >= threshold:
            cooldown = max(1, int(self.cfg.account_cooldown_cycles))
            state.tripped_until_cycle = self.cycle_no + cooldown
            LOGGER.warning(
                "service breaker tripped account=%s failures=%s cooldown_cycles=%s until_cycle=%s last_error=%s",
                account_id,
                state.failures,
                cooldown,
                state.tripped_until_cycle,
                error_text,
            )
        else:
            LOGGER.warning(
                "service manage failed account=%s failures=%s/%s error=%s",
                account_id,
                state.failures,
                threshold,
                error_text,
            )

    def run_manage_tick(self) -> Dict[str, Dict[str, object]]:
        self.cycle_no += 1
        account_ids = list(self.account_runtimes.keys())
        eligible_accounts = []
        outputs: Dict[str, Dict[str, object]] = {}
        for aid in account_ids:
            state = self.account_states.setdefault(aid, AccountRuntimeState())
            if state.tripped_until_cycle >= self.cycle_no:
                LOGGER.warning(
                    "service breaker active account=%s cycle=%s tripped_until=%s, skipped",
                    aid,
                    self.cycle_no,
                    state.tripped_until_cycle,
                )
                outputs[aid] = {"account_id": aid, "skipped": True, "reason": "BREAKER_TRIPPED"}
                continue
            eligible_accounts.append(aid)

        if not eligible_accounts:
            return outputs

        timeout_sec = max(0.1, float(self.cfg.account_task_timeout_sec))
        if len(eligible_accounts) == 1:
            only = eligible_accounts[0]
            try:
                result = self._run_manage_for_account(only)
                self.account_states[only].failures = 0
                outputs[only] = result
            except Exception as exc:  # noqa: BLE001
                self._record_account_failure(only, str(exc))
                outputs[only] = {"account_id": only, "error": str(exc)}
            return outputs

        with ThreadPoolExecutor(max_workers=min(self.max_account_workers, len(eligible_accounts))) as ex:
            futures = {ex.submit(self._run_manage_for_account, aid): aid for aid in eligible_accounts}
            for future in as_completed(futures):
                aid = futures[future]
                try:
                    outputs[aid] = future.result(timeout=timeout_sec)
                    self.account_states[aid].failures = 0
                except FutureTimeoutError:
                    future.cancel()
                    self._record_account_failure(aid, f"timeout>{timeout_sec}s")
                    outputs[aid] = {"account_id": aid, "error": "TIMEOUT"}
                except Exception as exc:  # noqa: BLE001
                    self._record_account_failure(aid, str(exc))
                    outputs[aid] = {"account_id": aid, "error": str(exc)}
        return outputs

    def run_cycle(
        self,
        now_local: Optional[datetime] = None,
        now_monotonic: Optional[float] = None,
    ) -> None:
        local_dt = now_local or datetime.now(self.timezone)
        mono = now_monotonic if now_monotonic is not None else time.monotonic()
        self._run_entry_if_due(local_dt)
        self._run_daily_loss_cut_if_due(local_dt)
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
