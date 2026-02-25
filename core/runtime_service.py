import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import Any, Dict, List, Optional
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

        account_ids = [
            aid
            for aid, ctx in self.account_runtimes.items()
            if str(ctx.get("mode", "full")).strip().lower() == "full"
        ]
        if not account_ids:
            LOGGER.warning("service entry skipped: no account is enabled for full mode")
            self._last_entry_local_date = now_local.date()
            return

        shared_top_gainers = self._build_shared_top_gainers(account_ids)
        results: Dict[str, object] = {}
        if len(account_ids) == 1:
            aid = account_ids[0]
            ctx = self.account_runtimes[aid]
            strategy = ctx.get("strategy")
            if strategy is not None:
                try:
                    results[aid] = self._run_entry_with_shared(
                        strategy=strategy,
                        shared_top_gainers=shared_top_gainers,
                    )
                except Exception as exc:  # noqa: BLE001
                    results[aid] = {"error": str(exc)}
            else:
                results[aid] = {"error": "strategy_missing"}
        else:
            timeout_sec = max(0.1, float(self.cfg.account_task_timeout_sec))
            done_futures = set()
            slow_accounts = set()
            with ThreadPoolExecutor(max_workers=min(self.max_account_workers, len(account_ids))) as ex:
                futures = {}
                for aid in account_ids:
                    strategy = self.account_runtimes[aid].get("strategy")
                    if strategy is None:
                        results[aid] = {"error": "strategy_missing"}
                        continue
                    futures[ex.submit(self._run_entry_with_shared, strategy, shared_top_gainers)] = aid

                try:
                    for future in as_completed(futures, timeout=timeout_sec):
                        done_futures.add(future)
                        aid = futures[future]
                        try:
                            results[aid] = future.result()
                        except Exception as exc:  # noqa: BLE001
                            results[aid] = {"error": str(exc)}
                except FutureTimeoutError:
                    pass

                for future, aid in futures.items():
                    if future in done_futures:
                        continue
                    slow_accounts.add(aid)
                    LOGGER.warning(
                        "service entry account exceeded soft-timeout %.2fs, waiting completion account=%s",
                        timeout_sec,
                        aid,
                    )
                    try:
                        result = future.result()
                        if isinstance(result, dict):
                            result.setdefault("slow", True)
                        results[aid] = result
                    except Exception as exc:  # noqa: BLE001
                        results[aid] = {"error": str(exc)}

                for aid in slow_accounts:
                    if isinstance(results.get(aid), dict):
                        results[aid].setdefault("slow", True)

        self._last_entry_local_date = now_local.date()
        LOGGER.info("service entry result: %s", results)

    def _run_entry_with_shared(
        self,
        strategy: object,
        shared_top_gainers: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, object]:
        run_entry = getattr(strategy, "run_entry", None)
        if run_entry is None:
            raise RuntimeError("strategy_missing")
        if shared_top_gainers is None:
            return run_entry()  # type: ignore[misc]
        try:
            return run_entry(shared_top_gainers=shared_top_gainers)  # type: ignore[misc]
        except TypeError:
            return run_entry()  # type: ignore[misc]

    def _build_shared_top_gainers(self, account_ids: List[str]) -> Optional[List[Dict[str, Any]]]:
        if len(account_ids) <= 1:
            return None

        strategies: List[Any] = []
        for aid in account_ids:
            strategy = self.account_runtimes.get(aid, {}).get("strategy")
            if strategy is None:
                return None
            required_attrs = (
                "top_n",
                "entry_rank_fetch_multiplier",
                "volume_threshold",
                "ranker_max_workers",
                "ranker_weight_limit_per_minute",
                "ranker_min_request_interval_ms",
                "store",
                "client",
            )
            if not all(hasattr(strategy, attr) for attr in required_attrs):
                return None
            if not hasattr(strategy.client, "session") or not hasattr(strategy.client, "base_url"):
                return None
            strategies.append(strategy)

        fetch_top_n = 0
        min_volume_threshold = float("inf")
        max_workers = 1
        weight_limit_per_minute = float("inf")
        min_request_interval_ms = 0

        for strategy in strategies:
            try:
                open_symbols = strategy.store.list_open_symbols()
                open_count = len(open_symbols)
            except Exception:  # noqa: BLE001
                open_count = 0
            strategy_top_n = int(strategy.top_n)
            strategy_fetch_top_n = max(
                strategy_top_n,
                strategy_top_n * int(strategy.entry_rank_fetch_multiplier),
                strategy_top_n + open_count,
            )
            fetch_top_n = max(fetch_top_n, strategy_fetch_top_n)
            min_volume_threshold = min(min_volume_threshold, float(strategy.volume_threshold))
            max_workers = max(max_workers, int(strategy.ranker_max_workers))
            weight_limit_per_minute = min(
                weight_limit_per_minute,
                int(strategy.ranker_weight_limit_per_minute),
            )
            min_request_interval_ms = max(
                min_request_interval_ms,
                int(strategy.ranker_min_request_interval_ms),
            )

        reference_strategy = strategies[0]
        try:
            from infra.binance_top10_monitor import build_top_gainers

            top_gainers = build_top_gainers(
                top_n=max(1, fetch_top_n),
                volume_threshold=0.0 if min_volume_threshold == float("inf") else max(0.0, min_volume_threshold),
                session=reference_strategy.client.session,
                base_url=reference_strategy.client.base_url,
                max_workers=max(1, max_workers),
                weight_limit_per_minute=max(100, int(weight_limit_per_minute)),
                min_request_interval_ms=max(0, int(min_request_interval_ms)),
            )
            LOGGER.info(
                "service shared ranking built once for accounts=%s fetched=%s",
                len(account_ids),
                len(top_gainers),
            )
            return top_gainers
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("service shared ranking unavailable, fallback to per-account ranking: %s", exc)
            return None

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
        account_ids = []
        for aid, ctx in self.account_runtimes.items():
            mode = str(ctx.get("mode", "full")).strip().lower()
            if mode not in {"full", "loss_cut_only"}:
                continue
            enabled_raw = ctx.get("daily_loss_cut_enabled", True)
            if isinstance(enabled_raw, str):
                enabled = enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
            else:
                enabled = bool(enabled_raw)
            if not enabled:
                continue
            account_ids.append(aid)
        if not account_ids:
            LOGGER.warning("service daily loss-cut skipped: no account is enabled")
            return

        results: Dict[str, object] = {}
        if len(account_ids) == 1:
            aid = account_ids[0]
            manager = self.account_runtimes[aid].get("manager")
            if manager is None or not hasattr(manager, "run_daily_loss_cut"):
                results[aid] = {"error": "manager_missing"}
            else:
                try:
                    results[aid] = manager.run_daily_loss_cut()  # type: ignore[attr-defined]
                except Exception as exc:  # noqa: BLE001
                    results[aid] = {"error": str(exc)}
        else:
            timeout_sec = max(0.1, float(self.cfg.account_task_timeout_sec))
            done_futures = set()
            slow_accounts = set()
            with ThreadPoolExecutor(max_workers=min(self.max_account_workers, len(account_ids))) as ex:
                futures = {}
                for aid in account_ids:
                    manager = self.account_runtimes[aid].get("manager")
                    if manager is None or not hasattr(manager, "run_daily_loss_cut"):
                        results[aid] = {"error": "manager_missing"}
                        continue
                    futures[ex.submit(manager.run_daily_loss_cut)] = aid  # type: ignore[attr-defined]

                try:
                    for future in as_completed(futures, timeout=timeout_sec):
                        done_futures.add(future)
                        aid = futures[future]
                        try:
                            results[aid] = future.result()
                        except Exception as exc:  # noqa: BLE001
                            results[aid] = {"error": str(exc)}
                except FutureTimeoutError:
                    pass

                for future, aid in futures.items():
                    if future in done_futures:
                        continue
                    slow_accounts.add(aid)
                    LOGGER.warning(
                        "service daily loss-cut account exceeded soft-timeout %.2fs, waiting completion account=%s",
                        timeout_sec,
                        aid,
                    )
                    try:
                        result = future.result()
                        if isinstance(result, dict):
                            result.setdefault("slow", True)
                        results[aid] = result
                    except Exception as exc:  # noqa: BLE001
                        results[aid] = {"error": str(exc)}
                for aid in slow_accounts:
                    if isinstance(results.get(aid), dict):
                        results[aid].setdefault("slow", True)
        LOGGER.info("service daily loss-cut result: %s", results)

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
        account_ids = [
            aid
            for aid, ctx in self.account_runtimes.items()
            if str(ctx.get("mode", "full")).strip().lower() == "full"
        ]
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
            done_futures = set()
            slow_accounts = set()
            try:
                for future in as_completed(futures, timeout=timeout_sec):
                    done_futures.add(future)
                    aid = futures[future]
                    try:
                        outputs[aid] = future.result()
                        self.account_states[aid].failures = 0
                    except Exception as exc:  # noqa: BLE001
                        self._record_account_failure(aid, str(exc))
                        outputs[aid] = {"account_id": aid, "error": str(exc)}
            except FutureTimeoutError:
                pass

            for future, aid in futures.items():
                if future in done_futures:
                    continue
                slow_accounts.add(aid)
                LOGGER.warning(
                    "service manage account exceeded soft-timeout %.2fs, waiting completion account=%s",
                    timeout_sec,
                    aid,
                )
                try:
                    outputs[aid] = future.result()
                    self.account_states[aid].failures = 0
                except Exception as exc:  # noqa: BLE001
                    self._record_account_failure(aid, str(exc))
                    outputs[aid] = {"account_id": aid, "error": str(exc)}
            for aid in slow_accounts:
                output = outputs.get(aid)
                if isinstance(output, dict):
                    output["slow"] = True
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
