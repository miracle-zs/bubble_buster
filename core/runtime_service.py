import logging
import inspect
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Set
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger(__name__)
PROTECTION_RESTART_GRACE = timedelta(hours=2)


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
    daily_loss_cut_grace_min: int = 30
    portfolio_loss_cut_enabled: bool = False
    portfolio_loss_cut_pct: float = 3.5
    portfolio_loss_cut_hour: int = 8
    portfolio_loss_cut_minute: int = 0
    portfolio_take_profit_enabled: bool = False
    portfolio_take_profit_pct: float = 9.0
    portfolio_take_profit_hour: int = 8
    portfolio_take_profit_minute: int = 0
    portfolio_take_profit_reduce_ratio: float = 1.0
    portfolio_take_profit_giveback_pct: float = 0.0
    max_account_workers: int = 1
    account_failure_threshold: int = 3
    account_cooldown_cycles: int = 2
    account_task_timeout_sec: float = 30.0
    noon_protection_enabled: bool = True
    noon_protection_hour: int = 12
    noon_protection_minute: int = 0
    noon_protection_retry_interval_sec: float = 60.0
    morning_protection_enabled: bool = False
    morning_protection_hour: int = 7
    morning_protection_minute: int = 55
    morning_protection_min_hold_hours: float = 6.0
    hourly_exchange_take_profit_enabled: bool = False
    hourly_exchange_take_profit_minute: int = 0
    hourly_exchange_take_profit_drop_pct: float = 18.0
    orphan_exit_order_cleanup_enabled: bool = True
    orphan_exit_order_cleanup_hour: int = 3
    orphan_exit_order_cleanup_minute: int = 30
    readonly_wallet_snapshot_interval_sec: float = 60.0


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
        self._last_entry_local_date_by_account: Dict[str, date] = {}
        self._last_entry_skipped_date_by_account: Dict[str, date] = {}
        self._last_loss_cut_local_date: Optional[date] = None
        self._last_loss_cut_skipped_date: Optional[date] = None
        self._last_noon_protection_local_date: Optional[date] = None
        self._noon_protection_pending_symbols_by_account: Dict[str, Optional[Set[str]]] = {}
        self._noon_protection_retry_due_local: Optional[datetime] = None
        self._noon_protection_retry_local_date: Optional[date] = None
        self._last_morning_protection_local_date_by_account: Dict[str, date] = {}
        self._last_hourly_exchange_take_profit_hour_by_account: Dict[str, str] = {}
        self._last_orphan_exit_order_cleanup_local_date: Optional[date] = None
        self._next_readonly_wallet_snapshot_monotonic_by_account: Dict[str, float] = {}
        self._shared_ranking_cache: Optional[List[Dict[str, Any]]] = None
        self._shared_ranking_cache_day: Optional[date] = None
        self._shared_ranking_cache_expires_at: Optional[datetime] = None
        self._entry_executor = ThreadPoolExecutor(
            max_workers=self.max_account_workers,
            thread_name_prefix="entry",
        )
        self._entry_futures: Dict[Future, str] = {}
        self._entry_future_trade_day_by_future: Dict[Future, date] = {}
        self._entry_future_started_at_by_account: Dict[str, float] = {}
        self._entry_future_warned_by_account: Dict[str, bool] = {}
        self._manage_executor = ThreadPoolExecutor(
            max_workers=self.max_account_workers,
            thread_name_prefix="manage",
        )
        self._manage_futures_by_account: Dict[str, Future] = {}
        self._scheduled_executor = ThreadPoolExecutor(
            max_workers=self.max_account_workers,
            thread_name_prefix="scheduled",
        )
        self._scheduled_futures_by_task_account: Dict[tuple[str, str], Future] = {}
        self._scheduled_future_day_by_task_account: Dict[tuple[str, str], date] = {}

    def _entry_schedule_for_day(self, day: date, account_id: Optional[str] = None) -> datetime:
        account_ctx = self.account_runtimes.get(account_id or "", {})
        entry_hour = int(account_ctx.get("entry_hour", self.cfg.entry_hour))
        entry_minute = int(account_ctx.get("entry_minute", self.cfg.entry_minute))
        return datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=entry_hour % 24,
            minute=entry_minute % 60,
            second=0,
            microsecond=0,
            tzinfo=self.timezone,
        )

    def _should_run_entry(self, account_id: str, now_local: datetime) -> bool:
        today = now_local.date()
        if self._last_entry_local_date_by_account.get(account_id) == today:
            return False

        target = self._entry_schedule_for_day(today, account_id=account_id)
        if now_local < target:
            return False

        if not self.cfg.entry_catchup_enabled and now_local > target:
            if self._last_entry_skipped_date_by_account.get(account_id) != today:
                LOGGER.warning(
                    "Entry missed scheduled time and catch-up disabled, skip for today: account=%s now=%s target=%s",
                    account_id,
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                )
                self._last_entry_skipped_date_by_account[account_id] = today
            self._last_entry_local_date_by_account[account_id] = today
            return False

        grace = timedelta(minutes=max(0, self.cfg.entry_misfire_grace_min))
        if now_local - target > grace:
            if self._last_entry_skipped_date_by_account.get(account_id) != today:
                LOGGER.warning(
                    "Entry missed beyond grace window, skip for today: account=%s now=%s target=%s grace_min=%s",
                    account_id,
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                    self.cfg.entry_misfire_grace_min,
                )
                self._last_entry_skipped_date_by_account[account_id] = today
            self._last_entry_local_date_by_account[account_id] = today
            return False

        return True

    @staticmethod
    def _as_bool(value: object) -> bool:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _portfolio_loss_cut_settings(self, account_id: str) -> tuple[bool, float, int, int]:
        ctx = self.account_runtimes.get(account_id, {})
        enabled = self._as_bool(ctx.get("portfolio_loss_cut_enabled", self.cfg.portfolio_loss_cut_enabled))
        try:
            loss_pct = float(ctx.get("portfolio_loss_cut_pct", self.cfg.portfolio_loss_cut_pct))
        except (TypeError, ValueError):
            loss_pct = float(self.cfg.portfolio_loss_cut_pct)
        try:
            reset_hour = int(ctx.get("portfolio_loss_cut_hour", self.cfg.portfolio_loss_cut_hour))
        except (TypeError, ValueError):
            reset_hour = int(self.cfg.portfolio_loss_cut_hour)
        try:
            reset_minute = int(ctx.get("portfolio_loss_cut_minute", self.cfg.portfolio_loss_cut_minute))
        except (TypeError, ValueError):
            reset_minute = int(self.cfg.portfolio_loss_cut_minute)
        return enabled, min(100.0, max(0.001, loss_pct)), reset_hour % 24, reset_minute % 60

    def _portfolio_take_profit_settings(
        self,
        account_id: str,
    ) -> tuple[bool, float, int, int, float, float]:
        ctx = self.account_runtimes.get(account_id, {})
        enabled = self._as_bool(
            ctx.get("portfolio_take_profit_enabled", self.cfg.portfolio_take_profit_enabled)
        )
        try:
            profit_pct = float(ctx.get("portfolio_take_profit_pct", self.cfg.portfolio_take_profit_pct))
        except (TypeError, ValueError):
            profit_pct = float(self.cfg.portfolio_take_profit_pct)
        try:
            reset_hour = int(ctx.get("portfolio_take_profit_hour", self.cfg.portfolio_take_profit_hour))
        except (TypeError, ValueError):
            reset_hour = int(self.cfg.portfolio_take_profit_hour)
        try:
            reset_minute = int(ctx.get("portfolio_take_profit_minute", self.cfg.portfolio_take_profit_minute))
        except (TypeError, ValueError):
            reset_minute = int(self.cfg.portfolio_take_profit_minute)
        try:
            reduce_ratio = float(
                ctx.get(
                    "portfolio_take_profit_reduce_ratio",
                    self.cfg.portfolio_take_profit_reduce_ratio,
                )
            )
        except (TypeError, ValueError):
            reduce_ratio = float(self.cfg.portfolio_take_profit_reduce_ratio)
        try:
            giveback_pct = float(
                ctx.get(
                    "portfolio_take_profit_giveback_pct",
                    self.cfg.portfolio_take_profit_giveback_pct,
                )
            )
        except (TypeError, ValueError):
            giveback_pct = float(self.cfg.portfolio_take_profit_giveback_pct)
        return (
            enabled,
            min(100.0, max(0.001, profit_pct)),
            reset_hour % 24,
            reset_minute % 60,
            min(1.0, max(0.05, reduce_ratio)),
            min(100.0, max(0.0, giveback_pct)),
        )

    @staticmethod
    def _is_entry_result_complete(result: object) -> bool:
        if not isinstance(result, dict):
            return False
        status = str(result.get("status", "")).strip().upper()
        return status in {"SUCCESS", "SKIPPED", "DISABLED"}

    @staticmethod
    def _account_task_succeeded(result: object) -> bool:
        if not isinstance(result, dict):
            return False
        if result.get("error") or result.get("slow") or result.get("running"):
            return False
        try:
            if int(result.get("errors", 0) or 0) > 0:
                return False
            if int(result.get("failed", 0) or 0) > 0:
                return False
        except (TypeError, ValueError):
            return False
        return not bool(result.get("failed_symbols"))

    def _collect_entry_futures(self, now_local: datetime) -> None:
        timeout_sec = max(0.1, float(self.cfg.account_task_timeout_sec))
        for future, aid in list(self._entry_futures.items()):
            if not future.done():
                started_at = self._entry_future_started_at_by_account.get(aid)
                if (
                    started_at is not None
                    and time.monotonic() - started_at >= timeout_sec
                    and not self._entry_future_warned_by_account.get(aid, False)
                ):
                    self._entry_future_warned_by_account[aid] = True
                    LOGGER.warning(
                        "service entry account exceeded soft-timeout %.2fs, keep running in background account=%s",
                        timeout_sec,
                        aid,
                    )
                continue

            self._entry_futures.pop(future, None)
            trade_day = self._entry_future_trade_day_by_future.pop(future, now_local.date())
            self._entry_future_started_at_by_account.pop(aid, None)
            self._entry_future_warned_by_account.pop(aid, None)
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("service entry background failed account=%s: %s", aid, exc)
                continue
            if self._is_entry_result_complete(result):
                self._last_entry_local_date_by_account[aid] = trade_day
            LOGGER.info("service entry background result account=%s: %s", aid, result)

    def _run_entry_if_due(self, now_local: datetime) -> None:
        self._collect_entry_futures(now_local)
        account_ids = [
            aid
            for aid, ctx in self.account_runtimes.items()
            if str(ctx.get("mode", "full")).strip().lower() == "full"
        ]
        if not account_ids:
            LOGGER.warning("service entry skipped: no account is enabled for full mode")
            return

        running_account_ids = set(self._entry_futures.values())
        due_account_ids = []
        pending_wait_by_account: Dict[str, bool] = {}
        for aid in account_ids:
            if aid in running_account_ids:
                continue
            strategy = self.account_runtimes[aid].get("strategy")
            has_pending_wait = bool(
                strategy is not None
                and hasattr(strategy, "has_pending_entry_wait")
                and strategy.has_pending_entry_wait()  # type: ignore[attr-defined]
            )
            pending_wait_by_account[aid] = has_pending_wait
            if has_pending_wait or self._should_run_entry(aid, now_local):
                due_account_ids.append(aid)
        if not due_account_ids:
            return

        entry_safe_account_ids: List[str] = []
        for aid in due_account_ids:
            manager = self.account_runtimes[aid].get("manager")
            cleanup = getattr(manager, "cleanup_portfolio_take_profit_orders_before_entry", None)
            if not callable(cleanup):
                entry_safe_account_ids.append(aid)
                continue
            try:
                cleanup_result = cleanup()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("service pre-entry portfolio limit cleanup failed account=%s: %s", aid, exc)
                continue
            if int(cleanup_result.get("failed", 0) or 0) > 0:
                LOGGER.warning(
                    "service entry skipped because portfolio limit cleanup is incomplete account=%s result=%s",
                    aid,
                    cleanup_result,
                )
                continue
            entry_safe_account_ids.append(aid)
        due_account_ids = entry_safe_account_ids
        if not due_account_ids:
            return

        shared_top_gainers = self._get_entry_ranking(now_local, due_account_ids)
        submitted: List[str] = []
        for aid in due_account_ids:
            strategy = self.account_runtimes[aid].get("strategy")
            if strategy is None:
                LOGGER.warning("service entry skipped account=%s: strategy_missing", aid)
                continue
            trade_day = now_local.date()
            if pending_wait_by_account.get(aid, False) and hasattr(strategy, "get_pending_entry_trade_day"):
                pending_trade_day = strategy.get_pending_entry_trade_day()  # type: ignore[attr-defined]
                if isinstance(pending_trade_day, date):
                    trade_day = pending_trade_day
            future = self._entry_executor.submit(
                self._run_entry_with_shared,
                strategy,
                shared_top_gainers,
                trade_day,
            )
            self._entry_futures[future] = aid
            self._entry_future_trade_day_by_future[future] = trade_day
            self._entry_future_started_at_by_account[aid] = time.monotonic()
            self._entry_future_warned_by_account[aid] = False
            submitted.append(aid)
        if submitted:
            LOGGER.info("service entry submitted background accounts=%s", submitted)
            self._collect_entry_futures(now_local)

    def _get_entry_ranking(
        self,
        now_local: datetime,
        due_account_ids: List[str],
    ) -> Optional[List[Dict[str, Any]]]:
        today = now_local.date()
        if (
            self._shared_ranking_cache is not None
            and self._shared_ranking_cache_day == today
            and self._shared_ranking_cache_expires_at is not None
            and now_local <= self._shared_ranking_cache_expires_at
        ):
            return self._shared_ranking_cache

        shared_top_gainers = self._build_shared_top_gainers(due_account_ids)
        if shared_top_gainers is None:
            return None

        self._shared_ranking_cache = shared_top_gainers
        self._shared_ranking_cache_day = today
        self._shared_ranking_cache_expires_at = now_local + timedelta(minutes=10)
        return shared_top_gainers

    def _run_entry_with_shared(
        self,
        strategy: object,
        shared_top_gainers: Optional[List[Dict[str, Any]]],
        trade_day: date,
    ) -> Dict[str, object]:
        run_entry = getattr(strategy, "run_entry", None)
        if run_entry is None:
            raise RuntimeError("strategy_missing")
        kwargs: Dict[str, object] = {}
        try:
            parameters = inspect.signature(run_entry).parameters
            accepts_kwargs = any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())
            if "trade_day_utc" in parameters or accepts_kwargs:
                kwargs["trade_day_utc"] = trade_day.isoformat()
            if shared_top_gainers is not None:
                kwargs["shared_top_gainers"] = shared_top_gainers
        except (TypeError, ValueError):
            # Builtin/mock callables may not expose a signature; actual strategies
            # accept both keywords, so keep the complete call as the fallback.
            kwargs = {"trade_day_utc": trade_day.isoformat()}
            if shared_top_gainers is not None:
                kwargs["shared_top_gainers"] = shared_top_gainers
        return run_entry(**kwargs)  # type: ignore[misc]

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
                rate_limit_coordinator=getattr(reference_strategy.client, "rate_limit_coordinator", None),
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

    def _run_account_task_calls(
        self,
        calls: Dict[str, Callable[[], object]],
        task_name: str,
        task_day: Optional[date] = None,
    ) -> Dict[str, object]:
        if not calls:
            return {}
        results: Dict[str, object] = {}
        futures: Dict[Future, str] = {}
        for aid, call in calls.items():
            key = (task_name, aid)
            existing = self._scheduled_futures_by_task_account.get(key)
            if existing is not None:
                if not existing.done():
                    results[aid] = {"slow": True, "running": True}
                    continue
                self._scheduled_futures_by_task_account.pop(key, None)
                existing_day = self._scheduled_future_day_by_task_account.pop(key, None)
                try:
                    existing_result = existing.result()
                except Exception as exc:  # noqa: BLE001
                    existing_result = {"error": str(exc)}
                if task_day is None or existing_day is None or existing_day == task_day:
                    results[aid] = existing_result
                    continue
                LOGGER.warning(
                    "Discard completed stale scheduled task result and submit current day: task=%s account=%s previous_day=%s current_day=%s",
                    task_name,
                    aid,
                    existing_day,
                    task_day,
                )
            future = self._scheduled_executor.submit(call)
            self._scheduled_futures_by_task_account[key] = future
            if task_day is not None:
                self._scheduled_future_day_by_task_account[key] = task_day
            futures[future] = aid
        if not futures:
            return results
        timeout_sec = max(0.1, float(self.cfg.account_task_timeout_sec))
        done, pending = wait(set(futures), timeout=timeout_sec)
        for future in done:
            aid = futures[future]
            self._scheduled_futures_by_task_account.pop((task_name, aid), None)
            self._scheduled_future_day_by_task_account.pop((task_name, aid), None)
            try:
                results[aid] = future.result()
            except Exception as exc:  # noqa: BLE001
                results[aid] = {"error": str(exc)}
        for future in pending:
            aid = futures[future]
            LOGGER.warning(
                "service %s account exceeded hard scheduler timeout %.2fs, continue in background account=%s",
                task_name,
                timeout_sec,
                aid,
            )
            results[aid] = {"slow": True, "running": True}
        return results

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
        today = now_local.date()
        if self._last_loss_cut_local_date == today:
            return
        target = self._loss_cut_schedule_for_day(today)
        if now_local < target:
            return
        window_minutes = max(1, int(getattr(self.cfg, "daily_loss_cut_grace_min", 30)))
        window_end = target + timedelta(minutes=window_minutes)
        if now_local >= window_end:
            if self._last_loss_cut_skipped_date != today:
                LOGGER.warning(
                    "Daily loss-cut missed execution window, skip for today: now=%s target=%s window=%smin",
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                    window_minutes,
                )
                self._last_loss_cut_skipped_date = today
            self._last_loss_cut_local_date = today
            return

        account_ids = []
        for aid, ctx in self.account_runtimes.items():
            mode = str(ctx.get("mode", "full")).strip().lower()
            if mode not in {"full", "loss_cut_only"}:
                continue
            enabled_raw = ctx.get("daily_loss_cut_enabled", self.cfg.daily_loss_cut_enabled)
            if isinstance(enabled_raw, str):
                enabled = enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
            else:
                enabled = bool(enabled_raw)
            if not enabled:
                continue
            account_ids.append(aid)
        if not account_ids:
            LOGGER.warning("service daily loss-cut skipped: no account is enabled")
            self._last_loss_cut_local_date = today
            return

        results: Dict[str, object] = {}
        calls: Dict[str, Callable[[], object]] = {}
        for aid in account_ids:
            manager = self.account_runtimes[aid].get("manager")
            if manager is None or not hasattr(manager, "run_daily_loss_cut"):
                results[aid] = {"error": "manager_missing"}
            else:
                calls[aid] = manager.run_daily_loss_cut  # type: ignore[attr-defined]
        results.update(self._run_account_task_calls(calls, "daily-loss-cut", task_day=today))
        if len(results) == len(account_ids) and all(
            self._account_task_succeeded(results.get(aid)) for aid in account_ids
        ):
            self._last_loss_cut_local_date = today
        else:
            LOGGER.warning("service daily loss-cut not fully successful, will retry in the same window: %s", results)
        LOGGER.info("service daily loss-cut result: %s", results)

    def _noon_protection_schedule_for_day(self, day: date) -> datetime:
        return datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=self.cfg.noon_protection_hour % 24,
            minute=self.cfg.noon_protection_minute % 60,
            second=0,
            microsecond=0,
            tzinfo=self.timezone,
        )

    def _run_noon_protection_if_due(self, now_local: datetime) -> None:
        today = now_local.date()
        target = self._noon_protection_schedule_for_day(today)
        if self._noon_protection_retry_local_date != today:
            self._noon_protection_pending_symbols_by_account = {}
            self._noon_protection_retry_due_local = None
            self._noon_protection_retry_local_date = None

        is_retry = self._last_noon_protection_local_date == today
        if is_retry:
            if not self._noon_protection_pending_symbols_by_account:
                return
            if (
                self._noon_protection_retry_due_local is not None
                and now_local < self._noon_protection_retry_due_local
            ):
                return
        else:
            if now_local < target:
                return
            if now_local - target > PROTECTION_RESTART_GRACE:
                LOGGER.warning(
                    "Noon protection missed beyond restart grace, skip for today: now=%s target=%s grace_hours=2",
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                )
                self._last_noon_protection_local_date = today
                return

        day_start_local = target.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_utc = day_start_local.astimezone(timezone.utc)
        noon_time_utc = target.astimezone(timezone.utc)
        if is_retry:
            account_ids = list(self._noon_protection_pending_symbols_by_account)
        else:
            account_ids = []
            for aid, ctx in self.account_runtimes.items():
                if str(ctx.get("mode", "full")).strip().lower() not in {"full", "loss_cut_only"}:
                    continue
                enabled_raw = ctx.get("noon_protection_enabled", self.cfg.noon_protection_enabled)
                if isinstance(enabled_raw, str):
                    enabled = enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
                else:
                    enabled = bool(enabled_raw)
                if enabled:
                    account_ids.append(aid)
        self._last_noon_protection_local_date = today
        if not account_ids:
            LOGGER.warning("service noon protection skipped: no account is enabled")
            return

        results: Dict[str, object] = {}
        calls: Dict[str, Callable[[], object]] = {}
        for aid in account_ids:
            manager = self.account_runtimes[aid].get("manager")
            if manager is None or not hasattr(manager, "run_noon_protection_stop"):
                results[aid] = {"error": "manager_missing"}
            else:
                retry_symbols = (
                    self._noon_protection_pending_symbols_by_account.get(aid)
                    if is_retry
                    else None
                )
                if is_retry and retry_symbols:
                    calls[aid] = lambda manager=manager, retry_symbols=frozenset(retry_symbols): manager.run_noon_protection_stop(  # type: ignore[attr-defined]
                        day_start_utc=day_start_utc,
                        noon_time_utc=noon_time_utc,
                        symbols=set(retry_symbols),
                    )
                else:
                    calls[aid] = lambda manager=manager: manager.run_noon_protection_stop(  # type: ignore[attr-defined]
                        day_start_utc=day_start_utc,
                        noon_time_utc=noon_time_utc,
                    )
        results.update(self._run_account_task_calls(calls, "noon-protection", task_day=today))
        LOGGER.info("service noon protection result: %s", results)

        pending_symbols_by_account: Dict[str, Optional[Set[str]]] = {}
        for aid, result in results.items():
            if not isinstance(result, dict):
                pending_symbols_by_account[aid] = None
                continue
            raw_failed_symbols = result.get("failed_symbols")
            failed_symbols = {
                str(symbol or "").strip().upper()
                for symbol in (raw_failed_symbols if isinstance(raw_failed_symbols, (list, tuple, set)) else [])
                if str(symbol or "").strip()
            }
            try:
                error_count = int(result.get("errors", 0) or 0)
            except (TypeError, ValueError):
                error_count = 1
            needs_retry = bool(
                failed_symbols
                or error_count > 0
                or result.get("error")
                or result.get("slow")
                or result.get("running")
            )
            if needs_retry:
                pending_symbols_by_account[aid] = failed_symbols or None

        if pending_symbols_by_account:
            self._noon_protection_pending_symbols_by_account = pending_symbols_by_account
            self._noon_protection_retry_local_date = today
            retry_interval = max(1.0, float(self.cfg.noon_protection_retry_interval_sec))
            self._noon_protection_retry_due_local = now_local + timedelta(seconds=retry_interval)
            LOGGER.warning(
                "service noon protection pending retry accounts=%s retry_in_sec=%s",
                pending_symbols_by_account,
                retry_interval,
            )
        else:
            self._noon_protection_pending_symbols_by_account = {}
            self._noon_protection_retry_local_date = None
            self._noon_protection_retry_due_local = None

    def _morning_protection_schedule_for_day(self, day: date, account_id: Optional[str] = None) -> datetime:
        account_ctx = self.account_runtimes.get(account_id or "", {})
        morning_hour = int(account_ctx.get("morning_protection_hour", self.cfg.morning_protection_hour))
        morning_minute = int(account_ctx.get("morning_protection_minute", self.cfg.morning_protection_minute))
        return datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=morning_hour % 24,
            minute=morning_minute % 60,
            second=0,
            microsecond=0,
            tzinfo=self.timezone,
        )

    def _run_morning_protection_if_due(self, now_local: datetime) -> None:
        results: Dict[str, object] = {}

        for aid, ctx in self.account_runtimes.items():
            mode = str(ctx.get("mode", "full")).strip().lower()
            if mode not in {"full", "loss_cut_only"}:
                continue

            enabled_raw = ctx.get("morning_protection_enabled", self.cfg.morning_protection_enabled)
            if isinstance(enabled_raw, str):
                enabled = enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
            else:
                enabled = bool(enabled_raw)
            if not enabled:
                continue

            today = now_local.date()
            if self._last_morning_protection_local_date_by_account.get(aid) == today:
                continue

            target = self._morning_protection_schedule_for_day(today, account_id=aid)
            if now_local < target:
                continue
            if now_local - target > PROTECTION_RESTART_GRACE:
                LOGGER.warning(
                    "Morning protection missed beyond restart grace, skip for today: account=%s now=%s target=%s grace_hours=2",
                    aid,
                    now_local.isoformat(timespec="seconds"),
                    target.isoformat(timespec="seconds"),
                )
                self._last_morning_protection_local_date_by_account[aid] = today
                continue

            manager = ctx.get("manager")
            if manager is None or not hasattr(manager, "run_morning_protection_stop"):
                results[aid] = {"error": "manager_missing"}
                continue

            min_hold_hours = float(
                ctx.get(
                    "morning_protection_min_hold_hours",
                    self.cfg.morning_protection_min_hold_hours,
                )
            )
            try:
                results[aid] = manager.run_morning_protection_stop(  # type: ignore[attr-defined]
                    check_time_utc=target.astimezone(timezone.utc),
                    min_hold_hours=min_hold_hours,
                )
            except Exception as exc:  # noqa: BLE001
                results[aid] = {"error": str(exc)}
            if self._account_task_succeeded(results[aid]):
                self._last_morning_protection_local_date_by_account[aid] = today

        if results:
            LOGGER.info("service morning protection result: %s", results)

    def _run_hourly_exchange_take_profit_if_due(self, now_local: datetime) -> None:
        results: Dict[str, object] = {}
        hour_key = now_local.strftime("%Y-%m-%dT%H")

        for aid, ctx in self.account_runtimes.items():
            mode = str(ctx.get("mode", "full")).strip().lower()
            if mode not in {"full", "loss_cut_only"}:
                continue

            enabled_raw = ctx.get(
                "hourly_exchange_take_profit_enabled",
                self.cfg.hourly_exchange_take_profit_enabled,
            )
            if isinstance(enabled_raw, str):
                enabled = enabled_raw.strip().lower() in {"1", "true", "yes", "on"}
            else:
                enabled = bool(enabled_raw)
            if not enabled:
                continue

            target_minute = int(
                ctx.get(
                    "hourly_exchange_take_profit_minute",
                    self.cfg.hourly_exchange_take_profit_minute,
                )
            ) % 60
            if now_local.minute < target_minute:
                continue
            if self._last_hourly_exchange_take_profit_hour_by_account.get(aid) == hour_key:
                continue

            manager = ctx.get("manager")
            if manager is None or not hasattr(manager, "run_hourly_exchange_take_profit"):
                results[aid] = {"error": "manager_missing"}
                continue

            drop_pct = float(
                ctx.get(
                    "hourly_exchange_take_profit_drop_pct",
                    self.cfg.hourly_exchange_take_profit_drop_pct,
                )
            )
            try:
                results[aid] = manager.run_hourly_exchange_take_profit(  # type: ignore[attr-defined]
                    now_local=now_local,
                    drop_pct=drop_pct,
                )
            except Exception as exc:  # noqa: BLE001
                results[aid] = {"error": str(exc)}
            if self._account_task_succeeded(results[aid]):
                self._last_hourly_exchange_take_profit_hour_by_account[aid] = hour_key

        if results:
            LOGGER.info("service hourly exchange take-profit result: %s", results)

    def _run_manage_if_due(self, now_monotonic: float, now_local: Optional[datetime] = None) -> None:
        if now_monotonic < self._next_manage_monotonic:
            return

        run_count = 0
        while now_monotonic >= self._next_manage_monotonic and run_count < max(1, self.cfg.manager_max_catch_up_runs):
            summary = self.run_manage_tick(now_local=now_local)
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

    def _run_manage_for_account(
        self,
        account_id: str,
        now_local: Optional[datetime] = None,
    ) -> Dict[str, object]:
        ctx = self.account_runtimes[account_id]
        manager = ctx["manager"]
        strategy = ctx.get("strategy")
        balance_sampler = ctx.get("balance_sampler")
        local_dt = now_local or datetime.now(self.timezone)
        wallet_summary: Optional[Dict[str, object]] = None
        portfolio_result: Optional[Dict[str, object]] = None
        portfolio_take_profit_result: Optional[Dict[str, object]] = None

        pending_entry_recovery = None
        if strategy is not None and hasattr(strategy, "recover_pending_entries"):
            pending_entry_recovery = strategy.recover_pending_entries()  # type: ignore[attr-defined]
            if isinstance(pending_entry_recovery, dict) and int(pending_entry_recovery.get("total", 0)) > 0:
                LOGGER.warning(
                    "service pending entry recovery account=%s: %s",
                    account_id,
                    pending_entry_recovery,
                )

        preclose_structure_recovery = None
        if strategy is not None and hasattr(strategy, "recover_preclose_structure_protections"):
            preclose_structure_recovery = strategy.recover_preclose_structure_protections()  # type: ignore[attr-defined]
            if (
                isinstance(preclose_structure_recovery, dict)
                and int(preclose_structure_recovery.get("total", 0)) > 0
            ):
                LOGGER.warning(
                    "service preclose structure recovery account=%s: %s",
                    account_id,
                    preclose_structure_recovery,
                )

        pending_recovery = None
        if strategy is not None and hasattr(strategy, "recover_pending_exit_setups"):
            pending_recovery = strategy.recover_pending_exit_setups()  # type: ignore[attr-defined]
            if isinstance(pending_recovery, dict) and int(pending_recovery.get("total", 0)) > 0:
                LOGGER.warning("service pending exit setup recovery account=%s: %s", account_id, pending_recovery)

        summary = manager.run_once()  # type: ignore[attr-defined]
        if balance_sampler is not None:
            try:
                sampled = balance_sampler.run_once()  # type: ignore[attr-defined]
                if isinstance(sampled, dict):
                    wallet_summary = sampled
                LOGGER.info("service wallet snapshot account=%s: %s", account_id, wallet_summary)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("service wallet snapshot failed account=%s: %s", account_id, exc)

        (
            take_profit_enabled,
            profit_pct,
            take_profit_hour,
            take_profit_minute,
            reduce_ratio,
            giveback_pct,
        ) = self._portfolio_take_profit_settings(account_id)
        if take_profit_enabled and hasattr(manager, "run_portfolio_take_profit"):
            equity = wallet_summary.get("equity") if wallet_summary is not None else None
            if equity is None:
                portfolio_take_profit_result = {
                    "status": "SKIPPED",
                    "reason": "NO_CURRENT_EQUITY_SNAPSHOT",
                }
            else:
                try:
                    result = manager.run_portfolio_take_profit(  # type: ignore[attr-defined]
                        current_equity_usdt=float(equity),
                        now_local=local_dt,
                        profit_pct=profit_pct,
                        reset_hour=take_profit_hour,
                        reset_minute=take_profit_minute,
                        reduce_ratio=reduce_ratio,
                        giveback_pct=giveback_pct,
                    )
                    if isinstance(result, dict):
                        portfolio_take_profit_result = result
                        if str(result.get("status") or "").upper() in {"TRIGGERED", "TRIGGERED_RETRY"}:
                            LOGGER.warning(
                                "service portfolio take-profit account=%s result=%s",
                                account_id,
                                result,
                            )
                except Exception as exc:  # noqa: BLE001
                    portfolio_take_profit_result = {"status": "ERROR", "error": str(exc)}
                    LOGGER.exception("service portfolio take-profit failed account=%s: %s", account_id, exc)

        enabled, loss_pct, reset_hour, reset_minute = self._portfolio_loss_cut_settings(account_id)
        if enabled and hasattr(manager, "run_portfolio_loss_cut"):
            equity = wallet_summary.get("equity") if wallet_summary is not None else None
            if equity is None:
                portfolio_result = {"status": "SKIPPED", "reason": "NO_CURRENT_EQUITY_SNAPSHOT"}
            else:
                try:
                    result = manager.run_portfolio_loss_cut(  # type: ignore[attr-defined]
                        current_equity_usdt=float(equity),
                        now_local=local_dt,
                        loss_pct=loss_pct,
                        reset_hour=reset_hour,
                        reset_minute=reset_minute,
                    )
                    if isinstance(result, dict):
                        portfolio_result = result
                        if bool(result.get("triggered")):
                            LOGGER.warning(
                                "service portfolio loss-cut account=%s result=%s",
                                account_id,
                                result,
                            )
                except Exception as exc:  # noqa: BLE001
                    portfolio_result = {"status": "ERROR", "error": str(exc)}
                    LOGGER.exception("service portfolio loss-cut failed account=%s: %s", account_id, exc)

        # The fixed daily-baseline rule replaces the legacy rolling-low
        # recovery rule for an account; never let both exit engines act on the
        # same positions in one manage cycle.
        if (
            not take_profit_enabled
            and strategy is not None
            and hasattr(strategy, "run_equity_recovery_take_profit")
        ):
            try:
                result = strategy.run_equity_recovery_take_profit()  # type: ignore[attr-defined]
                if isinstance(result, dict) and result.get("status") in {"TRIGGERED", "PARTIAL"}:
                    LOGGER.info("service equity recovery take-profit account=%s result: %s", account_id, result)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("service equity recovery take-profit failed account=%s: %s", account_id, exc)

        return {
            "account_id": account_id,
            "summary": summary,
            "pending_entry_recovery": pending_entry_recovery,
            "preclose_structure_recovery": preclose_structure_recovery,
            "pending_recovery": pending_recovery,
            "wallet_summary": wallet_summary,
            "portfolio_take_profit": portfolio_take_profit_result,
            "portfolio_loss_cut": portfolio_result,
        }

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

    def run_manage_tick(self, now_local: Optional[datetime] = None) -> Dict[str, Dict[str, object]]:
        self.cycle_no += 1
        account_ids = [
            aid
            for aid, ctx in self.account_runtimes.items()
            if str(ctx.get("mode", "full")).strip().lower() == "full"
        ]
        outputs: Dict[str, Dict[str, object]] = {}
        completed_accounts = set()
        for aid, future in list(self._manage_futures_by_account.items()):
            if not future.done():
                continue
            self._manage_futures_by_account.pop(aid, None)
            self._consume_manage_future(aid, future, outputs)
            completed_accounts.add(aid)

        eligible_accounts = []
        for aid in account_ids:
            if aid in completed_accounts:
                continue
            if aid in self._manage_futures_by_account:
                outputs.setdefault(
                    aid,
                    {"account_id": aid, "slow": True, "running": True, "reason": "PREVIOUS_CYCLE_STILL_RUNNING"},
                )
                continue
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

        submitted: Dict[Future, str] = {}
        for aid in eligible_accounts:
            future = self._manage_executor.submit(self._run_manage_for_account, aid, now_local)
            self._manage_futures_by_account[aid] = future
            submitted[future] = aid

        if submitted:
            timeout_sec = max(0.1, float(self.cfg.account_task_timeout_sec))
            done, pending = wait(set(submitted), timeout=timeout_sec)
            for future in done:
                aid = submitted[future]
                self._manage_futures_by_account.pop(aid, None)
                self._consume_manage_future(aid, future, outputs)
            for future in pending:
                aid = submitted[future]
                LOGGER.warning(
                    "service manage account exceeded hard scheduler timeout %.2fs, continue in background account=%s",
                    timeout_sec,
                    aid,
                )
                outputs[aid] = {"account_id": aid, "slow": True, "running": True}
        return outputs

    def _consume_manage_future(
        self,
        account_id: str,
        future: Future,
        outputs: Dict[str, Dict[str, object]],
    ) -> None:
        try:
            outputs[account_id] = future.result()
            self.account_states.setdefault(account_id, AccountRuntimeState()).failures = 0
        except Exception as exc:  # noqa: BLE001
            self._record_account_failure(account_id, str(exc))
            outputs[account_id] = {"account_id": account_id, "error": str(exc)}

    def _orphan_exit_order_cleanup_schedule_for_day(self, day: date) -> datetime:
        return datetime(
            year=day.year,
            month=day.month,
            day=day.day,
            hour=self.cfg.orphan_exit_order_cleanup_hour % 24,
            minute=self.cfg.orphan_exit_order_cleanup_minute % 60,
            second=0,
            microsecond=0,
            tzinfo=self.timezone,
        )

    def _run_orphan_exit_order_cleanup_if_due(self, now_local: datetime) -> None:
        if not self.cfg.orphan_exit_order_cleanup_enabled:
            return
        today = now_local.date()
        if self._last_orphan_exit_order_cleanup_local_date == today:
            return
        target = self._orphan_exit_order_cleanup_schedule_for_day(today)
        if now_local < target:
            return
        results: Dict[str, Dict[str, object]] = {}
        for aid, ctx in self.account_runtimes.items():
            if str(ctx.get("mode", "full")).strip().lower() != "full":
                continue
            manager = ctx.get("manager")
            if manager is None or not hasattr(manager, "cleanup_orphan_exit_orders_once_per_day"):
                results[aid] = {"account_id": aid, "error": "MANAGER_UNAVAILABLE"}
                continue
            try:
                results[aid] = manager.cleanup_orphan_exit_orders_once_per_day()  # type: ignore[attr-defined]
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("service orphan exit order cleanup failed account=%s: %s", aid, exc)
                results[aid] = {"account_id": aid, "error": str(exc)}
        if not any(
            "error" in result or int(result.get("failed", 0)) > 0
            for result in results.values()
        ):
            self._last_orphan_exit_order_cleanup_local_date = today
        LOGGER.info("service orphan exit order cleanup summary: %s", results)

    def run_cycle(
        self,
        now_local: Optional[datetime] = None,
        now_monotonic: Optional[float] = None,
    ) -> None:
        local_dt = now_local or datetime.now(self.timezone)
        mono = now_monotonic if now_monotonic is not None else time.monotonic()
        self._run_entry_if_due(local_dt)
        self._run_daily_loss_cut_if_due(local_dt)
        self._run_noon_protection_if_due(local_dt)
        self._run_morning_protection_if_due(local_dt)
        self._run_hourly_exchange_take_profit_if_due(local_dt)
        self._run_orphan_exit_order_cleanup_if_due(local_dt)
        self._run_manage_if_due(mono, now_local=local_dt)
        self._run_balance_snapshot_for_readonly_accounts(mono)

    def _run_balance_snapshot_for_readonly_accounts(self, now_monotonic: float) -> None:
        """为 readonly 账户执行余额快照采集。"""
        readonly_accounts = [
            aid
            for aid, ctx in self.account_runtimes.items()
            if str(ctx.get("mode", "full")).strip().lower() == "readonly"
        ]
        if not readonly_accounts:
            return

        for aid in readonly_accounts:
            ctx = self.account_runtimes.get(aid)
            if not ctx:
                continue
            balance_sampler = ctx.get("balance_sampler")
            if balance_sampler is None:
                continue
            next_due = self._next_readonly_wallet_snapshot_monotonic_by_account.get(aid, 0.0)
            if now_monotonic < next_due:
                continue
            try:
                wallet_summary = balance_sampler.run_once()  # type: ignore[attr-defined]
                LOGGER.info("service readonly wallet snapshot account=%s: %s", aid, wallet_summary)
                interval = max(1.0, float(self.cfg.readonly_wallet_snapshot_interval_sec))
                self._next_readonly_wallet_snapshot_monotonic_by_account[aid] = now_monotonic + interval
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("service readonly wallet snapshot failed account=%s: %s", aid, exc)

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
        try:
            while not stopper.is_set():
                try:
                    self.run_cycle()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception("runtime service cycle failed: %s", exc)
                stopper.wait(timeout=max(0.2, self.cfg.loop_sleep_sec))
        finally:
            for ctx in self.account_runtimes.values():
                strategy = ctx.get("strategy")
                if strategy is not None and hasattr(strategy, "request_entry_wait_stop"):
                    try:
                        strategy.request_entry_wait_stop()  # type: ignore[attr-defined]
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning("failed to stop entry wait during service shutdown: %s", exc)
            self._entry_executor.shutdown(wait=True, cancel_futures=True)
            self._manage_executor.shutdown(wait=True, cancel_futures=True)
            self._scheduled_executor.shutdown(wait=True, cancel_futures=True)
            LOGGER.info("runtime service stopped")
