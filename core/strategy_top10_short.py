import logging
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
import hashlib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

from core.entry_structure_protection import (
    EntryStructureProtection,
    EntryStructureProtectionState,
)
from core.market_fill_reconciler import MarketFillReconciler
from core.state_store import StateStore
from infra.binance_futures_client import BinanceAPIError, BinanceFuturesClient, OrderStateUnknownError
from infra.binance_top10_monitor import build_top_gainers
from infra.notifier import (
    ServerChanNotifier,
    format_markdown_kv_table,
    format_markdown_list_section,
)

LOGGER = logging.getLogger(__name__)


def _serialized_account_mutation(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with self._mutation_lock:
            return method(self, *args, **kwargs)

    return wrapped


@dataclass(frozen=True)
class RankEntry:
    symbol: str
    pct_change: float
    last_price: float
    quote_volume: float


@dataclass(frozen=True)
class PlannedOrder:
    symbol: str
    base_margin_usdt: float
    target_notional_usdt: float
    qty: float


@dataclass(frozen=True)
class ReadyEntry:
    entry: RankEntry
    reference_price: float
    signal_time_utc: datetime
    bearish_close_time_utc: Optional[datetime]
    preclose_entry: bool = False
    preclose_time_utc: Optional[datetime] = None
    signal_hour_open_utc: Optional[datetime] = None
    provisional_open_price: Optional[float] = None
    provisional_close_price: Optional[float] = None


@dataclass(frozen=True)
class EntryStructureWindow:
    bearish_close_time_utc: datetime
    window_start_utc: datetime
    highest_price: float


@dataclass(frozen=True)
class RebalancePlan:
    position_id: int
    symbol: str
    side: str
    qty: float
    ref_price: float
    est_notional: float
    current_notional: float
    target_notional: float
    deviation_notional: float
    deadband_notional: float
    max_adjust_notional: float
    requested_adjust_notional: float


class Top10ShortStrategy:
    INSUFFICIENT_MARGIN_ERROR_CODES = {-2019, -2027, -2028}
    COOLING_OFF_ERROR_CODES = {-4192}
    REBALANCE_MODE_EQUAL_RISK = "equal_risk"
    REBALANCE_MODE_AGE_DECAY = "age_decay"
    EQUITY_RECOVERY_LOCK_NAME = "equity_recovery_take_profit_v1"
    ENTRY_WAIT_LOCK_NAME = "bearish_hour_entry_wait_v1"
    PENDING_EXIT_SETUP_RECOVERY_GRACE_SEC = 30.0

    def __init__(
        self,
        client: BinanceFuturesClient,
        store: StateStore,
        notifier: ServerChanNotifier,
        leverage: int,
        top_n: int,
        volume_threshold: float,
        tp_price_drop_pct: float,
        sl_liq_buffer_pct: float,
        max_hold_hours: float,
        trigger_price_type: str,
        allocation_splits: int,
        entry_fee_buffer_pct: float,
        entry_shrink_retry_count: int,
        entry_shrink_step_pct: float,
        entry_rank_fetch_multiplier: int,
        ranker_max_workers: int,
        ranker_weight_limit_per_minute: int,
        ranker_min_request_interval_ms: int,
        fixed_take_profit_enabled: bool = True,
        rebalance_enabled: bool = False,
        rebalance_pre_entry_reduce: bool = True,
        rebalance_after_entry: bool = True,
        rebalance_utilization: float = 0.9,
        rebalance_deadband_pct: float = 0.10,
        rebalance_min_adjust_notional_usdt: float = 20.0,
        rebalance_max_single_adjust_pct: float = 0.40,
        rebalance_max_adjust_orders: int = 30,
        rebalance_mode: str = REBALANCE_MODE_EQUAL_RISK,
        rebalance_age_decay_half_life_hours: float = 36.0,
        equity_recovery_take_profit_enabled: bool = False,
        equity_recovery_lookback_hours: float = 24.0,
        equity_recovery_trigger_pct: float = 0.10,
        equity_recovery_reduce_ratio: float = 0.50,
        entry_initial_delay_sec: int = 0,
        entry_symbol_interval_sec: int = 0,
        entry_wait_bearish_hour_enabled: bool = False,
        entry_wait_poll_sec: int = 30,
        entry_wait_close_grace_sec: float = 1.0,
        entry_wait_close_retry_sec: float = 1.0,
        entry_wait_close_retry_count: int = 5,
        entry_wait_max_hours: float = 16.0,
        entry_preclose_sec: float = 0.0,
        cooling_off_retry_count: int = 0,
        cooling_off_retry_delay_sec: int = 0,
        runtime_timezone: str = "Asia/Shanghai",
        account_id: str = "default",
        protection_exempt_symbols: Optional[Set[str]] = None,
        mutation_lock: Optional[Any] = None,
    ):
        self.client = client
        self.store = store
        self.notifier = notifier
        self.leverage = leverage
        self.top_n = top_n
        self.volume_threshold = volume_threshold
        self.tp_price_drop_pct = tp_price_drop_pct
        self.sl_liq_buffer_pct = sl_liq_buffer_pct
        self.max_hold_hours = max_hold_hours
        self.trigger_price_type = trigger_price_type
        self.allocation_splits = allocation_splits
        self.entry_fee_buffer_pct = min(95.0, max(0.0, float(entry_fee_buffer_pct)))
        self.entry_shrink_retry_count = max(0, int(entry_shrink_retry_count))
        self.entry_shrink_step_pct = min(50.0, max(1.0, float(entry_shrink_step_pct)))
        self.entry_rank_fetch_multiplier = max(1, int(entry_rank_fetch_multiplier))
        self.ranker_max_workers = max(1, int(ranker_max_workers))
        self.ranker_weight_limit_per_minute = max(100, int(ranker_weight_limit_per_minute))
        self.ranker_min_request_interval_ms = max(0, int(ranker_min_request_interval_ms))
        self.fixed_take_profit_enabled = bool(fixed_take_profit_enabled)
        self.rebalance_enabled = bool(rebalance_enabled)
        self.rebalance_pre_entry_reduce = bool(rebalance_pre_entry_reduce)
        self.rebalance_after_entry = bool(rebalance_after_entry)
        self.rebalance_utilization = min(0.99, max(0.1, float(rebalance_utilization)))
        self.rebalance_deadband_pct = min(0.5, max(0.0, float(rebalance_deadband_pct)))
        self.rebalance_min_adjust_notional_usdt = max(1.0, float(rebalance_min_adjust_notional_usdt))
        self.rebalance_max_single_adjust_pct = min(0.95, max(0.05, float(rebalance_max_single_adjust_pct)))
        self.rebalance_max_adjust_orders = max(1, int(rebalance_max_adjust_orders))
        self.rebalance_mode = self._normalize_rebalance_mode(rebalance_mode)
        self.rebalance_age_decay_half_life_hours = max(1.0, float(rebalance_age_decay_half_life_hours))
        self.equity_recovery_take_profit_enabled = bool(equity_recovery_take_profit_enabled)
        self.equity_recovery_lookback_hours = max(1.0, float(equity_recovery_lookback_hours))
        self.equity_recovery_trigger_pct = min(1.0, max(0.001, float(equity_recovery_trigger_pct)))
        self.equity_recovery_reduce_ratio = min(1.0, max(0.05, float(equity_recovery_reduce_ratio)))
        self.entry_initial_delay_sec = max(0, int(entry_initial_delay_sec))
        self.entry_symbol_interval_sec = max(0, int(entry_symbol_interval_sec))
        self.entry_wait_bearish_hour_enabled = bool(entry_wait_bearish_hour_enabled)
        self.entry_wait_poll_sec = max(1, int(entry_wait_poll_sec))
        self.entry_wait_close_grace_sec = max(0.0, float(entry_wait_close_grace_sec))
        self.entry_wait_close_retry_sec = max(0.1, float(entry_wait_close_retry_sec))
        self.entry_wait_close_retry_count = max(1, int(entry_wait_close_retry_count))
        self.entry_wait_max_hours = max(1.0, float(entry_wait_max_hours))
        self.entry_preclose_sec = min(59.0, max(0.0, float(entry_preclose_sec)))
        self.cooling_off_retry_count = max(0, int(cooling_off_retry_count))
        self.cooling_off_retry_delay_sec = max(0, int(cooling_off_retry_delay_sec))
        self.runtime_timezone_name = (runtime_timezone or "").strip() or "UTC"
        try:
            self.runtime_timezone = ZoneInfo(self.runtime_timezone_name)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Invalid runtime_timezone=%s, fallback to UTC", self.runtime_timezone_name)
            self.runtime_timezone_name = "UTC"
            self.runtime_timezone = ZoneInfo("UTC")
        self.account_id = (account_id or "").strip() or "default"
        self.protection_exempt_symbols = {
            str(symbol or "").strip().upper() for symbol in (protection_exempt_symbols or set()) if str(symbol or "").strip()
        }
        self._entry_wait_stop_event = threading.Event()
        self._entry_wait_interrupted = False
        self._entry_prewarmed_leverage_symbols: Set[str] = set()
        self._entry_structure_protection_state = EntryStructureProtectionState(store)
        self._market_fill_reconciler = MarketFillReconciler(client, store)
        self._mutation_lock = mutation_lock or threading.RLock()

    def _is_protection_exempt(self, symbol: str) -> bool:
        return str(symbol or "").strip().upper() in self.protection_exempt_symbols

    def has_pending_entry_wait(self) -> bool:
        state = self._load_entry_wait_state()
        pending = state.get("pending")
        if not isinstance(pending, dict) or not pending:
            return False
        deadline_raw = str(state.get("deadline_utc") or "").strip()
        if deadline_raw:
            try:
                expired = self._utc_now_datetime() >= self._parse_iso_utc(deadline_raw)
            except ValueError:
                expired = True
            if expired:
                run_id = str(state.get("run_id") or "").strip()
                if run_id:
                    self.store.finalize_run(run_id, "FAILED", "WAIT_BEARISH_HOUR_EXPIRED_DURING_RESTART")
                self._clear_entry_wait_state()
                return False
        return True

    def get_pending_entry_trade_day(self) -> Optional[date]:
        """Return the persisted trade day used by an in-flight entry wait."""
        state = self._load_entry_wait_state()
        pending = state.get("pending")
        if not isinstance(pending, dict) or not pending:
            return None
        raw_trade_day = str(state.get("trade_day_utc") or "").strip()
        if not raw_trade_day:
            run_id = str(state.get("run_id") or "").strip()
            if run_id:
                try:
                    run_state = self.store.get_run(run_id)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning(
                        "Failed to load entry run while resolving pending trade day: account=%s run_id=%s error=%s",
                        self.account_id,
                        run_id,
                        exc,
                    )
                    run_state = None
                raw_trade_day = str(getattr(run_state, "trade_day_utc", "") or "").strip()
        if not raw_trade_day:
            return None
        try:
            return date.fromisoformat(raw_trade_day[:10])
        except ValueError:
            LOGGER.warning(
                "Invalid persisted entry wait trade day: account=%s trade_day=%s",
                self.account_id,
                raw_trade_day,
            )
            return None

    def request_entry_wait_stop(self) -> None:
        self._entry_wait_stop_event.set()

    def run_entry(
        self,
        trade_day_utc: Optional[str] = None,
        shared_top_gainers: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, object]:
        trade_day = (trade_day_utc or "").strip() or datetime.now(timezone.utc).date().isoformat()
        trade_day_utc = trade_day
        # A portfolio-loss stop can interrupt a bearish-hour wait. The event is
        # scoped to that entry run; a new trade day must be allowed to wait again.
        self._entry_wait_stop_event.clear()
        pending_entry_recovery = self.recover_pending_entries()
        if int(pending_entry_recovery.get("total", 0) or 0) > 0:
            LOGGER.warning(
                "Recovered incomplete entries before entry run account=%s result=%s",
                self.account_id,
                pending_entry_recovery,
            )
        preclose_structure_recovery = self.recover_preclose_structure_protections()
        if int(preclose_structure_recovery.get("total", 0) or 0) > 0:
            LOGGER.warning(
                "Recovered preclose structure protections before entry run account=%s result=%s",
                self.account_id,
                preclose_structure_recovery,
            )
        active_wait = self._load_entry_wait_state()
        active_pending = active_wait.get("pending")
        active_run_id = str(active_wait.get("run_id") or "").strip()
        active_run = self.store.get_run(active_run_id) if active_run_id else None
        if (
            isinstance(active_pending, dict)
            and active_pending
            and active_run is not None
            and str(active_run.status).upper() == "RUNNING"
        ):
            run_id = active_run_id
            trade_day_utc = str(active_wait.get("trade_day_utc") or trade_day_utc)
            created = False
        else:
            run_id, created = self.store.create_run(trade_day_utc, account_id=self.account_id)
        resumed_wait = False
        if not created:
            run_state = self.store.get_run(run_id)
            wait_state = self._load_entry_wait_state()
            if run_state is not None and str(run_state.status).upper() == "RUNNING":
                has_persisted_wait = (
                    str(wait_state.get("run_id") or "") == run_id
                    and isinstance(wait_state.get("pending"), dict)
                    and bool(wait_state.get("pending"))
                )
                if has_persisted_wait:
                    resumed_wait = True
                    shared_top_gainers = self._ranking_payload_from_wait_state(wait_state)
                    LOGGER.warning(
                        "Resume persisted bearish-hour entry wait: account=%s run_id=%s pending=%s",
                        self.account_id,
                        run_id,
                        len(wait_state["pending"]),
                    )
                else:
                    LOGGER.warning(
                        "Resume incomplete RUNNING entry from ranking stage: account=%s run_id=%s",
                        self.account_id,
                        run_id,
                    )
            else:
                LOGGER.info("Entry skipped: run already exists for trade_day_utc=%s", trade_day_utc)
                return {
                    "status": "SKIPPED",
                    "run_id": run_id,
                    "reason": "RUN_ALREADY_EXISTS",
                    "opened": 0,
                    "failed": 0,
                    "entry_failed": 0,
                    "exit_setup_failed": 0,
                }

        opened_count = 0
        if not created:
            opened_count = self.store.count_run_opened_positions(run_id)
        self._entry_wait_interrupted = False
        entry_failed_count = 0
        exit_setup_failed_count = 0
        skipped_symbols: List[str] = []
        opened_symbols: List[str] = []
        entry_failure_details: List[str] = []
        exit_setup_failure_details: List[str] = []
        risk_off_details: List[str] = []
        shrink_retry_details: List[str] = []
        preclose_audit_pending: List[Dict[str, Any]] = []
        pre_rebalance_summary: Optional[Dict[str, object]] = None
        post_rebalance_summary: Optional[Dict[str, object]] = None

        try:
            active_symbols = self.store.list_active_symbols()
            open_symbols = (
                active_symbols
                if isinstance(active_symbols, set)
                else self.store.list_open_symbols()
            )
            fetch_top_n = max(
                self.top_n,
                self.top_n * self.entry_rank_fetch_multiplier,
                self.top_n + len(open_symbols),
            )
            if shared_top_gainers is None:
                top_gainers = build_top_gainers(
                    top_n=fetch_top_n,
                    volume_threshold=self.volume_threshold,
                    session=self.client.session,
                    base_url=self.client.base_url,
                    max_workers=self.ranker_max_workers,
                    weight_limit_per_minute=self.ranker_weight_limit_per_minute,
                    min_request_interval_ms=self.ranker_min_request_interval_ms,
                )
            else:
                top_gainers = shared_top_gainers
                fetch_top_n = max(fetch_top_n, len(top_gainers))

            ranked = self._build_ranked_entries(top_gainers)
            ranked.sort(key=lambda item: item.pct_change, reverse=True)
            if self.volume_threshold > 0:
                ranked = [item for item in ranked if item.quote_volume >= self.volume_threshold]

            if not ranked:
                self.store.finalize_run(run_id, "SUCCESS", "No ranked symbols")
                self.notifier.send(
                    "【Top10做空】本次无可交易标的",
                    self._build_brief_notification(
                        run_id=run_id,
                        trade_day_utc=trade_day_utc,
                        status="SUCCESS",
                        reason="榜单为空",
                    ),
                )
                return {
                    "status": "SUCCESS",
                    "run_id": run_id,
                    "opened": 0,
                    "failed": 0,
                    "entry_failed": 0,
                    "exit_setup_failed": 0,
                }

            candidates, skipped_symbols = self._select_entry_candidates(
                ranked=ranked,
                open_symbols=open_symbols,
                target_count=self.top_n,
            )
            expected_total_positions = len(open_symbols) + len(candidates)

            if len(candidates) < self.top_n:
                LOGGER.warning(
                    "Entry candidates are fewer than target: target=%s selected=%s skipped_existing=%s fetched_rank=%s",
                    self.top_n,
                    len(candidates),
                    len(skipped_symbols),
                    len(ranked),
                )

            if not candidates:
                if resumed_wait:
                    self._clear_entry_wait_state()
                self.store.finalize_run(run_id, "SUCCESS", "All symbols already have open strategy positions")
                self.notifier.send(
                    "【Top10做空】本次未开仓",
                    self._build_brief_notification(
                        run_id=run_id,
                        trade_day_utc=trade_day_utc,
                        status="SUCCESS",
                        reason=f"候选窗口内均已有策略持仓（榜单窗口={fetch_top_n}）",
                        extra_rows=[
                            ("跳过币种数", len(skipped_symbols)),
                            ("跳过币种", self._join_symbols(skipped_symbols)),
                            (
                                "再平衡(后校准)",
                                self._format_rebalance_summary(post_rebalance_summary),
                            ),
                        ],
                    ),
                )
                return {
                    "status": "SUCCESS",
                    "run_id": run_id,
                    "opened": 0,
                    "failed": 0,
                    "entry_failed": 0,
                    "exit_setup_failed": 0,
                    "rebalance_pre": pre_rebalance_summary,
                    "rebalance_post": post_rebalance_summary,
                }

            if self.rebalance_enabled and self.rebalance_pre_entry_reduce:
                if expected_total_positions > 0:
                    try:
                        pre_rebalance_summary = self._rebalance_to_target(
                            target_count=expected_total_positions,
                            reduce_only=True,
                            reason_tag="pre",
                            run_id=run_id,
                        )
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.exception("Pre-entry rebalance failed: %s", exc)

            available_balance = self.client.get_available_balance("USDT")
            if available_balance <= 0:
                self.store.finalize_run(run_id, "FAILED", "No available USDT balance")
                self.notifier.send(
                    "【Top10做空】执行失败",
                    self._build_brief_notification(
                        run_id=run_id,
                        trade_day_utc=trade_day_utc,
                        status="FAILED",
                        reason="可用USDT余额为0",
                    ),
                )
                return {
                    "status": "FAILED",
                    "run_id": run_id,
                    "opened": 0,
                    "failed": len(candidates),
                    "entry_failed": len(candidates),
                    "exit_setup_failed": 0,
                }

            effective_balance = available_balance * (1 - self.entry_fee_buffer_pct / 100.0)
            if effective_balance <= 0:
                self.store.finalize_run(run_id, "FAILED", "No effective USDT balance after fee buffer")
                self.notifier.send(
                    "【Top10做空】执行失败",
                    self._build_brief_notification(
                        run_id=run_id,
                        trade_day_utc=trade_day_utc,
                        status="FAILED",
                        reason=(
                            f"手续费缓冲后可用余额不足: available={available_balance:.6f} "
                            f"buffer={self.entry_fee_buffer_pct:.2f}%"
                        ),
                    ),
                )
                return {
                    "status": "FAILED",
                    "run_id": run_id,
                    "opened": 0,
                    "failed": len(candidates),
                    "entry_failed": len(candidates),
                    "exit_setup_failed": 0,
                }

            base_margin = effective_balance / float(self.allocation_splits)
            target_notional = base_margin * float(self.leverage)
            entry_target_mode = "available_balance"
            if self.rebalance_enabled and expected_total_positions > 0:
                try:
                    risk_rows_for_entry_sizing = self.client.get_position_risk()
                    equity_for_entry_sizing = self._compute_account_equity_usdt(
                        risk_rows=risk_rows_for_entry_sizing
                    )
                    if equity_for_entry_sizing > 0:
                        rebalance_target_notional = (
                            equity_for_entry_sizing
                            * float(self.leverage)
                            * self.rebalance_utilization
                            / float(expected_total_positions)
                        )
                        if rebalance_target_notional > 0:
                            target_notional = rebalance_target_notional
                            base_margin = target_notional / float(self.leverage)
                            entry_target_mode = "equity_rebalance"
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception("Failed to compute entry target notional from equity rebalance target: %s", exc)

            failed_notional = 0.0

            self._prewarm_entry_candidates(
                candidates=candidates,
                target_notional=target_notional,
            )

            if self.entry_initial_delay_sec > 0 and not resumed_wait:
                LOGGER.info(
                    "Entry initial delay: account=%s wait_sec=%s",
                    self.account_id,
                    self.entry_initial_delay_sec,
                )
                time.sleep(self.entry_initial_delay_sec)

            wait_state = self._load_entry_wait_state() if resumed_wait else {}
            persisted_signal_time = str(wait_state.get("signal_base_time_utc") or "").strip()
            entry_signal_base_time = (
                self._parse_iso_utc(persisted_signal_time)
                if persisted_signal_time
                else self._utc_now_datetime()
            )
            self._last_entry_wait_expired_symbols: List[str] = []
            ready_entries = self._iter_ready_entries_after_bearish_hour(
                candidates=candidates,
                signal_base_time_utc=entry_signal_base_time,
                run_id=run_id,
                trade_day_utc=trade_day_utc,
            )
            for idx, ready_entry in enumerate(ready_entries):
                if self._entry_wait_stop_event.is_set():
                    self._entry_wait_interrupted = True
                    break
                entry = ready_entry.entry
                reference_price = ready_entry.reference_price
                position_id: Optional[int] = None
                mutation_acquired = False
                try:
                    entry_structure_window = self._prepare_entry_structure_window(ready_entry)
                    self._mutation_lock.acquire()
                    mutation_acquired = True
                    normalized_entry_symbol = entry.symbol.strip().upper()
                    if normalized_entry_symbol not in self._entry_prewarmed_leverage_symbols:
                        self.client.ensure_isolated_and_leverage(entry.symbol, self.leverage)
                    qty_diagnostic = self.client.diagnose_order_qty(entry.symbol, target_notional, reference_price)
                    qty = float(qty_diagnostic["normalized_qty"])
                    plan = PlannedOrder(
                        symbol=entry.symbol,
                        base_margin_usdt=base_margin,
                        target_notional_usdt=target_notional,
                        qty=qty,
                    )
                    if plan.qty <= 0:
                        failed_notional += target_notional
                        entry_failed_count += 1
                        entry_failure_details.append(
                            f"{entry.symbol}: qty归一化后为0(不满足最小下单规则)"
                        )
                        LOGGER.warning(
                            "Skip %s due to invalid qty after filter normalization: "
                            "target_notional=%.6f price=%.10f has_rules=%s raw_qty=%.10f "
                            "normalized_qty=%.10f normalized_notional=%.10f step_size=%s "
                            "min_qty=%s min_notional=%s reject_reason=%s",
                            entry.symbol,
                            target_notional,
                            reference_price,
                            qty_diagnostic["has_rules"],
                            float(qty_diagnostic["raw_qty"]),
                            float(qty_diagnostic["normalized_qty"]),
                            float(qty_diagnostic["normalized_notional"]),
                            qty_diagnostic["step_size"],
                            qty_diagnostic["min_qty"],
                            qty_diagnostic["min_notional"],
                            qty_diagnostic["reject_reason"],
                        )
                        continue

                    intent_opened_at = self._utc_now_datetime()
                    position_id = self.store.insert_position(
                        run_id=run_id,
                        symbol=plan.symbol,
                        side="SHORT",
                        qty=plan.qty,
                        entry_price=reference_price,
                        liq_price_open=None,
                        tp_price=None,
                        sl_price=None,
                        tp_order_id=None,
                        sl_order_id=None,
                        tp_client_order_id=None,
                        sl_client_order_id=None,
                        opened_at_utc=intent_opened_at.isoformat(),
                        expire_at_utc=(intent_opened_at + timedelta(hours=self.max_hold_hours)).isoformat(),
                        status="PENDING_ENTRY",
                    )

                    open_order, retry_count_used = self._place_market_short_with_shrink_retry(
                        symbol=plan.symbol,
                        target_notional=plan.target_notional_usdt,
                        reference_price=reference_price,
                        client_id_tag="ent",
                    )
                    if retry_count_used > 0:
                        shrink_retry_details.append(f"{plan.symbol}: 缩量重试{retry_count_used}次后成功")
                    entry_event_time = self._utc_now_iso()
                    entry_event_id = self.store.add_order_event(
                        symbol=plan.symbol,
                        position_id=position_id,
                        event_time_utc=entry_event_time,
                        order_payload=open_order,
                    )

                    position_risk = self._load_short_position(plan.symbol)
                    if position_risk is None:
                        raise RuntimeError(f"No short position returned after entry order for {plan.symbol}")

                    entry_price = float(position_risk.get("entryPrice") or reference_price)
                    liq_price = self._safe_positive_float(position_risk.get("liquidationPrice"))
                    qty_now = abs(float(position_risk.get("positionAmt", plan.qty)))
                    observed_opened_at = self._utc_now_datetime()
                    opened_at = self._resolve_entry_fill_time(open_order, observed_opened_at)
                    expire_at = opened_at + timedelta(hours=self.max_hold_hours)

                    self.store.set_position_entry_fill(
                        position_id=position_id,
                        qty=qty_now,
                        entry_price=entry_price,
                        liq_price_open=liq_price,
                        opened_at_utc=opened_at.isoformat(),
                        expire_at_utc=expire_at.isoformat(),
                    )

                    reference_price_float = float(reference_price or 0.0)
                    bearish_close_time = getattr(ready_entry, "bearish_close_time_utc", None)
                    adverse_slippage_pct = (
                        ((reference_price_float - entry_price) / reference_price_float) * 100.0
                        if reference_price_float > 0
                        else None
                    )
                    close_to_fill_ms = None
                    if isinstance(bearish_close_time, datetime):
                        close_to_fill_ms = max(
                            0.0,
                            (opened_at - bearish_close_time).total_seconds() * 1000.0,
                        )
                    submit_to_fill_ms = max(
                        0.0,
                        (opened_at - intent_opened_at).total_seconds() * 1000.0,
                    )
                    is_preclose_entry = bool(getattr(ready_entry, "preclose_entry", False))
                    preclose_time = getattr(ready_entry, "preclose_time_utc", None)
                    signal_hour_open = getattr(ready_entry, "signal_hour_open_utc", None)
                    preclose_to_fill_ms = None
                    if is_preclose_entry and isinstance(preclose_time, datetime):
                        preclose_to_fill_ms = max(
                            0.0,
                            (opened_at - preclose_time).total_seconds() * 1000.0,
                        )
                    entry_audit = {
                        "entry_mode": "PRECLOSE" if is_preclose_entry else "CONFIRMED_CLOSE",
                        "reference_price": reference_price_float,
                        "fill_price": entry_price,
                        "adverse_slippage_pct": adverse_slippage_pct,
                        "close_to_fill_ms": close_to_fill_ms,
                        "submit_to_fill_ms": submit_to_fill_ms,
                        "preclose_to_fill_ms": preclose_to_fill_ms,
                        "retry_count": retry_count_used,
                        "bearish_close_time_utc": (
                            bearish_close_time.isoformat()
                            if isinstance(bearish_close_time, datetime)
                            else None
                        ),
                        "preclose_time_utc": (
                            preclose_time.isoformat() if isinstance(preclose_time, datetime) else None
                        ),
                        "signal_hour_open_utc": (
                            signal_hour_open.isoformat() if isinstance(signal_hour_open, datetime) else None
                        ),
                        "provisional_open_price": getattr(ready_entry, "provisional_open_price", None),
                        "provisional_close_price": getattr(ready_entry, "provisional_close_price", None),
                        "final_candle_available": None,
                        "final_candle_bearish": None,
                        "final_candle_open_price": None,
                        "final_candle_close_price": None,
                        "final_candle_close_time_utc": None,
                        "filled_at_utc": opened_at.isoformat(),
                    }
                    LOGGER.info(
                        "Entry fill audit: account=%s position_id=%s symbol=%s reference_price=%.10f "
                        "fill_price=%.10f adverse_slippage_pct=%s close_to_fill_ms=%s submit_to_fill_ms=%s",
                        self.account_id,
                        position_id,
                        plan.symbol,
                        reference_price_float,
                        entry_price,
                        f"{adverse_slippage_pct:.6f}" if adverse_slippage_pct is not None else "n/a",
                        f"{close_to_fill_ms:.1f}" if close_to_fill_ms is not None else "n/a",
                        f"{submit_to_fill_ms:.1f}",
                    )
                    audited_order = dict(open_order)
                    audited_order["entry_audit"] = entry_audit
                    update_order_event = getattr(self.store, "update_order_event", None)
                    if callable(update_order_event) and entry_event_id:
                        try:
                            update_order_event(
                                order_event_id=entry_event_id,
                                symbol=plan.symbol,
                                position_id=position_id,
                                event_time_utc=opened_at.isoformat(),
                                order_payload=audited_order,
                            )
                        except Exception as audit_exc:  # noqa: BLE001
                            LOGGER.warning(
                                "Failed to persist entry audit fields: account=%s position_id=%s symbol=%s error=%s",
                                self.account_id,
                                position_id,
                                plan.symbol,
                                audit_exc,
                            )
                    if is_preclose_entry and entry_event_id:
                        signal_hour_open_value = signal_hour_open
                        if isinstance(signal_hour_open_value, datetime):
                            preclose_audit_pending.append(
                                {
                                    "order_event_id": entry_event_id,
                                    "position_id": position_id,
                                    "symbol": plan.symbol,
                                    "hour_open_utc": signal_hour_open_value.astimezone(timezone.utc),
                                    "order_payload": audited_order,
                                }
                            )

                    opened_symbols.append(plan.symbol)
                    opened_count += 1
                    try:
                        entry_structure_protection = self._complete_entry_structure_protection(
                            symbol=plan.symbol,
                            window=entry_structure_window,
                            fill_time_utc=opened_at,
                            entry_price=entry_price,
                        )
                        if entry_structure_protection is not None:
                            self._entry_structure_protection_state.put(
                                position_id=position_id,
                                protection=entry_structure_protection,
                            )
                            LOGGER.info(
                                "Entry structure protection ready: account=%s position_id=%s symbol=%s "
                                "window_start=%s bearish_close=%s fill=%s stop_price=%.10f",
                                self.account_id,
                                position_id,
                                plan.symbol,
                                entry_structure_protection.window_start_utc.isoformat(),
                                entry_structure_protection.bearish_close_time_utc.isoformat(),
                                entry_structure_protection.window_end_utc.isoformat(),
                                entry_structure_protection.stop_price,
                            )
                            self._place_exit_orders(
                                position_id=position_id,
                                symbol=plan.symbol,
                                entry_structure_stop_price=entry_structure_protection.stop_price,
                            )
                        else:
                            self._place_exit_orders(
                                position_id=position_id,
                                symbol=plan.symbol,
                            )
                        self.store.mark_position_open(position_id)
                    except Exception as exc:  # noqa: BLE001
                        exit_setup_failed_count += 1
                        exit_setup_failure_details.append(f"{plan.symbol}: {exc}")
                        LOGGER.exception("Failed to place exit orders for %s: %s", plan.symbol, exc)
                        self.store.set_position_error(position_id, f"exit_setup: {exc}")
                        risk_off_result = self._force_close_position(
                            position_id=position_id,
                            symbol=plan.symbol,
                            reason="EXIT_SETUP_FAILED",
                        )
                        risk_off_details.append(
                            (
                                f"{risk_off_result['symbol']}: status={risk_off_result['status']}, "
                                f"qty={risk_off_result['qty']}, reason={risk_off_result['reason']}"
                            )
                        )
                        continue

                except Exception as exc:  # noqa: BLE001
                    if position_id is not None:
                        try:
                            current = self.store.get_position(position_id)
                            if current is not None and str(current.get("status") or "") in {
                                "PENDING_ENTRY",
                                "PENDING_EXIT_SETUP",
                            }:
                                risk = self._load_short_position(entry.symbol)
                                if risk is None:
                                    self.store.mark_position_closed(
                                        position_id=position_id,
                                        status="ENTRY_FAILED",
                                        close_reason="ENTRY_NOT_FOUND_AFTER_FAILURE",
                                    )
                                else:
                                    self._force_close_position(
                                        position_id=position_id,
                                        symbol=entry.symbol,
                                        reason="ENTRY_WORKFLOW_FAILED",
                                    )
                        except Exception as recovery_exc:  # noqa: BLE001
                            LOGGER.exception(
                                "Failed to recover incomplete entry account=%s position_id=%s symbol=%s: %s",
                                self.account_id,
                                position_id,
                                entry.symbol,
                                recovery_exc,
                            )
                    failed_notional += target_notional
                    entry_failed_count += 1
                    entry_failure_details.append(f"{entry.symbol}: {exc}")
                    LOGGER.exception("Initial entry failed for %s: %s", entry.symbol, exc)
                finally:
                    if mutation_acquired:
                        self._mutation_lock.release()

                if (
                    not self.entry_wait_bearish_hour_enabled
                    and self.entry_symbol_interval_sec > 0
                    and idx < len(candidates) - 1
                ):
                    LOGGER.info(
                        "Entry pacing sleep: account=%s symbol=%s next_wait_sec=%s",
                        self.account_id,
                        entry.symbol,
                        self.entry_symbol_interval_sec,
                    )
                    time.sleep(self.entry_symbol_interval_sec)

            self._finalize_preclose_entry_audits(preclose_audit_pending)

            for symbol in self._last_entry_wait_expired_symbols:
                entry_failed_count += 1
                entry_failure_details.append(f"{symbol}: WAIT_BEARISH_HOUR_EXPIRED")

            if self._entry_wait_interrupted:
                LOGGER.info(
                    "Entry wait interrupted for graceful shutdown: account=%s run_id=%s",
                    self.account_id,
                    run_id,
                )
                return {
                    "status": "INTERRUPTED",
                    "run_id": run_id,
                    "opened": opened_count,
                    "failed": entry_failed_count + exit_setup_failed_count,
                    "entry_failed": entry_failed_count,
                    "exit_setup_failed": exit_setup_failed_count,
                }

            failed_count = entry_failed_count + exit_setup_failed_count
            summary = (
                f"run_id={run_id}, opened={opened_count}, failed={failed_count}, "
                f"entry_failed={entry_failed_count}, exit_setup_failed={exit_setup_failed_count}, "
                f"skipped_existing={len(skipped_symbols)}, failed_notional={failed_notional:.4f}, "
                f"fee_buffer_pct={self.entry_fee_buffer_pct:.2f}, shrink_retry_success={len(shrink_retry_details)}, "
                f"entry_target_mode={entry_target_mode}, entry_target_notional={target_notional:.4f}, "
                f"rebalance_pre={self._format_rebalance_summary(pre_rebalance_summary)}, "
                f"rebalance_post={self._format_rebalance_summary(post_rebalance_summary)}"
            )
            run_status = "SUCCESS" if opened_count > 0 or failed_count == 0 else "FAILED"
            self.store.finalize_run(run_id, run_status, summary)

            title = "【Top10做空】建仓完成" if run_status == "SUCCESS" else "【Top10做空】建仓失败"
            self.notifier.send(
                title,
                self._build_entry_notification(
                    run_id=run_id,
                    trade_day_utc=trade_day_utc,
                    run_status=run_status,
                    opened_symbols=opened_symbols,
                    skipped_symbols=skipped_symbols,
                    entry_failure_details=entry_failure_details,
                    exit_setup_failure_details=exit_setup_failure_details,
                    risk_off_details=risk_off_details,
                    shrink_retry_details=shrink_retry_details,
                    failed_notional=failed_notional,
                    opened_count=opened_count,
                    failed_count=failed_count,
                    entry_failed_count=entry_failed_count,
                    exit_setup_failed_count=exit_setup_failed_count,
                    available_balance=available_balance,
                    effective_balance=effective_balance,
                ),
            )
            return {
                "status": run_status,
                "run_id": run_id,
                "opened": opened_count,
                "failed": failed_count,
                "entry_failed": entry_failed_count,
                "exit_setup_failed": exit_setup_failed_count,
                "skipped": len(skipped_symbols),
                "skipped_symbols": skipped_symbols,
                "entry_failed_symbols": self._extract_symbol_prefixes(entry_failure_details),
                "exit_setup_failed_symbols": self._extract_symbol_prefixes(exit_setup_failure_details),
                "rebalance_pre": pre_rebalance_summary,
                "rebalance_post": post_rebalance_summary,
            }

        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Entry run failed: %s", exc)
            self._clear_entry_wait_state()
            self.store.finalize_run(run_id, "FAILED", str(exc))
            self.notifier.send(
                "【Top10做空】执行失败",
                self._build_brief_notification(
                    run_id=run_id,
                    trade_day_utc=trade_day_utc,
                    status="FAILED",
                    reason=str(exc),
                ),
            )
            return {
                "status": "FAILED",
                "run_id": run_id,
                "error": str(exc),
                "opened": opened_count,
                "failed": entry_failed_count + exit_setup_failed_count,
                "entry_failed": entry_failed_count,
                "exit_setup_failed": exit_setup_failed_count,
                "skipped_symbols": [],
                "entry_failed_symbols": self._extract_symbol_prefixes(entry_failure_details),
                "exit_setup_failed_symbols": self._extract_symbol_prefixes(exit_setup_failure_details),
            }

    @staticmethod
    def _extract_symbol_prefixes(items: List[str]) -> List[str]:
        symbols: List[str] = []
        for item in items:
            text = str(item or "").strip()
            if not text:
                continue
            symbol = text.split(":", 1)[0].strip().upper()
            if not symbol:
                continue
            if symbol not in symbols:
                symbols.append(symbol)
        return symbols

    def _prewarm_entry_candidates(
        self,
        candidates: List[RankEntry],
        target_notional: float,
    ) -> None:
        """Prepare the REST trading path before the boundary candle closes.

        The concrete Binance client owns the details of time synchronization,
        exchange-rule caching, leverage setup and quantity diagnosis.  Keeping
        this as a capability check lets lightweight test/fake clients continue
        to work, while a transient warm-up failure remains non-fatal because
        the normal entry path repeats the checks immediately before submitting.
        """
        prewarm = getattr(self.client, "prewarm_entry", None)
        if not callable(prewarm):
            return
        symbols = [entry.symbol for entry in candidates]
        reference_prices = {entry.symbol.strip().upper(): entry.last_price for entry in candidates}
        started = time.monotonic()
        self._entry_prewarmed_leverage_symbols = set()
        try:
            result = prewarm(
                symbols=symbols,
                leverage=self.leverage,
                target_notional=target_notional,
                reference_prices=reference_prices,
            )
            diagnosed = len(result) if isinstance(result, dict) else 0
            if isinstance(result, dict):
                self._entry_prewarmed_leverage_symbols = {
                    str(symbol).strip().upper()
                    for symbol, diagnostic in result.items()
                    if isinstance(diagnostic, dict) and diagnostic.get("prewarm_leverage_ready") is True
                }
            LOGGER.info(
                "Entry candidate prewarm finished: account=%s candidates=%s diagnosed=%s leverage_ready=%s elapsed_ms=%s",
                self.account_id,
                len(symbols),
                diagnosed,
                len(self._entry_prewarmed_leverage_symbols),
                int(max(0.0, (time.monotonic() - started) * 1000)),
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Entry candidate prewarm failed; continue with just-in-time checks: account=%s error=%s",
                self.account_id,
                exc,
            )

    @staticmethod
    def _empty_preclose_structure_summary(total: int = 0) -> Dict[str, int]:
        return {
            "total": max(0, int(total)),
            "replaced": 0,
            "kept": 0,
            "closed": 0,
            "skipped": 0,
            "unavailable": 0,
            "errors": 0,
        }

    def recover_preclose_structure_protections(self) -> Dict[str, int]:
        list_pending = getattr(
            self.store,
            "list_open_preclose_entry_audits_needing_structure",
            None,
        )
        if not callable(list_pending):
            return self._empty_preclose_structure_summary()

        pending_rows = list_pending()
        audits: List[Dict[str, Any]] = []
        invalid = 0
        for row in pending_rows or []:
            audit = dict(row) if isinstance(row, dict) else {}
            hour_open_raw = audit.get("hour_open_utc")
            try:
                hour_open = (
                    hour_open_raw.astimezone(timezone.utc)
                    if isinstance(hour_open_raw, datetime)
                    else self._parse_iso_utc(str(hour_open_raw or ""))
                )
            except (TypeError, ValueError):
                invalid += 1
                continue
            audit["hour_open_utc"] = hour_open
            audits.append(audit)

        summary = self._finalize_preclose_entry_audits(audits)
        summary["total"] += invalid
        summary["errors"] += invalid
        return summary

    def _finalize_preclose_entry_audits(
        self,
        audits: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """Attach the final candle outcome to entries submitted before the close."""
        summary = self._empty_preclose_structure_summary(total=len(audits))
        if not audits:
            return summary
        update_order_event = getattr(self.store, "update_order_event", None)
        for audit in audits:
            symbol = str(audit.get("symbol") or "").strip().upper()
            hour_open = audit.get("hour_open_utc")
            order_event_id = audit.get("order_event_id")
            payload = dict(audit.get("order_payload") or {})
            entry_audit = dict(payload.get("entry_audit") or {})
            if not symbol or not isinstance(hour_open, datetime):
                summary["errors"] += 1
                continue

            final_available_at = hour_open + timedelta(hours=1, seconds=self.entry_wait_close_grace_sec)
            now = self._utc_now_datetime()
            if now < final_available_at and not self._entry_wait_stop_event.is_set():
                wait_sec = max(0.0, (final_available_at - now).total_seconds())
                LOGGER.info(
                    "Waiting for final candle audit: account=%s symbol=%s wait_sec=%.3f",
                    self.account_id,
                    symbol,
                    wait_sec,
                )
                self._entry_wait_stop_event.wait(timeout=wait_sec)

            if self._entry_wait_stop_event.is_set():
                entry_audit["final_candle_available"] = False
                entry_audit["final_candle_status"] = "SKIPPED_INTERRUPTED"
            else:
                final_candle = self._fetch_hour_candle_with_retry(symbol, hour_open)
                if final_candle is None:
                    entry_audit["final_candle_available"] = False
                    entry_audit["final_candle_status"] = "UNAVAILABLE"
                else:
                    open_price, close_price, close_time = final_candle
                    entry_audit["final_candle_available"] = True
                    entry_audit["final_candle_status"] = "OK"
                    entry_audit["final_candle_bearish"] = close_price < open_price
                    entry_audit["final_candle_open_price"] = open_price
                    entry_audit["final_candle_close_price"] = close_price
                    entry_audit["final_candle_close_time_utc"] = close_time.isoformat()
                    try:
                        provisional_close = float(entry_audit.get("provisional_close_price") or 0.0)
                    except (TypeError, ValueError):
                        provisional_close = 0.0
                    if provisional_close > 0:
                        entry_audit["final_vs_provisional_close_pct"] = (
                            (close_price - provisional_close) / provisional_close * 100.0
                        )
                    try:
                        protection = self._build_finalized_preclose_structure_protection(
                            symbol=symbol,
                            final_close_time_utc=close_time,
                        )
                        structure_status = self._apply_finalized_preclose_structure_protection(
                            position_id=int(audit["position_id"]),
                            symbol=symbol,
                            protection=protection,
                        )
                        entry_audit["structure_stop_status"] = structure_status
                        entry_audit["structure_stop_price"] = protection.stop_price
                        entry_audit["structure_window_start_utc"] = (
                            protection.window_start_utc.isoformat()
                        )
                        entry_audit["structure_window_end_utc"] = (
                            protection.window_end_utc.isoformat()
                        )
                    except Exception as exc:  # noqa: BLE001
                        entry_audit["structure_stop_status"] = "ERROR"
                        entry_audit["structure_stop_error"] = str(exc)
                        LOGGER.exception(
                            "Failed to apply finalized preclose structure stop: "
                            "account=%s position_id=%s symbol=%s error=%s",
                            self.account_id,
                            audit.get("position_id"),
                            symbol,
                            exc,
                        )

            payload["entry_audit"] = entry_audit
            if callable(update_order_event) and order_event_id:
                try:
                    update_order_event(
                        order_event_id=order_event_id,
                        symbol=symbol,
                        position_id=audit.get("position_id"),
                        event_time_utc=str(entry_audit.get("filled_at_utc") or self._utc_now_iso()),
                        order_payload=payload,
                    )
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning(
                        "Failed to persist final preclose candle audit: account=%s symbol=%s error=%s",
                        self.account_id,
                        symbol,
                        exc,
                    )
            LOGGER.info(
                "Preclose entry final candle audit: account=%s symbol=%s final_available=%s "
                "final_bearish=%s structure_stop_status=%s structure_stop_price=%s",
                self.account_id,
                symbol,
                entry_audit.get("final_candle_available"),
                entry_audit.get("final_candle_bearish"),
                entry_audit.get("structure_stop_status"),
                entry_audit.get("structure_stop_price"),
            )
            structure_status = str(entry_audit.get("structure_stop_status") or "").upper()
            if structure_status.startswith("REPLACED"):
                summary["replaced"] += 1
            elif structure_status.startswith("KEPT_"):
                summary["kept"] += 1
            elif structure_status.startswith("CLOSED_"):
                summary["closed"] += 1
            elif structure_status == "ERROR":
                summary["errors"] += 1
            elif not entry_audit.get("final_candle_available"):
                summary["unavailable"] += 1
            else:
                summary["skipped"] += 1
        return summary

    def _iter_ready_entries_after_bearish_hour(
        self,
        candidates: List[RankEntry],
        signal_base_time_utc: datetime,
        run_id: Optional[str] = None,
        trade_day_utc: Optional[str] = None,
    ) -> Iterator[ReadyEntry]:
        if not self.entry_wait_bearish_hour_enabled:
            for idx, entry in enumerate(candidates):
                yield ReadyEntry(
                    entry=entry,
                    reference_price=entry.last_price,
                    signal_time_utc=signal_base_time_utc + timedelta(seconds=idx * self.entry_symbol_interval_sec),
                    bearish_close_time_utc=None,
                )
            return

        pending = self._restore_or_create_entry_wait(
            candidates=candidates,
            signal_base_time_utc=signal_base_time_utc,
            run_id=run_id,
            trade_day_utc=trade_day_utc,
        )

        while pending:
            if self._entry_wait_stop_event.is_set():
                self._entry_wait_interrupted = True
                return
            now = self._utc_now_datetime()
            wait_state = self._load_entry_wait_state()
            deadline_raw = str(wait_state.get("deadline_utc") or "").strip()
            deadline = self._parse_iso_utc(deadline_raw) if deadline_raw else None
            if deadline is not None and now >= deadline:
                self._last_entry_wait_expired_symbols = [
                    str(state["entry"].symbol)
                    for state in pending.values()
                    if isinstance(state.get("entry"), RankEntry)
                ]
                LOGGER.warning(
                    "Entry bearish-hour wait expired: account=%s run_id=%s symbols=%s deadline=%s",
                    self.account_id,
                    run_id,
                    self._last_entry_wait_expired_symbols,
                    deadline.isoformat(timespec="seconds"),
                )
                pending.clear()
                self._clear_entry_wait_state()
                break
            ready_this_round: List[Tuple[int, ReadyEntry]] = []
            next_check_times: List[datetime] = []

            due_preclose_states: List[Tuple[int, RankEntry, datetime]] = []
            due_final_states: List[Tuple[int, RankEntry, datetime]] = []
            for idx, state in list(pending.items()):
                entry = state.get("entry")
                hour_open = state.get("hour_open")
                if not isinstance(entry, RankEntry) or not isinstance(hour_open, datetime):
                    continue
                hour_close = hour_open + timedelta(hours=1)
                preclose_checked = bool(state.get("preclose_checked", False))
                if self.entry_preclose_sec > 0 and not preclose_checked:
                    available_at = hour_close - timedelta(seconds=self.entry_preclose_sec)
                    if now < available_at:
                        next_check_times.append(available_at)
                        continue
                    final_available_at = hour_close + timedelta(seconds=self.entry_wait_close_grace_sec)
                    if now >= final_available_at:
                        due_final_states.append((idx, entry, hour_open))
                        continue
                    due_preclose_states.append((idx, entry, hour_open))
                    continue

                available_at = hour_close + timedelta(seconds=self.entry_wait_close_grace_sec)
                if now < available_at:
                    next_check_times.append(available_at)
                    continue
                due_final_states.append((idx, entry, hour_open))

            due_states = due_preclose_states + due_final_states
            preclose_candle_results = self._fetch_hour_candles_parallel(
                due_preclose_states,
                snapshot_as_of_utc=now,
            )
            final_candle_results = self._fetch_hour_candles_parallel(due_final_states)
            retry_round_pause_sec = min(
                float(self.entry_wait_poll_sec),
                max(
                    self.entry_wait_close_retry_sec,
                    self.entry_wait_close_retry_sec * self.entry_wait_close_retry_count,
                ),
            )

            for idx, entry, hour_open in due_preclose_states:
                state = pending.get(idx)
                if state is None:
                    continue
                candle = preclose_candle_results.get(idx)
                if candle is None:
                    LOGGER.warning(
                        "Entry preclose snapshot unavailable after retries: account=%s symbol=%s "
                        "hour_open=%s retry_count=%s retry_sec=%s",
                        self.account_id,
                        entry.symbol,
                        hour_open.isoformat(timespec="seconds"),
                        self.entry_wait_close_retry_count,
                        self.entry_wait_close_retry_sec,
                    )
                    next_check_times.append(now + timedelta(seconds=retry_round_pause_sec))
                    continue

                open_price, close_price, _snapshot_time = candle
                state["preclose_checked"] = True
                self._persist_entry_wait_pending(
                    pending=pending,
                    run_id=run_id,
                    trade_day_utc=trade_day_utc,
                    signal_base_time_utc=signal_base_time_utc,
                )
                hour_close = hour_open + timedelta(hours=1)
                if close_price < open_price:
                    ready_entry = ReadyEntry(
                        entry=entry,
                        reference_price=close_price,
                        signal_time_utc=state["signal_time"] if isinstance(state["signal_time"], datetime) else signal_base_time_utc,
                        bearish_close_time_utc=None,
                        preclose_entry=True,
                        preclose_time_utc=now,
                        signal_hour_open_utc=hour_open,
                        provisional_open_price=open_price,
                        provisional_close_price=close_price,
                    )
                    ready_this_round.append((idx, ready_entry))
                    LOGGER.info(
                        "Entry preclose ready: account=%s symbol=%s trigger=%s hour_open=%s "
                        "open=%.10f provisional_close=%.10f",
                        self.account_id,
                        entry.symbol,
                        now.isoformat(timespec="seconds"),
                        hour_open.isoformat(timespec="seconds"),
                        open_price,
                        close_price,
                    )
                else:
                    LOGGER.info(
                        "Entry preclose not bearish; wait for final candle: account=%s symbol=%s "
                        "trigger=%s open=%.10f provisional_close=%.10f",
                        self.account_id,
                        entry.symbol,
                        now.isoformat(timespec="seconds"),
                        open_price,
                        close_price,
                    )
                    next_check_times.append(hour_close + timedelta(seconds=self.entry_wait_close_grace_sec))

            for idx, entry, hour_open in due_final_states:
                state = pending.get(idx)
                if state is None:
                    continue
                candle = final_candle_results.get(idx)
                if candle is None:
                    LOGGER.warning(
                        "Entry bearish-hour wait missing kline after second-level retries: "
                        "account=%s symbol=%s hour_open=%s retry_count=%s retry_sec=%s",
                        self.account_id,
                        entry.symbol,
                        hour_open.isoformat(timespec="seconds"),
                        self.entry_wait_close_retry_count,
                        self.entry_wait_close_retry_sec,
                    )
                    state["hour_open"] = hour_open
                    next_check_times.append(now + timedelta(seconds=retry_round_pause_sec))
                    continue

                open_price, close_price, close_time = candle
                if close_price < open_price:
                    ready_entry = ReadyEntry(
                        entry=entry,
                        reference_price=close_price,
                        signal_time_utc=state["signal_time"] if isinstance(state["signal_time"], datetime) else signal_base_time_utc,
                        bearish_close_time_utc=close_time,
                        signal_hour_open_utc=hour_open,
                    )
                    ready_this_round.append((idx, ready_entry))
                    LOGGER.info(
                        "Entry bearish-hour ready: account=%s symbol=%s signal=%s close=%s open=%.10f close_price=%.10f",
                        self.account_id,
                        entry.symbol,
                        ready_entry.signal_time_utc.isoformat(timespec="seconds"),
                        close_time.isoformat(timespec="seconds"),
                        open_price,
                        close_price,
                    )
                    continue

                LOGGER.info(
                    "Entry bearish-hour still waiting: account=%s symbol=%s hour_open=%s open=%.10f close=%.10f",
                    self.account_id,
                    entry.symbol,
                    hour_open.isoformat(timespec="seconds"),
                    open_price,
                    close_price,
                )
                state["hour_open"] = hour_open + timedelta(hours=1)
                state["preclose_checked"] = False
                self._persist_entry_wait_pending(
                    pending=pending,
                    run_id=run_id,
                    trade_day_utc=trade_day_utc,
                    signal_base_time_utc=signal_base_time_utc,
                )

            if ready_this_round:
                for _idx, item in sorted(
                    ready_this_round,
                    key=lambda pair: (
                        pair[1].bearish_close_time_utc or pair[1].signal_time_utc,
                        pair[0],
                    ),
                ):
                    yield item
                    pending.pop(_idx, None)
                    self._persist_entry_wait_pending(
                        pending=pending,
                        run_id=run_id,
                        trade_day_utc=trade_day_utc,
                        signal_base_time_utc=signal_base_time_utc,
                    )

            if not pending:
                break
            # A due candle that was confirmed bullish advances to the next
            # hour.  Re-evaluate that state immediately instead of falling
            # back to the coarse pre-close heartbeat.
            sleep_sec = 0.0 if due_states and not next_check_times else float(self.entry_wait_poll_sec)
            if next_check_times:
                wait_until = min(next_check_times)
                sleep_sec = min(
                    sleep_sec,
                    max(0.0, (wait_until - now).total_seconds()),
                )
            if sleep_sec <= 0:
                continue
            LOGGER.info(
                "Entry bearish-hour wait sleep: account=%s pending=%s wait_sec=%.3f",
                self.account_id,
                len(pending),
                sleep_sec,
            )
            if self._entry_wait_stop_event.wait(timeout=sleep_sec):
                self._entry_wait_interrupted = True
                return

        return

    def _load_entry_wait_state(self) -> Dict[str, Any]:
        state = self.store.get_lock_state(self.ENTRY_WAIT_LOCK_NAME) or {}
        return state if isinstance(state, dict) else {}

    def _clear_entry_wait_state(self) -> None:
        self.store.set_lock_state(self.ENTRY_WAIT_LOCK_NAME, {})

    def _ranking_payload_from_wait_state(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        pending = state.get("pending")
        if not isinstance(pending, dict):
            return []
        payload: List[Dict[str, Any]] = []
        for item in pending.values():
            if not isinstance(item, dict):
                continue
            payload.append(
                {
                    "symbol": item.get("symbol"),
                    "change": item.get("pct_change", 0.0),
                    "current_price": item.get("last_price", 0.0),
                    "volume": item.get("quote_volume", 0.0),
                }
            )
        return payload

    def _restore_or_create_entry_wait(
        self,
        candidates: List[RankEntry],
        signal_base_time_utc: datetime,
        run_id: Optional[str],
        trade_day_utc: Optional[str],
    ) -> Dict[int, Dict[str, object]]:
        existing = self._load_entry_wait_state()
        raw_pending = existing.get("pending")
        active_symbols: Set[str] = set()
        try:
            raw_active_symbols = self.store.list_active_symbols()
            if isinstance(raw_active_symbols, (set, list, tuple)):
                active_symbols = {
                    str(symbol or "").strip().upper()
                    for symbol in raw_active_symbols
                    if str(symbol or "").strip()
                }
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning(
                "Failed to load active symbols while restoring entry wait: account=%s error=%s",
                self.account_id,
                exc,
            )
        if run_id and str(existing.get("run_id") or "") == run_id and isinstance(raw_pending, dict):
            restored: Dict[int, Dict[str, object]] = {}
            for idx, item in enumerate(raw_pending.values()):
                if not isinstance(item, dict):
                    continue
                try:
                    entry = RankEntry(
                        symbol=str(item["symbol"]),
                        pct_change=float(item.get("pct_change") or 0.0),
                        last_price=float(item.get("last_price") or 0.0),
                        quote_volume=float(item.get("quote_volume") or 0.0),
                    )
                    signal_time = self._parse_iso_utc(str(item["signal_time_utc"]))
                    hour_open = self._parse_iso_utc(str(item["hour_open_utc"]))
                except (KeyError, TypeError, ValueError):
                    continue
                if entry.symbol.strip().upper() in active_symbols:
                    LOGGER.warning(
                        "Drop already-active symbol from persisted entry wait: account=%s symbol=%s run_id=%s",
                        self.account_id,
                        entry.symbol,
                        run_id,
                    )
                    continue
                restored[idx] = {
                    "entry": entry,
                    "signal_time": signal_time,
                    "hour_open": hour_open,
                    "preclose_checked": bool(item.get("preclose_checked", False)),
                }
            if restored:
                if len(restored) != len(raw_pending):
                    self._persist_entry_wait_pending(
                        pending=restored,
                        run_id=run_id,
                        trade_day_utc=str(existing.get("trade_day_utc") or trade_day_utc or ""),
                        signal_base_time_utc=signal_base_time_utc,
                    )
                return restored

        pending: Dict[int, Dict[str, object]] = {}
        next_idx = 0
        for entry in candidates:
            if entry.symbol.strip().upper() in active_symbols:
                LOGGER.warning(
                    "Skip already-active symbol while creating entry wait: account=%s symbol=%s run_id=%s",
                    self.account_id,
                    entry.symbol,
                    run_id,
                )
                continue
            idx = next_idx
            next_idx += 1
            signal_time = signal_base_time_utc + timedelta(seconds=idx * self.entry_symbol_interval_sec)
            pending[idx] = {
                "entry": entry,
                "signal_time": signal_time,
                "hour_open": self._floor_to_utc_hour(signal_time),
                "preclose_checked": False,
            }
        self._persist_entry_wait_pending(
            pending=pending,
            run_id=run_id,
            trade_day_utc=trade_day_utc,
            signal_base_time_utc=signal_base_time_utc,
        )
        return pending

    def _persist_entry_wait_pending(
        self,
        pending: Dict[int, Dict[str, object]],
        run_id: Optional[str],
        trade_day_utc: Optional[str],
        signal_base_time_utc: datetime,
    ) -> None:
        if not pending:
            self._clear_entry_wait_state()
            return
        existing = self._load_entry_wait_state()
        deadline_raw = str(existing.get("deadline_utc") or "").strip()
        if deadline_raw and str(existing.get("run_id") or "") == str(run_id or ""):
            deadline = self._parse_iso_utc(deadline_raw)
        else:
            local_signal = signal_base_time_utc.astimezone(self.runtime_timezone)
            next_local_day = (local_signal + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            deadline = min(
                signal_base_time_utc + timedelta(hours=self.entry_wait_max_hours),
                next_local_day.astimezone(timezone.utc),
            )
        serialized: Dict[str, Dict[str, Any]] = {}
        for idx, state in pending.items():
            entry = state.get("entry")
            signal_time = state.get("signal_time")
            hour_open = state.get("hour_open")
            if not isinstance(entry, RankEntry) or not isinstance(signal_time, datetime) or not isinstance(hour_open, datetime):
                continue
            serialized[str(idx)] = {
                "symbol": entry.symbol,
                "pct_change": entry.pct_change,
                "last_price": entry.last_price,
                "quote_volume": entry.quote_volume,
                "signal_time_utc": signal_time.astimezone(timezone.utc).isoformat(),
                "hour_open_utc": hour_open.astimezone(timezone.utc).isoformat(),
                "preclose_checked": bool(state.get("preclose_checked", False)),
            }
        self.store.set_lock_state(
            self.ENTRY_WAIT_LOCK_NAME,
            {
                "run_id": run_id,
                "trade_day_utc": trade_day_utc,
                "signal_base_time_utc": signal_base_time_utc.astimezone(timezone.utc).isoformat(),
                "deadline_utc": deadline.astimezone(timezone.utc).isoformat(),
                "pending": serialized,
                "updated_at_utc": self._utc_now_iso(),
            },
        )

    def _fetch_hour_candle_with_retry(
        self,
        symbol: str,
        hour_open_utc: datetime,
        snapshot_as_of_utc: Optional[datetime] = None,
    ) -> Optional[Tuple[float, float, datetime]]:
        """Fetch a candle snapshot or just-closed candle with short REST retries."""
        last_error: Optional[Exception] = None
        for attempt in range(1, self.entry_wait_close_retry_count + 1):
            try:
                candle = (
                    self._fetch_hour_candle_snapshot(symbol, hour_open_utc, snapshot_as_of_utc)
                    if snapshot_as_of_utc is not None
                    else self._fetch_hour_candle(symbol, hour_open_utc)
                )
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                candle = None
            if candle is not None:
                if attempt > 1:
                    LOGGER.info(
                        "Entry %s kline recovered after retry: account=%s symbol=%s "
                        "hour_open=%s attempt=%s/%s",
                        "preclose snapshot" if snapshot_as_of_utc is not None else "bearish-hour",
                        self.account_id,
                        symbol,
                        hour_open_utc.isoformat(timespec="seconds"),
                        attempt,
                        self.entry_wait_close_retry_count,
                    )
                return candle
            if attempt >= self.entry_wait_close_retry_count:
                break
            if self._entry_wait_stop_event.wait(timeout=self.entry_wait_close_retry_sec):
                self._entry_wait_interrupted = True
                return None

        if last_error is not None:
            LOGGER.warning(
                "Entry %s kline fetch failed after retries: account=%s symbol=%s "
                "hour_open=%s attempts=%s error=%s",
                "preclose snapshot" if snapshot_as_of_utc is not None else "bearish-hour",
                self.account_id,
                symbol,
                hour_open_utc.isoformat(timespec="seconds"),
                self.entry_wait_close_retry_count,
                last_error,
            )
        return None

    def _fetch_hour_candles_parallel(
        self,
        due_states: List[Tuple[int, RankEntry, datetime]],
        snapshot_as_of_utc: Optional[datetime] = None,
    ) -> Dict[int, Optional[Tuple[float, float, datetime]]]:
        """Fetch all due candle snapshots through the REST connection pool."""
        if not due_states:
            return {}
        if len(due_states) == 1:
            idx, entry, hour_open = due_states[0]
            return {
                idx: self._fetch_hour_candle_with_retry(
                    entry.symbol,
                    hour_open,
                    snapshot_as_of_utc=snapshot_as_of_utc,
                )
            }

        workers = min(len(due_states), max(1, min(10, self.ranker_max_workers)))
        results: Dict[int, Optional[Tuple[float, float, datetime]]] = {}
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="entry-kline") as executor:
            futures = {
                idx: executor.submit(
                    self._fetch_hour_candle_with_retry,
                    entry.symbol,
                    hour_open,
                    snapshot_as_of_utc,
                )
                for idx, entry, hour_open in due_states
            }
            for idx, future in futures.items():
                try:
                    results[idx] = future.result()
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning(
                        "Entry bearish-hour parallel kline fetch failed: account=%s idx=%s error=%s",
                        self.account_id,
                        idx,
                        exc,
                    )
                    results[idx] = None
        return results

    def _fetch_hour_candle_snapshot(
        self,
        symbol: str,
        hour_open_utc: datetime,
        as_of_utc: datetime,
    ) -> Optional[Tuple[float, float, datetime]]:
        """Read the still-forming hourly candle as of a preclose timestamp."""
        hour_open_utc = self._floor_to_utc_hour(hour_open_utc)
        start_ms = int(hour_open_utc.timestamp() * 1000)
        boundary_ms = int((hour_open_utc + timedelta(hours=1)).timestamp() * 1000)
        as_of_ms = min(max(start_ms, int(as_of_utc.timestamp() * 1000)), boundary_ms - 1)
        rows = self.client.get_klines(
            symbol=symbol,
            interval="1h",
            start_time=start_ms,
            end_time=as_of_ms,
            limit=1,
        )
        for row in rows or []:
            if len(row) < 5:
                continue
            try:
                row_open_ms = int(row[0])
            except (TypeError, ValueError):
                continue
            if row_open_ms != start_ms:
                continue
            open_price = self._safe_positive_float(row[1])
            close_price = self._safe_positive_float(row[4])
            if open_price is None or close_price is None:
                continue
            return (
                open_price,
                close_price,
                datetime.fromtimestamp(as_of_ms / 1000, tz=timezone.utc),
            )
        return None

    def _fetch_hour_candle(self, symbol: str, hour_open_utc: datetime) -> Optional[Tuple[float, float, datetime]]:
        hour_open_utc = self._floor_to_utc_hour(hour_open_utc)
        start_ms = int(hour_open_utc.timestamp() * 1000)
        end_ms = int((hour_open_utc + timedelta(hours=1)).timestamp() * 1000) - 1
        rows = self.client.get_klines(
            symbol=symbol,
            interval="1h",
            start_time=start_ms,
            end_time=end_ms,
            limit=1,
        )
        for row in rows or []:
            if len(row) < 7:
                continue
            try:
                row_open_ms = int(row[0])
            except (TypeError, ValueError):
                continue
            if row_open_ms != start_ms:
                continue
            open_price = self._safe_positive_float(row[1])
            close_price = self._safe_positive_float(row[4])
            if open_price is None or close_price is None:
                continue
            try:
                close_time_ms = int(row[6])
            except (TypeError, ValueError):
                close_time_ms = end_ms
            close_time = datetime.fromtimestamp(close_time_ms / 1000, tz=timezone.utc)
            return open_price, close_price, close_time
        return None

    def _build_entry_structure_protection(
        self,
        ready_entry: ReadyEntry,
        fill_time_utc: datetime,
        entry_price: float,
    ) -> Optional[EntryStructureProtection]:
        window = self._prepare_entry_structure_window(ready_entry)
        return self._complete_entry_structure_protection(
            symbol=ready_entry.entry.symbol,
            window=window,
            fill_time_utc=fill_time_utc,
            entry_price=entry_price,
        )

    def _prepare_entry_structure_window(
        self,
        ready_entry: ReadyEntry,
    ) -> Optional[EntryStructureWindow]:
        bearish_close_raw = getattr(ready_entry, "bearish_close_time_utc", None)
        if not self.entry_wait_bearish_hour_enabled or not isinstance(bearish_close_raw, datetime):
            return None

        bearish_close = self._closed_hour_boundary(bearish_close_raw)
        return self._fetch_entry_structure_window(
            symbol=ready_entry.entry.symbol,
            bearish_close_time_utc=bearish_close,
        )

    def _fetch_entry_structure_window(
        self,
        symbol: str,
        bearish_close_time_utc: datetime,
    ) -> EntryStructureWindow:
        bearish_close = self._closed_hour_boundary(bearish_close_time_utc)

        window_start = bearish_close - timedelta(hours=2)
        start_ms = int(window_start.timestamp() * 1000)
        end_ms = int(bearish_close.timestamp() * 1000) - 1
        rows = self.client.get_klines(
            symbol=symbol,
            interval="1h",
            start_time=start_ms,
            end_time=end_ms,
            limit=2,
        )
        expected_open_times = {
            start_ms,
            start_ms + 60 * 60 * 1000,
        }
        highs_by_open: Dict[int, float] = {}
        for row in rows or []:
            if len(row) < 3:
                continue
            try:
                row_open_ms = int(row[0])
            except (TypeError, ValueError):
                continue
            if row_open_ms not in expected_open_times:
                continue
            high_price = self._safe_positive_float(row[2])
            if high_price is not None:
                highs_by_open[row_open_ms] = high_price
        missing = expected_open_times.difference(highs_by_open)
        if missing:
            raise RuntimeError(
                f"Missing entry structure candles for {symbol}: "
                f"expected={sorted(expected_open_times)} missing={sorted(missing)}"
            )
        return EntryStructureWindow(
            bearish_close_time_utc=bearish_close,
            window_start_utc=window_start,
            highest_price=max(highs_by_open.values()),
        )

    def _build_finalized_preclose_structure_protection(
        self,
        symbol: str,
        final_close_time_utc: datetime,
    ) -> EntryStructureProtection:
        window = self._fetch_entry_structure_window(
            symbol=symbol,
            bearish_close_time_utc=final_close_time_utc,
        )
        stop_price = self.client.normalize_trigger_price(
            symbol,
            window.highest_price,
            round_up=True,
        )
        if stop_price <= 0:
            raise RuntimeError(f"Invalid finalized preclose structure stop for {symbol}: {stop_price}")
        return EntryStructureProtection(
            stop_price=stop_price,
            bearish_close_time_utc=window.bearish_close_time_utc,
            window_start_utc=window.window_start_utc,
            window_end_utc=window.bearish_close_time_utc,
        )

    @_serialized_account_mutation
    def _apply_finalized_preclose_structure_protection(
        self,
        position_id: int,
        symbol: str,
        protection: EntryStructureProtection,
    ) -> str:
        if self._is_protection_exempt(symbol):
            return "SKIPPED_EXEMPT"

        position = self.store.get_position(position_id)
        if position is None or str(position.get("status") or "").upper() != "OPEN":
            return "SKIPPED_POSITION_NOT_OPEN"

        position_risk = self._load_short_position(symbol)
        if not position_risk:
            return "SKIPPED_POSITION_NOT_ON_EXCHANGE"

        position_amt = abs(float(position_risk.get("positionAmt", "0") or 0))
        if position_amt <= 0:
            return "SKIPPED_POSITION_NOT_ON_EXCHANGE"

        liq_price = self._safe_positive_float(position_risk.get("liquidationPrice"))
        stop_price = self.client.normalize_trigger_price(
            symbol,
            protection.stop_price,
            round_up=True,
        )
        if stop_price <= 0:
            raise RuntimeError(f"Invalid finalized preclose stop for {symbol}: {stop_price}")

        normalized_protection = EntryStructureProtection(
            stop_price=stop_price,
            bearish_close_time_utc=protection.bearish_close_time_utc,
            window_start_utc=protection.window_start_utc,
            window_end_utc=protection.window_end_utc,
        )
        self._entry_structure_protection_state.put(
            position_id=position_id,
            protection=normalized_protection,
        )

        old_sl_price = self._safe_positive_float(position.get("sl_price"))
        if old_sl_price is not None and old_sl_price <= stop_price:
            return "KEPT_TIGHTER_EXISTING_STOP"

        sl_stop_price = self.client.format_trigger_price(
            symbol,
            stop_price,
            round_up=True,
        )
        try:
            sl_order = self._create_exit_order_with_fallback(
                symbol=symbol,
                order_type="STOP_MARKET",
                stop_price=sl_stop_price,
                qty=position_amt,
                client_order_id=self._new_client_id("sls", symbol),
            )
        except BinanceAPIError as exc:
            try:
                error_code = int(exc.code)
            except (TypeError, ValueError):
                error_code = None
            if error_code != -2021 and "immediately trigger" not in str(exc).lower():
                raise
            self._force_close_position(
                position_id=position_id,
                symbol=symbol,
                reason="PRECLOSE_STRUCTURE_IMMEDIATE_TRIGGER",
            )
            return "CLOSED_IMMEDIATE_TRIGGER"

        try:
            self.store.update_stop_loss(
                position_id=position_id,
                sl_order_id=sl_order.get("orderId"),
                sl_client_order_id=sl_order.get("clientOrderId"),
                sl_price=stop_price,
                liq_price_latest=liq_price,
            )
            self.store.add_order_event(
                symbol=symbol,
                position_id=position_id,
                event_time_utc=self._utc_now_iso(),
                order_payload=sl_order,
            )
        except Exception:
            self._cancel_order_after_setup_failure(
                symbol,
                sl_order.get("orderId"),
                sl_order.get("clientOrderId"),
            )
            raise

        try:
            self._cancel_order_if_exists(
                symbol,
                position.get("sl_order_id"),
                position.get("sl_client_order_id"),
            )
        except RuntimeError as exc:
            self.store.set_position_error(position_id, f"preclose_structure_old_sl_cancel: {exc}")
            LOGGER.error(
                "Finalized preclose structure stop is live but old stop cancellation failed: "
                "account=%s position_id=%s symbol=%s error=%s",
                self.account_id,
                position_id,
                symbol,
                exc,
            )
            return "REPLACED_OLD_STOP_CANCEL_FAILED"

        self.store.clear_position_error(position_id)
        return "REPLACED"

    def _complete_entry_structure_protection(
        self,
        symbol: str,
        window: Optional[EntryStructureWindow],
        fill_time_utc: datetime,
        entry_price: float,
    ) -> Optional[EntryStructureProtection]:
        if window is None:
            return None
        fill_time = fill_time_utc.astimezone(timezone.utc)
        fill_local = fill_time.astimezone(self.runtime_timezone)
        local_noon = fill_local.replace(hour=12, minute=0, second=0, microsecond=0)
        if fill_local < local_noon:
            return None
        if fill_time < window.bearish_close_time_utc:
            raise RuntimeError(
                f"Entry fill precedes bearish close for {symbol}: "
                f"fill={fill_time.isoformat()} close={window.bearish_close_time_utc.isoformat()}"
            )
        high_candidates = [window.highest_price, float(entry_price)]
        post_close_high = self._fetch_agg_trade_high(
            symbol=symbol,
            start_utc=window.bearish_close_time_utc,
            end_utc=fill_time,
        )
        if post_close_high is not None:
            high_candidates.append(post_close_high)
        stop_price = self.client.normalize_trigger_price(
            symbol,
            max(high_candidates),
            round_up=True,
        )
        if stop_price <= 0:
            raise RuntimeError(f"Invalid entry structure stop for {symbol}: {stop_price}")
        return EntryStructureProtection(
            stop_price=stop_price,
            bearish_close_time_utc=window.bearish_close_time_utc,
            window_start_utc=window.window_start_utc,
            window_end_utc=fill_time,
        )

    def _fetch_agg_trade_high(
        self,
        symbol: str,
        start_utc: datetime,
        end_utc: datetime,
    ) -> Optional[float]:
        start_ms = int(start_utc.timestamp() * 1000)
        end_ms = int(end_utc.timestamp() * 1000)
        if end_ms <= start_ms:
            return None
        max_window_ms = 60 * 60 * 1000 - 1
        if end_ms - start_ms > max_window_ms:
            highs: List[float] = []
            chunk_start_ms = start_ms
            while chunk_start_ms <= end_ms:
                chunk_end_ms = min(end_ms, chunk_start_ms + max_window_ms)
                chunk_high = self._fetch_agg_trade_high(
                    symbol=symbol,
                    start_utc=datetime.fromtimestamp(chunk_start_ms / 1000.0, tz=timezone.utc),
                    end_utc=datetime.fromtimestamp(chunk_end_ms / 1000.0, tz=timezone.utc),
                )
                if chunk_high is not None:
                    highs.append(chunk_high)
                chunk_start_ms = chunk_end_ms + 1
            return max(highs) if highs else None

        page = self.client.get_agg_trades(
            symbol=symbol,
            start_time=start_ms,
            end_time=end_ms,
            limit=1000,
        )
        highest_price: Optional[float] = None
        previous_last_id: Optional[int] = None
        for _page_number in range(100):
            if not page:
                break
            last_time_ms = 0
            last_id: Optional[int] = None
            for trade in page:
                try:
                    trade_time_ms = int(trade.get("T", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if trade_time_ms < start_ms:
                    continue
                if trade_time_ms > end_ms:
                    last_time_ms = max(last_time_ms, trade_time_ms)
                    continue
                price = self._safe_positive_float(trade.get("p"))
                if price is not None:
                    highest_price = price if highest_price is None else max(highest_price, price)
                last_time_ms = max(last_time_ms, trade_time_ms)
                try:
                    last_id = int(trade.get("a"))
                except (TypeError, ValueError):
                    pass
            if len(page) < 1000 or last_time_ms >= end_ms:
                break
            if last_id is None or last_id == previous_last_id:
                raise RuntimeError(f"Cannot paginate aggregate trades for {symbol}")
            previous_last_id = last_id
            page = self.client.get_agg_trades(
                symbol=symbol,
                from_id=last_id + 1,
                limit=1000,
            )
        else:
            raise RuntimeError(f"Aggregate trade pagination exceeded limit for {symbol}")
        return highest_price

    @staticmethod
    def _closed_hour_boundary(value: datetime) -> datetime:
        close_time = value.astimezone(timezone.utc)
        boundary = close_time.replace(minute=0, second=0, microsecond=0)
        if close_time > boundary:
            boundary += timedelta(hours=1)
        return boundary

    @staticmethod
    def _resolve_entry_fill_time(order: Dict[str, object], fallback: datetime) -> datetime:
        for key in ("updateTime", "transactTime", "time"):
            try:
                timestamp_ms = int(order.get(key) or 0)
            except (TypeError, ValueError):
                continue
            if timestamp_ms > 0:
                return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        return fallback.astimezone(timezone.utc)

    @staticmethod
    def _floor_to_utc_hour(value: datetime) -> datetime:
        return value.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    @_serialized_account_mutation
    def run_equity_recovery_take_profit(self) -> Dict[str, object]:
        if not self.equity_recovery_take_profit_enabled:
            return {"status": "DISABLED"}

        latest = self.store.get_latest_wallet_snapshot()
        if not latest:
            return {"status": "SKIPPED", "reason": "NO_WALLET_SNAPSHOT"}
        current_time_utc = str(latest.get("captured_at_utc") or "").strip()
        current_equity = self._safe_float(latest.get("balance_usdt"), default=0.0)
        if not current_time_utc or current_equity <= 0:
            return {"status": "SKIPPED", "reason": "INVALID_CURRENT_SNAPSHOT"}
        if self._is_equity_recovery_time_blocked(current_time_utc):
            return {"status": "SKIPPED", "reason": "TIME_WINDOW_BLOCKED"}

        state = self.store.get_lock_state(self.EQUITY_RECOVERY_LOCK_NAME) or {}
        current_dt = self._parse_iso_utc(current_time_utc)
        rolling_start_dt = current_dt - timedelta(hours=self.equity_recovery_lookback_hours)
        anchored_start_utc = str(state.get("window_start_utc") or "").strip()
        if anchored_start_utc:
            try:
                anchored_start_dt = self._parse_iso_utc(anchored_start_utc)
                if anchored_start_dt > rolling_start_dt:
                    rolling_start_dt = anchored_start_dt
            except Exception:  # noqa: BLE001
                pass
        start_time_utc = rolling_start_dt.replace(microsecond=0).isoformat()
        min_snapshot = self.store.get_wallet_snapshot_min_since(
            start_captured_at_utc=start_time_utc,
            end_captured_at_utc=current_time_utc,
        )
        if not min_snapshot:
            return {"status": "SKIPPED", "reason": "NO_MIN_SNAPSHOT"}

        cycle_min_time = str(min_snapshot.get("captured_at_utc") or "").strip()
        cycle_min_equity = self._safe_float(min_snapshot.get("balance_usdt"), default=0.0)
        if not cycle_min_time or cycle_min_equity <= 0:
            return {"status": "SKIPPED", "reason": "INVALID_MIN_SNAPSHOT"}

        cycle_key = start_time_utc
        threshold_equity = cycle_min_equity * (1.0 + self.equity_recovery_trigger_pct)
        state_cycle_key = str(state.get("cycle_key") or "").strip()
        state_triggered = bool(state.get("triggered", False))

        if state_cycle_key != cycle_key:
            state_triggered = False

        if state_triggered:
            return {"status": "SKIPPED", "reason": "ALREADY_TRIGGERED_IN_CYCLE", "cycle_key": cycle_key}

        # Avoid floating-point edge misses (e.g. 900 * 1.1 -> 990.0000000000001).
        threshold_eps = max(1e-9, abs(threshold_equity) * 1e-12)
        if current_equity + threshold_eps < threshold_equity:
            self.store.set_lock_state(
                self.EQUITY_RECOVERY_LOCK_NAME,
                {
                    "cycle_key": cycle_key,
                    "cycle_min_equity": cycle_min_equity,
                    "triggered": False,
                    "window_start_utc": start_time_utc,
                    "updated_at_utc": self._utc_now_iso(),
                },
            )
            return {
                "status": "SKIPPED",
                "reason": "THRESHOLD_NOT_REACHED",
                "cycle_key": cycle_key,
                "current_equity": round(current_equity, 6),
                "threshold_equity": round(threshold_equity, 6),
            }

        open_positions = self.store.list_open_positions()
        if not open_positions:
            # Threshold reached with no positions: start a new anchored window from now.
            self.store.set_lock_state(
                self.EQUITY_RECOVERY_LOCK_NAME,
                {
                    "cycle_key": current_time_utc,
                    "cycle_min_equity": cycle_min_equity,
                    "triggered": False,
                    "window_start_utc": current_time_utc,
                    "updated_at_utc": self._utc_now_iso(),
                },
            )
            return {"status": "SKIPPED", "reason": "NO_OPEN_POSITIONS", "cycle_key": current_time_utc}

        adjusted = 0
        errors = 0
        reduced_notional = 0.0
        sync_candidates: Dict[int, Tuple[str, float]] = {}
        detail_rows: List[Dict[str, object]] = []
        risk_rows: List[Dict[str, Any]] = []
        try:
            risk_rows = self.client.get_position_risk()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch position risk in equity recovery stage, fallback per-symbol query: %s", exc)
        risk_map: Dict[str, Dict[str, Any]] = {
            str(row.get("symbol") or "").strip(): row
            for row in risk_rows
            if str(row.get("symbol") or "").strip()
        }
        symbol_rules: Dict[str, Any] = {}
        try:
            symbol_rules = self.client.get_symbol_rules()
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to fetch symbol rules in equity recovery stage: %s", exc)
        for pos in open_positions:
            position_id = int(pos["id"])
            symbol = str(pos["symbol"])
            if self._is_protection_exempt(symbol):
                detail_rows.append({"symbol": symbol, "position_id": position_id, "status": "SKIPPED_EXEMPT"})
                continue
            try:
                risk = risk_map.get(symbol)
                if not risk:
                    risk = self._load_short_position(symbol)
                if not risk:
                    detail_rows.append({"symbol": symbol, "position_id": position_id, "status": "SKIPPED_NO_RISK"})
                    continue
                position_amt_raw = str(risk.get("positionAmt") or "").strip()
                try:
                    position_amt_decimal = abs(Decimal(position_amt_raw))
                except (InvalidOperation, TypeError, ValueError):
                    position_amt_decimal = Decimal("0")
                position_amt = float(position_amt_decimal)
                mark_price = (
                    self._safe_positive_float(risk.get("markPrice"))
                    or self._safe_positive_float(risk.get("entryPrice"))
                    or self._safe_positive_float(pos.get("entry_price"))
                )
                if position_amt <= 0 or not mark_price:
                    detail_rows.append({"symbol": symbol, "position_id": position_id, "status": "SKIPPED_INVALID_QTY"})
                    continue

                if self.equity_recovery_reduce_ratio >= 1.0 - 1e-12:
                    reduce_target_qty: object = position_amt_raw.lstrip("+-")
                else:
                    reduce_target_qty = position_amt_decimal * Decimal(str(self.equity_recovery_reduce_ratio))
                reduce_qty_text = self.client.format_order_qty(symbol, reduce_target_qty)
                reduce_qty = self._safe_float(reduce_qty_text, default=0.0)
                if reduce_qty <= 0:
                    detail_rows.append({"symbol": symbol, "position_id": position_id, "status": "SKIPPED_QTY_ZERO"})
                    continue
                rules = symbol_rules.get(symbol)
                min_qty = self._safe_float(getattr(rules, "min_qty", 0.0), default=0.0)
                if min_qty > 0 and reduce_qty + 1e-12 < min_qty:
                    detail_rows.append({"symbol": symbol, "position_id": position_id, "status": "SKIPPED_BELOW_MIN_QTY"})
                    continue
                min_notional = self._safe_float(getattr(rules, "min_notional", 0.0), default=0.0)
                reduce_notional = reduce_qty * mark_price
                if min_notional > 0 and reduce_notional + 1e-12 < min_notional:
                    detail_rows.append(
                        {"symbol": symbol, "position_id": position_id, "status": "SKIPPED_BELOW_MIN_NOTIONAL"}
                    )
                    continue

                order = self.client.create_order(
                    symbol=symbol,
                    side="BUY",
                    type="MARKET",
                    quantity=reduce_qty_text,
                    reduceOnly=True,
                    newClientOrderId=self._new_client_id("ptp", symbol),
                    newOrderRespType="RESULT",
                )
                self._market_fill_reconciler.record_market_order(
                    symbol=symbol,
                    position_id=position_id,
                    order=order,
                )
                reduced_notional += reduce_qty * mark_price
                adjusted += 1
                sync_candidates[position_id] = (symbol, mark_price)
                detail_rows.append(
                    {
                        "symbol": symbol,
                        "position_id": position_id,
                        "status": "ADJUSTED",
                        "reduce_qty": reduce_qty,
                        "mark_price": mark_price,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                errors += 1
                self.store.set_position_error(position_id, f"equity_recovery_tp: {exc}")
                LOGGER.exception(
                    "Equity recovery take-profit failed for symbol=%s position_id=%s: %s",
                    symbol,
                    position_id,
                    exc,
                )
                detail_rows.append(
                    {
                        "symbol": symbol,
                        "position_id": position_id,
                        "status": "ERROR",
                        "error": str(exc),
                    }
                )

        synced_position_ids = self._sync_positions_after_adjustment_bulk(sync_candidates)
        if synced_position_ids:
            self._refresh_exit_orders_for_positions(synced_position_ids)

        event_id = self.store.add_equity_recovery_event(
            cycle_key=current_time_utc,
            cycle_min_captured_at_utc=cycle_min_time,
            cycle_min_equity_usdt=cycle_min_equity,
            current_captured_at_utc=current_time_utc,
            current_equity_usdt=current_equity,
            trigger_pct=self.equity_recovery_trigger_pct,
            threshold_equity_usdt=threshold_equity,
            reduce_ratio=self.equity_recovery_reduce_ratio,
            open_positions=len(open_positions),
            adjusted_positions=adjusted,
            reduced_notional_usdt=reduced_notional,
            error_count=errors,
            details={"positions": detail_rows},
        )
        lock_triggered = errors == 0
        next_window_start = current_time_utc if lock_triggered else start_time_utc
        self.store.set_lock_state(
            self.EQUITY_RECOVERY_LOCK_NAME,
            {
                "cycle_key": next_window_start,
                "cycle_min_equity": cycle_min_equity,
                "triggered": lock_triggered,
                "window_start_utc": next_window_start,
                "triggered_at_utc": self._utc_now_iso(),
                "event_id": event_id,
                "updated_at_utc": self._utc_now_iso(),
            },
        )
        return {
            "status": "TRIGGERED" if lock_triggered else "PARTIAL",
            "cycle_key": next_window_start,
            "event_id": event_id,
            "open_positions": len(open_positions),
            "adjusted": adjusted,
            "errors": errors,
            "current_equity": round(current_equity, 6),
            "threshold_equity": round(threshold_equity, 6),
            "reduced_notional": round(reduced_notional, 6),
        }

    def _sync_positions_after_adjustment_bulk(
        self,
        position_rows: Dict[int, Tuple[str, float]],
    ) -> Set[int]:
        if not position_rows:
            return set()
        try:
            risk_rows = self.client.get_position_risk()
            risk_map: Dict[str, Dict[str, Any]] = {
                str(row.get("symbol") or "").strip(): row
                for row in risk_rows
                if str(row.get("symbol") or "").strip()
            }
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Bulk sync position risk failed, fallback per-position sync: %s", exc)
            synced: Set[int] = set()
            for position_id, (symbol, fallback_price) in position_rows.items():
                if self._sync_position_after_adjustment(
                    position_id=position_id,
                    symbol=symbol,
                    fallback_price=fallback_price,
                ):
                    synced.add(position_id)
            return synced

        synced: Set[int] = set()
        for position_id, (symbol, fallback_price) in position_rows.items():
            risk = risk_map.get(symbol)
            if not risk:
                self.store.set_position_error(position_id, "equity_recovery sync: short position not found")
                continue
            position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
            if position_amt >= 0:
                self.store.set_position_error(position_id, "equity_recovery sync: short position qty is zero")
                continue
            qty_now = abs(position_amt)
            if qty_now <= 0:
                self.store.set_position_error(position_id, "equity_recovery sync: short position qty is zero")
                continue
            entry_price_now = self._safe_positive_float(risk.get("entryPrice")) or fallback_price
            self.store.set_position_qty(position_id, qty_now, entry_price_now)
            self.store.clear_position_error(position_id)
            synced.add(position_id)
        return synced

    @_serialized_account_mutation
    def recover_pending_entries(self) -> Dict[str, object]:
        positions = self.store.list_pending_entry_positions()
        if not isinstance(positions, list):
            positions = []
        summary: Dict[str, object] = {
            "total": len(positions),
            "deferred": 0,
            "recovered": 0,
            "closed_missing": 0,
            "risk_off": 0,
            "errors": 0,
        }
        if not positions:
            return summary
        now_utc = self._utc_now_datetime()
        for pos in positions:
            position_id = int(pos["id"])
            symbol = str(pos["symbol"])
            created_at_text = str(pos.get("created_at_utc") or pos.get("opened_at_utc") or "").strip()
            if created_at_text:
                try:
                    age_sec = (now_utc - self._parse_iso_utc(created_at_text)).total_seconds()
                except (TypeError, ValueError):
                    age_sec = self.PENDING_EXIT_SETUP_RECOVERY_GRACE_SEC
                if age_sec < self.PENDING_EXIT_SETUP_RECOVERY_GRACE_SEC:
                    summary["deferred"] = int(summary["deferred"]) + 1
                    continue
            try:
                risk = self._load_short_position(symbol)
                if risk is None:
                    self.store.mark_position_closed(
                        position_id=position_id,
                        status="ENTRY_FAILED",
                        close_reason="PENDING_ENTRY_POSITION_NOT_FOUND",
                    )
                    summary["closed_missing"] = int(summary["closed_missing"]) + 1
                    continue
                qty = abs(self._safe_float(risk.get("positionAmt"), default=0.0))
                entry_price = self._safe_positive_float(risk.get("entryPrice"))
                if qty <= 0 or entry_price is None:
                    raise RuntimeError(f"Invalid pending entry exchange snapshot for {symbol}")
                opened_at = self._parse_iso_utc(str(pos.get("opened_at_utc") or self._utc_now_iso()))
                self.store.set_position_entry_fill(
                    position_id=position_id,
                    qty=qty,
                    entry_price=entry_price,
                    liq_price_open=self._safe_positive_float(risk.get("liquidationPrice")),
                    opened_at_utc=opened_at.isoformat(),
                    expire_at_utc=(opened_at + timedelta(hours=self.max_hold_hours)).isoformat(),
                )
                self._place_exit_orders(position_id=position_id, symbol=symbol)
                self.store.mark_position_open(position_id)
                self.store.clear_position_error(position_id)
                summary["recovered"] = int(summary["recovered"]) + 1
            except Exception as exc:  # noqa: BLE001
                summary["errors"] = int(summary["errors"]) + 1
                self.store.set_position_error(position_id, f"pending_entry_recovery: {exc}")
                LOGGER.exception(
                    "Pending entry recovery failed account=%s position_id=%s symbol=%s: %s",
                    self.account_id,
                    position_id,
                    symbol,
                    exc,
                )
                try:
                    self._force_close_position(position_id, symbol, "PENDING_ENTRY_RECOVERY_FAILED")
                    summary["risk_off"] = int(summary["risk_off"]) + 1
                except Exception as close_exc:  # noqa: BLE001
                    LOGGER.exception(
                        "Pending entry recovery risk-off failed account=%s position_id=%s symbol=%s: %s",
                        self.account_id,
                        position_id,
                        symbol,
                        close_exc,
                    )
        return summary

    @_serialized_account_mutation
    def recover_pending_exit_setups(self) -> Dict[str, object]:
        positions = self.store.list_pending_exit_setup_positions()
        summary: Dict[str, object] = {
            "total": len(positions),
            "deferred": 0,
            "recovered": 0,
            "closed_external": 0,
            "risk_off": 0,
            "errors": 0,
        }
        now_utc = self._utc_now_datetime()
        for pos in positions:
            position_id = int(pos["id"])
            symbol = str(pos["symbol"])
            created_at_text = str(pos.get("created_at_utc") or pos.get("opened_at_utc") or "").strip()
            if created_at_text:
                try:
                    age_sec = (now_utc - self._parse_iso_utc(created_at_text)).total_seconds()
                except (TypeError, ValueError):
                    age_sec = self.PENDING_EXIT_SETUP_RECOVERY_GRACE_SEC
                if age_sec < self.PENDING_EXIT_SETUP_RECOVERY_GRACE_SEC:
                    summary["deferred"] = int(summary["deferred"]) + 1
                    continue
            try:
                risk = self._load_short_position(symbol)
                if risk is None:
                    self.store.mark_position_closed(
                        position_id=position_id,
                        status="CLOSED_EXTERNAL",
                        close_reason="PENDING_EXIT_SETUP_POSITION_NOT_FOUND",
                    )
                    summary["closed_external"] = int(summary["closed_external"]) + 1
                    continue
                self._place_exit_orders(position_id=position_id, symbol=symbol)
                self.store.mark_position_open(position_id)
                self.store.clear_position_error(position_id)
                summary["recovered"] = int(summary["recovered"]) + 1
            except Exception as exc:  # noqa: BLE001
                summary["errors"] = int(summary["errors"]) + 1
                self.store.set_position_error(position_id, f"pending_exit_recovery: {exc}")
                LOGGER.exception(
                    "Pending exit setup recovery failed account=%s position_id=%s symbol=%s: %s",
                    self.account_id,
                    position_id,
                    symbol,
                    exc,
                )
                try:
                    result = self._force_close_position(
                        position_id=position_id,
                        symbol=symbol,
                        reason="PENDING_EXIT_SETUP_RECOVERY_FAILED",
                    )
                    if str(result.get("status") or "").startswith("CLOSED"):
                        summary["risk_off"] = int(summary["risk_off"]) + 1
                except Exception as close_exc:  # noqa: BLE001
                    LOGGER.exception(
                        "Pending exit setup risk-off failed account=%s position_id=%s symbol=%s: %s",
                        self.account_id,
                        position_id,
                        symbol,
                        close_exc,
                    )
        return summary

    def _place_exit_orders(
        self,
        position_id: int,
        symbol: str,
        entry_structure_stop_price: Optional[float] = None,
    ) -> None:
        if self._is_protection_exempt(symbol):
            LOGGER.info("Skip initial exit orders for exempt symbol account=%s symbol=%s", self.account_id, symbol)
            return
        position_risk = self._load_short_position(symbol)
        if not position_risk:
            raise RuntimeError(f"Cannot place exits, no position risk for {symbol}")

        entry_price = self._safe_positive_float(position_risk.get("entryPrice"))
        liq_price = self._safe_positive_float(position_risk.get("liquidationPrice"))
        position_amt = abs(float(position_risk.get("positionAmt", "0") or 0))
        if not entry_price or position_amt <= 0:
            raise RuntimeError(f"Invalid position snapshot for {symbol}")

        tp_order = None
        tp_price = None
        if self.fixed_take_profit_enabled:
            tp_raw = entry_price * (1 - self.tp_price_drop_pct / 100.0)
            tp_price = self.client.normalize_trigger_price(symbol, tp_raw, round_up=False)
            tp_stop_price = self.client.format_trigger_price(symbol, tp_price, round_up=False)

        if not liq_price:
            raise RuntimeError(f"No liquidation price available for {symbol}; cannot place stop loss")
        sl_raw = liq_price * (1 - self.sl_liq_buffer_pct / 100.0)
        sl_price = self.client.normalize_trigger_price(symbol, sl_raw, round_up=True)
        if entry_structure_stop_price is None:
            stored_protection = self._entry_structure_protection_state.get(position_id)
            if stored_protection is not None:
                entry_structure_stop_price = stored_protection.stop_price
        structure_stop = self._safe_positive_float(entry_structure_stop_price)
        if structure_stop is not None:
            normalized_structure_stop = self.client.normalize_trigger_price(
                symbol,
                structure_stop,
                round_up=True,
            )
            sl_price = min(sl_price, normalized_structure_stop)
        sl_stop_price = self.client.format_trigger_price(symbol, sl_price, round_up=True)

        sl_order = None
        try:
            if self.fixed_take_profit_enabled:
                tp_order = self._create_exit_order_with_fallback(
                    symbol=symbol,
                    order_type="TAKE_PROFIT_MARKET",
                    stop_price=tp_stop_price,
                    qty=position_amt,
                    client_order_id=self._new_client_id("tp", symbol),
                )

            if sl_price <= 0:
                raise RuntimeError(f"Invalid stop loss price computed for {symbol}: {sl_price}")
            sl_order = self._create_exit_order_with_fallback(
                symbol=symbol,
                order_type="STOP_MARKET",
                stop_price=sl_stop_price,
                qty=position_amt,
                client_order_id=self._new_client_id("sl", symbol),
            )

            self.store.update_position_orders(
                position_id=position_id,
                tp_order_id=tp_order.get("orderId") if tp_order else None,
                sl_order_id=sl_order.get("orderId"),
                tp_client_order_id=tp_order.get("clientOrderId") if tp_order else None,
                sl_client_order_id=sl_order.get("clientOrderId"),
                tp_price=tp_price,
                sl_price=sl_price,
                liq_price_latest=liq_price,
            )
        except Exception:
            if sl_order is not None:
                self._cancel_order_after_setup_failure(
                    symbol,
                    sl_order.get("orderId"),
                    sl_order.get("clientOrderId"),
                )
            if tp_order is not None:
                self._cancel_order_after_setup_failure(
                    symbol,
                    tp_order.get("orderId"),
                    tp_order.get("clientOrderId"),
                )
            raise
        self.store.set_position_qty(position_id, position_amt, entry_price)

        if tp_order is not None:
            self.store.add_order_event(
                symbol=symbol,
                position_id=position_id,
                event_time_utc=self._utc_now_iso(),
                order_payload=tp_order,
            )
        self.store.add_order_event(
            symbol=symbol,
            position_id=position_id,
            event_time_utc=self._utc_now_iso(),
            order_payload=sl_order,
        )

    @_serialized_account_mutation
    def _force_close_position(self, position_id: int, symbol: str, reason: str) -> Dict[str, object]:
        persisted = self.store.get_position(position_id)
        cancel_errors: List[str] = []
        position_risk = self._load_short_position(symbol)
        if not position_risk:
            cancel_errors = self._cancel_risk_off_exit_orders(persisted, symbol, position_id)
            self.store.mark_position_closed(
                position_id=position_id,
                status="CLOSED_EXTERNAL",
                close_reason=reason,
            )
            return {
                "symbol": symbol,
                "position_id": position_id,
                "status": "CLOSED_EXTERNAL",
                "reason": reason,
                "qty": 0.0,
                "close_order_id": None,
                "cancel_errors": cancel_errors,
            }

        qty = abs(float(position_risk.get("positionAmt", "0") or 0))
        if qty <= 0:
            cancel_errors = self._cancel_risk_off_exit_orders(persisted, symbol, position_id)
            self.store.mark_position_closed(
                position_id=position_id,
                status="CLOSED_EXTERNAL",
                close_reason=reason,
            )
            return {
                "symbol": symbol,
                "position_id": position_id,
                "status": "CLOSED_EXTERNAL",
                "reason": reason,
                "qty": 0.0,
                "close_order_id": None,
                "cancel_errors": cancel_errors,
            }

        close_order = self.client.create_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quantity=self.client.format_order_qty(symbol, qty),
            reduceOnly=True,
            newClientOrderId=self._new_client_id("rf", symbol),
            newOrderRespType="RESULT",
        )
        self._market_fill_reconciler.record_market_order(
            symbol=symbol,
            position_id=position_id,
            order=close_order,
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_RISK_OFF",
            close_reason=reason,
            close_order_id=close_order.get("orderId"),
        )
        cancel_errors = self._cancel_risk_off_exit_orders(persisted, symbol, position_id)
        return {
            "symbol": symbol,
            "position_id": position_id,
            "status": "CLOSED_RISK_OFF",
            "reason": reason,
            "qty": qty,
            "close_order_id": close_order.get("orderId"),
            "cancel_errors": cancel_errors,
        }

    def _cancel_risk_off_exit_orders(
        self,
        persisted: Optional[Dict[str, object]],
        symbol: str,
        position_id: int,
    ) -> List[str]:
        cancel_errors: List[str] = []
        if persisted is None:
            return cancel_errors
        for order_id, client_order_id in (
            (persisted.get("tp_order_id"), persisted.get("tp_client_order_id")),
            (persisted.get("sl_order_id"), persisted.get("sl_client_order_id")),
        ):
            try:
                self._cancel_order_if_exists(symbol, order_id, client_order_id)
            except RuntimeError as exc:
                cancel_errors.append(str(exc))
                LOGGER.error(
                    "Exit cancellation failed after risk-off close "
                    "account=%s position_id=%s symbol=%s: %s",
                    self.account_id,
                    position_id,
                    symbol,
                    exc,
                )
        return cancel_errors

    @_serialized_account_mutation
    def _rebalance_to_target(
        self,
        target_count: int,
        reduce_only: bool,
        reason_tag: str,
        run_id: Optional[str] = None,
    ) -> Dict[str, object]:
        summary: Dict[str, object] = {
            "target_count": max(0, int(target_count)),
            "open_positions": 0,
            "planned": 0,
            "adjusted": 0,
            "errors": 0,
            "reduced_notional": 0.0,
            "added_notional": 0.0,
            "target_notional_per_position": 0.0,
            "target_gross_notional": 0.0,
            "equity_usdt": 0.0,
            "mode": self.rebalance_mode,
            "virtual_slots": 0,
        }
        skip_reason: Optional[str] = None
        cycle_id: Optional[int] = None
        try:
            cycle_id = self.store.create_rebalance_cycle(
                run_id=run_id,
                reason_tag=reason_tag,
                mode=self.rebalance_mode,
                reduce_only=reduce_only,
                target_count=target_count,
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Failed to create rebalance cycle row: %s", exc)
        summary["cycle_id"] = cycle_id

        def _finalize_and_return(reason: Optional[str]) -> Dict[str, object]:
            try:
                if cycle_id is not None:
                    self.store.finalize_rebalance_cycle(cycle_id=cycle_id, summary=summary, skip_reason=reason)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to finalize rebalance cycle row: %s", exc)
            return summary

        if target_count <= 0:
            skip_reason = "INVALID_TARGET_COUNT"
            return _finalize_and_return(skip_reason)

        positions = self.store.list_open_positions()
        summary["open_positions"] = len(positions)
        if not positions:
            skip_reason = "NO_OPEN_POSITIONS"
            return _finalize_and_return(skip_reason)

        risk_rows = self.client.get_position_risk()
        risk_map: Dict[str, Dict[str, Any]] = {
            str(row.get("symbol") or "").strip(): row
            for row in risk_rows
            if str(row.get("symbol") or "").strip()
        }
        equity_usdt = self._compute_account_equity_usdt(risk_rows=risk_rows)
        if equity_usdt <= 0:
            skip_reason = "NON_POSITIVE_EQUITY"
            return _finalize_and_return(skip_reason)

        target_gross_notional = equity_usdt * float(self.leverage) * self.rebalance_utilization
        if target_gross_notional <= 0:
            skip_reason = "NON_POSITIVE_TARGET_GROSS_NOTIONAL"
            return _finalize_and_return(skip_reason)
        target_notional_per_position = target_gross_notional / float(target_count)
        target_notional_by_position, virtual_slots = self._build_target_notional_map(
            positions=positions,
            target_count=target_count,
            target_gross_notional=target_gross_notional,
        )
        summary["equity_usdt"] = round(equity_usdt, 6)
        summary["target_notional_per_position"] = round(target_notional_per_position, 6)
        summary["target_gross_notional"] = round(target_gross_notional, 6)
        summary["virtual_slots"] = virtual_slots

        reduce_plans: List[RebalancePlan] = []
        increase_plans: List[RebalancePlan] = []
        action_id_by_position: Dict[int, int] = {}
        for pos in positions:
            if len(reduce_plans) + len(increase_plans) >= self.rebalance_max_adjust_orders:
                break
            position_id = int(pos["id"])
            plan, evaluation = self._build_rebalance_plan(
                pos=pos,
                risk_map=risk_map,
                target_notional=target_notional_by_position.get(position_id, target_notional_per_position),
                reduce_only=reduce_only,
            )
            if cycle_id is not None:
                try:
                    action_id = self.store.add_rebalance_action(
                        cycle_id=cycle_id,
                        run_id=run_id,
                        position_id=position_id,
                        symbol=str(pos.get("symbol") or ""),
                        action_side=str(evaluation.get("side") or "") or None,
                        reduce_only=reduce_only,
                        ref_price=self._safe_float(evaluation.get("ref_price"), default=0.0) or None,
                        current_notional_usdt=self._safe_float(evaluation.get("current_notional"), default=0.0),
                        target_notional_usdt=self._safe_float(evaluation.get("target_notional"), default=0.0),
                        deviation_notional_usdt=self._safe_float(evaluation.get("deviation_notional"), default=0.0),
                        deadband_notional_usdt=self._safe_float(evaluation.get("deadband_notional"), default=0.0),
                        max_adjust_notional_usdt=self._safe_float(
                            evaluation.get("max_adjust_notional"),
                            default=0.0,
                        ),
                        requested_adjust_notional_usdt=self._safe_float(
                            evaluation.get("requested_adjust_notional"),
                            default=0.0,
                        ),
                        qty=self._safe_float(evaluation.get("qty"), default=0.0),
                        est_notional_usdt=self._safe_float(evaluation.get("est_notional"), default=0.0),
                        status="PLANNED" if plan is not None else "SKIPPED",
                        skip_reason=None if plan is not None else str(evaluation.get("reason") or "SKIPPED"),
                    )
                    if plan is not None:
                        action_id_by_position[position_id] = action_id
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception(
                        "Failed to write rebalance action row for position_id=%s symbol=%s: %s",
                        position_id,
                        pos.get("symbol"),
                        exc,
                    )
            if plan is None:
                continue
            if plan.side == "BUY":
                reduce_plans.append(plan)
            else:
                increase_plans.append(plan)

        summary["planned"] = len(reduce_plans) + len(increase_plans)
        if not summary["planned"]:
            skip_reason = "NO_ADJUSTMENT_PLAN"
            return _finalize_and_return(skip_reason)

        touched_position_ids: Set[int] = set()
        reduced_notional = 0.0
        added_notional = 0.0

        for plan in reduce_plans + increase_plans:
            try:
                if plan.side == "SELL":
                    self._ensure_sell_mode_for_rebalance(symbol=plan.symbol, risk=risk_map.get(plan.symbol))
                order_params: Dict[str, object] = {
                    "symbol": plan.symbol,
                    "side": plan.side,
                    "type": "MARKET",
                    "quantity": self.client.format_order_qty(plan.symbol, plan.qty),
                    "newOrderRespType": "RESULT",
                }
                if plan.side == "BUY":
                    order_params["newClientOrderId"] = self._new_client_id(f"rb{reason_tag}", plan.symbol)
                    order_params["reduceOnly"] = True
                if plan.side == "SELL":
                    order = self._create_order_with_cooling_off_retry(
                        submit_order=lambda order_params=order_params.copy(): self.client.create_order(
                            **{
                                **order_params,
                                "newClientOrderId": self._new_client_id(f"rb{reason_tag}", plan.symbol),
                            }
                        ),
                        symbol=plan.symbol,
                        side=plan.side,
                        context=f"rebalance_{reason_tag}",
                    )
                else:
                    order = self.client.create_order(**order_params)
                if plan.side == "BUY":
                    self._market_fill_reconciler.record_market_order(
                        symbol=plan.symbol,
                        position_id=plan.position_id,
                        order=order,
                    )
                else:
                    self.store.add_order_event(
                        symbol=plan.symbol,
                        position_id=plan.position_id,
                        event_time_utc=self._utc_now_iso(),
                        order_payload=order,
                    )
                action_id = action_id_by_position.get(plan.position_id)
                if action_id is not None:
                    self.store.update_rebalance_action_result(
                        action_id=action_id,
                        status="ADJUSTED",
                        order_id=self._safe_int(order.get("orderId")),
                        client_order_id=str(order.get("clientOrderId") or "") or None,
                    )
                if plan.side == "BUY":
                    reduced_notional += plan.est_notional
                else:
                    added_notional += plan.est_notional
                if self._sync_position_after_adjustment(
                    position_id=plan.position_id,
                    symbol=plan.symbol,
                    fallback_price=plan.ref_price,
                ):
                    touched_position_ids.add(plan.position_id)
                summary["adjusted"] = int(summary["adjusted"]) + 1
            except Exception as exc:  # noqa: BLE001
                summary["errors"] = int(summary["errors"]) + 1
                if isinstance(exc, BinanceAPIError) and self._is_insufficient_margin_error(exc):
                    self._log_margin_shortfall_context(
                        stage=f"rebalance_{reason_tag}",
                        symbol=plan.symbol,
                        side=plan.side,
                        requested_notional_usdt=plan.est_notional,
                        error=exc,
                    )
                action_id = action_id_by_position.get(plan.position_id)
                if action_id is not None:
                    try:
                        self.store.update_rebalance_action_result(
                            action_id=action_id,
                            status="ERROR",
                            error=str(exc),
                        )
                    except Exception:  # noqa: BLE001
                        LOGGER.exception(
                            "Failed to update rebalance action error row for position_id=%s",
                            plan.position_id,
                        )
                self.store.set_position_error(plan.position_id, f"rebalance: {exc}")
                LOGGER.exception(
                    "Rebalance order failed for symbol=%s position_id=%s side=%s: %s",
                    plan.symbol,
                    plan.position_id,
                    plan.side,
                    exc,
                )

        summary["reduced_notional"] = round(reduced_notional, 6)
        summary["added_notional"] = round(added_notional, 6)

        if touched_position_ids:
            self._refresh_exit_orders_for_positions(touched_position_ids)
        return _finalize_and_return(skip_reason)

    def _build_rebalance_plan(
        self,
        pos: Dict[str, object],
        risk_map: Dict[str, Dict[str, Any]],
        target_notional: float,
        reduce_only: bool,
    ) -> tuple[Optional[RebalancePlan], Dict[str, object]]:
        position_id = int(pos["id"])
        symbol = str(pos["symbol"])
        evaluation: Dict[str, object] = {
            "position_id": position_id,
            "symbol": symbol,
            "status": "SKIPPED",
            "reason": "UNKNOWN",
            "side": None,
            "ref_price": None,
            "current_notional": 0.0,
            "target_notional": float(target_notional),
            "deviation_notional": 0.0,
            "deadband_notional": 0.0,
            "max_adjust_notional": 0.0,
            "requested_adjust_notional": 0.0,
            "qty": 0.0,
            "est_notional": 0.0,
        }
        risk = risk_map.get(symbol)
        if not risk:
            evaluation["reason"] = "MISSING_POSITION_RISK"
            return None, evaluation

        position_amt = self._safe_float(risk.get("positionAmt"), default=0.0)
        if position_amt >= 0:
            evaluation["reason"] = "NON_SHORT_POSITION"
            return None, evaluation

        mark_price = (
            self._safe_positive_float(risk.get("markPrice"))
            or self._safe_positive_float(risk.get("entryPrice"))
            or self._safe_positive_float(pos.get("entry_price"))
        )
        if not mark_price:
            evaluation["reason"] = "MISSING_MARK_PRICE"
            return None, evaluation

        current_notional = abs(position_amt) * mark_price
        evaluation["ref_price"] = mark_price
        evaluation["current_notional"] = current_notional
        if current_notional <= 0:
            evaluation["reason"] = "NON_POSITIVE_CURRENT_NOTIONAL"
            return None, evaluation

        deviation_notional = target_notional - current_notional
        deadband = max(target_notional, 0.0) * self.rebalance_deadband_pct
        evaluation["deviation_notional"] = deviation_notional
        evaluation["deadband_notional"] = deadband
        if abs(deviation_notional) <= deadband:
            evaluation["reason"] = "WITHIN_DEADBAND"
            return None, evaluation
        if reduce_only and deviation_notional > 0:
            evaluation["reason"] = "REDUCE_ONLY_BLOCKED_INCREASE"
            return None, evaluation

        max_adjust_notional = current_notional * self.rebalance_max_single_adjust_pct
        adjust_notional = min(abs(deviation_notional), max_adjust_notional)
        evaluation["max_adjust_notional"] = max_adjust_notional
        evaluation["requested_adjust_notional"] = adjust_notional
        if adjust_notional < self.rebalance_min_adjust_notional_usdt:
            evaluation["reason"] = "BELOW_MIN_ADJUST_NOTIONAL"
            return None, evaluation

        qty = self.client.normalize_order_qty(symbol, adjust_notional, mark_price)
        evaluation["qty"] = qty
        if qty <= 0:
            evaluation["reason"] = "QTY_NORMALIZED_ZERO"
            return None, evaluation

        side = "BUY" if deviation_notional < 0 else "SELL"
        evaluation["side"] = side
        if reduce_only and side != "BUY":
            evaluation["reason"] = "REDUCE_ONLY_BLOCKED_INCREASE"
            return None, evaluation

        est_notional = qty * mark_price
        evaluation["est_notional"] = est_notional
        if est_notional < self.rebalance_min_adjust_notional_usdt:
            evaluation["reason"] = "EST_NOTIONAL_BELOW_MIN"
            return None, evaluation

        evaluation["status"] = "PLANNED"
        evaluation["reason"] = "PLANNED"
        return (
            RebalancePlan(
                position_id=position_id,
                symbol=symbol,
                side=side,
                qty=qty,
                ref_price=mark_price,
                est_notional=est_notional,
                current_notional=current_notional,
                target_notional=target_notional,
                deviation_notional=deviation_notional,
                deadband_notional=deadband,
                max_adjust_notional=max_adjust_notional,
                requested_adjust_notional=adjust_notional,
            ),
            evaluation,
        )

    def _sync_position_after_adjustment(self, position_id: int, symbol: str, fallback_price: float) -> bool:
        position_risk = self._load_short_position(symbol)
        if not position_risk:
            self.store.set_position_error(position_id, "rebalance sync: short position not found")
            return False

        qty_now = abs(float(position_risk.get("positionAmt", "0") or 0))
        if qty_now <= 0:
            self.store.set_position_error(position_id, "rebalance sync: short position qty is zero")
            return False

        entry_price_now = self._safe_positive_float(position_risk.get("entryPrice")) or fallback_price
        self.store.set_position_qty(position_id, qty_now, entry_price_now)
        self.store.clear_position_error(position_id)
        return True

    def _build_target_notional_map(
        self,
        positions: List[Dict[str, object]],
        target_count: int,
        target_gross_notional: float,
    ) -> tuple[Dict[int, float], int]:
        if not positions or target_count <= 0 or target_gross_notional <= 0:
            return {}, 0

        target_per_position = target_gross_notional / float(target_count)
        if self.rebalance_mode == self.REBALANCE_MODE_EQUAL_RISK:
            return {int(pos["id"]): target_per_position for pos in positions}, max(0, target_count - len(positions))

        now_utc = self._utc_now_datetime()
        weighted_rows: List[Tuple[int, float]] = []
        for pos in positions:
            position_id = int(pos["id"])
            age_hours = self._position_age_hours(pos=pos, now_utc=now_utc)
            weight = self._age_decay_weight(age_hours=age_hours)
            weighted_rows.append((position_id, weight))

        virtual_slots = max(0, target_count - len(weighted_rows))
        total_weight = sum(weight for _, weight in weighted_rows) + float(virtual_slots)
        if total_weight <= 1e-12:
            return {int(pos["id"]): target_per_position for pos in positions}, virtual_slots

        target_map: Dict[int, float] = {}
        for position_id, weight in weighted_rows:
            target_map[position_id] = target_gross_notional * (weight / total_weight)
        return target_map, virtual_slots

    def _age_decay_weight(self, age_hours: float) -> float:
        if age_hours <= 0:
            return 1.0
        half_life = max(1.0, self.rebalance_age_decay_half_life_hours)
        decay = math.exp(-math.log(2.0) * (age_hours / half_life))
        return max(1e-4, decay)

    @classmethod
    def _position_age_hours(cls, pos: Dict[str, object], now_utc: datetime) -> float:
        opened_at = str(pos.get("opened_at_utc") or "").strip()
        if not opened_at:
            return 0.0
        try:
            opened_dt = cls._parse_iso_utc(opened_at)
        except Exception:  # noqa: BLE001
            return 0.0
        delta_sec = (now_utc - opened_dt).total_seconds()
        if delta_sec <= 0:
            return 0.0
        return delta_sec / 3600.0

    @staticmethod
    def _parse_iso_utc(text: str) -> datetime:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _is_equity_recovery_time_blocked(self, current_time_utc: str) -> bool:
        local_time = self._parse_iso_utc(current_time_utc).astimezone(self.runtime_timezone).timetz().replace(tzinfo=None)
        return dt_time(7, 30) <= local_time <= dt_time(12, 0)

    @_serialized_account_mutation
    def _refresh_exit_orders_for_positions(self, position_ids: Set[int]) -> None:
        open_positions = self.store.list_open_positions()
        open_by_id = {int(row["id"]): row for row in open_positions}
        for position_id in sorted(position_ids):
            pos = open_by_id.get(position_id)
            if not pos:
                continue
            symbol = str(pos["symbol"])
            try:
                old_tp_order_id = pos.get("tp_order_id")
                old_tp_client_order_id = pos.get("tp_client_order_id")
                old_sl_order_id = pos.get("sl_order_id")
                old_sl_client_order_id = pos.get("sl_client_order_id")
                self._place_exit_orders(position_id=position_id, symbol=symbol)
                self._cancel_order_if_exists(symbol, old_tp_order_id, old_tp_client_order_id)
                self._cancel_order_if_exists(symbol, old_sl_order_id, old_sl_client_order_id)
                self.store.clear_position_error(position_id)
            except Exception as exc:  # noqa: BLE001
                self.store.set_position_error(position_id, f"rebalance_exit_refresh: {exc}")
                LOGGER.exception(
                    "Refresh exit orders failed for rebalance position_id=%s symbol=%s: %s",
                    position_id,
                    symbol,
                    exc,
                )

    def _compute_account_equity_usdt(self, risk_rows: Optional[List[Dict[str, Any]]] = None) -> float:
        balances = self.client.get_balance()
        wallet_balance = 0.0
        for item in balances:
            if str(item.get("asset", "")).upper() != "USDT":
                continue
            raw = item.get("balance")
            if raw is None:
                raw = item.get("crossWalletBalance")
            if raw is None:
                raw = item.get("availableBalance")
            wallet_balance = self._safe_float(raw, default=0.0)
            break

        rows = risk_rows if risk_rows is not None else self.client.get_position_risk()
        unrealized_pnl = 0.0
        for row in rows:
            unrealized_pnl += self._safe_float(row.get("unRealizedProfit"), default=0.0)
        return wallet_balance + unrealized_pnl

    def _ensure_sell_mode_for_rebalance(self, symbol: str, risk: Optional[Dict[str, Any]]) -> None:
        current_margin_type = str((risk or {}).get("marginType") or "").strip().upper()
        current_leverage = self._safe_int((risk or {}).get("leverage"))
        if current_margin_type == "ISOLATED" and current_leverage == self.leverage:
            return

        try:
            self.client.ensure_isolated_and_leverage(symbol, self.leverage)
        except BinanceAPIError as exc:
            code = self._safe_int(getattr(exc, "code", None))
            if code == -4067:
                LOGGER.warning(
                    "Rebalance SELL continue without ensure for %s due to -4067 (open orders exist): "
                    "marginType=%s leverage=%s target_leverage=%s",
                    symbol,
                    current_margin_type or "-",
                    current_leverage if current_leverage is not None else "-",
                    self.leverage,
                )
                return
            raise

    def _cancel_order_if_exists(self, symbol: str, order_id: object, client_order_id: object) -> None:
        if not order_id and not client_order_id:
            return
        try:
            parsed_order_id = int(order_id) if order_id else None
            parsed_client_order_id = str(client_order_id) if client_order_id else None
            self.client.cancel_order(
                symbol=symbol,
                order_id=parsed_order_id,
                orig_client_order_id=parsed_client_order_id,
            )
        except BinanceAPIError as exc:
            raise RuntimeError(
                f"cancel_order failed for {symbol}/{order_id or '-'}/{client_order_id or '-'}: {exc}"
            ) from exc

    def _cancel_order_after_setup_failure(
        self,
        symbol: str,
        order_id: object,
        client_order_id: object,
    ) -> None:
        try:
            self._cancel_order_if_exists(symbol, order_id, client_order_id)
        except RuntimeError as exc:
            LOGGER.error("Failed to clean up partial exit setup: %s", exc)

    @staticmethod
    def _format_rebalance_summary(summary: Optional[Dict[str, object]]) -> str:
        if not summary:
            return "-"
        return (
            f"mode={summary.get('mode', '-')}, "
            f"planned={int(summary.get('planned', 0))}, "
            f"adjusted={int(summary.get('adjusted', 0))}, "
            f"errors={int(summary.get('errors', 0))}"
        )

    @classmethod
    def _normalize_rebalance_mode(cls, raw_mode: str) -> str:
        normalized = str(raw_mode or "").strip().lower()
        if normalized in {cls.REBALANCE_MODE_EQUAL_RISK, cls.REBALANCE_MODE_AGE_DECAY}:
            return normalized
        if normalized:
            LOGGER.warning("Invalid rebalance_mode=%s, fallback to %s", normalized, cls.REBALANCE_MODE_EQUAL_RISK)
        return cls.REBALANCE_MODE_EQUAL_RISK

    def _build_entry_notification(
        self,
        run_id: str,
        trade_day_utc: str,
        run_status: str,
        opened_symbols: List[str],
        skipped_symbols: List[str],
        entry_failure_details: List[str],
        exit_setup_failure_details: List[str],
        risk_off_details: List[str],
        shrink_retry_details: List[str],
        failed_notional: float,
        opened_count: int,
        failed_count: int,
        entry_failed_count: int,
        exit_setup_failed_count: int,
        available_balance: float,
        effective_balance: float,
    ) -> str:
        summary_rows: List[Tuple[object, object]] = [
            ("run_id", f"`{run_id}`"),
            ("trade_day_utc", f"`{trade_day_utc}`"),
            ("状态", run_status),
            ("可用余额", f"{available_balance:.6f} USDT"),
            ("手续费缓冲", f"{self.entry_fee_buffer_pct:.2f}%"),
            ("缓冲后可用余额", f"{effective_balance:.6f} USDT"),
            ("开仓成功数", opened_count),
            ("失败总数", failed_count),
            ("初始开仓失败", entry_failed_count),
            ("止盈止损挂单失败", exit_setup_failed_count),
            ("缩量重试成功", len(shrink_retry_details)),
            ("跳过(已有仓位)", len(skipped_symbols)),
            ("失败未用名义资金", f"{failed_notional:.4f} USDT"),
        ]
        lines = [
            "### Top10 做空建仓结果",
            "",
            f"- 生成时间(UTC): `{self._utc_now_iso()}`",
            "",
            "### 摘要",
            "",
            format_markdown_kv_table(summary_rows),
        ]

        opened_block = format_markdown_list_section(
            "开仓成功币种",
            [f"`{symbol}`" for symbol in opened_symbols],
            max_items=20,
        )
        if opened_block:
            lines.extend(["", opened_block])

        skipped_block = format_markdown_list_section(
            "已持仓跳过币种",
            [f"`{symbol}`" for symbol in skipped_symbols],
            max_items=20,
        )
        if skipped_block:
            lines.extend(["", skipped_block])

        entry_failed_block = format_markdown_list_section(
            "初始开仓失败明细",
            entry_failure_details,
            max_items=15,
        )
        if entry_failed_block:
            lines.extend(["", entry_failed_block])

        exit_failed_block = format_markdown_list_section(
            "止盈止损挂单失败明细",
            exit_setup_failure_details,
            max_items=15,
        )
        if exit_failed_block:
            lines.extend(["", exit_failed_block])

        risk_off_block = format_markdown_list_section(
            "自动风险平仓明细",
            risk_off_details,
            max_items=15,
        )
        if risk_off_block:
            lines.extend(["", risk_off_block])

        shrink_retry_block = format_markdown_list_section(
            "缩量重试成功明细",
            shrink_retry_details,
            max_items=15,
        )
        if shrink_retry_block:
            lines.extend(["", shrink_retry_block])

        return "\n".join(lines)

    def _build_brief_notification(
        self,
        run_id: str,
        trade_day_utc: str,
        status: str,
        reason: str,
        extra_rows: Optional[List[Tuple[object, object]]] = None,
    ) -> str:
        rows: List[Tuple[object, object]] = [
            ("run_id", f"`{run_id}`"),
            ("trade_day_utc", f"`{trade_day_utc}`"),
            ("状态", status),
            ("原因", reason),
        ]
        if extra_rows:
            rows.extend(extra_rows)
        return "\n".join(
            [
                "### Top10 做空执行通知",
                "",
                f"- 生成时间(UTC): `{self._utc_now_iso()}`",
                "",
                format_markdown_kv_table(rows),
            ]
        )

    @staticmethod
    def _join_symbols(symbols: List[str], max_items: int = 20) -> str:
        if not symbols:
            return "-"
        shown = symbols[: max(1, int(max_items))]
        rendered = ", ".join(f"`{symbol}`" for symbol in shown)
        hidden = len(symbols) - len(shown)
        if hidden > 0:
            rendered += f" ... (+{hidden})"
        return rendered

    @staticmethod
    def _select_entry_candidates(
        ranked: List[RankEntry],
        open_symbols: set[str],
        target_count: int,
    ) -> tuple[List[RankEntry], List[str]]:
        candidates: List[RankEntry] = []
        skipped_symbols: List[str] = []
        target = max(0, int(target_count))
        if target == 0:
            return candidates, skipped_symbols
        for entry in ranked:
            if entry.symbol in open_symbols:
                skipped_symbols.append(entry.symbol)
                continue
            candidates.append(entry)
            if len(candidates) >= target:
                break
        return candidates, skipped_symbols

    @staticmethod
    def _build_ranked_entries(top_gainers: List[Dict[str, Any]]) -> List[RankEntry]:
        ranked: List[RankEntry] = []
        for item in top_gainers:
            try:
                ranked.append(
                    RankEntry(
                        symbol=str(item["symbol"]),
                        pct_change=float(item["change"]),
                        last_price=float(item["current_price"]),
                        quote_volume=float(item["volume"]),
                    )
                )
            except Exception:  # noqa: BLE001
                continue
        return ranked

    def _place_market_short_with_shrink_retry(
        self,
        symbol: str,
        target_notional: float,
        reference_price: float,
        client_id_tag: str,
    ) -> tuple[Dict[str, object], int]:
        notional = max(0.0, float(target_notional))
        retries_used = 0
        last_error: Optional[Exception] = None

        for attempt in range(self.entry_shrink_retry_count + 1):
            qty = self.client.normalize_order_qty(symbol, notional, reference_price)
            if qty <= 0:
                break

            try:
                order = self._create_order_with_cooling_off_retry(
                    submit_order=lambda qty_str=self.client.format_order_qty(symbol, qty): self.client.create_order(
                        symbol=symbol,
                        side="SELL",
                        type="MARKET",
                        quantity=qty_str,
                        newClientOrderId=self._new_client_id(client_id_tag, symbol),
                        newOrderRespType="RESULT",
                    ),
                    symbol=symbol,
                    side="SELL",
                    context=self._describe_cooling_off_context(client_id_tag),
                )
                return order, retries_used
            except OrderStateUnknownError as exc:
                risk = self._wait_for_short_position_after_unknown_order(symbol)
                if risk is None:
                    raise
                qty_now = abs(self._safe_float(risk.get("positionAmt"), default=0.0))
                LOGGER.error(
                    "Recovered uncertain entry from exchange position account=%s symbol=%s "
                    "client_id=%s qty=%s",
                    self.account_id,
                    symbol,
                    exc.client_order_id,
                    qty_now,
                )
                return (
                    {
                        "orderId": None,
                        "clientOrderId": exc.client_order_id,
                        "symbol": symbol,
                        "side": "SELL",
                        "type": "MARKET",
                        "status": "POSITION_RECONCILED",
                        "origQty": str(qty_now),
                        "executedQty": str(qty_now),
                        "avgPrice": str(risk.get("entryPrice") or reference_price),
                    },
                    retries_used,
                )
            except BinanceAPIError as exc:
                last_error = exc
                if not self._is_insufficient_margin_error(exc):
                    raise
                if attempt >= self.entry_shrink_retry_count:
                    self._log_margin_shortfall_context(
                        stage="entry",
                        symbol=symbol,
                        side="SELL",
                        requested_notional_usdt=notional,
                        error=exc,
                    )
                    raise

                shrink_factor = 1.0 - (self.entry_shrink_step_pct / 100.0)
                next_notional = notional * shrink_factor
                if next_notional <= 0 or next_notional >= notional:
                    raise

                retries_used += 1
                LOGGER.warning(
                    "Entry shrink-retry %s/%s for %s due to insufficient margin: notional %.6f -> %.6f",
                    retries_used,
                    self.entry_shrink_retry_count,
                    symbol,
                    notional,
                    next_notional,
                )
                notional = next_notional

        if last_error is not None:
            raise last_error
        raise RuntimeError(
            f"{symbol}: qty归一化后为0(缩量重试后不满足最小下单规则)"
        )

    def _wait_for_short_position_after_unknown_order(
        self,
        symbol: str,
        attempts: int = 5,
        delay_sec: float = 0.2,
    ) -> Optional[Dict[str, Any]]:
        for attempt in range(max(1, int(attempts))):
            risk = self._load_short_position(symbol)
            if risk is not None and self._safe_float(risk.get("positionAmt"), default=0.0) < 0:
                return risk
            if attempt + 1 < attempts and delay_sec > 0:
                time.sleep(delay_sec * (2 ** attempt))
        return None

    @classmethod
    def _is_insufficient_margin_error(cls, exc: BinanceAPIError) -> bool:
        code: Optional[int]
        try:
            code = int(exc.code)
        except (TypeError, ValueError):
            code = None
        if code in cls.INSUFFICIENT_MARGIN_ERROR_CODES:
            return True
        msg = str(exc.message or "").lower()
        return "insufficient" in msg and "margin" in msg

    @classmethod
    def _is_cooling_off_error(cls, exc: BinanceAPIError) -> bool:
        code = cls._safe_int(getattr(exc, "code", None))
        return code in cls.COOLING_OFF_ERROR_CODES

    @staticmethod
    def _describe_cooling_off_context(client_id_tag: str) -> str:
        tag = (client_id_tag or "").strip().lower()
        if tag == "ent":
            return "entry"
        if tag == "red":
            return "redistribute"
        return tag or "order"

    def _create_order_with_cooling_off_retry(
        self,
        submit_order: Callable[[], Dict[str, object]],
        symbol: str,
        side: str,
        context: str,
    ) -> Dict[str, object]:
        max_retries = self.cooling_off_retry_count
        delay_sec = self.cooling_off_retry_delay_sec

        for attempt in range(max_retries + 1):
            try:
                return submit_order()
            except BinanceAPIError as exc:
                if not self._is_cooling_off_error(exc):
                    raise
                if max_retries <= 0 or delay_sec <= 0 or attempt >= max_retries:
                    raise
                LOGGER.warning(
                    "Cooling-off retry scheduled: account=%s symbol=%s side=%s context=%s wait_sec=%s retry=%s/%s",
                    self.account_id,
                    symbol,
                    str(side or "").upper() or "-",
                    context,
                    delay_sec,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(delay_sec)

        raise RuntimeError(f"Cooling-off retry exhausted unexpectedly for {symbol}")

    def _log_margin_shortfall_context(
        self,
        stage: str,
        symbol: str,
        side: str,
        requested_notional_usdt: float,
        error: Exception,
    ) -> None:
        requested_notional = max(0.0, float(requested_notional_usdt))
        is_short_add = str(side or "").upper() == "SELL"
        required_margin = (
            requested_notional / float(self.leverage)
            if is_short_add and self.leverage > 0
            else 0.0
        )
        available_balance: Optional[float] = None
        available_balance_err: Optional[str] = None
        try:
            available_balance = float(self.client.get_available_balance("USDT"))
        except Exception as fetch_exc:  # noqa: BLE001
            available_balance_err = str(fetch_exc)
        shortfall = (
            max(0.0, required_margin - available_balance)
            if available_balance is not None
            else None
        )
        LOGGER.warning(
            "Margin shortfall detail: stage=%s symbol=%s side=%s requested_notional=%.6f required_initial_margin=%.6f "
            "available_balance=%s shortfall=%s error=%s%s",
            stage,
            symbol,
            str(side or "").upper() or "-",
            requested_notional,
            required_margin,
            f"{available_balance:.6f}" if available_balance is not None else "n/a",
            f"{shortfall:.6f}" if shortfall is not None else "n/a",
            str(error),
            f" balance_fetch_error={available_balance_err}" if available_balance_err else "",
        )

    def _load_short_position(self, symbol: str) -> Optional[Dict[str, str]]:
        # Retry with exponential backoff for eventual consistency after market order execution.
        max_retries = 8
        base_delay = 0.2
        for attempt in range(max_retries):
            risk_rows = self.client.get_position_risk(symbol=symbol)
            for row in risk_rows:
                if row.get("symbol") != symbol:
                    continue
                amt = float(row.get("positionAmt", "0") or 0)
                if amt < 0:
                    return row
            # Exponential backoff: 0.2s, 0.4s, 0.8s, 1.6s, ...
            delay = min(base_delay * (2 ** attempt), 5.0)
            LOGGER.debug(
                "Position not found yet, retrying: symbol=%s attempt=%s/%s delay=%.2fs",
                symbol,
                attempt + 1,
                max_retries,
                delay,
            )
            time.sleep(delay)
        LOGGER.warning("Position not found after %s retries: symbol=%s", max_retries, symbol)
        return None

    def _create_exit_order_with_fallback(
        self,
        symbol: str,
        order_type: str,
        stop_price: str,
        qty: float,
        client_order_id: str,
    ) -> Dict[str, object]:
        try:
            return self.client.create_order(
                symbol=symbol,
                side="BUY",
                type=order_type,
                stopPrice=stop_price,
                closePosition=True,
                workingType=self.trigger_price_type,
                priceProtect=True,
                newClientOrderId=client_order_id,
            )
        except BinanceAPIError as exc:
            try:
                code = int(exc.code)
            except (TypeError, ValueError):
                code = None
            if code not in {-4120, -4130}:
                raise

            LOGGER.warning(
                "Fallback to reduceOnly conditional order for %s/%s due to Binance error code=%s",
                symbol,
                order_type,
                code,
            )
            return self.client.create_order(
                symbol=symbol,
                side="BUY",
                type=order_type,
                stopPrice=stop_price,
                quantity=self.client.format_order_qty(symbol, qty),
                reduceOnly=True,
                workingType=self.trigger_price_type,
                priceProtect=True,
                newClientOrderId=client_order_id,
            )

    @staticmethod
    def _safe_positive_float(value: object) -> Optional[float]:
        if value is None:
            return None
        try:
            number = float(value)
            if number <= 0:
                return None
            return number
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_int(value: object) -> Optional[int]:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sanitize_client_id_part(value: str, fallback: str, max_len: int) -> str:
        allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
        cleaned = "".join(ch for ch in str(value) if ch in allowed)
        if not cleaned:
            cleaned = fallback
        return cleaned[: max(1, int(max_len))]

    @staticmethod
    def _new_client_id(tag: str, symbol: str) -> str:
        tag_part = Top10ShortStrategy._sanitize_client_id_part(tag, fallback="x", max_len=8).lower()
        symbol_part = Top10ShortStrategy._sanitize_client_id_part(symbol, fallback="sym", max_len=6).upper()
        symbol_hash = hashlib.sha1(str(symbol).encode("utf-8")).hexdigest()[:6]
        nonce = uuid4().hex[:8]
        # Binance Futures requires newClientOrderId to match ^[.A-Z:/a-z0-9_-]{1,36}$.
        return f"t10s-{tag_part}-{symbol_part}-{symbol_hash}-{nonce}"

    @staticmethod
    def _utc_now_datetime() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
