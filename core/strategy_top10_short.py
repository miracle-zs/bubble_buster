import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import uuid4

from core.state_store import StateStore
from infra.binance_futures_client import BinanceAPIError, BinanceFuturesClient
from infra.binance_top10_monitor import build_top_gainers
from infra.notifier import (
    ServerChanNotifier,
    format_markdown_kv_table,
    format_markdown_list_section,
)

LOGGER = logging.getLogger(__name__)


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
    REBALANCE_MODE_EQUAL_RISK = "equal_risk"
    REBALANCE_MODE_AGE_DECAY = "age_decay"

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

    def run_entry(self, trade_day_utc: Optional[str] = None) -> Dict[str, object]:
        trade_day = (trade_day_utc or "").strip() or datetime.now(timezone.utc).date().isoformat()
        trade_day_utc = trade_day
        run_id, created = self.store.create_run(trade_day_utc)
        if not created:
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
        entry_failed_count = 0
        exit_setup_failed_count = 0
        skipped_symbols: List[str] = []
        opened_symbols: List[str] = []
        entry_failure_details: List[str] = []
        exit_setup_failure_details: List[str] = []
        risk_off_details: List[str] = []
        shrink_retry_details: List[str] = []
        pre_rebalance_summary: Optional[Dict[str, object]] = None
        post_rebalance_summary: Optional[Dict[str, object]] = None

        try:
            open_symbols = self.store.list_open_symbols()
            fetch_top_n = max(
                self.top_n,
                self.top_n * self.entry_rank_fetch_multiplier,
                self.top_n + len(open_symbols),
            )
            top_gainers = build_top_gainers(
                top_n=fetch_top_n,
                volume_threshold=self.volume_threshold,
                session=self.client.session,
                base_url=self.client.base_url,
                max_workers=self.ranker_max_workers,
                weight_limit_per_minute=self.ranker_weight_limit_per_minute,
                min_request_interval_ms=self.ranker_min_request_interval_ms,
            )

            ranked = [
                RankEntry(
                    symbol=item["symbol"],
                    pct_change=float(item["change"]),
                    last_price=float(item["current_price"]),
                    quote_volume=float(item["volume"]),
                )
                for item in top_gainers
            ]

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
                if self.rebalance_enabled and self.rebalance_after_entry:
                    try:
                        post_rebalance_summary = self._rebalance_to_target(
                            target_count=max(1, len(open_symbols)),
                            reduce_only=False,
                            reason_tag="post",
                            run_id=run_id,
                        )
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.exception("Post-entry rebalance failed when no new candidates: %s", exc)
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
            if self.rebalance_enabled and self.rebalance_after_entry and expected_total_positions > 0:
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

            successful_positions: List[Dict[str, object]] = []
            failed_notional = 0.0

            for entry in candidates:
                try:
                    self.client.ensure_isolated_and_leverage(entry.symbol, self.leverage)
                    qty = self.client.normalize_order_qty(entry.symbol, target_notional, entry.last_price)
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
                        LOGGER.warning("Skip %s due to invalid qty after filter normalization", entry.symbol)
                        continue

                    open_order, retry_count_used = self._place_market_short_with_shrink_retry(
                        symbol=plan.symbol,
                        target_notional=plan.target_notional_usdt,
                        reference_price=entry.last_price,
                        client_id_tag="ent",
                    )
                    if retry_count_used > 0:
                        shrink_retry_details.append(f"{plan.symbol}: 缩量重试{retry_count_used}次后成功")
                    self.store.add_order_event(
                        symbol=plan.symbol,
                        position_id=None,
                        event_time_utc=self._utc_now_iso(),
                        order_payload=open_order,
                    )

                    position_risk = self._load_short_position(plan.symbol)
                    if position_risk is None:
                        raise RuntimeError(f"No short position returned after entry order for {plan.symbol}")

                    entry_price = float(position_risk.get("entryPrice") or entry.last_price)
                    liq_price = self._safe_positive_float(position_risk.get("liquidationPrice"))
                    qty_now = abs(float(position_risk.get("positionAmt", plan.qty)))
                    opened_at = self._utc_now_datetime()
                    expire_at = opened_at + timedelta(hours=self.max_hold_hours)

                    position_id = self.store.insert_position(
                        run_id=run_id,
                        symbol=plan.symbol,
                        side="SHORT",
                        qty=qty_now,
                        entry_price=entry_price,
                        liq_price_open=liq_price,
                        tp_price=None,
                        sl_price=None,
                        tp_order_id=None,
                        sl_order_id=None,
                        tp_client_order_id=None,
                        sl_client_order_id=None,
                        opened_at_utc=opened_at.replace(microsecond=0).isoformat(),
                        expire_at_utc=expire_at.replace(microsecond=0).isoformat(),
                        status="OPEN",
                    )

                    successful_positions.append(
                        {
                            "position_id": position_id,
                            "symbol": plan.symbol,
                            "used_notional": qty_now * entry_price,
                        }
                    )
                    opened_symbols.append(plan.symbol)
                    opened_count += 1
                except Exception as exc:  # noqa: BLE001
                    failed_notional += target_notional
                    entry_failed_count += 1
                    entry_failure_details.append(f"{entry.symbol}: {exc}")
                    LOGGER.exception("Initial entry failed for %s: %s", entry.symbol, exc)

            if failed_notional > 0 and successful_positions:
                self._redistribute_failed_notional(successful_positions, failed_notional)

            for pos in successful_positions:
                position_id = int(pos["position_id"])
                symbol = str(pos["symbol"])
                try:
                    self._place_exit_orders(
                        position_id=position_id,
                        symbol=symbol,
                    )
                except Exception as exc:  # noqa: BLE001
                    exit_setup_failed_count += 1
                    exit_setup_failure_details.append(f"{symbol}: {exc}")
                    LOGGER.exception("Failed to place exit orders for %s: %s", symbol, exc)
                    self.store.set_position_error(position_id, f"exit_setup: {exc}")
                    risk_off_result = self._force_close_position(
                        position_id=position_id,
                        symbol=symbol,
                        reason="EXIT_SETUP_FAILED",
                    )
                    risk_off_details.append(
                        (
                            f"{risk_off_result['symbol']}: status={risk_off_result['status']}, "
                            f"qty={risk_off_result['qty']}, reason={risk_off_result['reason']}"
                        )
                    )

            if self.rebalance_enabled and self.rebalance_after_entry:
                open_positions_after_entry = self.store.list_open_positions()
                if open_positions_after_entry:
                    try:
                        post_rebalance_summary = self._rebalance_to_target(
                            target_count=len(open_positions_after_entry),
                            reduce_only=False,
                            reason_tag="post",
                            run_id=run_id,
                        )
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.exception("Post-entry rebalance failed: %s", exc)

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
                "rebalance_pre": pre_rebalance_summary,
                "rebalance_post": post_rebalance_summary,
            }

        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Entry run failed: %s", exc)
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
            }

    def _redistribute_failed_notional(
        self,
        successful_positions: List[Dict[str, object]],
        failed_notional: float,
    ) -> None:
        total_used = sum(float(item["used_notional"]) for item in successful_positions)
        if total_used <= 0:
            return

        for item in successful_positions:
            symbol = str(item["symbol"])
            position_id = int(item["position_id"])
            weight = float(item["used_notional"]) / total_used
            extra_notional = failed_notional * weight
            try:
                price = self.client.get_symbol_price(symbol)
                extra_qty = self.client.normalize_order_qty(symbol, extra_notional, price)
                if extra_qty <= 0:
                    continue
                add_order, retry_count_used = self._place_market_short_with_shrink_retry(
                    symbol=symbol,
                    target_notional=extra_notional,
                    reference_price=price,
                    client_id_tag="red",
                )
                if retry_count_used > 0:
                    LOGGER.warning(
                        "Redistribution shrink-retry succeeded for %s after %s retries",
                        symbol,
                        retry_count_used,
                    )
                self.store.add_order_event(
                    symbol=symbol,
                    position_id=position_id,
                    event_time_utc=self._utc_now_iso(),
                    order_payload=add_order,
                )

                position_risk = self._load_short_position(symbol)
                if position_risk:
                    qty_now = abs(float(position_risk.get("positionAmt", extra_qty)))
                    entry_price_now = self._safe_positive_float(position_risk.get("entryPrice")) or price
                    self.store.set_position_qty(position_id, qty_now, entry_price_now)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Redistribution failed for %s: %s", symbol, exc)
                self.store.set_position_error(position_id, f"redistribute: {exc}")

    def _place_exit_orders(self, position_id: int, symbol: str) -> None:
        position_risk = self._load_short_position(symbol)
        if not position_risk:
            raise RuntimeError(f"Cannot place exits, no position risk for {symbol}")

        entry_price = self._safe_positive_float(position_risk.get("entryPrice"))
        liq_price = self._safe_positive_float(position_risk.get("liquidationPrice"))
        position_amt = abs(float(position_risk.get("positionAmt", "0") or 0))
        if not entry_price or position_amt <= 0:
            raise RuntimeError(f"Invalid position snapshot for {symbol}")

        tp_raw = entry_price * (1 - self.tp_price_drop_pct / 100.0)
        tp_price = self.client.normalize_trigger_price(symbol, tp_raw, round_up=False)
        tp_stop_price = self.client.format_trigger_price(symbol, tp_price, round_up=False)

        if not liq_price:
            raise RuntimeError(f"No liquidation price available for {symbol}; cannot place stop loss")
        sl_raw = liq_price * (1 - self.sl_liq_buffer_pct / 100.0)
        sl_price = self.client.normalize_trigger_price(symbol, sl_raw, round_up=True)
        sl_stop_price = self.client.format_trigger_price(symbol, sl_price, round_up=True)

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
            tp_order_id=tp_order.get("orderId"),
            sl_order_id=sl_order.get("orderId"),
            tp_client_order_id=tp_order.get("clientOrderId"),
            sl_client_order_id=sl_order.get("clientOrderId"),
            tp_price=tp_price,
            sl_price=sl_price,
            liq_price_latest=liq_price,
        )
        self.store.set_position_qty(position_id, position_amt, entry_price)

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

    def _force_close_position(self, position_id: int, symbol: str, reason: str) -> Dict[str, object]:
        position_risk = self._load_short_position(symbol)
        if not position_risk:
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
            }

        qty = abs(float(position_risk.get("positionAmt", "0") or 0))
        if qty <= 0:
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
        self.store.add_order_event(
            symbol=symbol,
            position_id=position_id,
            event_time_utc=self._utc_now_iso(),
            order_payload=close_order,
        )
        self.store.mark_position_closed(
            position_id=position_id,
            status="CLOSED_RISK_OFF",
            close_reason=reason,
            close_order_id=close_order.get("orderId"),
        )
        return {
            "symbol": symbol,
            "position_id": position_id,
            "status": "CLOSED_RISK_OFF",
            "reason": reason,
            "qty": qty,
            "close_order_id": close_order.get("orderId"),
        }

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
                    "newClientOrderId": self._new_client_id(f"rb{reason_tag}", plan.symbol),
                    "newOrderRespType": "RESULT",
                }
                if plan.side == "BUY":
                    order_params["reduceOnly"] = True
                order = self.client.create_order(**order_params)
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

    def _refresh_exit_orders_for_positions(self, position_ids: Set[int]) -> None:
        open_positions = self.store.list_open_positions()
        open_by_id = {int(row["id"]): row for row in open_positions}
        for position_id in sorted(position_ids):
            pos = open_by_id.get(position_id)
            if not pos:
                continue
            symbol = str(pos["symbol"])
            try:
                self._cancel_order_if_exists(symbol, pos.get("tp_order_id"), pos.get("tp_client_order_id"))
                self._cancel_order_if_exists(symbol, pos.get("sl_order_id"), pos.get("sl_client_order_id"))
                self._place_exit_orders(position_id=position_id, symbol=symbol)
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
            LOGGER.debug("cancel_order ignored for rebalance %s/%s/%s: %s", symbol, order_id, client_order_id, exc)

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
                order = self.client.create_order(
                    symbol=symbol,
                    side="SELL",
                    type="MARKET",
                    quantity=self.client.format_order_qty(symbol, qty),
                    newClientOrderId=self._new_client_id(client_id_tag, symbol),
                    newOrderRespType="RESULT",
                )
                return order, retries_used
            except BinanceAPIError as exc:
                last_error = exc
                if not self._is_insufficient_margin_error(exc):
                    raise
                if attempt >= self.entry_shrink_retry_count:
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

    def _load_short_position(self, symbol: str) -> Optional[Dict[str, str]]:
        # Retry a few times for eventual consistency after market order execution.
        for _ in range(5):
            risk_rows = self.client.get_position_risk(symbol=symbol)
            for row in risk_rows:
                if row.get("symbol") != symbol:
                    continue
                amt = float(row.get("positionAmt", "0") or 0)
                if amt < 0:
                    return row
            time.sleep(0.3)
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
                newClientOrderId=client_order_id,
            )
        except BinanceAPIError as exc:
            try:
                code = int(exc.code)
            except (TypeError, ValueError):
                code = None
            if code != -4120:
                raise

            LOGGER.warning(
                "Fallback to reduceOnly conditional order for %s/%s due to -4120",
                symbol,
                order_type,
            )
            return self.client.create_order(
                symbol=symbol,
                side="BUY",
                type=order_type,
                stopPrice=stop_price,
                quantity=self.client.format_order_qty(symbol, qty),
                reduceOnly=True,
                workingType=self.trigger_price_type,
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
    def _new_client_id(tag: str, symbol: str) -> str:
        return f"t10s-{tag}-{symbol}-{uuid4().hex[:8]}"[:36]

    @staticmethod
    def _utc_now_datetime() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
