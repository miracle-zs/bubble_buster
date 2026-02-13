import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
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


class Top10ShortStrategy:
    INSUFFICIENT_MARGIN_ERROR_CODES = {-2019, -2027, -2028}

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

            if len(candidates) < self.top_n:
                LOGGER.warning(
                    "Entry candidates are fewer than target: target=%s selected=%s skipped_existing=%s fetched_rank=%s",
                    self.top_n,
                    len(candidates),
                    len(skipped_symbols),
                    len(ranked),
                )

            if not candidates:
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
                }

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

            failed_count = entry_failed_count + exit_setup_failed_count
            summary = (
                f"run_id={run_id}, opened={opened_count}, failed={failed_count}, "
                f"entry_failed={entry_failed_count}, exit_setup_failed={exit_setup_failed_count}, "
                f"skipped_existing={len(skipped_symbols)}, failed_notional={failed_notional:.4f}, "
                f"fee_buffer_pct={self.entry_fee_buffer_pct:.2f}, shrink_retry_success={len(shrink_retry_details)}"
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
    def _safe_positive_float(value: Optional[str]) -> Optional[float]:
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
    def _new_client_id(tag: str, symbol: str) -> str:
        return f"t10s-{tag}-{symbol}-{uuid4().hex[:8]}"[:36]

    @staticmethod
    def _utc_now_datetime() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
