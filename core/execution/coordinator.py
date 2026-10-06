"""AccountCoordinator: serialized short-step trading coordinator for a single account.

Implements Phase D requirements:
1. Coordinates short, serialized execution steps bounded by max_duration_sec.
2. Recovers in-flight UNKNOWN order attempts before new orders are submitted.
3. Resolves due TaskOccurrences and EntryPlan wakeups.
4. Assembles immutable AccountView and MarketView.
5. Invokes pure DecisionKernel for conflict arbitration.
6. Submits arbitrated intents via ExecutionEngine.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

from infra.binance_futures_client import BinanceFuturesClient
from core.execution.engine import ExecutionEngine
from core.execution.ledger import TradingLedger
from core.execution.models import (
    AccountView,
    AttemptStatus,
    DataQuality,
    EpisodeStatus,
    MarketView,
    OrderAttempt,
    OrderIntent,
    PositionEpisode,
    TaskOccurrence,
    utc_now_iso,
)
from core.state_store import StateStore

if TYPE_CHECKING:
    from core.decision.kernel import DecisionKernel


LOGGER = logging.getLogger(__name__)


class AccountCoordinator:
    """Coordinates execution steps and enforces state machine invariants for a single account."""

    def __init__(
        self,
        account_id: str,
        store: StateStore,
        engine: ExecutionEngine,
        ledger: Optional[TradingLedger] = None,
        client: Optional[Any] = None,
        kernel: Optional[DecisionKernel] = None,
        now_iso_fn: Optional[Callable[[], str]] = None,
        strategy: Optional[Any] = None,
        manager: Optional[Any] = None,
    ) -> None:
        self.account_id = account_id
        self.store = store
        self.engine = engine
        self.client = client
        self.strategy = strategy
        self.manager = manager
        self.now_iso_fn = now_iso_fn or utc_now_iso
        self.ledger = ledger or TradingLedger(store=store, now_iso_fn=self.now_iso_fn)
        if kernel is None:
            from core.decision.kernel import DecisionKernel
            self.kernel = DecisionKernel()
        else:
            self.kernel = kernel
        self._revision: int = 0

    def recover_unknown_attempts(self) -> Tuple[List[OrderAttempt], Set[str]]:
        """Query and actively recover in-flight UNKNOWN attempts.

        Returns:
            Tuple of (list_of_attempts_processed, set_of_symbols_still_uncertain)
        """
        uncertain_symbols: Set[str] = set()
        recovered_attempts: List[OrderAttempt] = []

        raw_unknowns = self.store.list_unknown_order_attempts()
        for row in raw_unknowns:
            attempt_id = str(row["attempt_id"])
            symbol = str(row["symbol"])
            try:
                attempt = self.engine.recover_unknown_attempt(attempt_id)
                recovered_attempts.append(attempt)
                if attempt.status == AttemptStatus.UNKNOWN.value:
                    uncertain_symbols.add(symbol)
            except Exception as exc:
                LOGGER.error(
                    "AccountCoordinator: failed to recover unknown attempt=%s symbol=%s: %s",
                    attempt_id,
                    symbol,
                    exc,
                )
                uncertain_symbols.add(symbol)

        return recovered_attempts, uncertain_symbols

    def process_due_tasks(
        self,
        task_handlers: Optional[Dict[str, Callable[[Dict[str, Any]], Any]]] = None,
    ) -> List[Dict[str, Any]]:
        """Retrieve and process pending task occurrences that are due."""
        now_iso = self.now_iso_fn()
        due_tasks = self.store.list_due_task_occurrences(now_iso=now_iso)
        processed: List[Dict[str, Any]] = []

        handlers = task_handlers or {}
        for task in due_tasks:
            task_id = str(task["task_occurrence_id"])
            task_type = str(task["task_type"])
            self.store.update_task_occurrence_status(task_id, status="RUNNING")

            handler = handlers.get(task_type)
            if handler:
                try:
                    handler(task)
                    self.store.update_task_occurrence_status(task_id, status="COMPLETED")
                    processed.append(task)
                except Exception as exc:
                    LOGGER.error(
                        "AccountCoordinator: task execution failed task_id=%s task_type=%s: %s",
                        task_id,
                        task_type,
                        exc,
                    )
                    self.store.update_task_occurrence_status(
                        task_id, status="FAILED", error_message=str(exc)
                    )
            else:
                self.store.update_task_occurrence_status(task_id, status="COMPLETED")
                processed.append(task)

        return processed

    def check_entry_plan_wakeups(self) -> Optional[Dict[str, Any]]:
        """Check if an active entry plan waiting for candle close has reached its wakeup time."""
        plan = self.store.get_active_entry_plan()
        if not plan:
            return None
        if plan.get("status") == "WAITING_KLINE":
            next_wakeup = plan.get("next_wakeup_utc", "")
            if next_wakeup and next_wakeup <= self.now_iso_fn():
                return plan
        return None

    def build_account_view(
        self,
        uncertain_symbols: Optional[Set[str]] = None,
        wallet_balance: Optional[float] = None,
        equity: Optional[float] = None,
    ) -> AccountView:
        """Construct an immutable AccountView from database and memory state."""
        self._revision += 1
        uncertain = set(uncertain_symbols or ())

        # Load open positions
        open_positions_raw = self.store.list_open_positions()
        positions_map: Dict[str, Dict[str, Any]] = {}
        total_unrealized_pnl = 0.0

        for pos in open_positions_raw:
            sym = str(pos["symbol"])
            positions_map[sym] = {
                "id": pos.get("id"),
                "symbol": sym,
                "side": str(pos.get("side") or "SHORT").upper(),
                "qty": float(pos.get("qty") or 0.0),
                "entry_price": float(pos.get("entry_price") or 0.0) if pos.get("entry_price") is not None else None,
                "sl_price": float(pos.get("sl_price")) if pos.get("sl_price") is not None else None,
                "tp_price": float(pos.get("tp_price")) if pos.get("tp_price") is not None else None,
                "sl_order_id": pos.get("sl_order_id"),
                "sl_client_order_id": pos.get("sl_client_order_id"),
                "tp_order_id": pos.get("tp_order_id"),
                "tp_client_order_id": pos.get("tp_client_order_id"),
                "liq_price_latest": pos.get("liq_price_latest"),
                "hold_hours": int(pos.get("hold_hours") or 0),
                "opened_at_utc": pos.get("opened_at_utc"),
                "expire_at_utc": pos.get("expire_at_utc"),
                "episode_id": pos.get("episode_id"),
            }

        # Load open episodes
        open_episodes_list = self.ledger.list_open_episodes()
        episodes_map: Dict[str, PositionEpisode] = {
            ep.symbol: ep for ep in open_episodes_list
        }

        # Load in-flight intents
        active_intents = self.ledger.list_active_intents()

        bal = wallet_balance if wallet_balance is not None else 1000.0
        eq = equity if equity is not None else (bal + total_unrealized_pnl)

        return AccountView(
            account_id=self.account_id,
            revision=self._revision,
            wallet_balance=bal,
            equity=eq,
            positions=positions_map,
            episodes=episodes_map,
            uncertain_symbols=uncertain,
            data_quality=DataQuality.RECONCILING.value if uncertain else DataQuality.CERTAIN.value,
        )


    def build_market_view(
        self,
        prices: Optional[Dict[str, float]] = None,
        top_gainers: Optional[List[Dict[str, Any]]] = None,
        open_symbols: Optional[Set[str]] = None,
    ) -> MarketView:
        """Construct MarketView from available market observations."""
        p_map = dict(prices or {})
        if self.client is not None and open_symbols:
            for sym in open_symbols:
                if sym not in p_map:
                    try:
                        p_map[sym] = float(self.client.get_symbol_price(sym))
                    except Exception as exc:
                        LOGGER.warning("AccountCoordinator: failed to fetch mark price for %s: %s", sym, exc)
        gainers = list(top_gainers or [])
        return MarketView(
            as_of_utc=self.now_iso_fn(),
            prices=p_map,
            top_gainers=gainers,
            data_quality=DataQuality.CERTAIN.value,
        )

    def run_entry_step(
        self,
        trade_day_utc: Optional[str] = None,
        shared_top_gainers: Optional[List[Dict[str, Any]]] = None,
        strategy: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Execute an entry step under coordinator supervision."""
        return self.step(
            action="entry",
            trade_day_utc=trade_day_utc,
            shared_top_gainers=shared_top_gainers,
            strategy=strategy,
        )

    def run_manage_step(
        self,
        wallet_balance: Optional[float] = None,
        equity: Optional[float] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute position management step under coordinator supervision."""
        return self.step(
            action="manage",
            max_duration_sec=30.0,
            wallet_balance=wallet_balance,
            equity=equity,
            config=config,
        )

    def advance_entry_plans(self) -> List[Dict[str, Any]]:
        """Advance awakened entry plans: check confirmation and update status."""
        plan = self.check_entry_plan_wakeups()
        if not plan:
            return []

        plan_id = str(plan.get("plan_id"))
        symbol = str(plan.get("symbol"))
        now_iso = self.now_iso_fn()
        LOGGER.info(
            "AccountCoordinator: entry plan awakened account=%s symbol=%s plan_id=%s",
            self.account_id,
            symbol,
            plan_id,
        )

        is_bearish = True
        if self.client is not None and hasattr(self.client, "get_klines"):
            try:
                klines = self.client.get_klines(symbol, "1h", limit=2)
                if klines and len(klines) >= 1:
                    candle = klines[-2] if len(klines) >= 2 else klines[-1]
                    op = float(candle.get("open") if isinstance(candle, dict) else candle[1])
                    cl = float(candle.get("close") if isinstance(candle, dict) else candle[4])
                    is_bearish = cl < op
            except Exception as exc:
                LOGGER.warning("AccountCoordinator: failed to verify kline for plan %s: %s", plan_id, exc)

        new_status = "CONFIRMED" if is_bearish else "CANCELLED"
        self.store.save_entry_plan(
            plan_id=plan_id,
            symbol=symbol,
            status=new_status,
            plan_payload={"awakened_at_utc": now_iso, "is_bearish": is_bearish},
        )
        plan["status"] = new_status
        return [plan]

    def _execute_protection_update(
        self,
        intent: OrderIntent,
        policy_key: str,
        policy_type: str,
        prefix: str,
        account_view: AccountView,
    ) -> bool:
        """Submit new STOP_MARKET protection order, and on confirmation update state and cancel old order."""
        if not intent.target_price or float(intent.target_price) <= 0:
            return False

        pos = account_view.positions.get(intent.symbol)
        pos_id = pos.get("id") if pos else intent.position_id
        side = intent.side
        qty = intent.target_qty or (float(pos.get("qty") or 0.0) if pos else 0.0)

        # 1. Format trigger price
        stop_price = str(intent.target_price)
        if self.client is not None and hasattr(self.client, "format_trigger_price"):
            try:
                round_up = (side == "BUY")
                res = self.client.format_trigger_price(intent.symbol, intent.target_price, round_up=round_up)
                if isinstance(res, (str, int, float)):
                    stop_price = str(res)
            except Exception:
                pass

        client_order_id = f"{prefix}_{intent.symbol}_{int(time.time() * 1000)}"
        attempt = None

        # 2. Submit new stop order via execution engine
        try:
            attempt = self.engine.submit_intent(
                intent=intent,
                client_order_id=client_order_id,
                close_position=True,
                stop_price=stop_price,
                price_protect=True,
                raise_on_error=True,
            )
        except Exception as exc:
            code = int(getattr(exc, "code", 0) or 0)
            if code in {-4120, -4130}:
                # Fallback to reduceOnly if closePosition is not supported
                try:
                    attempt = self.engine.submit_intent(
                        intent=intent,
                        client_order_id=client_order_id,
                        close_position=False,
                        reduce_only=True,
                        stop_price=stop_price,
                        price_protect=True,
                        raise_on_error=True,
                    )
                except Exception as inner_exc:
                    exc = inner_exc
                    code = int(getattr(exc, "code", 0) or 0)

            # Check if order would trigger immediately (code -2021)
            msg = str(getattr(exc, "message", "") or exc).lower()
            if code == -2021 or "immediately trigger" in msg:
                LOGGER.warning(
                    "AccountCoordinator: Protection stop would trigger immediately for %s. Closing immediately via market order.",
                    intent.symbol,
                )
                close_intent = OrderIntent(
                    intent_id=str(uuid.uuid4()),
                    account_id=self.account_id,
                    client_intent_key=f"prot_mkt_{intent.symbol}_{pos_id}_{int(time.time())}",
                    symbol=intent.symbol,
                    side=side,
                    order_type="MARKET",
                    target_qty=qty,
                    intent_scope="EXIT",
                    position_id=pos_id,
                    episode_id=intent.episode_id,
                    reason=f"{intent.reason}_IMMEDIATE_TRIGGER",
                )
                self.engine.submit_intent(
                    intent=close_intent,
                    client_order_id=f"{prefix}i_{intent.symbol}_{int(time.time() * 1000)}",
                    reduce_only=True,
                    raise_on_error=False,
                )
                # Cancel old exit orders
                if pos:
                    for old_id, old_cid in [
                        (pos.get("sl_order_id"), pos.get("sl_client_order_id")),
                        (pos.get("tp_order_id"), pos.get("tp_client_order_id")),
                    ]:
                        if old_id or old_cid:
                            try:
                                self.engine.cancel_order(
                                    symbol=intent.symbol,
                                    order_id=old_id,
                                    client_order_id=old_cid,
                                    reason="CANCEL_AFTER_IMMEDIATE_PROTECTION_CLOSE",
                                )
                            except Exception as cancel_exc:
                                LOGGER.warning("Failed to cancel old order for %s: %s", intent.symbol, cancel_exc)
                return True

            if attempt is None or attempt.status not in {AttemptStatus.ACKNOWLEDGED.value, AttemptStatus.FILLED.value}:
                LOGGER.error("AccountCoordinator: Failed to submit protection stop for %s: %s", intent.symbol, exc)
                return False

        # 3. Verify attempt was confirmed / acknowledged by exchange
        is_confirmed = (
            attempt is not None
            and (
                attempt.status in {AttemptStatus.ACKNOWLEDGED.value, AttemptStatus.FILLED.value}
                or bool(attempt.exchange_order_id)
            )
        )
        if not is_confirmed:
            LOGGER.error(
                "AccountCoordinator: Protection stop not confirmed by exchange for %s: %s",
                intent.symbol,
                getattr(attempt, "error_message", None),
            )
            return False

        # 4. Now that the new protection order is confirmed on exchange:
        # Save protection policy state
        payload_key = "noon_sl_price" if prefix == "nsl" else "morning_sl_price"
        self.store.save_protection_policy_state(
            policy_key=policy_key,
            policy_type=policy_type,
            payload={
                "symbol": intent.symbol,
                payload_key: intent.target_price,
                "order_id": attempt.exchange_order_id,
                "client_order_id": attempt.client_order_id,
            },
        )

        # Update local position stop loss in DB
        new_sl_order_id = (
            int(attempt.exchange_order_id)
            if attempt.exchange_order_id and str(attempt.exchange_order_id).isdigit()
            else None
        )
        if pos_id is not None:
            self.store.update_stop_loss(
                position_id=int(pos_id),
                sl_order_id=new_sl_order_id,
                sl_client_order_id=attempt.client_order_id,
                sl_price=float(intent.target_price),
                liq_price_latest=pos.get("liq_price_latest") if pos else None,
            )

        # Cancel the previous old stop-loss order on exchange
        old_sl_order_id = pos.get("sl_order_id") if pos else None
        old_sl_client_id = pos.get("sl_client_order_id") if pos else None
        if (old_sl_order_id or old_sl_client_id) and (
            old_sl_order_id != new_sl_order_id and old_sl_client_id != attempt.client_order_id
        ):
            try:
                self.engine.cancel_order(
                    symbol=intent.symbol,
                    order_id=old_sl_order_id,
                    client_order_id=old_sl_client_id,
                    reason=f"REPLACE_{policy_key}_STOP_LOSS",
                )
            except Exception as exc:
                LOGGER.warning("AccountCoordinator: Failed to cancel old SL order for %s: %s", intent.symbol, exc)

        return True

    def _execute_intents(
        self,
        intents: List[OrderIntent],
        start_mono: float,
        max_duration_sec: float,
        account_view: Optional[AccountView] = None,
    ) -> Tuple[List[OrderAttempt], bool]:
        """Execute arbitrated intents within the bounded execution budget."""
        submitted_attempts: List[OrderAttempt] = []
        timed_out = False

        for intent in intents:
            elapsed = time.monotonic() - start_mono
            if elapsed >= max_duration_sec:
                LOGGER.warning(
                    "AccountCoordinator: step exceeded max_duration_sec=%.2f, deferring remaining intents",
                    max_duration_sec,
                )
                timed_out = True
                break

            try:
                if (
                    intent.order_type == "STOP_MARKET"
                    and intent.intent_scope == "PROTECTION"
                    and account_view is not None
                ):
                    is_morning = "MORNING" in (intent.reason or "").upper()
                    pol_key = "MORNING_PROTECTION" if is_morning else "NOON_PROTECTION"
                    pol_type = "MORNING_CAPS" if is_morning else "NOON_CAPS"
                    pfx = "msl" if is_morning else "nsl"
                    self._execute_protection_update(
                        intent=intent,
                        policy_key=pol_key,
                        policy_type=pol_type,
                        prefix=pfx,
                        account_view=account_view,
                    )
                else:
                    attempt = self.engine.submit_intent(intent)
                    submitted_attempts.append(attempt)
            except Exception as exc:
                LOGGER.error(
                    "AccountCoordinator: error submitting intent=%s: %s",
                    intent.intent_id,
                    exc,
                )

        return submitted_attempts, timed_out

    def step(
        self,
        max_duration_sec: float = 30.0,
        action: Optional[str] = None,
        trade_day_utc: Optional[str] = None,
        shared_top_gainers: Optional[List[Dict[str, Any]]] = None,
        strategy: Optional[Any] = None,
        now_local: Optional[datetime] = None,
        day_start_utc: Optional[str] = None,
        noon_time_utc: Optional[str] = None,
        check_time_utc: Optional[datetime] = None,
        symbols: Optional[Set[str]] = None,
        min_hold_hours: Optional[float] = None,
        prices: Optional[Dict[str, float]] = None,
        top_gainers: Optional[List[Dict[str, Any]]] = None,
        task_handlers: Optional[Dict[str, Callable[[Dict[str, Any]], Any]]] = None,
        config: Optional[Dict[str, Any]] = None,
        wallet_balance: Optional[float] = None,
        equity: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Execute a single bounded, serialized coordination step natively.

        Lifecycle:
        1. Measure bounded execution budget.
        2. Actively recover in-flight UNKNOWN order attempts.
        3. Dispatch specific action workflow (entry, loss_cut, protection, cleanup) or general step.
        4. Detect EntryPlan wakeups and due tasks.
        5. Build AccountView and MarketView.
        6. Pure DecisionKernel arbitration (respects uncertainty freeze).
        7. Execute arbitrated intents within budget.
        """
        start_mono = time.monotonic()
        cfg = dict(config or {})

        # 1. Recover UNKNOWN attempts first on any step
        recovered_attempts, uncertain_symbols = self.recover_unknown_attempts()

        if action == "entry":
            gainers = list(shared_top_gainers or top_gainers or [])
            if not gainers:
                if self.client is not None and hasattr(self.client, "session"):
                    try:
                        from core.ranking_top_gainers import build_top_gainers
                        fetch_top_n = int(cfg.get("top_n", 10)) * 2
                        gainers = build_top_gainers(
                            top_n=fetch_top_n,
                            volume_threshold=float(cfg.get("volume_threshold", 5000000.0)),
                            session=self.client.session,
                            base_url=getattr(self.client, "base_url", None),
                        )
                    except Exception as exc:
                        LOGGER.warning("AccountCoordinator: failed to build top gainers: %s", exc)
                        gainers = []

            advanced_plans = self.advance_entry_plans()
            for p in advanced_plans:
                if p.get("status") == "CONFIRMED":
                    gainers.append({"symbol": p["symbol"], "price": 0.0})

            account_view = self.build_account_view(
                uncertain_symbols=uncertain_symbols,
                wallet_balance=wallet_balance,
                equity=equity,
            )
            market_view = self.build_market_view(
                prices=prices,
                top_gainers=gainers,
                open_symbols=set(account_view.positions.keys()),
            )

            arbitrated_intents = self.kernel.decide(
                account_view=account_view,
                market_view=market_view,
                config=cfg,
            )

            submitted_attempts, timed_out = self._execute_intents(
                intents=arbitrated_intents,
                start_mono=start_mono,
                max_duration_sec=max_duration_sec,
            )

            opened_count = sum(1 for a in submitted_attempts if a.status == AttemptStatus.FILLED.value)
            failed_count = sum(1 for a in submitted_attempts if a.status in {AttemptStatus.REJECTED.value, AttemptStatus.CANCELED.value})

            return {
                "status": "TIMED_OUT" if timed_out else "COMPLETED",
                "opened": opened_count,
                "failed": failed_count,
                "entry_failed": failed_count,
                "exit_setup_failed": 0,
                "arbitrated_intents_count": len(arbitrated_intents),
                "submitted_intents_count": len(submitted_attempts),
                "total": len(account_view.positions),
                "errors": 1 if timed_out else 0,
            }

        if action == "loss_cut":
            loss_cfg = dict(cfg)
            loss_cfg["portfolio_loss_cut_enabled"] = True

            cycle_key = trade_day_utc or self.now_iso_fn()[:10]
            existing_target_set = self.store.get_risk_cycle_target_set("LOSS_CUT", cycle_key)
            if existing_target_set and existing_target_set.get("status") == "COMPLETED":
                return {
                    "status": "ALREADY_TRIGGERED",
                    "triggered": True,
                    "close_complete": True,
                    "cycle_date": cycle_key,
                    "total": 0,
                    "errors": 0,
                }

            account_view = self.build_account_view(
                uncertain_symbols=uncertain_symbols,
                wallet_balance=wallet_balance,
                equity=equity,
            )
            market_view = self.build_market_view(
                prices=prices,
                top_gainers=top_gainers,
                open_symbols=set(account_view.positions.keys()),
            )

            arbitrated_intents = self.kernel.decide(
                account_view=account_view,
                market_view=market_view,
                config=loss_cfg,
            )

            loss_cut_intents = [it for it in arbitrated_intents if it.reason == "PORTFOLIO_LOSS_CUT"]
            if loss_cut_intents:
                self.store.save_risk_cycle_target_set(
                    cycle_type="LOSS_CUT",
                    cycle_key=cycle_key,
                    targets={"cycle_date": cycle_key, "count": len(loss_cut_intents)},
                    status="TRIGGERED",
                )

            submitted_attempts, timed_out = self._execute_intents(
                intents=loss_cut_intents or arbitrated_intents,
                start_mono=start_mono,
                max_duration_sec=max_duration_sec,
            )

            close_complete = all(a.status == AttemptStatus.FILLED.value for a in submitted_attempts)
            if loss_cut_intents and close_complete:
                self.store.save_risk_cycle_target_set(
                    cycle_type="LOSS_CUT",
                    cycle_key=cycle_key,
                    targets={"cycle_date": cycle_key, "count": len(loss_cut_intents), "close_complete": True},
                    status="COMPLETED",
                )

            return {
                "status": "TRIGGERED" if loss_cut_intents else "MONITORING",
                "triggered": bool(loss_cut_intents),
                "close_complete": close_complete,
                "cycle_date": cycle_key,
                "total": len(account_view.positions),
                "closed": sum(1 for a in submitted_attempts if a.status == AttemptStatus.FILLED.value),
                "errors": 1 if timed_out else 0,
            }

        if action == "noon_protection":
            noon_cfg = dict(cfg)
            noon_cfg["noon_protection_enabled"] = True

            account_view = self.build_account_view(
                uncertain_symbols=uncertain_symbols,
                wallet_balance=wallet_balance,
                equity=equity,
            )

            noon_prices = dict(prices or {})
            if self.client is not None and hasattr(self.client, "get_klines"):
                for sym in account_view.positions:
                    if sym not in noon_prices:
                        try:
                            klines = self.client.get_klines(sym, "1h", limit=12)
                            if klines:
                                highs = [float(k.get("high") if isinstance(k, dict) else k[2]) for k in klines]
                                noon_prices[sym] = max(highs)
                        except Exception as exc:
                            LOGGER.warning("AccountCoordinator: failed to fetch noon klines for %s: %s", sym, exc)

            market_view = self.build_market_view(
                prices=noon_prices,
                top_gainers=top_gainers,
                open_symbols=set(account_view.positions.keys()),
            )

            arbitrated_intents = self.kernel.decide(
                account_view=account_view,
                market_view=market_view,
                config=noon_cfg,
            )

            updated_sl_count = 0
            for intent in arbitrated_intents:
                if intent.reason == "NOON_PROTECTION_UPDATE" and intent.target_price:
                    if self._execute_protection_update(
                        intent=intent,
                        policy_key="NOON_PROTECTION",
                        policy_type="NOON_CAPS",
                        prefix="nsl",
                        account_view=account_view,
                    ):
                        updated_sl_count += 1

            return {
                "status": "COMPLETED",
                "total": len(account_view.positions),
                "updated_sl": updated_sl_count,
                "skipped": len(account_view.positions) - updated_sl_count,
                "errors": 0,
            }

        if action == "morning_protection":
            morning_cfg = dict(cfg)
            morning_cfg["morning_protection_enabled"] = True

            account_view = self.build_account_view(
                uncertain_symbols=uncertain_symbols,
                wallet_balance=wallet_balance,
                equity=equity,
            )
            market_view = self.build_market_view(
                prices=prices,
                top_gainers=top_gainers,
                open_symbols=set(account_view.positions.keys()),
            )

            arbitrated_intents = self.kernel.decide(
                account_view=account_view,
                market_view=market_view,
                config=morning_cfg,
            )

            updated_sl_count = 0
            for intent in arbitrated_intents:
                if intent.reason == "MORNING_PROTECTION_UPDATE" and intent.target_price:
                    if self._execute_protection_update(
                        intent=intent,
                        policy_key="MORNING_PROTECTION",
                        policy_type="MORNING_CAPS",
                        prefix="msl",
                        account_view=account_view,
                    ):
                        updated_sl_count += 1

            return {
                "status": "COMPLETED",
                "total": len(account_view.positions),
                "updated_sl": updated_sl_count,
                "skipped": len(account_view.positions) - updated_sl_count,
                "errors": 0,
            }

        if action == "hourly_take_profit":
            htp_cfg = dict(cfg)
            htp_cfg["hourly_exchange_take_profit_enabled"] = True
            drop_pct = float(cfg.get("hourly_exchange_take_profit_drop_pct", 5.0))
            htp_cfg["hourly_exchange_take_profit_drop_pct"] = drop_pct

            account_view = self.build_account_view(
                uncertain_symbols=uncertain_symbols,
                wallet_balance=wallet_balance,
                equity=equity,
            )

            if self.client is not None and hasattr(self.client, "get_klines"):
                for sym in account_view.positions:
                    try:
                        klines = self.client.get_klines(sym, "1h", limit=2)
                        if klines and len(klines) >= 1:
                            k = klines[-2] if len(klines) >= 2 else klines[-1]
                            op = float(k.get("open") if isinstance(k, dict) else k[1])
                            cl = float(k.get("close") if isinstance(k, dict) else k[4])
                            if cl > op:
                                htp_cfg[f"hourly_bullish_{sym}"] = True
                    except Exception as exc:
                        LOGGER.warning("AccountCoordinator: failed to get 1h klines for %s: %s", sym, exc)

            market_view = self.build_market_view(
                prices=prices,
                top_gainers=top_gainers,
                open_symbols=set(account_view.positions.keys()),
            )

            arbitrated_intents = self.kernel.decide(
                account_view=account_view,
                market_view=market_view,
                config=htp_cfg,
            )

            tp_intents = [it for it in arbitrated_intents if it.reason == "HOURLY_EXCHANGE_TAKE_PROFIT"]
            submitted_attempts, timed_out = self._execute_intents(
                intents=tp_intents,
                start_mono=start_mono,
                max_duration_sec=max_duration_sec,
            )

            closed_tp = sum(1 for a in submitted_attempts if a.status == AttemptStatus.FILLED.value)
            if closed_tp > 0:
                self.store.save_protection_policy_state(
                    policy_key="HOURLY_TP",
                    policy_type="HOURLY_TP",
                    payload={"closed_tp": closed_tp, "updated_at_utc": self.now_iso_fn()},
                )

            return {
                "status": "COMPLETED",
                "total": len(account_view.positions),
                "closed_tp": closed_tp,
                "errors": 1 if timed_out else 0,
            }

        if action == "orphan_cleanup":
            day_key = self.now_iso_fn()[:10]
            cleanup_state = self.store.get_protection_policy_state("ORPHAN_CLEANUP")
            if isinstance(cleanup_state, dict) and str(cleanup_state.get("day_key") or "") == day_key:
                return {
                    "status": "SKIPPED",
                    "canceled": 0,
                    "details": [],
                    "skipped": True,
                    "day_key": day_key,
                }

            account_view = self.build_account_view(
                uncertain_symbols=uncertain_symbols,
                wallet_balance=wallet_balance,
                equity=equity,
            )
            active_symbols = set(account_view.positions.keys()) | {
                ep.symbol for ep in account_view.episodes.values()
                if ep.status in {EpisodeStatus.OPEN.value, EpisodeStatus.CLOSING.value}
            }

            open_orders: List[Dict[str, Any]] = []
            if self.client is not None and hasattr(self.client, "get_open_orders"):
                try:
                    open_orders = self.client.get_open_orders()
                except Exception as exc:
                    LOGGER.warning("AccountCoordinator: failed to fetch open orders: %s", exc)
            if not open_orders:
                open_orders = self.store.list_exchange_order_state(active_only=True)

            canceled_count = 0
            details: List[str] = []
            for order in open_orders:
                sym = str(order.get("symbol") or "").strip().upper()
                if not sym or sym in active_symbols:
                    continue
                oid = order.get("orderId") or order.get("order_id")
                cid = order.get("clientOrderId") or order.get("client_order_id")
                try:
                    self.engine.cancel_order(
                        symbol=sym,
                        order_id=int(oid) if oid else None,
                        client_order_id=str(cid) if cid else None,
                    )
                    canceled_count += 1
                    details.append(f"{sym}(order_id={oid}, client_id={cid})")
                except Exception as exc:
                    LOGGER.error("AccountCoordinator: failed to cancel orphan order %s for %s: %s", oid or cid, sym, exc)

            self.store.save_protection_policy_state(
                policy_key="ORPHAN_CLEANUP",
                policy_type="ORPHAN_CLEANUP",
                payload={"day_key": day_key, "canceled": canceled_count, "updated_at_utc": self.now_iso_fn()},
            )

            return {
                "status": "COMPLETED",
                "canceled": canceled_count,
                "details": details,
                "skipped": False,
                "day_key": day_key,
            }

        # 2. Process due tasks
        processed_tasks = self.process_due_tasks(task_handlers=task_handlers)

        # 3. Check entry plan wakeup
        advanced_plans = self.advance_entry_plans()

        # 4. Build views
        account_view = self.build_account_view(
            uncertain_symbols=uncertain_symbols,
            wallet_balance=wallet_balance,
            equity=equity,
        )
        market_view = self.build_market_view(
            prices=prices,
            top_gainers=top_gainers,
            open_symbols=set(account_view.positions.keys()),
        )

        # 5. Pure DecisionKernel arbitration
        arbitrated_intents = self.kernel.decide(
            account_view=account_view,
            market_view=market_view,
            config=cfg,
        )

        # 6. Execute arbitrated intents within duration budget
        submitted_attempts, timed_out = self._execute_intents(
            intents=arbitrated_intents,
            start_mono=start_mono,
            max_duration_sec=max_duration_sec,
            account_view=account_view,
        )

        duration = time.monotonic() - start_mono

        tp_count = sum(1 for it in arbitrated_intents if any(k in (it.reason or "").upper() for k in ("TP", "TAKE_PROFIT")))
        sl_count = sum(1 for it in arbitrated_intents if any(k in (it.reason or "").upper() for k in ("SL", "STOP_LOSS", "LOSS_CUT")))
        timeout_count = sum(1 for it in arbitrated_intents if any(k in (it.reason or "").upper() for k in ("TIMEOUT", "EXPIRY")))

        return {
            "account_id": self.account_id,
            "revision": account_view.revision,
            "duration_sec": duration,
            "status": "TIMED_OUT" if timed_out else "COMPLETED",
            "recovered_attempts_count": len(recovered_attempts),
            "uncertain_symbols": sorted(list(uncertain_symbols)),
            "processed_tasks_count": len(processed_tasks),
            "entry_plan_ready": len(advanced_plans) > 0,
            "submitted_intents_count": len(submitted_attempts),
            "arbitrated_intents_count": len(arbitrated_intents),
            "total": len(account_view.positions),
            "closed_tp": tp_count,
            "closed_sl": sl_count,
            "closed_timeout": timeout_count,
            "closed_external": 0,
            "updated_sl": 0,
            "errors": 1 if timed_out else 0,
        }
