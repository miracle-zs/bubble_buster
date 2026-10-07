"""DecisionKernel: pure deterministic trading decision kernel.

Implements the canonical decision architecture:
1. Pure function with zero network or database I/O.
2. Accepts AccountView, MarketView, and rules_config.
3. Evaluates exit triggers (SL, TP, expiry, structure protection, noon/morning protection, portfolio risk).
4. Evaluates rebalance triggers (deadband, age decay, equal risk / target notional).
5. Evaluates entry triggers (rank, confirmation candles, scale-in tranches).
6. Strictly arbitrates conflicts according to canonical priority:
   Full exit > Partial reduce / Portfolio TP > Rebalance > Scale-in > Initial entry.
7. Enforces uncertainty freezing: symbols with UNKNOWN attempts cannot enter or scale in.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional, Set

from core.execution.models import (
    AccountView,
    MarketView,
    OrderIntent,
    utc_now_iso,
)
from core.risk.evaluators import (
    calculate_merged_stop_loss,
    evaluate_portfolio_loss_cut_threshold,
    evaluate_portfolio_take_profit_threshold,
)
from core.strategy.rebalance import RebalanceCalculator

LOGGER = logging.getLogger(__name__)


class DecisionKernel:
    """Pure, side-effect-free decision engine for position management and entry."""

    def decide(
        self,
        account_view: AccountView,
        market_view: MarketView,
        config: Optional[Dict[str, Any]] = None,
    ) -> List[OrderIntent]:
        """Evaluate current views and emit arbitrated OrderIntents."""
        cfg = config or {}
        raw_intents: List[OrderIntent] = []

        # 1. Portfolio-level risk triggers (loss cut, portfolio take profit, equity recovery)
        portfolio_intents = self.evaluate_portfolio_risk(account_view, cfg)
        raw_intents.extend(portfolio_intents)

        # 2. Position-level exit triggers (SL, TP, expiry, protection policies)
        position_exit_intents = self.evaluate_position_exits(account_view, market_view, cfg)
        raw_intents.extend(position_exit_intents)

        # 3. Position-level protection updates (noon caps, morning stops)
        protection_intents = self.evaluate_protections(account_view, market_view, cfg)
        raw_intents.extend(protection_intents)

        # 4. Portfolio rebalancing triggers
        rebalance_intents = self.evaluate_rebalance(account_view, market_view, cfg)
        raw_intents.extend(rebalance_intents)

        # 5. Entry triggers (initial ranking, scale-in tranches)
        entry_intents = self.evaluate_entries(account_view, market_view, cfg)
        raw_intents.extend(entry_intents)

        # 6. Canonical arbitration
        return self.arbitrate(raw_intents, account_view)

    def evaluate_portfolio_risk(
        self,
        account_view: AccountView,
        config: Dict[str, Any],
    ) -> List[OrderIntent]:
        """Evaluate portfolio-level circuit breakers (loss cut / target profit / equity recovery)."""
        intents: List[OrderIntent] = []
        baseline_equity = float(config.get("baseline_equity", account_view.wallet_balance or 0.0))
        current_equity = float(account_view.equity)

        # A. Portfolio loss cut
        loss_cut_enabled = bool(config.get("portfolio_loss_cut_enabled", False))
        loss_cut_pct = float(config.get("portfolio_loss_cut_pct", 3.5))

        if loss_cut_enabled and baseline_equity > 0 and current_equity > 0:
            loss_res = evaluate_portfolio_loss_cut_threshold(
                baseline_equity=baseline_equity,
                current_equity=current_equity,
                loss_pct=loss_cut_pct,
                already_triggered=bool(config.get("portfolio_loss_cut_already_triggered", False)),
                close_complete=bool(config.get("portfolio_loss_cut_close_complete", False)),
            )
            if loss_res.should_trigger:
                for symbol, pos in account_view.positions.items():
                    qty = abs(float(pos.get("qty") or pos.get("current_qty") or 0.0))
                    if qty > 0:
                        intents.append(
                            OrderIntent(
                                intent_id=str(uuid.uuid4()),
                                account_id=account_view.account_id,
                                client_intent_key=f"losscut_{symbol}_{account_view.revision}",
                                symbol=symbol,
                                side="BUY" if str(pos.get("side", "SHORT")).upper() == "SHORT" else "SELL",
                                order_type="MARKET",
                                target_qty=qty,
                                intent_scope="PROTECTION",
                                position_id=pos.get("id"),
                                episode_id=pos.get("episode_id"),
                                reason="PORTFOLIO_LOSS_CUT",
                            )
                        )
                # When loss cut triggers across the portfolio, other portfolio profit checks are skipped
                return intents

        # B. Portfolio take profit
        tp_enabled = bool(config.get("portfolio_take_profit_enabled", False))
        tp_pct = float(config.get("portfolio_take_profit_pct", 9.0))
        giveback_pct = float(config.get("portfolio_take_profit_giveback_pct", 0.0))
        reduce_ratio = min(1.0, max(0.05, float(config.get("portfolio_take_profit_reduce_ratio", 0.5))))
        persisted_peak = float(config.get("portfolio_take_profit_peak_equity", baseline_equity))

        if tp_enabled and baseline_equity > 0 and current_equity > 0:
            tp_res = evaluate_portfolio_take_profit_threshold(
                baseline_equity=baseline_equity,
                current_equity=current_equity,
                persisted_peak_equity=persisted_peak,
                profit_pct=tp_pct,
                giveback_pct=giveback_pct,
                reduce_ratio=reduce_ratio,
                armed=bool(config.get("portfolio_take_profit_armed", False)),
                already_triggered=bool(config.get("portfolio_take_profit_already_triggered", False)),
                close_complete=bool(config.get("portfolio_take_profit_close_complete", False)),
            )
            if tp_res.should_trigger:
                for symbol, pos in account_view.positions.items():
                    qty = abs(float(pos.get("qty") or pos.get("current_qty") or 0.0))
                    if qty > 0:
                        target_qty = qty * reduce_ratio
                        intents.append(
                            OrderIntent(
                                intent_id=str(uuid.uuid4()),
                                account_id=account_view.account_id,
                                client_intent_key=f"porttp_{symbol}_{account_view.revision}",
                                symbol=symbol,
                                side="BUY" if str(pos.get("side", "SHORT")).upper() == "SHORT" else "SELL",
                                order_type="MARKET",
                                target_qty=target_qty,
                                intent_scope="EXIT",
                                position_id=pos.get("id"),
                                episode_id=pos.get("episode_id"),
                                reason="PORTFOLIO_TAKE_PROFIT",
                            )
                        )

        # C. Equity recovery take profit
        equity_rec_enabled = bool(config.get("equity_recovery_take_profit_enabled", False))
        if equity_rec_enabled and current_equity > 0:
            cycle_min_equity = float(config.get("cycle_min_equity", current_equity))
            trigger_pct = float(config.get("equity_recovery_trigger_pct", 0.10))
            rec_reduce_ratio = float(config.get("equity_recovery_reduce_ratio", 0.50))
            if cycle_min_equity > 0:
                recovery_pct = (current_equity - cycle_min_equity) / cycle_min_equity
                if recovery_pct >= trigger_pct:
                    for symbol, pos in account_view.positions.items():
                        qty = abs(float(pos.get("qty") or pos.get("current_qty") or 0.0))
                        if qty > 0:
                            target_qty = qty * rec_reduce_ratio
                            intents.append(
                                OrderIntent(
                                    intent_id=str(uuid.uuid4()),
                                    account_id=account_view.account_id,
                                    client_intent_key=f"eqrec_{symbol}_{account_view.revision}",
                                    symbol=symbol,
                                    side="BUY" if str(pos.get("side", "SHORT")).upper() == "SHORT" else "SELL",
                                    order_type="MARKET",
                                    target_qty=target_qty,
                                    intent_scope="EXIT",
                                    position_id=pos.get("id"),
                                    episode_id=pos.get("episode_id"),
                                    reason="EQUITY_RECOVERY_TAKE_PROFIT",
                                )
                            )

        return intents

    def evaluate_position_exits(
        self,
        account_view: AccountView,
        market_view: MarketView,
        config: Dict[str, Any],
    ) -> List[OrderIntent]:
        """Evaluate individual position stop loss, take profit, expiry, and protection."""
        intents: List[OrderIntent] = []
        now_iso = config.get("now_iso") or market_view.as_of_utc or utc_now_iso()

        for symbol, pos in account_view.positions.items():
            qty = abs(float(pos.get("qty") or pos.get("current_qty") or 0.0))
            if qty <= 0:
                continue

            current_price = market_view.prices.get(symbol)
            side = str(pos.get("side", "SHORT")).upper()
            close_side = "BUY" if side == "SHORT" else "SELL"
            position_id = pos.get("id")
            episode_id = pos.get("episode_id")

            # A. Expiry check
            expire_at_utc = pos.get("expire_at_utc")
            if expire_at_utc and now_iso >= expire_at_utc:
                intents.append(
                    OrderIntent(
                        intent_id=str(uuid.uuid4()),
                        account_id=account_view.account_id,
                        client_intent_key=f"exp_{symbol}_{position_id}",
                        symbol=symbol,
                        side=close_side,
                        order_type="MARKET",
                        target_qty=qty,
                        intent_scope="EXIT",
                        position_id=position_id,
                        episode_id=episode_id,
                        reason="HOLD_EXPIRY",
                    )
                )
                continue

            # B. Price-based checks if mark price is available
            if current_price is not None and current_price > 0:
                # Stop loss check
                sl_price = pos.get("sl_price")
                if sl_price is not None:
                    sl_val = float(sl_price)
                    if (side == "SHORT" and current_price >= sl_val) or (side == "LONG" and current_price <= sl_val):
                        intents.append(
                            OrderIntent(
                                intent_id=str(uuid.uuid4()),
                                account_id=account_view.account_id,
                                client_intent_key=f"sl_{symbol}_{position_id}",
                                symbol=symbol,
                                side=close_side,
                                order_type="MARKET",
                                target_qty=qty,
                                intent_scope="EXIT",
                                position_id=position_id,
                                episode_id=episode_id,
                                reason="STOP_LOSS",
                            )
                        )
                        continue

                # Take profit check
                tp_price = pos.get("tp_price")
                if tp_price is not None:
                    tp_val = float(tp_price)
                    if (side == "SHORT" and current_price <= tp_val) or (side == "LONG" and current_price >= tp_val):
                        intents.append(
                            OrderIntent(
                                intent_id=str(uuid.uuid4()),
                                account_id=account_view.account_id,
                                client_intent_key=f"tp_{symbol}_{position_id}",
                                symbol=symbol,
                                side=close_side,
                                order_type="MARKET",
                                target_qty=qty,
                                intent_scope="EXIT",
                                position_id=position_id,
                                episode_id=episode_id,
                                reason="TAKE_PROFIT",
                            )
                        )
                        continue

                # Hourly exchange take profit check
                hourly_tp_enabled = bool(config.get("hourly_exchange_take_profit_enabled", False))
                hourly_tp_drop_pct = float(config.get("hourly_exchange_take_profit_drop_pct", 5.0 if hourly_tp_enabled else 18.0))
                entry_price = float(pos.get("entry_price") or 0.0)
                if entry_price > 0 and side == "SHORT":
                    drop_pct = (entry_price - current_price) / entry_price * 100.0
                    hourly_bullish = bool(config.get(f"hourly_bullish_{symbol}", hourly_tp_enabled))
                    if drop_pct >= hourly_tp_drop_pct and hourly_bullish:
                        intents.append(
                            OrderIntent(
                                intent_id=str(uuid.uuid4()),
                                account_id=account_view.account_id,
                                client_intent_key=f"ht_p_{symbol}_{position_id}",
                                symbol=symbol,
                                side=close_side,
                                order_type="MARKET",
                                target_qty=qty,
                                intent_scope="EXIT",
                                position_id=position_id,
                                episode_id=episode_id,
                                reason="HOURLY_EXCHANGE_TAKE_PROFIT",
                            )
                        )
                        continue

        return intents

    def evaluate_protections(
        self,
        account_view: AccountView,
        market_view: MarketView,
        config: Dict[str, Any],
    ) -> List[OrderIntent]:
        """Evaluate protection stop loss tightenings (noon caps and morning stops)."""
        intents: List[OrderIntent] = []
        noon_enabled = bool(config.get("noon_protection_enabled", False))
        morning_enabled = bool(config.get("morning_protection_enabled", False))

        if not (noon_enabled or morning_enabled):
            return intents

        for symbol, pos in account_view.positions.items():
            qty = abs(float(pos.get("qty") or pos.get("current_qty") or 0.0))
            if qty <= 0:
                continue

            side = str(pos.get("side", "SHORT")).upper()
            close_side = "BUY" if side == "SHORT" else "SELL"
            old_sl = pos.get("sl_price")
            old_sl_price = float(old_sl) if old_sl is not None else None
            position_id = pos.get("id")
            episode_id = pos.get("episode_id")

            # A. Noon protection
            if noon_enabled:
                noon_ref = config.get(f"noon_ref_price_{symbol}")
                if noon_ref is None and isinstance(config.get("noon_ref_prices"), dict):
                    noon_ref = config["noon_ref_prices"].get(symbol)
                if noon_ref is not None and float(noon_ref) > 0:
                    noon_ref_price = float(noon_ref)
                    merged_sl_price, should_update = calculate_merged_stop_loss(
                        old_sl_price=old_sl_price,
                        new_ref_price=noon_ref_price,
                        close_side=close_side,
                        tick_size=float(config.get(f"tick_size_{symbol}", 1e-6)),
                    )
                    if should_update and merged_sl_price is not None:
                        intents.append(
                            OrderIntent(
                                intent_id=str(uuid.uuid4()),
                                account_id=account_view.account_id,
                                client_intent_key=f"noon_{symbol}_{position_id or episode_id or account_view.revision}",
                                symbol=symbol,
                                side=close_side,
                                order_type="STOP_MARKET",
                                target_qty=qty,
                                target_price=merged_sl_price,
                                intent_scope="PROTECTION",
                                position_id=position_id,
                                episode_id=episode_id,
                                reason="NOON_PROTECTION_UPDATE",
                            )
                        )
                        continue

            # B. Morning protection
            if morning_enabled:
                morning_ref = config.get(f"morning_ref_price_{symbol}")
                if morning_ref is None and isinstance(config.get("morning_ref_prices"), dict):
                    morning_ref = config["morning_ref_prices"].get(symbol)
                if morning_ref is not None and float(morning_ref) > 0:
                    morning_ref_price = float(morning_ref)
                    merged_sl_price, should_update = calculate_merged_stop_loss(
                        old_sl_price=old_sl_price,
                        new_ref_price=morning_ref_price,
                        close_side=close_side,
                        tick_size=float(config.get(f"tick_size_{symbol}", 1e-6)),
                    )
                    if should_update and merged_sl_price is not None:
                        intents.append(
                            OrderIntent(
                                intent_id=str(uuid.uuid4()),
                                account_id=account_view.account_id,
                                client_intent_key=f"morn_{symbol}_{position_id or episode_id or account_view.revision}",
                                symbol=symbol,
                                side=close_side,
                                order_type="STOP_MARKET",
                                target_qty=qty,
                                target_price=merged_sl_price,
                                intent_scope="PROTECTION",
                                position_id=position_id,
                                episode_id=episode_id,
                                reason="MORNING_PROTECTION_UPDATE",
                            )
                        )

        return intents

    def evaluate_rebalance(
        self,
        account_view: AccountView,
        market_view: MarketView,
        config: Dict[str, Any],
    ) -> List[OrderIntent]:
        """Evaluate portfolio rebalance adjustments across active positions."""
        intents: List[OrderIntent] = []
        if not bool(config.get("rebalance_enabled", False)):
            return intents

        positions_list = list(account_view.positions.values())
        if not positions_list:
            return intents

        prices = dict(market_view.prices)
        total_equity = float(account_view.equity)
        allocation_splits = int(config.get("allocation_splits", 10))
        rebalance_utilization = float(config.get("rebalance_utilization", 0.90))
        rebalance_deadband_pct = float(config.get("rebalance_deadband_pct", 0.10))
        rebalance_min_adjust_notional = float(config.get("rebalance_min_adjust_notional_usdt", 20.0))
        rebalance_max_single_adjust_pct = float(config.get("rebalance_max_single_adjust_pct", 0.40))
        rebalance_mode = str(config.get("rebalance_mode", "equal_risk")).strip()
        decay_half_life = float(config.get("rebalance_age_decay_half_life_hours", 36.0))

        plan, _eval_data = RebalanceCalculator.build_rebalance_plan(
            positions=positions_list,
            prices=prices,
            total_equity=total_equity,
            allocation_splits=allocation_splits,
            rebalance_utilization=rebalance_utilization,
            rebalance_deadband_pct=rebalance_deadband_pct,
            rebalance_min_adjust_notional_usdt=rebalance_min_adjust_notional,
            rebalance_max_single_adjust_pct=rebalance_max_single_adjust_pct,
            rebalance_mode=rebalance_mode,
            decay_half_life_hours=decay_half_life,
        )

        for item in plan:
            symbol = item.get("symbol")
            action = item.get("action")  # "REDUCE" or "ADD"
            delta_qty = float(item.get("delta_qty", 0.0))
            if not symbol or delta_qty <= 0:
                continue

            pos = account_view.positions.get(symbol, {})
            pos_id = pos.get("id")
            ep_id = pos.get("episode_id")

            if action == "REDUCE":
                # For short positions, reduction is a BUY
                intents.append(
                    OrderIntent(
                        intent_id=str(uuid.uuid4()),
                        account_id=account_view.account_id,
                        client_intent_key=f"reb_red_{symbol}_{account_view.revision}",
                        symbol=symbol,
                        side="BUY",
                        order_type="MARKET",
                        target_qty=delta_qty,
                        intent_scope="EXIT",
                        position_id=pos_id,
                        episode_id=ep_id,
                        reason="PARTIAL_REDUCE",
                    )
                )
            elif action == "ADD":
                # For short positions, addition is a SELL
                intents.append(
                    OrderIntent(
                        intent_id=str(uuid.uuid4()),
                        account_id=account_view.account_id,
                        client_intent_key=f"reb_add_{symbol}_{account_view.revision}",
                        symbol=symbol,
                        side="SELL",
                        order_type="MARKET",
                        target_qty=delta_qty,
                        intent_scope="REBALANCE",
                        position_id=pos_id,
                        episode_id=ep_id,
                        reason="REBALANCE_ADD",
                    )
                )

        return intents

    def evaluate_entries(
        self,
        account_view: AccountView,
        market_view: MarketView,
        config: Dict[str, Any],
    ) -> List[OrderIntent]:
        """Evaluate candidate symbols for new entries and scale-in tranches."""
        intents: List[OrderIntent] = []
        max_positions = int(config.get("max_positions", 10))
        current_active_symbols = set(account_view.positions.keys())
        target_notional_per_pos = float(config.get("target_notional_per_pos", 100.0))
        scale_in_mode = str(config.get("entry_scale_in_mode", "none")).strip().lower()
        first_ratio = min(0.95, max(0.05, float(config.get("entry_scale_in_first_ratio", 0.50))))

        # A. Scale-in evaluation for existing active positions
        if scale_in_mode == "bullish_then_bearish":
            scale_in_signals = config.get("scale_in_ready_symbols", set())
            if isinstance(scale_in_signals, (list, set, tuple)):
                scale_in_set = set(scale_in_signals)
                for sym in scale_in_set:
                    if sym in current_active_symbols and sym not in account_view.uncertain_symbols:
                        pos = account_view.positions.get(sym, {})
                        ep = account_view.episodes.get(sym)
                        episode_id = ep.episode_id if ep else pos.get("episode_id")
                        price = market_view.prices.get(sym, 0.0)
                        if price > 0:
                            # Sizing: remaining ratio
                            remaining_notional = target_notional_per_pos * (1.0 - first_ratio)
                            qty = remaining_notional / price
                            intents.append(
                                OrderIntent(
                                    intent_id=str(uuid.uuid4()),
                                    account_id=account_view.account_id,
                                    client_intent_key=f"scalein_{sym}_{account_view.revision}",
                                    symbol=sym,
                                    side="SELL",
                                    order_type="MARKET",
                                    target_qty=qty,
                                    intent_scope="ENTRY",
                                    position_id=pos.get("id"),
                                    episode_id=episode_id,
                                    reason="SCALE_IN",
                                )
                            )

        # B. Initial entries
        if len(current_active_symbols) >= max_positions:
            return intents

        slots_available = max_positions - len(current_active_symbols)
        initial_notional = (
            target_notional_per_pos * first_ratio
            if scale_in_mode == "bullish_then_bearish"
            else target_notional_per_pos
        )

        for candidate in market_view.top_gainers:
            if slots_available <= 0:
                break
            symbol = str(candidate.get("symbol") or "").upper().strip()
            if not symbol or symbol in current_active_symbols or symbol in account_view.uncertain_symbols:
                continue

            price = float(candidate.get("current_price") or candidate.get("last_price") or 0.0)
            if price <= 0:
                price = market_view.prices.get(symbol, 0.0)
            if price <= 0:
                continue

            raw_qty = initial_notional / price
            intents.append(
                OrderIntent(
                    intent_id=str(uuid.uuid4()),
                    account_id=account_view.account_id,
                    client_intent_key=f"entry_{symbol}_{account_view.revision}",
                    symbol=symbol,
                    side="SELL",
                    order_type="MARKET",
                    target_qty=raw_qty,
                    intent_scope="ENTRY",
                    reason="TOP10_SHORT_SIGNAL",
                )
            )
            slots_available -= 1

        return intents

    def arbitrate(
        self,
        intents: List[OrderIntent],
        account_view: AccountView,
    ) -> List[OrderIntent]:
        """Arbitrate conflicting actions according to canonical rules:

        Priority:
        1. Full exits (PORTFOLIO_LOSS_CUT, STOP_LOSS, HOLD_EXPIRY, TAKE_PROFIT) supersede all.
        2. Partial reductions (PARTIAL_REDUCE, PORTFOLIO_TAKE_PROFIT, EQUITY_RECOVERY_TAKE_PROFIT)
           supersede additions and entries.
        3. Freeze any entry/scale-in on symbols with UNKNOWN attempts.
        4. Guarantee at most one primary intent per symbol per step.
        """
        symbol_actions: Dict[str, List[OrderIntent]] = {}
        for intent in intents:
            symbol_actions.setdefault(intent.symbol, []).append(intent)

        arbitrated: List[OrderIntent] = []

        for symbol, action_list in symbol_actions.items():
            # If symbol has uncertain in-flight attempt, filter out entries and scale-in
            if symbol in account_view.uncertain_symbols:
                action_list = [a for a in action_list if a.intent_scope != "ENTRY"]
                if not action_list:
                    continue

            # 1. Full exits: PROTECTION / EXIT with terminal reason
            full_exits = [
                a for a in action_list
                if (a.intent_scope in {"PROTECTION", "EXIT"} and a.reason in {
                    "PORTFOLIO_LOSS_CUT",
                    "STOP_LOSS",
                    "HOLD_EXPIRY",
                    "TAKE_PROFIT",
                    "HOURLY_EXCHANGE_TAKE_PROFIT",
                })
            ]
            if full_exits:
                arbitrated.append(full_exits[0])
                continue

            # 2. Partial exits / Portfolio TP reductions
            partial_exits = [
                a for a in action_list
                if a.reason in {"PARTIAL_REDUCE", "PORTFOLIO_TAKE_PROFIT", "EQUITY_RECOVERY_TAKE_PROFIT"}
            ]
            if partial_exits:
                arbitrated.append(partial_exits[0])
                continue

            # 3. Protection updates (stop loss tightening)
            protection_updates = [
                a for a in action_list
                if a.intent_scope == "PROTECTION" and a.reason in {"NOON_PROTECTION_UPDATE", "MORNING_PROTECTION_UPDATE"}
            ]
            if protection_updates:
                arbitrated.append(protection_updates[0])
                continue

            # 3. Rebalance additions
            rebalance_adds = [
                a for a in action_list
                if a.intent_scope == "REBALANCE" and a.reason == "REBALANCE_ADD"
            ]
            if rebalance_adds:
                arbitrated.append(rebalance_adds[0])
                continue

            # 4. Scale-in tranches
            scale_ins = [
                a for a in action_list
                if a.intent_scope == "ENTRY" and a.reason == "SCALE_IN"
            ]
            if scale_ins:
                arbitrated.append(scale_ins[0])
                continue

            # 5. Initial entries
            initial_entries = [
                a for a in action_list
                if a.intent_scope == "ENTRY" and a.reason == "TOP10_SHORT_SIGNAL"
            ]
            if initial_entries:
                arbitrated.append(initial_entries[0])
                continue

            # Default fallback: take first remaining intent
            if action_list:
                arbitrated.append(action_list[0])

        return arbitrated
