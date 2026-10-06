"""Unit tests for pure deterministic DecisionKernel."""

import unittest

from core.decision.kernel import DecisionKernel
from core.execution.models import (
    AccountView,
    DataQuality,
    MarketView,
    OrderIntent,
    PositionEpisode,
)


class TestDecisionKernel(unittest.TestCase):
    def setUp(self):
        self.kernel = DecisionKernel()

    def test_canonical_arbitration_priority_exit_over_entry(self):
        """Full exit must supersede entry for the same symbol."""
        account_view = AccountView(
            account_id="acc01",
            revision=1,
            wallet_balance=1000.0,
            equity=1000.0,
            positions={"BTCUSDT": {"id": 1, "symbol": "BTCUSDT", "side": "SHORT", "qty": 1.0, "sl_price": 50000.0}},
        )
        market_view = MarketView(
            as_of_utc="2026-10-05T00:00:00Z",
            prices={"BTCUSDT": 51000.0},  # Triggers SL
            top_gainers=[{"symbol": "BTCUSDT"}],  # Also candidate for entry
        )

        intents = self.kernel.decide(account_view, market_view, config={"max_positions": 10})
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].symbol, "BTCUSDT")
        self.assertEqual(intents[0].intent_scope, "EXIT")
        self.assertEqual(intents[0].reason, "STOP_LOSS")

    def test_uncertainty_freezing_blocks_entry(self):
        """Symbols with in-flight UNKNOWN attempts must be frozen from entry."""
        account_view = AccountView(
            account_id="acc01",
            revision=1,
            wallet_balance=1000.0,
            equity=1000.0,
            positions={},
            uncertain_symbols={"ETHUSDT"},
            data_quality=DataQuality.RECONCILING.value,
        )
        market_view = MarketView(
            as_of_utc="2026-10-05T00:00:00Z",
            prices={"ETHUSDT": 3000.0, "SOLUSDT": 150.0},
            top_gainers=[{"symbol": "ETHUSDT"}, {"symbol": "SOLUSDT"}],
        )

        intents = self.kernel.decide(account_view, market_view, config={"max_positions": 10})
        # ETHUSDT must be frozen/omitted, SOLUSDT should be allowed
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].symbol, "SOLUSDT")
        self.assertEqual(intents[0].intent_scope, "ENTRY")

    def test_portfolio_loss_cut_circuit_breaker(self):
        """Portfolio loss cut must trigger exits across all active positions."""
        account_view = AccountView(
            account_id="acc01",
            revision=1,
            wallet_balance=1000.0,
            equity=950.0,  # 5% drawdown
            positions={
                "BTCUSDT": {"id": 1, "symbol": "BTCUSDT", "side": "SHORT", "qty": 1.0},
                "ETHUSDT": {"id": 2, "symbol": "ETHUSDT", "side": "SHORT", "qty": 10.0},
            },
        )
        market_view = MarketView(as_of_utc="2026-10-05T00:00:00Z", prices={})

        config = {
            "portfolio_loss_cut_enabled": True,
            "portfolio_loss_cut_pct": 3.5,
            "baseline_equity": 1000.0,
        }

        intents = self.kernel.decide(account_view, market_view, config=config)
        self.assertEqual(len(intents), 2)
        symbols = {i.symbol for i in intents}
        self.assertEqual(symbols, {"BTCUSDT", "ETHUSDT"})
        for intent in intents:
            self.assertEqual(intent.intent_scope, "PROTECTION")
            self.assertEqual(intent.reason, "PORTFOLIO_LOSS_CUT")

    def test_partial_reduce_supersedes_rebalance_add(self):
        """Partial reduce takes precedence over addition."""
        account_view = AccountView(
            account_id="acc01",
            revision=1,
            wallet_balance=1000.0,
            equity=1000.0,
            positions={"BTCUSDT": {"id": 1, "symbol": "BTCUSDT", "side": "SHORT", "qty": 2.0}},
        )
        raw_intents = [
            OrderIntent(
                intent_id="add_1",
                account_id="acc01",
                client_intent_key="k_add",
                symbol="BTCUSDT",
                side="SELL",
                order_type="MARKET",
                target_qty=1.0,
                intent_scope="REBALANCE",
                reason="REBALANCE_ADD",
            ),
            OrderIntent(
                intent_id="red_1",
                account_id="acc01",
                client_intent_key="k_red",
                symbol="BTCUSDT",
                side="BUY",
                order_type="MARKET",
                target_qty=1.0,
                intent_scope="EXIT",
                reason="PARTIAL_REDUCE",
            ),
        ]
        arbitrated = self.kernel.arbitrate(raw_intents, account_view)
        self.assertEqual(len(arbitrated), 1)
        self.assertEqual(arbitrated[0].intent_id, "red_1")
        self.assertEqual(arbitrated[0].reason, "PARTIAL_REDUCE")

    def test_portfolio_take_profit_intent(self):
        """Portfolio take profit must generate partial reduction intents."""
        account_view = AccountView(
            account_id="acc01",
            revision=1,
            wallet_balance=1000.0,
            equity=1100.0,  # +10% profit
            positions={"BTCUSDT": {"id": 1, "symbol": "BTCUSDT", "side": "SHORT", "qty": 2.0}},
        )
        market_view = MarketView(as_of_utc="2026-10-05T00:00:00Z", prices={})
        config = {
            "portfolio_take_profit_enabled": True,
            "portfolio_take_profit_pct": 9.0,
            "portfolio_take_profit_reduce_ratio": 0.5,
            "baseline_equity": 1000.0,
        }
        intents = self.kernel.decide(account_view, market_view, config=config)
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].symbol, "BTCUSDT")
        self.assertEqual(intents[0].reason, "PORTFOLIO_TAKE_PROFIT")
        self.assertAlmostEqual(intents[0].target_qty, 1.0)

    def test_equity_recovery_take_profit_intent(self):
        """Equity recovery take profit triggers when equity rebounds from cycle min."""
        account_view = AccountView(
            account_id="acc01",
            revision=1,
            wallet_balance=1000.0,
            equity=1120.0,
            positions={"BTCUSDT": {"id": 1, "symbol": "BTCUSDT", "side": "SHORT", "qty": 2.0}},
        )
        market_view = MarketView(as_of_utc="2026-10-05T00:00:00Z", prices={})
        config = {
            "equity_recovery_take_profit_enabled": True,
            "equity_recovery_trigger_pct": 0.10,
            "equity_recovery_reduce_ratio": 0.50,
            "cycle_min_equity": 1000.0,
        }
        intents = self.kernel.decide(account_view, market_view, config=config)
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].symbol, "BTCUSDT")
        self.assertEqual(intents[0].reason, "EQUITY_RECOVERY_TAKE_PROFIT")
        self.assertAlmostEqual(intents[0].target_qty, 1.0)

    def test_scale_in_evaluation(self):
        """Scale in tranche must bind to existing episode and be arbitrated correctly."""
        ep = PositionEpisode(
            episode_id="ep_btc",
            account_id="acc01",
            symbol="BTCUSDT",
            position_side="SHORT",
            opened_at_utc="2026-10-05T00:00:00Z",
            target_qty=0.002,
            current_qty=0.001,
        )
        account_view = AccountView(
            account_id="acc01",
            revision=1,
            wallet_balance=1000.0,
            equity=1000.0,
            positions={"BTCUSDT": {"id": 1, "symbol": "BTCUSDT", "side": "SHORT", "qty": 0.001, "episode_id": "ep_btc"}},
            episodes={"BTCUSDT": ep},
        )
        market_view = MarketView(
            as_of_utc="2026-10-05T01:00:00Z",
            prices={"BTCUSDT": 50000.0},
        )
        config = {
            "entry_scale_in_mode": "bullish_then_bearish",
            "entry_scale_in_first_ratio": 0.50,
            "target_notional_per_pos": 100.0,
            "scale_in_ready_symbols": ["BTCUSDT"],
        }
        intents = self.kernel.decide(account_view, market_view, config=config)
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0].symbol, "BTCUSDT")
        self.assertEqual(intents[0].reason, "SCALE_IN")
        self.assertEqual(intents[0].episode_id, "ep_btc")
        self.assertAlmostEqual(intents[0].target_qty, 50.0 / 50000.0)


if __name__ == "__main__":
    unittest.main()
