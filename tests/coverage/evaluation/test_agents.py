import asyncio
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))

from _env import install_stubs  # noqa: E402

install_stubs()

from evaluation.agents import (  # noqa: E402
    PositionAuditor,
    MarketAnalyst,
    FundamentalAnalyst,
    CryptoOnchainAnalyst,
    RiskAnalyst,
    StrategyResearcher,
    BacktestCritic,
    ApprovalDrafter,
)
from evaluation.base import Action, AgentResult, Philosophy  # noqa: E402


def _result(action, name="agent", confidence=0.8, dissenting=None):
    return AgentResult(
        agent_name=name, instrument="BTC-USD", action=action, confidence=confidence,
        rationale="r", risk_score=0.1, philosophy=Philosophy.VALUE,
        holding_period_hint="medium_term", dissenting=dissenting,
    )


class TestPositionAuditor(unittest.TestCase):
    def test_profit(self):
        r = PositionAuditor().evaluate("BTC-USD", {"entry_price": 100, "current_price": 130})
        self.assertEqual(r.action, Action.REDUCE)

    def test_loss(self):
        r = PositionAuditor().evaluate("BTC-USD", {"entry_price": 100, "current_price": 80})
        self.assertEqual(r.action, Action.EXIT)

    def test_hold(self):
        r = PositionAuditor().evaluate("BTC-USD", {"entry_price": 100, "current_price": 105})
        self.assertEqual(r.action, Action.HOLD)

    def test_zero_entry(self):
        r = PositionAuditor().evaluate("BTC-USD", {"entry_price": 0, "current_price": 50})
        self.assertEqual(r.action, Action.HOLD)


class TestMarketAnalyst(unittest.TestCase):
    def test_high_vol(self):
        r = MarketAnalyst().evaluate("BTC-USD", {"volatility_1h": 0.1, "volume_24h": 0})
        self.assertEqual(r.action, Action.WATCH)

    def test_high_vol_low(self):
        r = MarketAnalyst().evaluate("BTC-USD", {"volatility_1h": 0.02, "volume_24h": 2_000_000})
        self.assertEqual(r.action, Action.BUY)

    def test_low(self):
        r = MarketAnalyst().evaluate("BTC-USD", {"volatility_1h": 0.02, "volume_24h": 500_000})
        self.assertEqual(r.action, Action.HOLD)


class TestFundamentalAnalyst(unittest.TestCase):
    def test_no_dcf(self):
        r = FundamentalAnalyst().evaluate("BTC-USD", {"current_price": 100, "dcf_intrinsic_value": 0})
        self.assertEqual(r.action, Action.HOLD)
        self.assertEqual(r.confidence, 0.40)

    def test_strong_buy(self):
        r = FundamentalAnalyst().evaluate("BTC-USD", {"current_price": 100, "dcf_intrinsic_value": 200})
        self.assertEqual(r.action, Action.STRONG_BUY)
        self.assertEqual(r.confidence, 0.80)

    def test_buy(self):
        r = FundamentalAnalyst().evaluate("BTC-USD", {"current_price": 180, "dcf_intrinsic_value": 200})
        self.assertEqual(r.action, Action.BUY)

    def test_sell(self):
        r = FundamentalAnalyst().evaluate("BTC-USD", {"current_price": 130, "dcf_intrinsic_value": 100})
        self.assertEqual(r.action, Action.SELL)

    def test_hold_near(self):
        r = FundamentalAnalyst().evaluate("BTC-USD", {"current_price": 98, "dcf_intrinsic_value": 100})
        self.assertEqual(r.action, Action.HOLD)


class TestCryptoOnchainAnalyst(unittest.TestCase):
    def test_buy(self):
        r = CryptoOnchainAnalyst().evaluate("BTC-USD", {"tvl_usd": 50_000_000, "onchain_volume_24h": 5_000_000, "active_users_24h": 0})
        self.assertEqual(r.action, Action.BUY)

    def test_hold(self):
        r = CryptoOnchainAnalyst().evaluate("BTC-USD", {"tvl_usd": 1_000_000, "onchain_volume_24h": 1_000_000, "active_users_24h": 2000})
        self.assertEqual(r.action, Action.HOLD)

    def test_watch(self):
        r = CryptoOnchainAnalyst().evaluate("BTC-USD", {"tvl_usd": 1_000_000, "onchain_volume_24h": 1_000_000, "active_users_24h": 500})
        self.assertEqual(r.action, Action.WATCH)


class TestRiskAnalyst(unittest.TestCase):
    def test_exit(self):
        r = RiskAnalyst().evaluate("BTC-USD", {"value_at_risk": 0.2, "current_drawdown": 0, "correlation_to_index": 0.7})
        self.assertEqual(r.action, Action.EXIT)

    def test_reduce_var(self):
        r = RiskAnalyst().evaluate("BTC-USD", {"value_at_risk": 0.1, "current_drawdown": 0.05, "correlation_to_index": 0.7})
        self.assertEqual(r.action, Action.REDUCE)

    def test_reduce_corr(self):
        r = RiskAnalyst().evaluate("BTC-USD", {"value_at_risk": 0.05, "current_drawdown": 0.05, "correlation_to_index": 0.95})
        self.assertEqual(r.action, Action.REDUCE)

    def test_hold(self):
        r = RiskAnalyst().evaluate("BTC-USD", {"value_at_risk": 0.05, "current_drawdown": 0.05, "correlation_to_index": 0.7})
        self.assertEqual(r.action, Action.HOLD)


class TestStrategyResearcher(unittest.TestCase):
    def test_buy(self):
        r = StrategyResearcher().evaluate("BTC-USD", {"volatility_1h": 0.05, "spread_bps": 5})
        self.assertEqual(r.action, Action.BUY)

    def test_watch(self):
        r = StrategyResearcher().evaluate("BTC-USD", {"volatility_1h": 0.01, "spread_bps": 60})
        self.assertEqual(r.action, Action.WATCH)

    def test_hold(self):
        r = StrategyResearcher().evaluate("BTC-USD", {"volatility_1h": 0.01, "spread_bps": 20})
        self.assertEqual(r.action, Action.HOLD)


class TestBacktestCritic(unittest.TestCase):
    def test_watch(self):
        r = BacktestCritic().evaluate("BTC-USD", {"backtest_sharpe": 0.2, "backtest_max_drawdown": 0.1})
        self.assertEqual(r.action, Action.WATCH)

    def test_buy(self):
        r = BacktestCritic().evaluate("BTC-USD", {"backtest_sharpe": 2.5, "backtest_max_drawdown": 0.1})
        self.assertEqual(r.action, Action.BUY)

    def test_hold(self):
        r = BacktestCritic().evaluate("BTC-USD", {"backtest_sharpe": 1.0, "backtest_max_drawdown": 0.1})
        self.assertEqual(r.action, Action.HOLD)


class TestApprovalDrafter(unittest.TestCase):
    def test_empty(self):
        r = ApprovalDrafter().evaluate("BTC-USD", {"agent_results": []})
        self.assertEqual(r.action, Action.WATCH)
        self.assertEqual(r.confidence, 0.0)

    def test_buy(self):
        results = [_result(Action.BUY, "a1"), _result(Action.STRONG_BUY, "a2"),
                   _result(Action.BUY, "a3"), _result(Action.HOLD, "a4")]
        r = ApprovalDrafter().evaluate("BTC-USD", {"agent_results": results})
        self.assertEqual(r.action, Action.BUY)

    def test_sell(self):
        results = [_result(Action.SELL, "a1"), _result(Action.EXIT, "a2"),
                   _result(Action.SELL, "a3"), _result(Action.HOLD, "a4")]
        r = ApprovalDrafter().evaluate("BTC-USD", {"agent_results": results})
        self.assertEqual(r.action, Action.SELL)

    def test_hold_mixed(self):
        results = [_result(Action.BUY, "a1"), _result(Action.BUY, "a2"),
                   _result(Action.SELL, "a3"), _result(Action.SELL, "a4"),
                   _result(Action.HOLD, "a5")]
        r = ApprovalDrafter().evaluate("BTC-USD", {"agent_results": results})
        self.assertEqual(r.action, Action.HOLD)

    def test_dissenting(self):
        results = [_result(Action.BUY, "a1", dissenting="conflict")]
        r = ApprovalDrafter().evaluate("BTC-USD", {"agent_results": results})
        self.assertIsNotNone(r.dissenting)
        self.assertIn("a1", r.dissenting)


if __name__ == "__main__":
    unittest.main()
