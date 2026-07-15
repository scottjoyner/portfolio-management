import asyncio
import unittest
from unittest import mock

from trading_system.backtest.advanced_testing import (
    MarketRegimeAnalyzer,
    SlippageModel,
    TransactionCostAnalyzer,
    MultiStrategyEnsemble,
    run_advanced_backtesting_test,
)


class TestRegimeAnalyzer(unittest.TestCase):
    def test_classify_bull(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(asyncio.get_event_loop().run_until_complete(
            a.classify_regime(2.0, 0.02, 0.7)), "bull_market")

    def test_classify_bear(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(asyncio.get_event_loop().run_until_complete(
            a.classify_regime(-2.0, -0.03, -0.7)), "bear_market")

    def test_classify_choppy(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(asyncio.get_event_loop().run_until_complete(
            a.classify_regime(0.4, 0.08, 0.2)), "choppy")

    def test_classify_default_bull_zero_vol(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(asyncio.get_event_loop().run_until_complete(
            a.classify_regime(0.4, 0.0, 0.2)), "bull_market")

    def test_classify_default_bull_neg_vol(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(asyncio.get_event_loop().run_until_complete(
            a.classify_regime(0.4, -0.08, 0.2)), "bull_market")

    def test_get_regime_statistics(self):
        a = MarketRegimeAnalyzer()
        stats = asyncio.get_event_loop().run_until_complete(
            a.get_regime_statistics("2024-01-01", "2024-12-31"))
        self.assertIn("bull_market", stats)
        self.assertEqual(stats["bull_market"]["expected_sharpe"], 1.8)


class TestSlippageModel(unittest.TestCase):
    def test_get_slippage_known_small(self):
        s = SlippageModel()
        r = s.get_slippage("BTC-USD", 100.0)
        self.assertLess(r["bid"], 0)
        self.assertGreater(r["ask"], 0)
        self.assertGreater(r["max_slippage"], 0)

    def test_get_slippage_unknown_and_large(self):
        s = SlippageModel()
        r = s.get_slippage("ZZZ-USD", 1_000_000.0)
        self.assertLess(r["bid"], 0)
        self.assertGreater(r["ask"], 0)
        self.assertEqual(r["max_slippage"], 20)


class TestTransactionCostAnalyzer(unittest.TestCase):
    def test_calculate_positive(self):
        t = TransactionCostAnalyzer()
        r = t.calculate_tca("BTC-USD", 2.5)
        self.assertIn("true_pnl", r)
        self.assertLess(r["true_pnl"], 2.5)

    def test_calculate_negative(self):
        t = TransactionCostAnalyzer()
        r = t.calculate_tca("ETH-USD", -1.8)
        self.assertEqual(r["true_pnl"], -1.8)

    def test_calculate_zero(self):
        t = TransactionCostAnalyzer()
        r = t.calculate_tca("SOL-USD", 0.0)
        self.assertEqual(r["exchange_fee_bps"], 5)
        self.assertEqual(r["cost_ratio"], float("inf"))


class TestMultiStrategyEnsemble(unittest.TestCase):
    def test_simulate(self):
        e = MultiStrategyEnsemble()
        r = asyncio.get_event_loop().run_until_complete(
            e.simulate_ensemble_performance(365))
        self.assertEqual(r["strategies_count"], 4)
        self.assertIn("correlation_matrix", r)
        self.assertGreaterEqual(r["diversification_benefit"], -0.1)


class TestRunAdvanced(unittest.TestCase):
    def test_run(self):
        res = asyncio.get_event_loop().run_until_complete(
            run_advanced_backtesting_test())
        self.assertIn("regime_analysis", res)
        self.assertIn("ensemble_performance", res)
        self.assertTrue(res["transaction_costs"]["passed"])

    def test_run_with_bad_slippage(self):
        # Force the otherwise-always-true slippage checks to take their
        # False branches by stubbing SlippageModel with degenerate data.
        class _FakeSlip:
            def __init__(self):
                self.base_slippages = {
                    "BTC-USD": {"bid": 0, "ask": 15},
                    "ETH-USD": {"bid": -20, "ask": 20},
                    "SOL-USD": {"bid": -30, "ask": 0},
                }

        with mock.patch(
            "trading_system.backtest.advanced_testing.SlippageModel", _FakeSlip
        ):
            res = asyncio.get_event_loop().run_until_complete(
                run_advanced_backtesting_test())
        self.assertIn("slippage_modeling", res)


if __name__ == "__main__":
    unittest.main()
