"""Tests for coinbase/src/risk_parity.py"""
import math
import unittest
from unittest import mock

from coinbase.src import risk_parity as rp
from coinbase.src.protocols import Direction, Opportunity


def make_opp(pid="BTC-USD", entry=100.0):
    return Opportunity(product_id=pid, direction=Direction.LONG, instrument_type=None,
                       entry_price=entry, stop_price=entry * 0.9, target_price=entry * 1.1,
                       risk_reward=2.0, confidence=0.5, reason="r", strategy_name="s")


class TestRiskParityPortfolio(unittest.TestCase):
    def test_optimize_empty(self):
        p = rp.RiskParityPortfolio()
        r = p.optimize([], {})
        self.assertEqual(r.assets, [])
        self.assertEqual(r.target_vol, p.target_vol)

    def test_optimize_single(self):
        p = rp.RiskParityPortfolio()
        r = p.optimize(["BTC-USD"], {"BTC-USD": 0.3})
        self.assertEqual(len(r.assets), 1)
        self.assertAlmostEqual(r.assets[0].weight, 1.0)
        self.assertGreater(r.portfolio_vol, 0)

    def test_optimize_two_default_corr(self):
        p = rp.RiskParityPortfolio()
        r = p.optimize(["BTC-USD", "ETH-USD"], {"BTC-USD": 0.4, "ETH-USD": 0.5})
        self.assertEqual(len(r.assets), 2)
        self.assertTrue(r.converged)
        # weights sum ~ target_vol scaling
        self.assertGreater(r.concentration, 0)

    def test_optimize_two_with_corr(self):
        p = rp.RiskParityPortfolio()
        corr = {("BTC-USD", "ETH-USD"): 0.1}
        r = p.optimize(["BTC-USD", "ETH-USD"], {"BTC-USD": 0.4, "ETH-USD": 0.5}, corr)
        self.assertEqual(len(r.assets), 2)
        self.assertTrue(r.converged)

    def test_optimize_no_converge(self):
        p = rp.RiskParityPortfolio(max_iter=1)
        r = p.optimize(["BTC-USD", "ETH-USD"], {"BTC-USD": 0.4, "ETH-USD": 0.5})
        self.assertEqual(r.iterations, 1)
        self.assertFalse(r.converged)

    def test_optimize_missing_vol(self):
        p = rp.RiskParityPortfolio()
        r = p.optimize(["X-USD", "Y-USD"], {})
        self.assertEqual(len(r.assets), 2)

    def test_allocate(self):
        p = rp.RiskParityPortfolio()
        r = p.optimize(["BTC-USD", "ETH-USD"], {"BTC-USD": 0.4, "ETH-USD": 0.5})
        sizes = p.allocate(10000.0, r, {"BTC-USD": 100.0, "ETH-USD": 50.0})
        self.assertIn("BTC-USD", sizes)
        self.assertGreater(sizes["BTC-USD"], 0)

    def test_allocate_zero_price(self):
        p = rp.RiskParityPortfolio()
        r = p.optimize(["BTC-USD"], {"BTC-USD": 0.4})
        sizes = p.allocate(10000.0, r, {"BTC-USD": 0.0})
        self.assertNotIn("BTC-USD", sizes)

    def test_risk_budget_sizing_empty(self):
        p = rp.RiskParityPortfolio()
        self.assertEqual(p.risk_budget_sizing([], 1000.0, {}), [])

    def test_risk_budget_sizing(self):
        p = rp.RiskParityPortfolio()
        opps = [make_opp("BTC-USD", 100.0), make_opp("ETH-USD", 50.0)]
        out = p.risk_budget_sizing(opps, 10000.0, {"BTC-USD": 0.4, "ETH-USD": 0.5})
        self.assertEqual(len(out), 2)
        for o in out:
            self.assertIn("risk_parity_weight", o.meta)
            self.assertIn("risk_contribution", o.meta)

    def test_risk_budget_sizing_zero_notional(self):
        p = rp.RiskParityPortfolio()
        # force an opportunity whose allocation rounds to 0
        opp = make_opp("BTC-USD", 100.0)
        opp.entry_price = 1e9
        out = p.risk_budget_sizing([opp], 1.0, {"BTC-USD": 0.4})
        self.assertEqual(out[0].base_size, 0.0)

    def test_portfolio_vol(self):
        v = rp.RiskParityPortfolio._portfolio_vol([0.5, 0.5], [0.4, 0.5],
                                                  [[1.0, 0.3], [0.3, 1.0]])
        self.assertGreater(v, 0)

    def test_marginal_risk_contribution(self):
        m = rp.RiskParityPortfolio._marginal_risk_contribution(
            0, [0.5, 0.5], [0.4, 0.5], [[1.0, 0.3], [0.3, 1.0]])
        self.assertGreaterEqual(m, 0)

    def test_build_correlation_none(self):
        corr = rp.RiskParityPortfolio._build_correlation(2, ["BTC-USD", "ETH-USD"], None)
        self.assertEqual(corr[0][1], 0.5)

    def test_build_correlation_with_data(self):
        corr = rp.RiskParityPortfolio._build_correlation(
            2, ["BTC-USD", "ETH-USD"], {("BTC-USD", "ETH-USD"): 0.1})
        self.assertEqual(corr[0][1], 0.1)
        self.assertEqual(corr[1][0], 0.1)
        # reverse key lookup
        corr2 = rp.RiskParityPortfolio._build_correlation(
            2, ["BTC-USD", "ETH-USD"], {("ETH-USD", "BTC-USD"): 0.2})
        self.assertEqual(corr2[0][1], 0.2)


if __name__ == "__main__":
    unittest.main()
