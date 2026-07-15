"""Tests for coinbase/src/risk_manager.py"""
import math
import unittest
from unittest import mock

from coinbase.src import risk_manager as rm
from coinbase.src.protocols import Direction, Opportunity


def pos(side="long", size=1.0, entry=100.0, current=100.0, stop=90.0, lev=1.0,
        var=0.0, pid="BTC-USD"):
    return rm.PositionRisk(product_id=pid, side=side, size=size,
                           entry_price=entry, current_price=current,
                           stop_price=stop, leverage=lev, var_95=var)


class TestRiskProfile(unittest.TestCase):
    def test_templates(self):
        self.assertIn(rm.RiskLimit.CONSERVATIVE, rm.RISK_TEMPLATES)
        self.assertIn(rm.RiskLimit.MODERATE, rm.RISK_TEMPLATES)
        self.assertIn(rm.RiskLimit.AGGRESSIVE, rm.RISK_TEMPLATES)

    def test_notional_env(self):
        p = rm.RiskProfile()
        self.assertGreater(p.max_notional_per_trade, 0)


class TestPositionRisk(unittest.TestCase):
    def test_notional(self):
        p = pos(lev=2.0)
        self.assertAlmostEqual(p.notional, 50.0)

    def test_risk_if_stopped_none(self):
        p = pos(stop=None)
        self.assertEqual(p.risk_if_stopped, 0.0)

    def test_risk_if_stopped_long(self):
        p = pos(side="long", size=2.0, current=100.0, stop=90.0)
        self.assertAlmostEqual(p.risk_if_stopped, 20.0)

    def test_risk_if_stopped_short(self):
        p = pos(side="short", size=2.0, current=100.0, stop=110.0)
        self.assertAlmostEqual(p.risk_if_stopped, 20.0)


class TestRiskManager(unittest.TestCase):
    def setUp(self):
        self.rm = rm.RiskManager(limit=rm.RiskLimit.MODERATE)
        # normalize env-dependent fields for deterministic tests
        self.rm.profile.max_notional_per_trade = 10000.0
        self.rm.profile.risk_per_trade_pct = 0.01
        self.rm.profile.min_risk_reward = 1.5

    def test_check_empty(self):
        r = self.rm.check_portfolio([], 1000.0)
        self.assertTrue(r.passed_checks)

    def test_check_passes(self):
        positions = [pos(size=1.0, entry=100.0, current=100.0, stop=90.0, var=1.0)]
        r = self.rm.check_portfolio(positions, 10000.0)
        self.assertTrue(r.passed_checks)

    def test_check_leverage_fail(self):
        positions = [pos(size=200.0, entry=100.0, current=100.0, var=0.0)]
        r = self.rm.check_portfolio(positions, 100.0)
        self.assertFalse(r.passed_checks)
        self.assertTrue(any("Leverage" in f for f in r.failures))

    def test_check_positions_count_fail(self):
        positions = [pos(pid=f"P{i}-USD") for i in range(15)]
        r = self.rm.check_portfolio(positions, 100000.0)
        self.assertFalse(r.passed_checks)
        self.assertTrue(any("Positions" in f for f in r.failures))

    def test_check_largest_position_fail(self):
        positions = [pos(size=100.0, entry=100.0, current=100.0)]
        r = self.rm.check_portfolio(positions, 100.0)
        self.assertFalse(r.passed_checks)
        self.assertTrue(any("Largest" in f for f in r.failures))

    def test_check_daily_loss_fail(self):
        self.rm._daily_start_equity = 1000.0
        r = self.rm.check_portfolio([pos()], 900.0)  # -10% daily
        self.assertFalse(r.passed_checks)
        self.assertTrue(any("Daily loss" in f for f in r.failures))

    def test_check_drawdown_fail(self):
        self.rm._peak_equity = 1000.0
        r = self.rm.check_portfolio([pos()], 800.0)
        self.assertFalse(r.passed_checks)
        self.assertTrue(any("Drawdown" in f for f in r.failures))

    def test_check_correlation_fail(self):
        self.rm.update_correlation("BTC-USD", "ETH-USD", 0.99)
        positions = [pos(pid="BTC-USD"), pos(pid="ETH-USD")]
        r = self.rm.check_portfolio(positions, 10000.0)
        self.assertFalse(r.passed_checks)
        self.assertTrue(any("Correlation" in f for f in r.failures))

    def test_check_daily_start_set(self):
        self.rm._daily_start_equity = 0.0
        r = self.rm.check_portfolio([pos()], 500.0)
        self.assertEqual(self.rm._daily_start_equity, 500.0)

    def test_check_trade_ok(self):
        ok, msg = self.rm.check_trade("BTC-USD", "long", 1.0, 100.0, 90.0, 115.0,
                                      10000.0, [])
        self.assertTrue(ok)

    def test_check_trade_rr(self):
        ok, msg = self.rm.check_trade("BTC-USD", "long", 1.0, 100.0, 90.0, 95.0,
                                      10000.0, [])
        self.assertFalse(ok)
        self.assertIn("RR", msg)

    def test_check_trade_notional(self):
        ok, msg = self.rm.check_trade("BTC-USD", "long", 1000.0, 100.0, 90.0, 115.0,
                                      10000.0, [])
        self.assertFalse(ok)
        self.assertIn("Notional", msg)

    def test_check_trade_risk_pct(self):
        ok, msg = self.rm.check_trade("BTC-USD", "long", 20.0, 100.0, 99.9, 110.0,
                                      100.0, [])
        self.assertFalse(ok)
        self.assertIn("Risk", msg)

    def test_check_trade_leverage(self):
        ok, msg = self.rm.check_trade("BTC-USD", "long", 1.0, 100.0, 90.0, 110.0,
                                      100.0, [pos(size=1.0)])
        self.assertFalse(ok)

    def test_correlation_exposure_short(self):
        self.assertEqual(self.rm._correlation_exposure([pos()]), 0.0)

    def test_correlation_exposure_zero_weight(self):
        p1 = pos(pid="A-USD", size=0.0)
        p2 = pos(pid="B-USD", size=0.0)
        self.assertEqual(self.rm._correlation_exposure([p1, p2]), 0.0)

    def test_correlation_exposure_normal(self):
        self.rm.update_correlation("BTC-USD", "ETH-USD", 0.5)
        e = self.rm._correlation_exposure([pos(pid="BTC-USD"), pos(pid="ETH-USD")])
        self.assertGreaterEqual(e, 0.0)

    def test_update_correlation(self):
        self.rm.update_correlation("A", "B", 0.3)
        self.assertEqual(self.rm._correlation_matrix[("A", "B")], 0.3)
        self.assertEqual(self.rm._correlation_matrix[("B", "A")], 0.3)

    def test_update_daily_reset(self):
        self.rm.update_daily_reset(1234.0)
        self.assertEqual(self.rm._daily_start_equity, 1234.0)

    def test_compute_var(self):
        p = pos(size=1.0, current=100.0)
        v = rm.RiskManager.compute_var(p, 0.95)
        self.assertGreater(v, 0)
        v99 = rm.RiskManager.compute_var(p, 0.99)
        self.assertGreater(v99, v)

    def test_compute_var_default_conf(self):
        p = pos(size=1.0, current=100.0)
        self.assertGreater(rm.RiskManager.compute_var(p), 0)


class TestKellySizer(unittest.TestCase):
    def test_fraction_zero_loss(self):
        self.assertEqual(rm.KellySizer.fraction(0.5, 2.0, 0.0), 0.0)

    def test_fraction_zero_win(self):
        self.assertEqual(rm.KellySizer.fraction(0.0, 2.0, 1.0), 0.0)

    def test_fraction_b_zero(self):
        self.assertEqual(rm.KellySizer.fraction(0.5, 0.0, 1.0), 0.0)

    def test_fraction_normal(self):
        f = rm.KellySizer.fraction(0.6, 2.0, 1.0)
        self.assertGreaterEqual(f, 0.0)
        self.assertLessEqual(f, 0.25)

    def test_half_kelly(self):
        self.assertEqual(rm.KellySizer.half_kelly(0.6, 2.0, 1.0),
                         rm.KellySizer.fraction(0.6, 2.0, 1.0) * 0.5)

    def test_fractional_kelly(self):
        self.assertEqual(rm.KellySizer.fractional_kelly(0.6, 2.0, 1.0, 0.5),
                         rm.KellySizer.fraction(0.6, 2.0, 1.0) * 0.5)

    def test_size_for_risk_zero_unit(self):
        self.assertEqual(rm.KellySizer.size_for_risk(1000, 0.01, 100, 100), 0.0)

    def test_size_for_risk_normal(self):
        s = rm.KellySizer.size_for_risk(1000, 0.01, 100.0, 90.0)
        self.assertGreater(s, 0)


if __name__ == "__main__":
    unittest.main()
