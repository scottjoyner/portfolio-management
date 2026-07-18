"""Tests for coinbase/src/portfolio_risk.py"""
import time
import unittest
from unittest import mock

import numpy as np

from coinbase.src import portfolio_risk as pr


def mk_pos(pid, notional, side="LONG", cluster=None):
    return pr.Position(product_id=pid, side=side, size=1.0, entry_price=1.0,
                       current_price=1.0, unrealized_pnl=0.0, notional=notional,
                       leverage=1.0, cluster=cluster or "")


class TestInit(unittest.TestCase):
    def test_default_clusters(self):
        mgr = pr.PortfolioRiskManager()
        self.assertEqual(mgr.get_cluster("BTC-USD"), "btc")
        self.assertEqual(mgr.get_cluster("ETH-USD"), "eth")
        self.assertEqual(mgr.get_cluster("UNKNOWN-USD"), "other")

    def test_custom_limits(self):
        mgr = pr.PortfolioRiskManager(pr.RiskLimits(max_leverage=2.0))
        self.assertEqual(mgr.limits.max_leverage, 2.0)


class TestCorrelationMatrix(unittest.TestCase):
    def test_too_few(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_correlation_matrix({"A": [0.1, 0.2]})
        self.assertEqual(mgr._corr_matrix, {})

    def test_too_short(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_correlation_matrix({"A": [0.1] * 5, "B": [0.2] * 5})
        self.assertEqual(mgr._corr_matrix, {})

    def test_normal(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_correlation_matrix({
            "BTC-USD": [0.01, -0.02, 0.03, 0.01, -0.01, 0.02, 0.0, 0.01, -0.01, 0.02, 0.01, 0.0],
            "ETH-USD": [0.02, -0.01, 0.04, 0.0, -0.02, 0.03, 0.01, 0.0, -0.01, 0.03, 0.02, 0.0],
        })
        self.assertIn("BTC-USD", mgr._corr_matrix)
        self.assertIn("ETH-USD", mgr._corr_matrix)


class TestClusters(unittest.TestCase):
    def test_update_clusters_single(self):
        mgr = pr.PortfolioRiskManager()
        mgr._corr_matrix = {"BTC-USD": {"BTC-USD": 1.0, "ETH-USD": 0.2},
                            "ETH-USD": {"BTC-USD": 0.2, "ETH-USD": 1.0}}
        mgr._update_clusters_from_correlation()
        # low corr -> falls back to default cluster map
        self.assertEqual(mgr.get_cluster("BTC-USD"), "btc")

    def test_update_clusters_group(self):
        mgr = pr.PortfolioRiskManager()
        mgr._corr_matrix = {"BTC-USD": {"BTC-USD": 1.0, "ETH-USD": 0.9},
                            "ETH-USD": {"BTC-USD": 0.9, "ETH-USD": 1.0}}
        mgr._update_clusters_from_correlation()
        self.assertEqual(mgr.get_cluster("BTC-USD"), mgr.get_cluster("ETH-USD"))


class TestUpdatePositions(unittest.TestCase):
    def test_empty(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_positions({})  # _update_risk_metrics returns early

    def test_with_positions(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 500)})
        self.assertIn("BTC-USD", mgr._positions)


class TestUpdateEquity(unittest.TestCase):
    def test_first(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_equity(1000.0)
        self.assertEqual(mgr._daily_start_equity, 1000.0)
        self.assertEqual(mgr._daily_peak_equity, 1000.0)

    def test_peak(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_equity(1000.0)
        mgr.update_equity(1200.0)
        self.assertEqual(mgr._daily_peak_equity, 1200.0)
        self.assertEqual(mgr._daily_start_equity, 1000.0)


class TestCheckPreTrade(unittest.TestCase):
    def setUp(self):
        self.mgr = pr.PortfolioRiskManager()
        self.mgr.update_positions({})
        self.mgr.update_equity(10000.0)

    def test_ok(self):
        self.mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 500)})
        ok, reason, adj = self.mgr.check_pre_trade("ETH-USD", "LONG", 100.0, 50.0, 10000.0)
        self.assertTrue(ok)
        self.assertEqual(reason, "OK")

    def test_drawdown_reject(self):
        self.mgr.update_equity(8000.0)  # 20% drawdown
        ok, _, _ = self.mgr.check_pre_trade("BTC-USD", "LONG", 100.0, 50.0, 8000.0)
        self.assertFalse(ok)

    def test_daily_loss_reject(self):
        self.mgr.update_equity(9000.0)  # 10% daily loss
        ok, _, _ = self.mgr.check_pre_trade("BTC-USD", "LONG", 100.0, 50.0, 9000.0)
        self.assertFalse(ok)

    def test_cluster_scale(self):
        # Fresh BTC-USD, $3,500 requested on $10k equity.
        # Independent limits: cluster 30%=$3,000, single-asset 10%=$1,000,
        # gross 1.5x=$15,000. The tightest (single-asset) binds -> scaled to $1,000.
        # NOTE: prior code early-returned at the cluster scale-down and NEVER
        # evaluated the single-asset limit, so it wrongly returned $3,000. The
        # corrected check_pre_trade evaluates every limit and takes the minimum.
        ok, reason, adj = self.mgr.check_pre_trade("BTC-USD", "LONG", 3500.0, 50.0, 10000.0)
        self.assertTrue(ok)
        self.assertEqual(adj, 1000.0)
        self.assertIn("scaled", reason)

    def test_cluster_reject(self):
        self.mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 2950)})
        ok, _, _ = self.mgr.check_pre_trade("BTC-USD", "LONG", 100.0, 50.0, 10000.0)
        self.assertFalse(ok)

    def test_asset_scale(self):
        self.mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 500)})
        ok, reason, adj = self.mgr.check_pre_trade("BTC-USD", "LONG", 800.0, 50.0, 10000.0)
        self.assertTrue(ok)
        self.assertEqual(adj, 500.0)

    def test_asset_reject(self):
        self.mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 950)})
        ok, _, _ = self.mgr.check_pre_trade("BTC-USD", "LONG", 100.0, 50.0, 10000.0)
        self.assertFalse(ok)

    def test_correlation_reject(self):
        self.mgr.update_positions({
            "BTC-USD": mk_pos("BTC-USD", 100),
            "ETH-USD": mk_pos("ETH-USD", 100),
            "SOL-USD": mk_pos("SOL-USD", 100),
        })
        self.mgr._corr_matrix = {"NEWT-USD": {"BTC-USD": 0.9, "ETH-USD": 0.9, "SOL-USD": 0.9}}
        ok, _, _ = self.mgr.check_pre_trade("NEWT-USD", "LONG", 100.0, 50.0, 10000.0)
        self.assertFalse(ok)

    def test_leverage_scale(self):
        self.mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 14000)})
        ok, reason, adj = self.mgr.check_pre_trade("ETH-USD", "LONG", 2000.0, 50.0, 10000.0)
        self.assertTrue(ok)
        self.assertEqual(adj, 1000.0)

    def test_leverage_reject(self):
        self.mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 14950)})
        ok, _, _ = self.mgr.check_pre_trade("ZZZ-USD", "LONG", 1000.0, 50.0, 10000.0)
        self.assertFalse(ok)


class TestRiskMetrics(unittest.TestCase):
    def test_metrics(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 500, side="LONG"),
                               "ETH-USD": mk_pos("ETH-USD", 300, side="SHORT")})
        m = mgr.get_risk_metrics(10000.0)
        self.assertEqual(m.total_equity, 10000.0)
        self.assertGreater(m.total_notional, 0)
        self.assertIn("btc", m.cluster_exposures)

    def test_metrics_empty(self):
        mgr = pr.PortfolioRiskManager()
        m = mgr.get_risk_metrics(10000.0)
        self.assertEqual(m.total_notional, 0.0)

    def test_breaches(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_equity(10000.0)
        mgr.update_equity(8000.0)
        mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 5000)})
        m = mgr.get_risk_metrics(8000.0)
        self.assertTrue(any("DD" in b for b in m.limit_breaches))


class TestShouldReduceRisk(unittest.TestCase):
    def test_no_reduce(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 100)})
        reduce, scale = mgr.should_reduce_risk(10000.0)
        self.assertFalse(reduce)
        self.assertEqual(scale, 1.0)

    def test_reduce_drawdown(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_equity(8000.0)
        mgr.update_equity(12000.0)  # peak rises above start
        mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 5000)})
        reduce, scale = mgr.should_reduce_risk(8000.0)
        self.assertTrue(reduce)
        self.assertLess(scale, 1.0)

    def test_reduce_leverage(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_positions({"BTC-USD": mk_pos("BTC-USD", 14000)})
        reduce, scale = mgr.should_reduce_risk(10000.0)
        self.assertTrue(reduce)


class TestGetAllowedSize(unittest.TestCase):
    def test_allowed(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_equity(10000.0)
        size = mgr.get_allowed_size("ETH-USD", "LONG", 50.0, 10000.0)
        self.assertGreater(size, 0)

    def test_not_allowed(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_equity(10000.0)
        mgr.update_equity(8000.0)  # 20% drawdown -> hard reject
        size = mgr.get_allowed_size("ETH-USD", "LONG", 50.0, 8000.0)
        self.assertEqual(size, 0.0)

    def test_zero_price(self):
        mgr = pr.PortfolioRiskManager()
        mgr.update_equity(10000.0)
        self.assertEqual(mgr.get_allowed_size("ETH-USD", "LONG", 0.0, 10000.0), 0.0)


class TestGetRiskManager(unittest.TestCase):
    def test_global(self):
        with mock.patch.object(pr, "_RISK_MGR", None):
            m1 = pr.get_risk_manager()
            m2 = pr.get_risk_manager()
            self.assertIs(m1, m2)


if __name__ == "__main__":
    unittest.main()
