import math
import unittest

from trading_system.backtesters.metrics import (
    TradeResult,
    PortfolioValues,
    PerformanceMetrics,
    DrawdownMetrics,
)


def _varied(n=35):
    return [100.0 + 10.0 * math.sin(i / 3.0) + 2.0 * i for i in range(n)]


def _incr(n=35):
    return [100.0 + 5.0 * i for i in range(n)]


def _constant(n=35):
    return [100.0] * n


def _zero(n=35):
    return [0.0] * n


def _with_zero(n=35):
    return [0.0, 100.0] + _varied(n - 2)


class TestDataclasses(unittest.TestCase):
    def test_trade_result(self):
        t = TradeResult(1.0, 2.0, 10.0, 0.5, 100.0, 101.0)
        self.assertEqual(t.pnl_usd, 10.0)

    def test_portfolio_values(self):
        p = PortfolioValues([1.0, 2.0])
        self.assertEqual(p.values, [1.0, 2.0])
        self.assertIsNone(p.timestamps)


class TestTotalReturn(unittest.TestCase):
    def test_empty(self):
        m = PerformanceMetrics([])
        self.assertEqual(m.total_return_pct, 0.0)

    def test_normal(self):
        m = PerformanceMetrics([100.0, 110.0])
        self.assertEqual(m.total_return_pct, 10.0)


class TestAnnualized(unittest.TestCase):
    def test_empty(self):
        m = PerformanceMetrics([])
        self.assertEqual(m.annualized_return_pct, 0.0)

    def test_trading_days_zero(self):
        m = PerformanceMetrics([100.0, 110.0], trading_days=0)
        self.assertGreaterEqual(m.annualized_return_pct, 0.0)

    def test_negative_values(self):
        m = PerformanceMetrics([0.0, -10.0])
        self.assertEqual(m.annualized_return_pct, 0.0)

    def test_normal(self):
        m = PerformanceMetrics([100.0, 110.0])
        self.assertGreater(m.annualized_return_pct, 0.0)


class TestSharpe(unittest.TestCase):
    def test_short(self):
        m = PerformanceMetrics([100.0, 102.0])
        self.assertIsNone(m.sharpe_ratio)

    def test_zero_prev(self):
        m = PerformanceMetrics(_zero())
        self.assertIsNone(m.sharpe_ratio)

    def test_constant(self):
        m = PerformanceMetrics(_constant())
        self.assertEqual(m.sharpe_ratio, 0.0)

    def test_normal(self):
        m = PerformanceMetrics(_varied())
        self.assertIsInstance(m.sharpe_ratio, float)

    def test_prev_zero_branch(self):
        m = PerformanceMetrics(_with_zero())
        self.assertIsInstance(m.sharpe_ratio, float)


class TestSortino(unittest.TestCase):
    def test_short(self):
        m = PerformanceMetrics([100.0, 102.0])
        self.assertIsNone(m.sortino_ratio)

    def test_zero(self):
        m = PerformanceMetrics(_zero())
        self.assertIsNone(m.sortino_ratio)

    def test_constant_increase(self):
        # geometric increase -> downside nonempty -> finite sortino
        vals = [100.0 * (1.01 ** i) for i in range(35)]
        m = PerformanceMetrics(vals)
        self.assertIsInstance(m.sortino_ratio, float)

    def test_constant_negative(self):
        # geometric decrease -> downside nonempty -> finite sortino
        vals = [100.0]
        for _ in range(34):
            vals.append(vals[-1] * 0.95)
        m = PerformanceMetrics(vals)
        self.assertIsInstance(m.sortino_ratio, float)

    def test_single_downside(self):
        # 33 equal positive returns + 1 negative -> downside len 1
        vals = [100.0]
        r = [0.01] * 20 + [-0.05] + [0.01] * 14
        for rr in r:
            vals.append(vals[-1] * (1.0 + rr))
        m = PerformanceMetrics(vals)
        self.assertEqual(m.sortino_ratio, 0.0)

    def test_normal(self):
        m = PerformanceMetrics(_varied())
        self.assertIsInstance(m.sortino_ratio, float)


class TestMaxDrawdown(unittest.TestCase):
    def test_empty(self):
        m = PerformanceMetrics([])
        self.assertEqual(m.max_drawdown_pct, 0.0)

    def test_no_drawdown(self):
        m = PerformanceMetrics(_incr())
        self.assertEqual(m.max_drawdown_pct, 0.0)

    def test_with_drawdown(self):
        m = PerformanceMetrics([100.0, 90.0, 95.0, 80.0])
        self.assertEqual(m.max_drawdown_pct, 20.0)


class TestCalmar(unittest.TestCase):
    def test_none(self):
        m = PerformanceMetrics(_incr())
        self.assertIsNone(m.calmar_ratio)

    def test_with_drawdown(self):
        m = PerformanceMetrics([100.0, 90.0, 110.0, 80.0])
        self.assertIsInstance(m.calmar_ratio, float)


class TestWinRate(unittest.TestCase):
    def test_no_trades(self):
        m = PerformanceMetrics([100.0, 110.0])
        self.assertIsNone(m.win_rate)

    def test_all_loss(self):
        trs = [TradeResult(0, 1, -10.0, -0.1, 1.0, 0.9),
                TradeResult(0, 1, -5.0, -0.1, 1.0, 0.9)]
        m = PerformanceMetrics([100.0, 110.0], trade_results=trs)
        self.assertEqual(m.win_rate, 0.0)

    def test_normal(self):
        trs = [TradeResult(0, 1, 10.0, 0.1, 1.0, 1.1),
                TradeResult(0, 1, -5.0, -0.1, 1.0, 0.9)]
        m = PerformanceMetrics([100.0, 110.0], trade_results=trs)
        self.assertEqual(m.win_rate, 50.0)


class TestProfitFactor(unittest.TestCase):
    def test_no_trades(self):
        m = PerformanceMetrics([100.0, 110.0])
        self.assertEqual(m.profit_factor, 1.0)

    def test_no_loss(self):
        trs = [TradeResult(0, 1, 10.0, 0.1, 1.0, 1.1)]
        m = PerformanceMetrics([100.0, 110.0], trade_results=trs)
        self.assertEqual(m.profit_factor, float("inf"))

    def test_normal(self):
        trs = [TradeResult(0, 1, 10.0, 0.1, 1.0, 1.1),
                TradeResult(0, 1, -5.0, -0.1, 1.0, 0.9)]
        m = PerformanceMetrics([100.0, 110.0], trade_results=trs)
        self.assertEqual(m.profit_factor, 2.0)


class TestVaR(unittest.TestCase):
    def test_short(self):
        m = PerformanceMetrics([100.0, 102.0])
        self.assertIsNone(m.value_at_risk_95)

    def test_zero(self):
        m = PerformanceMetrics(_zero())
        self.assertIsNone(m.value_at_risk_95)

    def test_normal(self):
        m = PerformanceMetrics(_varied())
        self.assertIsInstance(m.value_at_risk_95, float)

    def test_prev_zero_branch(self):
        m = PerformanceMetrics(_with_zero())
        self.assertIsInstance(m.value_at_risk_95, float)


class TestCVaR(unittest.TestCase):
    def test_short(self):
        m = PerformanceMetrics([100.0, 102.0])
        self.assertIsNone(m.conditional_var_95)

    def test_zero(self):
        m = PerformanceMetrics(_zero())
        self.assertIsNone(m.conditional_var_95)

    def test_normal(self):
        m = PerformanceMetrics(_varied())
        self.assertIsInstance(m.conditional_var_95, float)

    def test_prev_zero_branch(self):
        m = PerformanceMetrics(_with_zero())
        self.assertIsInstance(m.conditional_var_95, float)


class TestSummary(unittest.TestCase):
    def test_summary(self):
        m = PerformanceMetrics(_varied())
        s = m.get_summary()
        self.assertIn("sharpe_ratio", s)
        self.assertIn("var_95_usd", s)
        self.assertIn("profit_factor", s)


class TestDrawdownMetrics(unittest.TestCase):
    def test_longest_empty(self):
        d = DrawdownMetrics([])
        self.assertIsNone(d.longest_drawdown_duration)

    def test_longest_recovery(self):
        # peak at 0, declines, new peak, recovers -> returns duration
        d = DrawdownMetrics([100.0, 90.0, 80.0, 110.0, 100.0])
        self.assertEqual(d.longest_drawdown_duration, 1)

    def test_longest_no_recovery(self):
        d = DrawdownMetrics([100.0, 90.0, 80.0, 70.0])
        self.assertEqual(d.longest_drawdown_duration, 1)

    def test_longest_intermediate(self):
        # intermediate value below peak keeps the loop going before recovery
        d = DrawdownMetrics([100.0, 90.0, 80.0, 70.0, 85.0, 110.0])
        self.assertEqual(d.longest_drawdown_duration, 2)

    def test_periods_empty(self):
        d = DrawdownMetrics([])
        self.assertEqual(d.get_drawdown_periods(), [])

    def test_periods_none(self):
        d = DrawdownMetrics([100.0, 101.0, 102.0])
        self.assertEqual(d.get_drawdown_periods(), [])

    def test_periods_with_recovery(self):
        d = DrawdownMetrics([100.0, 90.0, 95.0, 110.0])
        periods = d.get_drawdown_periods()
        self.assertEqual(len(periods), 1)
        self.assertGreaterEqual(periods[0]["max_drawdown_pct"], 10.0)

    def test_periods_final_in_drawdown(self):
        d = DrawdownMetrics([100.0, 90.0, 80.0])
        periods = d.get_drawdown_periods()
        self.assertEqual(len(periods), 1)
        self.assertIn("trough_value", periods[0])


if __name__ == "__main__":
    unittest.main()
