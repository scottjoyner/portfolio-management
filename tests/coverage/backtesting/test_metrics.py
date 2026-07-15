import unittest

from trading_system.backtesting.metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_sortino_ratio,
    calculate_profit_factor,
    calculate_win_rate,
    calculate_avg_profit_factor,
)


class TestBacktestingMetrics(unittest.TestCase):
    def test_sharpe_empty(self):
        self.assertEqual(calculate_sharpe_ratio([]), 0.0)
        self.assertEqual(calculate_sharpe_ratio([1.0]), 0.0)

    def test_sharpe_zero_vol(self):
        self.assertEqual(calculate_sharpe_ratio([1.0, 1.0, 1.0]), 0.0)

    def test_sharpe_normal(self):
        r = calculate_sharpe_ratio([1.0, -0.5, 2.0, 0.5])
        self.assertIsInstance(r, float)

    def test_max_drawdown_empty(self):
        self.assertEqual(calculate_max_drawdown([]), 0.0)
        self.assertEqual(calculate_max_drawdown([1.0]), 0.0)

    def test_max_drawdown_zero_peak(self):
        self.assertEqual(calculate_max_drawdown([0.0, -1.0]), 0.0)

    def test_max_drawdown_normal(self):
        self.assertEqual(calculate_max_drawdown([100.0, 90.0, 95.0, 80.0]), 20.0)

    def test_max_drawdown_rising(self):
        # exercises the peak-update branch
        self.assertEqual(calculate_max_drawdown([100.0, 90.0, 110.0]), 10.0)

    def test_sortino_empty(self):
        self.assertEqual(calculate_sortino_ratio([]), 0.0)
        self.assertEqual(calculate_sortino_ratio([1.0]), 0.0)

    def test_sortino_no_negatives(self):
        self.assertEqual(calculate_sortino_ratio([1.0, 0.5, 0.2]), 0.0)

    def test_sortino_zero_downside(self):
        self.assertEqual(calculate_sortino_ratio([0.0, 0.0, 0.0]), 0.0)

    def test_sortino_constant_negative(self):
        # negative returns with zero downside deviation -> returns 0.0
        self.assertEqual(calculate_sortino_ratio([-1.0, -1.0, -1.0]), 0.0)

    def test_sortino_normal(self):
        r = calculate_sortino_ratio([1.0, -2.0, 3.0, -1.0])
        self.assertIsInstance(r, float)

    def test_profit_factor_empty(self):
        self.assertEqual(calculate_profit_factor([], []), 1.0)
        self.assertEqual(calculate_profit_factor([1.0], []), 1.0)

    def test_profit_factor_no_loss(self):
        self.assertEqual(calculate_profit_factor([1.0, 2.0], [0.0]), float("inf"))

    def test_profit_factor_normal(self):
        self.assertEqual(calculate_profit_factor([10.0, 20.0], [-5.0, -5.0]), 3.0)

    def test_win_rate_zero(self):
        self.assertEqual(calculate_win_rate(0, 0), 0.0)

    def test_win_rate_normal(self):
        self.assertEqual(calculate_win_rate(3, 4), 75.0)

    def test_avg_profit_factor_empty(self):
        self.assertEqual(calculate_avg_profit_factor([], []), 1.0)
        self.assertEqual(calculate_avg_profit_factor([1.0], []), 1.0)

    def test_avg_profit_factor_zero(self):
        self.assertEqual(calculate_avg_profit_factor([0.0], [0.0]), 0.0)

    def test_avg_profit_factor_normal(self):
        self.assertEqual(calculate_avg_profit_factor([10.0], [5.0]), 7.5)


if __name__ == "__main__":
    unittest.main()
