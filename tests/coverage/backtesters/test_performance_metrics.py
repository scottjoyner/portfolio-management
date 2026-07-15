import unittest

from trading_system.backtesters.performance_metrics import (
    PerformanceMetricsCalculator,
    BacktestResultsExporter,
    simulate_slippage_with_vwap,
    simulate_flash_crash_detection,
)


def _vals(n=40, start=100.0, step=1.0):
    return [start + i * step for i in range(n)]


class TestPerformanceMetricsCalculator(unittest.TestCase):
    def test_init_logger_branches(self):
        # First instance adds a handler; second reuses existing logger
        a = PerformanceMetricsCalculator(log_level="DEBUG")
        b = PerformanceMetricsCalculator(log_level="INFO")
        self.assertIsNotNone(a.logger)
        self.assertIsNotNone(b.logger)

    def test_calculate_empty(self):
        m = PerformanceMetricsCalculator()
        res = m.calculate([])
        self.assertEqual(res["total_return_pct"], 0.0)
        res2 = m.calculate([100.0])
        self.assertEqual(res2["total_return_pct"], 0.0)

    def test_calculate_short_no_rolling(self):
        # < 13 values -> no rolling metrics block
        m = PerformanceMetricsCalculator()
        res = m.calculate([100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
        self.assertIn("volatility_pct", res)
        self.assertNotIn("rolling_sharpe_6m", res)

    def test_calculate_full(self):
        m = PerformanceMetricsCalculator()
        res = m.calculate(_vals(40), initial_capital=100.0)
        self.assertGreaterEqual(res["total_return_pct"], 0.0)
        self.assertIn("rolling_sharpe_6m", res)
        self.assertIn("var_95_pct", res)
        self.assertIn("skewness", res)

    def test_calculate_with_trades(self):
        m = PerformanceMetricsCalculator()
        trades = [
            {"pnl": 10.0, "profit_loss": 10.0},
            {"pnl": -5.0},
        ]
        res = m.calculate_with_trades(_vals(40), trades, initial_capital=100.0)
        self.assertEqual(res["total_trades"], 2)
        self.assertAlmostEqual(res["win_rate_pct"], 50.0)
        self.assertAlmostEqual(res["profit_factor"], 2.0)
        self.assertGreater(res["avg_win_pct"], 0)
        self.assertGreater(res["avg_loss_pct"], 0)

    def test_calculate_with_trades_all_wins(self):
        m = PerformanceMetricsCalculator()
        trades = [{"pnl": 10.0}, {"pnl": 5.0}]
        res = m.calculate_with_trades(_vals(40), trades, initial_capital=100.0)
        self.assertEqual(res["profit_factor"], float("inf"))
        self.assertEqual(res["avg_loss_pct"], 0)

    def test_calculate_with_trades_all_losses(self):
        m = PerformanceMetricsCalculator()
        trades = [{"pnl": -10.0}, {"pnl": -5.0}]
        res = m.calculate_with_trades(_vals(40), trades, initial_capital=100.0)
        self.assertEqual(res["win_rate_pct"], 0.0)
        self.assertGreater(res["avg_loss_pct"], 0)

    def test_calculate_with_trades_empty(self):
        m = PerformanceMetricsCalculator()
        res = m.calculate_with_trades(_vals(40), [], initial_capital=100.0)
        self.assertIsNone(res["win_rate_pct"])

    def test_generate_equity_curve_branches(self):
        m = PerformanceMetricsCalculator()
        # start, ATH, big drawdown, recover, normal
        curve = [100, 150, 200, 90, 95, 205, 200]
        out = m.generate_equity_curve(curve, initial_capital=100.0)
        self.assertEqual(len(out["values"]), len(curve))
        self.assertTrue(any(l.startswith("Drawdown:") for l in out["labels"]))
        self.assertIsNotNone(out["final_value_pct"])
        self.assertIsNotNone(out["start_value_pct"])

    def test_generate_equity_curve_empty(self):
        m = PerformanceMetricsCalculator()
        out = m.generate_equity_curve([], initial_capital=100.0)
        self.assertEqual(out["values"], [])
        self.assertIsNone(out["final_value_pct"])

    def test_helper_methods_short(self):
        m = PerformanceMetricsCalculator()
        pv = [100.0, 101.0]
        self.assertEqual(m._calculate_returns(pv), [0.01])
        self.assertEqual(m._calc_total_return(pv), 1.0)
        self.assertEqual(m._calc_annualized_return([]), 0.0)
        self.assertEqual(m._calc_sharpe_ratio([1.0, 2.0]), 0.0)
        self.assertEqual(m._calc_sortino_ratio([1.0, 2.0]), 0.0)
        self.assertEqual(m._calc_calmar_ratio(pv), 0.0)
        self.assertEqual(m._calc_max_drawdown(pv), 0.0)
        self.assertGreater(m._calc_cagr(pv, 100.0), 0.0)
        self.assertEqual(m._calc_volatility([1.0, 2.0]), 0.0)
        self.assertEqual(m._calc_downside_volatility([1.0, 2.0]), 0.0)
        self.assertEqual(m._calc_skewness([1.0, 2.0]), 0.0)
        self.assertEqual(m._calc_kurtosis([1.0, 2.0]), 0.0)
        self.assertEqual(m._calc_recovery_periods([100.0, 101.0]), 0.0)
        self.assertEqual(m._calc_var([100.0, 101.0]), 0.0)
        self.assertEqual(m._calc_rolling_sharpe([1.0, 2.0]), 0.0)
        self.assertEqual(m._calc_rolling_sortino([1.0, 2.0]), 0.0)

    def test_helper_methods_full(self):
        m = PerformanceMetricsCalculator()
        pv = _vals(40)
        self.assertIsNotNone(m._calc_sharpe_ratio(pv))
        self.assertIsNotNone(m._calc_sortino_ratio(pv))
        self.assertIsNotNone(m._calc_volatility(pv))
        self.assertIsNotNone(m._calc_skewness(pv))
        self.assertIsNotNone(m._calc_kurtosis(pv))
        self.assertIsNotNone(m._calc_recovery_periods(pv))
        self.assertIsNotNone(m._calc_var(pv))
        self.assertIsNotNone(m._calc_rolling_sharpe(pv))
        self.assertIsNotNone(m._calc_rolling_sortino(pv))
        self.assertIsNotNone(m._calc_cagr(pv, 100.0))
        self.assertIsNotNone(m._calc_calmar_ratio(pv))

    def test_cagr_nonpositive_end(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_cagr([100.0, 0.0], 100.0), float("inf"))
        self.assertEqual(m._calc_cagr([0.0, 100.0], 0.0), float("inf"))
        self.assertEqual(m._calc_cagr([0.0, 0.0], 0.0), 0.0)


class TestBacktestResultsExporter(unittest.TestCase):
    def test_to_json(self):
        exp = BacktestResultsExporter()
        metrics = {"sharpe_ratio": 1.5, "win_rate_pct": None}
        out = exp.to_json(metrics, symbols=["BTC-USD"])
        self.assertIn("sharpe_ratio", out)
        self.assertNotIn("win_rate_pct", out)
        out2 = exp.to_json(metrics)
        self.assertIn("BTC-USD", out2)

    def test_to_equity_curve_json(self):
        exp = BacktestResultsExporter()
        self.assertIn("values", exp.to_equity_curve_json({"values": [1, 2]}))

    def test_generate_summary_report(self):
        calc = PerformanceMetricsCalculator()
        metrics = calc.calculate_with_trades(
            _vals(40), [{"pnl": 10.0}, {"pnl": -5.0}], initial_capital=100.0
        )
        exp = BacktestResultsExporter()
        report = exp.generate_summary_report(metrics, initial_capital=100.0)
        self.assertIn("BACKTEST PERFORMANCE SUMMARY", report)


class TestUtilityFunctions(unittest.TestCase):
    def test_simulate_slippage_with_vwap(self):
        filled, slip = simulate_slippage_with_vwap(100.0, 100.0, 5.0, 10.0)
        self.assertGreater(filled, 0)
        self.assertGreaterEqual(slip, 0.0)
        filled2, slip2 = simulate_slippage_with_vwap(0.0, 100.0, 5.0, 10.0)
        self.assertEqual(filled2, 0.0)

    def test_simulate_flash_crash_detection(self):
        # A big drop with a recovery in lookback
        returns = [0.05, -0.05, 0.02, 0.0, 0.0]
        crashes = simulate_flash_crash_detection(returns, threshold_bps=2.0)
        self.assertEqual(len(crashes), 1)
        # No crash case
        self.assertEqual(simulate_flash_crash_detection([0.01, 0.01, 0.01]), [])
        # Drop with NO recovery in lookback -> not flagged as flash crash
        no_recovery = [0.0, -0.05, 0.0, 0.0, 0.0]
        self.assertEqual(simulate_flash_crash_detection(no_recovery, threshold_bps=2.0), [])


class TestHelperMethodBranches(unittest.TestCase):
    """Cover len<2 / empty / zero-deviation / recovery-path branches."""

    def test_calc_returns_short(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calculate_returns([100.0]), [])
        self.assertEqual(m._calculate_returns([]), [])

    def test_total_return_short(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_total_return([100.0]), 0.0)

    def test_sortino_zero_deviation(self):
        m = PerformanceMetricsCalculator()
        # 30 equal returns -> no downside -> 0.0
        self.assertEqual(m._calc_sortino_ratio([0.0] * 30), 0.0)

    def test_calmar_short(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_calmar_ratio([100.0]), 0.0)

    def test_calmar_with_drawdown(self):
        m = PerformanceMetricsCalculator()
        # drawdown present -> returns cagr/dd (line 315)
        self.assertIsNotNone(m._calc_calmar_ratio([100.0, 120.0, 80.0]))

    def test_max_drawdown_short(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_max_drawdown([100.0]), 0.0)

    def test_cagr_short(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_cagr([100.0], 100.0), 0.0)

    def test_downside_volatility_zero(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_downside_volatility([0.0] * 30), 0.0)

    def test_skewness_zero_std(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_skewness([0.0] * 30), 0.0)

    def test_kurtosis_zero_std(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_kurtosis([0.0] * 30), 0.0)

    def test_var_short(self):
        m = PerformanceMetricsCalculator()
        self.assertEqual(m._calc_var([100.0, 101.0]), 0.0)

    def test_recovery_periods_with_drawdown(self):
        m = PerformanceMetricsCalculator()
        # drawdown >= half of peak dd with a later recovery attempt
        vals = [100.0, 100.0, 80.0, 100.0, 100.0, 100.0]
        self.assertIsNotNone(m._calc_recovery_periods(vals))

    def test_rolling_sharpe_full(self):
        m = PerformanceMetricsCalculator()
        self.assertIsNotNone(m._calc_rolling_sharpe(_vals(80)))

    def test_rolling_sortino_full(self):
        m = PerformanceMetricsCalculator()
        self.assertIsNotNone(m._calc_rolling_sortino(_vals(80)))

    def test_equity_curve_peak_nonpositive(self):
        m = PerformanceMetricsCalculator()
        out = m.generate_equity_curve([0.0, 5.0, 10.0], initial_capital=1.0)
        self.assertEqual(len(out["values"]), 3)

    def test_recovery_periods_actual_recovery(self):
        m = PerformanceMetricsCalculator()
        vals = [100.0, 100.0, 80.0, 100.0, 110.0] + [100.0] * 7
        self.assertGreater(m._calc_recovery_periods(vals), 0.0)

    def test_rolling_sortino_empty_downside(self):
        m = PerformanceMetricsCalculator()
        # 80 identical returns -> no downside -> returns 0.0 (covers line 482)
        self.assertEqual(m._calc_rolling_sortino([0.01] * 80), 0.0)

    def test_var_with_confidence(self):
        m = PerformanceMetricsCalculator()
        self.assertIsNotNone(m._calc_var(_vals(40), confidence=90))

    def test_generate_summary_report_no_trades(self):
        exp = BacktestResultsExporter()
        report = exp.generate_summary_report({"win_rate_pct": None, "profit_factor": None})
        self.assertIn("BACKTEST PERFORMANCE SUMMARY", report)

    def test_calculate_with_trades_none(self):
        m = PerformanceMetricsCalculator()
        res = m.calculate_with_trades(_vals(40), None, initial_capital=100.0)
        self.assertIsNone(res["win_rate_pct"])


if __name__ == "__main__":
    unittest.main()
