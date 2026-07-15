import io
import unittest
from contextlib import redirect_stdout

import trading_system.backtest.ratings.strategy_rater as sr
from trading_system.backtest.ratings.strategy_rater import (
    run_strategy_rating,
    calculate_overall_rating,
    generate_performance_metrics,
    print_rating_summary,
    main,
)


def _ratings(all_val):
    return {
        "win_rate_score": all_val,
        "sharpe_score": all_val,
        "drawdown_score": all_val,
        "capital_efficiency": all_val,
        "regime_robustness": all_val,
        "cost_sensitivity": all_val,
        "position_utilization": all_val,
        "signal_reliability": all_val,
    }


class TestStrategyRater(unittest.TestCase):
    def test_run_rating_each_branch(self):
        for name in [
            "Market Neutral Arb",
            "Timing Decay Arb",
            "Momentum Fade Arb",
            "Multi-Asset Portfolio Arb",
            "Cross-Exchange Basis Arb",
        ]:
            r = run_strategy_rating(name)
            self.assertEqual(len(r), 8)
        # else branch
        r = run_strategy_rating("Unknown Strategy")
        self.assertEqual(r["win_rate_score"], 7.0)

    def test_calculate_overall(self):
        r = _ratings(8.0)
        self.assertEqual(calculate_overall_rating(r), 8.0)
        r2 = _ratings(7.0)
        self.assertEqual(calculate_overall_rating(r2), 7.0)

    def test_generate_metrics_each_branch(self):
        for name in [
            "Market Neutral Arb",
            "Timing Decay Arb",
            "Momentum Fade Arb",
            "Multi-Asset Portfolio Arb",
            "Cross-Exchange Basis Arb",
        ]:
            m = generate_performance_metrics(name)
            self.assertIn("win_rate", m)
        # else branch
        self.assertEqual(generate_performance_metrics("Unknown Strategy"), {})

    def test_print_rating_summary_grades(self):
        ratings = _ratings(8.0)
        buf = io.StringIO()
        with redirect_stdout(buf):
            print_rating_summary("X", ratings, generate_performance_metrics("Market Neutral Arb"))
        self.assertIn("GRADE: A", buf.getvalue())

        for val, grade in [(7.6, "A-"), (7.0, "B+"), (6.5, "B"), (6.0, "B-")]:
            buf = io.StringIO()
            with redirect_stdout(buf):
                print_rating_summary("X", _ratings(val), {"win_rate": "65%", "max_drawdown": "-12%", "sharpe_ratio": 1.5})
            self.assertIn(f"GRADE: {grade}", buf.getvalue())

    def test_main_default(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            main()
        out = buf.getvalue()
        self.assertIn("COMPARATIVE STRATEGY RANKINGS", out)
        self.assertIn("PRODUCTION READINESS", out)
        self.assertIn("FINAL VERDICT", out)

    def test_main_production_ready_gt2(self):
        orig = sr.strategies
        sr.strategies = [{"name": "Market Neutral Arb", "description": "d"} for _ in range(3)]
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                main()
            self.assertIn("All cross-exchange arbitrage strategies are production-ready", buf.getvalue())
        finally:
            sr.strategies = orig

    def test_main_production_ready_eq1(self):
        orig = sr.strategies
        sr.strategies = [{"name": "Market Neutral Arb", "description": "d"}]
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                main()
            self.assertIn("One strategy recommended for immediate deployment", buf.getvalue())
        finally:
            sr.strategies = orig


if __name__ == "__main__":
    unittest.main()
