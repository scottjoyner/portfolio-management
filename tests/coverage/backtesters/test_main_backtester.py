import io
import unittest
from contextlib import redirect_stdout

from trading_system.backtesters.main_backtester import (
    run_backtest_on_strategy,
    compare_strategies,
    generate_strategy_recommendation,
    benchmark_strategies,
)


class FakeStrat:
    def __init__(self, signals):
        self.signals = list(signals)

    def on_bar(self, bar):
        if self.signals:
            return self.signals.pop(0)
        return None

    @property
    def __name__(self):
        return "FakeStrat"


class TestMainBacktester(unittest.TestCase):
    def test_run_buy(self):
        data = [{"timestamp": i, "close": 100 + i} for i in range(5)]
        res = run_backtest_on_strategy(FakeStrat([{"action": "BUY", "entry_price": 100}]), data)
        self.assertIn("sharpe_ratio", res)
        self.assertEqual(res["strategy"], "FakeStrat")

    def test_run_sell_after_buy(self):
        data = [{"timestamp": i, "close": 100 + i} for i in range(5)]
        res = run_backtest_on_strategy(
            FakeStrat([
                {"action": "BUY", "entry_price": 100},
                {"action": "SELL"},
            ]),
            data,
        )
        self.assertIn("max_drawdown_pct", res)

    def test_run_zero_close_skipped(self):
        data = [{"timestamp": 0, "close": 0}, {"timestamp": 1, "close": 100}]
        res = run_backtest_on_strategy(FakeStrat([{"action": "BUY", "entry_price": 100}]), data)
        self.assertIn("sharpe_ratio", res)

    def test_run_with_trade_results(self):
        # Small initial capital so trade_results path triggers
        data = [{"timestamp": i, "close": 100 + i} for i in range(3)]
        res = run_backtest_on_strategy(
            FakeStrat([{"action": "BUY", "entry_price": 100}]),
            data,
            initial_capital=2.0,
        )
        self.assertEqual(res["strategy"], "FakeStrat")

    def test_compare_strategies(self):
        data = [{"timestamp": i, "close": 100 + i} for i in range(5)]
        strategies = {
            "A": FakeStrat([{"action": "BUY", "entry_price": 100}]),
            "B": FakeStrat([None, None]),
        }
        results = compare_strategies(data, strategies)
        self.assertEqual(set(results.keys()), {"A", "B"})

    def test_recommendation_empty(self):
        self.assertEqual(generate_strategy_recommendation({}), [])

    def test_recommendation_nonempty(self):
        results = {
            "A": {
                "sharpe_ratio": 1.5,
                "max_drawdown_pct": 10.0,
                "profit_factor": 1.8,
                "win_rate": 60.0,
                "calmar_ratio": 0.5,
            },
            "B": {
                "sharpe_ratio": None,
                "max_drawdown_pct": None,
                "profit_factor": None,
                "win_rate": None,
                "calmar_ratio": None,
            },
        }
        recs = generate_strategy_recommendation(results)
        self.assertEqual(len(recs), 2)
        self.assertEqual(recs[0]["strategy"], "A")

    def test_benchmark_strategies(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            benchmark_strategies()
        self.assertIn("STRATEGY BENCHMARK", buf.getvalue())

    def test_benchmark_strategies_with_metrics(self):
        # Cover the metric print guards (sharpe/dd/profit_factor truthy branches).
        from trading_system.backtesters import main_backtester as mb

        def fake_compare(ohlcv_data, strategies):
            return {
                "Conservative": {
                    "sharpe_ratio": 1.2,
                    "max_drawdown_pct": -5.0,
                    "profit_factor": 1.5,
                },
                "Aggressive": {
                    "sharpe_ratio": 0.0,
                    "max_drawdown_pct": 0.0,
                    "profit_factor": 0.0,
                },
            }

        def fake_recs(results):
            return [
                {
                    "strategy": "Conservative",
                    "composite_score": 10.0,
                    "sharpe_ratio": 1.2,
                    "max_drawdown_pct": -5.0,
                    "profit_factor": 1.5,
                },
                {
                    "strategy": "Aggressive",
                    "composite_score": 0.0,
                    "sharpe_ratio": 0.0,
                    "max_drawdown_pct": 0.0,
                    "profit_factor": 0.0,
                },
            ]

        import unittest.mock as m
        with m.patch.object(mb, "compare_strategies", fake_compare), \
                m.patch.object(mb, "generate_strategy_recommendation", fake_recs):
            buf = io.StringIO()
            with redirect_stdout(buf):
                benchmark_strategies()
        out = buf.getvalue()
        self.assertIn("STRATEGY BENCHMARK", out)
        self.assertIn("Sharpe Ratio: 1.20", out)
        self.assertIn("Max Drawdown: -5.00%", out)
        self.assertIn("Profit Factor: 1.50", out)

    def test_run_backtest_with_trade_results_profit(self):
        # Cover the `if pnl > 0` True branch inside run_backtest_on_strategy.
        data = [{"timestamp": i, "close": 100 + i * 10} for i in range(5)]
        res = run_backtest_on_strategy(
            FakeStrat([{"action": "BUY", "entry_price": 100}]),
            data,
            initial_capital=2.0,
        )
        self.assertIn("sharpe_ratio", res)


if __name__ == "__main__":
    unittest.main()
