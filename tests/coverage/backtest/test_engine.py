import asyncio
import unittest

from trading_system.backtest.engine import (
    Config,
    BacktestResultSummary,
    BacktesterEngine,
)


class TestConfig(unittest.TestCase):
    def test_init_defaults(self):
        c = Config(strategy_name="s", start_date="2024-01-01", end_date="2024-02-01")
        self.assertEqual(c.initial_capital, 1000.0)
        self.assertEqual(c.tick_size, 0.01)
        self.assertIsNone(c.slippage_bps)
        self.assertEqual(c.commission_bps, 5.0)
        self.assertTrue(c.use_mock_data)

    def test_init_explicit(self):
        c = Config("s", "2024-01-01", "2024-02-01", initial_capital=500.0,
                    tick_size=0.5, slippage_bps=1.0, commission_bps=2.0, use_mock_data=False)
        self.assertEqual(c.initial_capital, 500.0)
        self.assertEqual(c.tick_size, 0.5)
        self.assertEqual(c.slippage_bps, 1.0)
        self.assertEqual(c.commission_bps, 2.0)
        self.assertFalse(c.use_mock_data)

    def test_validate_ok(self):
        c = Config("s", "2024-01-01", "2024-02-01")
        self.assertTrue(c.validate())

    def test_validate_empty_name(self):
        c = Config("", "2024-01-01", "2024-02-01")
        with self.assertRaises(ValueError):
            c.validate()

    def test_validate_whitespace_name(self):
        c = Config("   ", "2024-01-01", "2024-02-01")
        with self.assertRaises(ValueError):
            c.validate()

    def test_validate_bad_order(self):
        c = Config("s", "2024-02-01", "2024-01-01")
        with self.assertRaises(ValueError):
            c.validate()


class TestResultSummary(unittest.TestCase):
    def test_to_dict_full(self):
        s = BacktestResultSummary("s", 12.5, sharpe_ratio=1.34,
                                  max_drawdown_pct=-15.2, num_trades=50,
                                  win_rate_pct=62.0, start_date="2024-01-01",
                                  end_date="2024-02-01")
        d = s.to_dict()
        self.assertEqual(d["strategy_name"], "s")
        self.assertEqual(d["total_return_pct"], 12.5)
        self.assertEqual(d["sharpe_ratio"], 1.34)
        self.assertEqual(d["max_drawdown_pct"], -15.2)
        self.assertEqual(d["num_trades"], 50)
        self.assertEqual(d["win_rate_pct"], 62.0)

    def test_to_dict_none_optionals(self):
        s = BacktestResultSummary("s", 1.0)
        d = s.to_dict()
        self.assertIsNone(d["sharpe_ratio"])
        self.assertIsNone(d["max_drawdown_pct"])
        self.assertIsNone(d["win_rate_pct"])


class _Engine(BacktesterEngine):
    async def _execute_backtest_simulation(self):
        return BacktestResultSummary(self.config.strategy_name, 5.0)


class TestBacktesterEngine(unittest.TestCase):
    def test_init_and_adapter(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        self.assertEqual(e.results_cache, {})
        e.set_market_adapter(object())
        self.assertIsNotNone(e._market_adapter)

    def test_run_no_adapter(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        with self.assertRaises(RuntimeError):
            asyncio.get_event_loop().run_until_complete(e.run_backtest())

    def test_run_execute_fails(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        e.set_market_adapter(object())
        with self.assertRaises(RuntimeError):
            asyncio.get_event_loop().run_until_complete(e.run_backtest())

    def test_run_success_and_get_results(self):
        e = _Engine(Config("s", "2024-01-01", "2024-02-01"))
        e.set_market_adapter(object())
        res = asyncio.get_event_loop().run_until_complete(e.run_backtest())
        self.assertEqual(res.total_return_pct, 5.0)
        self.assertIn("s:2024-01-01:2024-02-01", e.get_results())


if __name__ == "__main__":
    unittest.main()
