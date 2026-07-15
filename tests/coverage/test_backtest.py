from __future__ import annotations

import asyncio
from datetime import datetime
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import patch

from trading_system.backtest.adapter import (
    MarketDataAdapter,
    MockMarketDataAdapter,
)
from trading_system.backtest.advanced_testing import (
    MarketRegimeAnalyzer,
    MultiStrategyEnsemble,
    SlippageModel,
    TransactionCostAnalyzer,
    run_advanced_backtesting_test,
)
from trading_system.backtest.engine import (
    BacktestResultSummary,
    BacktesterEngine,
    Config,
)
from trading_system.backtest.models import (
    BacktestConfiguration,
    BacktestResult,
    BacktestTrade,
    EquityCurvePoint,
    PerformanceSignal,
    StrategyCertification,
    StrategyComparison,
)
from trading_system.backtest.simulator import (
    Fill,
    Signal,
    SimulationResult,
    StrategySimulator,
)


class TestBacktestModels(TestCase):
    def test_models_import(self):
        # Importing the module executes all ORM class definitions (100% line/branch).
        self.assertEqual(BacktestResult.__tablename__, "backtest_results")
        self.assertEqual(EquityCurvePoint.__tablename__, "equity_curve_points")
        self.assertEqual(BacktestTrade.__tablename__, "backtest_trades")
        self.assertEqual(PerformanceSignal.__tablename__, "performance_signals")
        self.assertEqual(StrategyCertification.__tablename__, "strategy_certifications")
        self.assertEqual(BacktestConfiguration.__tablename__, "backtest_configurations")
        self.assertEqual(StrategyComparison.__tablename__, "strategy_comparisons")


class TestAdapter(IsolatedAsyncioTestCase):
    def test_abstract_adapter_cannot_instantiate(self):
        with self.assertRaises(TypeError):
            MarketDataAdapter()

    async def test_mock_connect_disconnect(self):
        a = MockMarketDataAdapter()
        self.assertTrue(await a.connect())
        self.assertTrue(a.connected)
        await a.disconnect()
        self.assertFalse(a.connected)

    async def test_fetch_not_connected_raises(self):
        a = MockMarketDataAdapter()
        with self.assertRaises(RuntimeError):
            a.fetch_historical_data("BTC-USD", "2024-01-01", "2024-01-10")

    async def test_fetch_connected(self):
        a = MockMarketDataAdapter()
        await a.connect()
        bars = a.fetch_historical_data("BTC-USD", "2024-01-01", "2024-01-10")
        self.assertEqual(len(bars), 9)
        self.assertIn("close", bars[0])

    async def test_get_current_price_with_dash(self):
        a = MockMarketDataAdapter()
        self.assertEqual(a.get_current_price("BTC-USD"), 69000.0)

    async def test_get_current_price_no_dash(self):
        a = MockMarketDataAdapter()
        self.assertEqual(a.get_current_price("ETH"), 3800.0)

    async def test_get_current_price_unknown(self):
        a = MockMarketDataAdapter()
        self.assertIsNone(a.get_current_price("DOGE"))


class TestEngine(IsolatedAsyncioTestCase):
    def test_config_defaults(self):
        c = Config("s", "2024-01-01", "2024-02-01")
        self.assertEqual(c.tick_size, 0.01)
        self.assertTrue(c.use_mock_data)

    def test_config_validate_ok(self):
        self.assertTrue(Config("s", "2024-01-01", "2024-02-01").validate())

    def test_config_validate_empty_name(self):
        with self.assertRaises(ValueError):
            Config("", "2024-01-01", "2024-02-01").validate()

    def test_config_validate_bad_date(self):
        with self.assertRaises(ValueError):
            Config("s", "2024-13-01", "2024-02-01").validate()

    def test_config_validate_start_after_end(self):
        with self.assertRaises(ValueError):
            Config("s", "2024-03-01", "2024-02-01").validate()

    def test_result_summary_to_dict_full(self):
        r = BacktestResultSummary("s", 12.5, 1.3, -15.2, 50, 62.0)
        d = r.to_dict()
        self.assertEqual(d["strategy_name"], "s")
        self.assertEqual(d["sharpe_ratio"], 1.3)

    def test_result_summary_to_dict_none(self):
        r = BacktestResultSummary("s", 1.0)
        d = r.to_dict()
        self.assertIsNone(d["sharpe_ratio"])
        self.assertIsNone(d["max_drawdown_pct"])
        self.assertIsNone(d["win_rate_pct"])

    def test_set_market_adapter(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        e.set_market_adapter(MockMarketDataAdapter())
        self.assertIsNotNone(e._market_adapter)

    async def test_run_backtest_no_adapter(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        with self.assertRaises(RuntimeError):
            await e.run_backtest()

    async def test_run_backtest_success(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        e.set_market_adapter(MockMarketDataAdapter())
        summary = BacktestResultSummary("s", 5.0)

        async def fake_sim():
            return summary

        e._execute_backtest_simulation = fake_sim
        res = await e.run_backtest()
        self.assertIs(res, summary)
        self.assertIn("s:2024-01-01:2024-02-01", e.get_results())

    async def test_run_backtest_exception(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        e.set_market_adapter(MockMarketDataAdapter())

        async def fake_sim():
            raise ValueError("boom")

        e._execute_backtest_simulation = fake_sim
        with self.assertRaises(RuntimeError):
            await e.run_backtest()

    async def test_execute_sim_not_implemented(self):
        e = BacktesterEngine(Config("s", "2024-01-01", "2024-02-01"))
        with self.assertRaises(NotImplementedError):
            await e._execute_backtest_simulation()


class TestSimulator(IsolatedAsyncioTestCase):
    def test_signal_from_dict(self):
        s = Signal.from_dict({"strategy_id": "st", "product_id": "BTC-USD", "side": "buy", "quantity": 2})
        self.assertEqual(s.quantity, 2.0)
        self.assertEqual(s.order_type, "market")
        self.assertIsNone(s.limit_price)

    def test_signal_from_dict_with_fields(self):
        s = Signal.from_dict({"strategy_id": "st", "product_id": "BTC-USD", "side": "sell",
                               "quantity": 1, "order_type": "limit", "limit_price": 100})
        self.assertEqual(s.order_type, "limit")
        self.assertEqual(s.limit_price, 100)

    def test_simulation_result_to_dict(self):
        r = SimulationResult(strategy_id="st", start_time=datetime.now(), end_time=datetime.now(),
                             trade_count=3, total_traded_usd=100.0, realized_pnl=5.0)
        d = r.to_dict()
        self.assertEqual(d["strategy_id"], "st")
        self.assertEqual(d["trading_metrics"]["trade_count"], 3)
        self.assertEqual(d["signals"], 0)

    def test_configure_instrument(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", ticker="BTC")
        self.assertEqual(sim.instruments_config["BTC-USD"]["ticker"], "BTC")

    def test_load_historical_data_and_capital(self):
        sim = StrategySimulator()
        sim.load_historical_data("BTC-USD", [{"close": 1}])
        sim.set_initial_capital(5000.0)
        self.assertEqual(sim.historical_data["BTC-USD"][0]["close"], 1)
        self.assertEqual(sim.initial_capital, 5000.0)

    def test_generate_sample_signals(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD")
        sigs = sim.generate_sample_signals("st", ["BTC-USD"])
        self.assertTrue(10 <= len(sigs) <= 30)

    def test_generate_sample_signals_no_products(self):
        sim = StrategySimulator()
        sigs = sim.generate_sample_signals("st", [])
        self.assertEqual(sigs[0].product_id, "BTC-USDT")

    def test_simulate_signal_qty_too_small(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=0.001)
        s = Signal("st", "BTC-USD", "buy", 0.0001)
        self.assertIsNone(sim.simulate_signal_execution(s))

    def test_simulate_signal_with_base_price_buy(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD")
        s = Signal("st", "BTC-USD", "buy", 0.5, order_type="market")
        fill = sim.simulate_signal_execution(s, base_price=100.0)
        self.assertIsInstance(fill, Fill)
        self.assertEqual(fill.side, "buy")

    def test_simulate_signal_limit_sell(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD")
        s = Signal("st", "BTC-USD", "sell", 0.5, order_type="limit")
        fill = sim.simulate_signal_execution(s, base_price=100.0)
        self.assertIsInstance(fill, Fill)
        self.assertEqual(fill.side, "sell")

    def test_simulate_signal_without_base_price(self):
        sim = StrategySimulator()
        sim.configure_instrument("ETH-USD")
        s = Signal("st", "ETH-USD", "buy", 0.5, order_type="market")
        fill = sim.simulate_signal_execution(s)
        self.assertIsInstance(fill, Fill)

    def test_simulate_strategy_period_with_base_prices(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD")
        res = sim.simulate_strategy_period("st", ["BTC-USD"], datetime(2024, 1, 1),
                                           datetime(2024, 3, 1), base_prices={"BTC": 69000})
        self.assertIsInstance(res, SimulationResult)
        self.assertGreaterEqual(res.trade_count, 0)

    def test_simulate_strategy_period_default_base_prices(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD")
        res = sim.simulate_strategy_period("st", ["BTC-USD"], datetime(2024, 1, 1),
                                           datetime(2024, 3, 1))
        self.assertIsInstance(res, SimulationResult)


class TestAdvancedTesting(IsolatedAsyncioTestCase):
    async def test_classify_bull(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(await a.classify_regime(1.8, 0.02, 0.75), "bull_market")

    async def test_classify_bear(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(await a.classify_regime(-1.7, -0.03, -0.65), "bear_market")

    async def test_classify_choppy(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(await a.classify_regime(0.4, 0.08, 0.2), "choppy")

    async def test_classify_default_bull_vol_zero(self):
        a = MarketRegimeAnalyzer()
        self.assertEqual(await a.classify_regime(0.4, 0.0, 0.2), "bull_market")

    async def test_get_regime_statistics(self):
        a = MarketRegimeAnalyzer()
        stats = await a.get_regime_statistics("2024-01-01", "2024-12-31")
        self.assertIn("bull_market", stats)

    def test_slippage_known(self):
        s = SlippageModel()
        r = s.get_slippage("BTC-USD", 5000.0)
        self.assertIn("bid", r)
        self.assertIn("ask", r)

    def test_slippage_unknown(self):
        s = SlippageModel()
        r = s.get_slippage("DOGE-USD", 5000.0)
        self.assertEqual(r["bid"], -10.5)

    def test_tca_positive(self):
        t = TransactionCostAnalyzer()
        r = t.calculate_tca("BTC-USD", 2.5)
        self.assertLess(r["true_pnl"], 2.5)

    def test_tca_negative(self):
        t = TransactionCostAnalyzer()
        r = t.calculate_tca("ETH-USD", -1.8)
        self.assertEqual(r["true_pnl"], -1.8)

    async def test_ensemble(self):
        e = MultiStrategyEnsemble()
        res = await e.simulate_ensemble_performance(365)
        self.assertEqual(res["strategies_count"], 4)

    async def test_run_advanced(self):
        results = await run_advanced_backtesting_test()
        self.assertIn("regime_analysis", results)
        self.assertIn("ensemble_performance", results)


if __name__ == "__main__":
    import unittest

    unittest.main()
