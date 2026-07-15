import unittest
from datetime import datetime

from trading_system.backtest.simulator import (
    Signal,
    Fill,
    SimulationResult,
    StrategySimulator,
)


class TestSimulator(unittest.TestCase):
    def test_signal_from_dict_defaults(self):
        s = Signal.from_dict({"strategy_id": "s", "product_id": "BTC-USD",
                              "side": "buy", "quantity": "0.5"})
        self.assertEqual(s.order_type, "market")
        self.assertIsNone(s.limit_price)
        self.assertEqual(s.quantity, 0.5)

    def test_signal_from_dict_explicit(self):
        s = Signal.from_dict({"strategy_id": "s", "product_id": "BTC-USD",
                              "side": "sell", "quantity": "1.0",
                              "order_type": "limit", "limit_price": 100.0})
        self.assertEqual(s.order_type, "limit")
        self.assertEqual(s.limit_price, 100.0)

    def test_result_to_dict(self):
        r = SimulationResult(strategy_id="s", start_time=datetime(2024, 1, 1),
                             end_time=datetime(2024, 2, 1), trade_count=3,
                             total_traded_usd=10.0, realized_pnl=-1.0,
                             unrealized_pnl=2.0, sharpe_ratio=1.0,
                             max_drawdown_pct=-5.0, win_rate_pct=50.0,
                             profit_factor=1.2, fees_paid=0.1,
                             slippage_costs=0.2)
        d = r.to_dict()
        self.assertEqual(d["strategy_id"], "s")
        self.assertEqual(d["trading_metrics"]["trade_count"], 3)
        self.assertEqual(d["performance_metrics"]["profit_factor"], 1.2)
        self.assertEqual(d["signals"], 0)
        self.assertEqual(d["fills"], 0)

    def test_configure_instrument(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=0.5)
        self.assertEqual(sim.instruments_config["BTC-USD"]["ticker"], "BTC-USD")
        sim.configure_instrument("ETH-USD", ticker="ETH", min_qty=0.1, tick_size=0.02)
        self.assertEqual(sim.instruments_config["ETH-USD"]["ticker"], "ETH")
        self.assertEqual(sim.instruments_config["ETH-USD"]["tick_size"], 0.02)

    def test_load_and_capital(self):
        sim = StrategySimulator()
        sim.load_historical_data("BTC-USD", [{"c": 1}])
        self.assertEqual(sim.historical_data["BTC-USD"], [{"c": 1}])
        sim.set_initial_capital(500.0)
        self.assertEqual(sim.initial_capital, 500.0)

    def test_generate_sample_signals(self):
        sim = StrategySimulator()
        sigs = sim.generate_sample_signals("s", ["BTC-USD", "ETH-USD"])
        self.assertTrue(10 <= len(sigs) <= 30)
        self.assertTrue(all(isinstance(x, Signal) for x in sigs))
        # empty product list -> default symbol
        sigs2 = sim.generate_sample_signals("s", [])
        self.assertTrue(len(sigs2) >= 10)

    def test_simulate_execution_qty_too_small(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=10.0)
        s = Signal("s", "BTC-USD", "buy", 1.0)
        self.assertIsNone(sim.simulate_signal_execution(s))

    def test_simulate_execution_market_buy(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=0.0)
        s = Signal("s", "BTC-USD", "buy", 0.5, order_type="market")
        fill = sim.simulate_signal_execution(s, base_price=100.0)
        self.assertIsInstance(fill, Fill)
        # buy => fill_price includes positive slippage
        self.assertGreater(fill.fill_price, 95.0)

    def test_simulate_execution_limit_sell_zero_qty(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=0.0)
        s = Signal("s", "BTC-USD", "sell", 0.0, order_type="limit")
        fill = sim.simulate_signal_execution(s, base_price=100.0)
        self.assertIsNotNone(fill)
        self.assertEqual(fill.side, "sell")

    def test_simulate_execution_hash_price(self):
        sim = StrategySimulator()
        # no base_price -> hash-based price path, limit order
        s = Signal("x", "BTC-USD", "sell", 0.2, order_type="limit")
        fill = sim.simulate_signal_execution(s)
        self.assertIsNotNone(fill)

    def test_simulate_period_normal(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=0.0)
        res = sim.simulate_strategy_period(
            "s", ["BTC-USD"], datetime(2024, 1, 1), datetime(2024, 2, 1)
        )
        self.assertGreaterEqual(res.trade_count, 0)
        self.assertIsInstance(res, SimulationResult)
        self.assertTrue(res.fees_paid >= 0)

    def test_simulate_period_with_base_prices(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=0.0)
        res = sim.simulate_strategy_period(
            "s", ["BTC-USD"], datetime(2024, 1, 1), datetime(2024, 2, 1),
            base_prices={"BTC": 69000.0},
        )
        self.assertGreaterEqual(res.trade_count, 0)

    def test_simulate_period_no_fills(self):
        sim = StrategySimulator()
        sim.configure_instrument("BTC-USD", min_qty=10_000.0)
        res = sim.simulate_strategy_period(
            "s", ["BTC-USD"], datetime(2024, 1, 1), datetime(2024, 2, 1)
        )
        self.assertEqual(res.trade_count, 0)
        self.assertEqual(res.profit_factor, 1.5)


if __name__ == "__main__":
    unittest.main()
