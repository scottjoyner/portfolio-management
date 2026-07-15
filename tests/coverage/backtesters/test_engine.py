import unittest

from trading_system.backtesters.engine import (
    BacktestEngine,
    MarketEvent,
    OrderEvent,
    PortfolioSnapshot,
)


class Signal:
    def __init__(self, action, qty=1):
        self.action = action
        self.quantity = qty


class StratWithInit:
    def __init__(self, sig=None, exc=None):
        self.sig = sig
        self.exc = exc
        self.init_called = False

    def init(self, config):
        self.init_called = True

    def on_bar(self, event):
        if self.exc:
            raise self.exc
        return self.sig


class StratNoInit:
    def __init__(self, sig=None):
        self.sig = sig

    def on_bar(self, event):
        return self.sig


class TestBacktestEngine(unittest.TestCase):
    def test_init_defaults(self):
        e = BacktestEngine(initial_capital=5000)
        self.assertEqual(e.max_position_size_usd, 5000)
        self.assertEqual(e.portfolio_value, 5000)

    def test_init_custom_max(self):
        e = BacktestEngine(initial_capital=5000, max_position_size_usd=1000)
        self.assertEqual(e.max_position_size_usd, 1000)

    def test_load_market_data(self):
        e = BacktestEngine()
        data = [
            {"timestamp": 2, "symbol": "ETH", "close": 50, "volume": 10},
            {"timestamp": 1, "symbol": "ETH", "close": 40, "volume": 5},
        ]
        e.load_market_data(data)
        self.assertEqual(len(e.events), 2)
        self.assertEqual(e.events[0].timestamp, 1)
        self.assertEqual(e.events[1].symbol, "ETH")
        self.assertEqual(e.events[0].price, 40)

    def test_load_market_data_defaults(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1}])
        ev = e.events[0]
        self.assertEqual(ev.symbol, "BTC-USD")
        self.assertEqual(ev.price, 0)
        self.assertEqual(ev.volume, 0)

    def test_initialize_strategy_with_init(self):
        e = BacktestEngine()
        s = StratWithInit(Signal("BUY"))
        e.initialize_strategy(s, None, "BTC-USD")
        self.assertTrue(s.init_called)
        self.assertIn("BTC-USD", e.active_strategies)

    def test_initialize_strategy_no_init(self):
        e = BacktestEngine()
        s = StratNoInit(Signal("BUY"))
        e.initialize_strategy(s, {}, "ETH-USD")
        self.assertIn("ETH-USD", e.active_strategies)

    def test_step_no_events(self):
        e = BacktestEngine()
        self.assertIsNone(e.step())

    def test_step_hold_signal(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratNoInit(Signal("HOLD")), {}, "BTC-USD")
        self.assertIsNone(e.step())
        self.assertEqual(len(e.order_events), 0)

    def test_step_none_signal(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratNoInit(None), {}, "BTC-USD")
        self.assertIsNone(e.step())
        self.assertEqual(len(e.order_events), 0)

    def test_step_signal_without_action(self):
        class Sig:
            pass
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratNoInit(Sig()), {}, "BTC-USD")
        self.assertIsNone(e.step())
        self.assertEqual(len(e.order_events), 0)

    def test_step_buy_executes(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratNoInit(Signal("BUY", 2)), {}, "BTC-USD")
        self.assertIsNone(e.step())
        self.assertEqual(len(e.order_events), 1)
        self.assertEqual(e.held_positions["BTC-USD"]["quantity"], 2)

    def test_step_sell_executes(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratNoInit(Signal("SELL", 2)), {}, "BTC-USD")
        self.assertIsNone(e.step())
        self.assertEqual(e.held_positions["BTC-USD"]["quantity"], 2)

    def test_step_strategy_raises(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratWithInit(exc=RuntimeError("boom")), {}, "BTC-USD")
        # Should not raise
        self.assertIsNone(e.step())

    def test_execute_signal_no_price(self):
        e = BacktestEngine()
        self.assertIsNone(e.execute_signal("BTC", Signal("BUY"), 0.0))
        self.assertIsNone(e.execute_signal("BTC", Signal("BUY"), -5.0))

    def test_execute_signal_zero_quantity(self):
        e = BacktestEngine()
        self.assertIsNone(e.execute_signal("BTC", Signal("BUY", 0), 100.0))
        sig = Signal("BUY")
        sig.quantity = None
        self.assertIsNone(e.execute_signal("BTC", sig, 100.0))

    def test_execute_signal_exceeds_max(self):
        e = BacktestEngine(max_position_size_usd=100)
        # quantity 2 * price 100 = 200 > 100
        self.assertIsNone(e.execute_signal("BTC", Signal("BUY", 2), 100.0))

    def test_execute_signal_hold_action(self):
        e = BacktestEngine()
        # HOLD is treated as a buy for fill-price purposes
        res = e.execute_signal("BTC", Signal("HOLD", 1), 100.0, event_timestamp=1.0)
        self.assertIsNotNone(res)
        self.assertEqual(res.side, "HOLD")

    def test_execute_signal_buy_fill(self):
        e = BacktestEngine(slippage_bps=10.0)
        res = e.execute_signal("BTC", Signal("BUY", 1), 100.0, event_timestamp=1.0)
        self.assertGreater(res.filled_price, 100.0)
        self.assertGreater(res.commission_usd, 0.0)

    def test_finalize(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratNoInit(Signal("BUY", 1)), {}, "BTC-USD")
        e.step()
        summary = e.finalize()
        self.assertIn("final_position", summary)
        self.assertIn("total_trades", summary)
        self.assertEqual(summary["total_trades"], 1)
        self.assertIn("portfolio_value_usd", summary)

    def test_len(self):
        e = BacktestEngine()
        e.load_market_data([{"timestamp": 1, "close": 100}])
        e.initialize_strategy(StratNoInit(Signal("BUY", 1)), {}, "BTC-USD")
        e.step()
        self.assertEqual(len(e), 1)


class TestSnapshot(unittest.TestCase):
    def test_mark_to_market(self):
        s = PortfolioSnapshot(timestamp=1.0, equity_value=1000.0, margin_level=1.0)
        s.mark_to_market(10.0)
        self.assertAlmostEqual(s.equity_value, 1100.0)
        s2 = PortfolioSnapshot(timestamp=1.0, equity_value=0.0, margin_level=1.0)
        s2.mark_to_market(10.0)
        self.assertEqual(s2.equity_value, 0.0)


if __name__ == "__main__":
    unittest.main()
