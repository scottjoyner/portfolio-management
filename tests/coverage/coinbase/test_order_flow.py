import unittest
from unittest import mock

from coinbase.src.sentiment import order_flow
from coinbase.src.sentiment.order_flow import OrderFlowEngine, OrderFlowSignal


class TestOrderFlowSignal(unittest.TestCase):
    def test_to_opportunity(self):
        sig = OrderFlowSignal("BTC-USD", "BUY", 0.7, 2.0, 1e7, -2.0, True)
        d = sig.to_opportunity()
        self.assertEqual(d["action"], "BUY")
        self.assertIn("spread_bps", d)


class TestOrderFlowEngine(unittest.TestCase):
    def _seed(self, eng, pid, n, val):
        eng._spread_history[pid] = [val] * n

    def test_invalid_inputs(self):
        eng = OrderFlowEngine()
        self.assertIsNone(eng.evaluate("BTC", 0, 1, 100, 1e6))
        self.assertIsNone(eng.evaluate("BTC", 1, 0, 100, 1e6))
        self.assertIsNone(eng.evaluate("BTC", 1, 1, 100, 1e6))  # bid >= ask
        self.assertIsNone(eng.evaluate("BTC", 1, 2, 0, 1e6))    # price <= 0

    def test_eval_interval_gate(self):
        eng = OrderFlowEngine()
        self._seed(eng, "BTC", 19, 5.0)
        sig = eng.evaluate("BTC", 100.0, 100.0005, 100.0, 1e7)
        self.assertIsNotNone(sig)
        # immediate second call -> within 1s -> None
        self.assertIsNone(eng.evaluate("BTC", 100.0, 100.0005, 100.0, 1e7))

    def test_history_too_short(self):
        eng = OrderFlowEngine()
        self._seed(eng, "BTC", 5, 5.0)
        self.assertIsNone(eng.evaluate("BTC", 100.0, 100.0005, 100.0, 1e7))

    def test_neutral_no_signal(self):
        eng = OrderFlowEngine()
        # history with real variance around 5 bps -> degenerate std avoided
        eng._spread_history["BTC"] = [5.0 + ((i % 3) - 1) * 0.5 for i in range(19)]
        # current spread ~5 bps matching history mean -> z ~ 0 -> no action
        sig = eng.evaluate("BTC", 100.0, 100.05, 100.0, 1e7)
        self.assertIsNone(sig)

    def test_tight_buy(self):
        eng = OrderFlowEngine()
        self._seed(eng, "BTC", 19, 5.0)
        # current spread very tight (1 bps) vs mean 5 -> z negative large
        sig = eng.evaluate("BTC", 100.0, 100.01, 100.0, 10_000_000.0)
        self.assertEqual(sig.action, "BUY")
        self.assertTrue(sig.spread_tight)

    def test_wide_sell(self):
        eng = OrderFlowEngine()
        self._seed(eng, "BTC", 19, 5.0)
        # current spread wide (20 bps) vs mean 5 -> z positive large
        sig = eng.evaluate("BTC", 100.0, 100.2, 100.0, 1e6)
        self.assertEqual(sig.action, "SELL")

    def test_get_signal(self):
        eng = OrderFlowEngine()
        self._seed(eng, "BTC", 19, 5.0)
        sig = eng.evaluate("BTC", 100.0, 100.0001, 100.0, 10_000_000.0)
        self.assertIs(eng.get_signal("BTC"), sig)
        self.assertIsNone(eng.get_signal("ETH"))

    def test_window_overflow_truncates(self):
        # advance fake time past the 1s gate so every call is processed, then
        # push far more spreads than the window -> history must cap at window
        eng = OrderFlowEngine(window=50)
        clock = {"t": 0.0}

        def fake_time():
            clock["t"] += 2.0
            return clock["t"]

        with mock.patch.object(order_flow.time, "time", side_effect=fake_time):
            for _ in range(120):
                eng.evaluate("BTC", 100.0, 100.01, 100.0, 10_000_000.0)
        self.assertEqual(len(eng._spread_history["BTC"]), 50)


if __name__ == "__main__":
    unittest.main()
