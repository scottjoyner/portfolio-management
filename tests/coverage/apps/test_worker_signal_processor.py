import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from trading_system.apps.worker.signal_processor import SignalProcessor


class FakeEngine:
    def __init__(self, signals=None, order_result=(True, "ok")):
        self._signals = signals if signals is not None else []
        self._order_result = order_result
        self.calls = []

    def evaluate_market_state(self, product_id, market_state, mode="paper"):
        self.calls.append(("evaluate_market_state", product_id, mode))
        return self._signals

    def evaluate_order(self, signal, market_state, mode="paper"):
        self.calls.append(("evaluate_order", mode))
        return self._order_result


class FakeSignal:
    def __init__(self, product_id="BTC-USD", score=1.0, confidence=0.5):
        self.product_id = product_id
        self.score = score
        self.confidence = confidence


class TestSignalProcessor(unittest.TestCase):
    def test_evaluate_market_state_empty(self):
        proc = SignalProcessor(FakeEngine(signals=[]))
        out = proc.evaluate_market_state("BTC-USD", {"price": 100})
        self.assertEqual(out, [])

    def test_evaluate_market_state_applies_modifiers(self):
        sig = FakeSignal(product_id="BTC-USD", score=1.0, confidence=0.6)
        raw = {"strategy_id": "s1", "signal": sig, "explanation": "e"}
        engine = FakeEngine(signals=[raw])
        proc = SignalProcessor(engine)
        out = proc.evaluate_market_state(
            "BTC-USD",
            {"price": 100, "regime": "bull", "market_leaders": ["BTC-USD"],
             "sentiment_score": 0.3, "global_consensus": 0.2},
        )
        self.assertEqual(len(out), 1)
        self.assertIn("confidence_modifier", out[0])
        self.assertIsInstance(out[0]["signal"].confidence, float)
        # engine was invoked with the market state
        self.assertEqual(engine.calls[0][0], "evaluate_market_state")

    def test_evaluate_market_state_negative_score(self):
        sig = FakeSignal(product_id="ETH-USD", score=-0.5, confidence=0.4)
        raw = {"strategy_id": "s2", "signal": sig}
        proc = SignalProcessor(FakeEngine(signals=[raw]))
        out = proc.evaluate_market_state("ETH-USD", {"price": 10})
        self.assertEqual(out[0]["signal"].action if hasattr(out[0]["signal"], "action") else None, None)
        # SELL path: action computed as "SELL" inside _Signal
        self.assertEqual(out[0]["signal"].confidence, 0.4)

    def test_evaluate_order_delegates(self):
        engine = FakeEngine(order_result=(False, "not approved"))
        proc = SignalProcessor(engine)
        ok, reason = proc.evaluate_order({"x": 1}, {"price": 5}, mode="live")
        self.assertFalse(ok)
        self.assertEqual(reason, "not approved")
        self.assertEqual(engine.calls[-1][0], "evaluate_order")


if __name__ == "__main__":
    unittest.main()
