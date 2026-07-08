import unittest
from trading_system.signal_confidence import ConfidenceEngine
from dataclasses import dataclass


@dataclass
class MockSignal:
    symbol: str
    action: str
    strength: float
    strategy: str


class TestConfidenceEngine(unittest.TestCase):
    def setUp(self):
        self.engine = ConfidenceEngine(
            liquidity_tiers={"BTC-USD": 1, "GARBAGE-USD": 5},
            regime_caps={"volatile": 0.4, "trending": 1.0},
        )

    def test_liquidity_penalty(self):
        # Tier 5 should have a penalty
        signal = MockSignal("GARBAGE-USD", "BUY", 0.8, "test_strat")
        result = self.engine.apply_modifiers(signal, {"spread": 0.0})
        self.assertLess(result.modified_confidence, 0.8)
        self.assertIn("liquidity_tier", result.modifiers_applied)

    def test_spread_adjustment(self):
        # 50bps spread should reduce confidence
        signal = MockSignal("BTC-USD", "BUY", 0.8, "test_strat")
        result = self.engine.apply_modifiers(signal, {"spread": 0.005})
        self.assertAlmostEqual(result.modified_confidence, 0.795)
        self.assertIn("spread_adjustment", result.modifiers_applied)

    def test_sentiment_integration(self):
        # Positive sentiment boost
        signal = MockSignal("BTC-USD", "BUY", 0.5, "test_strat")
        result = self.engine.apply_modifiers(
            signal, {"spread": 0.0}, sentiment_score=0.8
        )
        self.assertGreater(result.modified_confidence, 0.5)
        self.assertIn("sentiment_integration", result.modifiers_applied)

        # Negative sentiment penalty
        signal = MockSignal("BTC-USD", "BUY", 0.5, "test_strat")
        result = self.engine.apply_modifiers(
            signal, {"spread": 0.0}, sentiment_score=-0.8
        )
        self.assertLess(result.modified_confidence, 0.5)
        self.assertIn("sentiment_penalty", result.modifiers_applied)

    def test_global_consensus(self):
        # High consensus boost
        signal = MockSignal("BTC-USD", "BUY", 0.5, "test_strat")
        result = self.engine.apply_modifiers(
            signal, {"spread": 0.0}, global_consensus=0.9
        )
        self.assertGreater(result.modified_confidence, 0.5)
        self.assertIn("global_consensus", result.modifiers_applied)

        # Low consensus penalty
        signal = MockSignal("BTC-USD", "BUY", 0.5, "test_strat")
        result = self.engine.apply_modifiers(
            signal, {"spread": 0.0}, global_consensus=0.2
        )
        self.assertLess(result.modified_confidence, 0.5)
        self.assertIn("global_consensus_penalty", result.modifiers_applied)

    def test_regime_gate(self):
        # Volatile regime cap
        signal = MockSignal("BTC-USD", "BUY", 0.9, "test_strat")
        result = self.engine.apply_modifiers(signal, {"spread": 0.0}, regime="volatile")
        self.assertEqual(result.modified_confidence, 0.4)
        self.assertIn("regime_gate", result.modifiers_applied)


if __name__ == "__main__":
    unittest.main()
