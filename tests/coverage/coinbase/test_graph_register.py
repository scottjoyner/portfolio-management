import unittest
from unittest import mock

from coinbase.src.graph import register
from coinbase.src.graph.register import register_graph_strategy, build_graph_strategy
from coinbase.src.strategies.graph_signal import GraphSignalStrategy


class FakeScanner:
    def __init__(self):
        self.registered = []

    def register(self, strategy):
        self.registered.append(strategy)


class TestRegister(unittest.TestCase):
    def test_register_graph_strategy(self):
        scanner = FakeScanner()
        strat = register_graph_strategy(scanner, min_graph_score=0.5)
        self.assertIsInstance(strat, GraphSignalStrategy)
        self.assertEqual(strat.min_graph_score, 0.5)
        self.assertEqual(scanner.registered, [strat])

    def test_build_graph_strategy(self):
        strat = build_graph_strategy(min_graph_score=0.6)
        self.assertIsInstance(strat, GraphSignalStrategy)
        self.assertEqual(strat.min_graph_score, 0.6)


if __name__ == "__main__":
    unittest.main()
