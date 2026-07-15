import unittest

from onchain.models import AtomicityClass
from onchain.strategies.hedging.hybrid_hedge import HybridHedgeLinker, HedgePlan


class TestHybridHedge(unittest.TestCase):
    def test_sell_atomic(self):
        p = HybridHedgeLinker().plan_coinbase_hedge("ETH", 100.0, 1000.0, 10)
        self.assertIsInstance(p, HedgePlan)
        self.assertEqual(p.side, "SELL")
        self.assertEqual(p.atomicity, AtomicityClass.EFFECTIVELY_ATOMIC)

    def test_buy_semi(self):
        p = HybridHedgeLinker().plan_coinbase_hedge("ETH", -100.0, 1000.0, 200)
        self.assertEqual(p.side, "BUY")
        self.assertEqual(p.atomicity, AtomicityClass.SEMI_ATOMIC)

    def test_sequential(self):
        p = HybridHedgeLinker().plan_coinbase_hedge("ETH", -100.0, 1000.0, 400)
        self.assertEqual(p.atomicity, AtomicityClass.SEQUENTIAL)

    def test_unsafe(self):
        p = HybridHedgeLinker().plan_coinbase_hedge("ETH", -100.0, 1.0, 900)
        self.assertEqual(p.atomicity, AtomicityClass.UNSAFE_SEQUENTIAL)


if __name__ == "__main__":
    unittest.main()
