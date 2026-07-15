import unittest
from decimal import Decimal

from onchain.dex.clmm.rebalance_policy import should_rebalance_position
from onchain.dex.clmm.schemas import LPRebalanceDecision


class TestRebalancePolicy(unittest.TestCase):
    def test_should_rebalance(self):
        d = should_rebalance_position(Decimal("3"), Decimal("2"), Decimal("4"), Decimal("100"), Decimal("10"), Decimal("20"))
        self.assertIsInstance(d, LPRebalanceDecision)
        self.assertTrue(d.should_rebalance)
        self.assertEqual(d.reason, "distance breach")

    def test_no_edge(self):
        d = should_rebalance_position(Decimal("3"), Decimal("2"), Decimal("4"), Decimal("100"), Decimal("50"), Decimal("20"))
        self.assertFalse(d.should_rebalance)
        self.assertEqual(d.reason, "insufficient edge")

    def test_distance_breach_edge_present(self):
        d = should_rebalance_position(Decimal("2.01"), Decimal("2"), Decimal("4"), Decimal("10"), Decimal("5"), Decimal("20"))
        self.assertFalse(d.should_rebalance)


if __name__ == "__main__":
    unittest.main()
