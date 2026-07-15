import unittest
from decimal import Decimal

from onchain.dex.clmm.position_state import classify_position_state, position_analytics
from onchain.dex.clmm.math import active_range_fraction, position_inventory_mix


class TestPositionState(unittest.TestCase):
    def test_below(self):
        self.assertEqual(classify_position_state(Decimal("1"), Decimal("2"), Decimal("4")), "BELOW_RANGE")

    def test_above(self):
        self.assertEqual(classify_position_state(Decimal("5"), Decimal("2"), Decimal("4")), "ABOVE_RANGE")

    def test_in_range(self):
        self.assertEqual(classify_position_state(Decimal("3"), Decimal("2"), Decimal("4")), "IN_RANGE")

    def test_analytics(self):
        a = position_analytics(Decimal("10"), Decimal("3"), Decimal("2"), Decimal("4"))
        self.assertIn("active_range_fraction", a)
        self.assertIn("token0_weight", a)


if __name__ == "__main__":
    unittest.main()
