import unittest
from decimal import Decimal

from onchain.simulation.slippage_model_mm import estimate_action_slippage


class TestSlippageModelMM(unittest.TestCase):
    def test_zero_depth(self):
        self.assertEqual(estimate_action_slippage(Decimal("100"), Decimal("0"), Decimal("0.5")), Decimal("1"))

    def test_normal(self):
        r = estimate_action_slippage(Decimal("100"), Decimal("1000"), Decimal("0.5"))
        self.assertEqual(r, (Decimal("100") / Decimal("1000")) * Decimal("1.5"))


if __name__ == "__main__":
    unittest.main()
