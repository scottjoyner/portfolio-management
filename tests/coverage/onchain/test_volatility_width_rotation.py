import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.volatility_width_rotation import rotate_width


class TestVolatilityWidthRotation(unittest.TestCase):
    def test_crisis(self):
        self.assertEqual(rotate_width(Decimal("100"), "CRISIS"), Decimal("250"))

    def test_elevated(self):
        self.assertEqual(rotate_width(Decimal("100"), "ELEVATED"), Decimal("150"))

    def test_calm(self):
        self.assertEqual(rotate_width(Decimal("100"), "CALM"), Decimal("80"))

    def test_normal(self):
        self.assertEqual(rotate_width(Decimal("100"), "NORMAL"), Decimal("100"))


if __name__ == "__main__":
    unittest.main()
