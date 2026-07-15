import unittest
from decimal import Decimal

from onchain.dex.amm.cpmm_math import constant_product_out


class TestCPMMMath(unittest.TestCase):
    def test_basic(self):
        out = constant_product_out(Decimal("100"), Decimal("1000"), Decimal("1000"), Decimal("30"))
        expected = (Decimal("100") * (Decimal("1") - Decimal("30") / Decimal("10000")) * Decimal("1000")) / (Decimal("1000") + Decimal("100") * (Decimal("1") - Decimal("30") / Decimal("10000")))
        self.assertEqual(out, expected)

    def test_zero_fee(self):
        out = constant_product_out(Decimal("10"), Decimal("100"), Decimal("100"), Decimal("0"))
        self.assertEqual(out, Decimal("10") * Decimal("100") / Decimal("110"))


if __name__ == "__main__":
    unittest.main()
