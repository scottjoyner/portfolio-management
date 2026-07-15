import unittest
from decimal import Decimal

from onchain.dex.amm.stable_math import stable_swap_out


class TestStableMath(unittest.TestCase):
    def test_basic(self):
        out = stable_swap_out(Decimal("100"), Decimal("1000"), Decimal("1000"), Decimal("100"), Decimal("30"))
        invariant = Decimal("2000")
        effective = Decimal("100") * Decimal("100")
        gross = effective * Decimal("1000") / invariant
        expected = gross * (Decimal("1") - Decimal("30") / Decimal("10000"))
        self.assertEqual(out, expected)

    def test_zero_invariant(self):
        out = stable_swap_out(Decimal("1"), Decimal("0"), Decimal("0"), Decimal("1"), Decimal("0"))
        self.assertEqual(out, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
