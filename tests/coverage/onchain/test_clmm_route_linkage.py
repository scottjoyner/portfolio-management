import unittest
from decimal import Decimal

from onchain.dex.clmm.route_linkage import classify_sequential_risk, semi_atomic_safety_score


class TestRouteLinkage(unittest.TestCase):
    def test_low(self):
        self.assertEqual(classify_sequential_risk(Decimal("0.9"), Decimal("0.9")), "LOW")

    def test_medium(self):
        self.assertEqual(classify_sequential_risk(Decimal("0.8"), Decimal("0.8")), "MEDIUM")

    def test_high(self):
        self.assertEqual(classify_sequential_risk(Decimal("0.4"), Decimal("0.4")), "HIGH")

    def test_safety_unwind_ready(self):
        """Unwind-ready adds a fixed 0.1 bonus; it does not saturate to 1.

        base is 0.9 * 0.9 = 0.81, so a ready unwind gives 0.91. Expecting 1 would
        mean the bonus scaled confidence, which it does not.
        """
        self.assertEqual(semi_atomic_safety_score(Decimal("0.9"), Decimal("0.9"), True),
                         Decimal("0.91"))

    def test_safety_not_ready(self):
        s = semi_atomic_safety_score(Decimal("0.9"), Decimal("0.9"), False)
        self.assertEqual(s, Decimal("0.81"))


if __name__ == "__main__":
    unittest.main()
