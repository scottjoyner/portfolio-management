import unittest
from decimal import Decimal

from onchain.dex.clmm.lifecycle import material_range_change, needs_approval_for_range_change


class TestLifecycle(unittest.TestCase):
    def test_material_normal(self):
        """Mid moving 50 -> 100 is a 100% change, not 50%.

        old_mid is (0+100)/2 = 50 and new_mid is (0+200)/2 = 100, so the relative
        change is 50/50 = 1.0. The expected 0.5 is the answer for an upper bound
        of 150, which is the other case below.
        """
        r = material_range_change(0, 100, 0, 200)
        self.assertEqual(r, Decimal("1"))
        self.assertEqual(material_range_change(0, 100, 0, 150), Decimal("0.5"))

    def test_material_zero_old_mid(self):
        self.assertEqual(material_range_change(0, 0, 0, 200), Decimal("1"))

    def test_needs_approval_true(self):
        self.assertTrue(needs_approval_for_range_change(Decimal("0.5"), Decimal("0.1")))

    def test_needs_approval_false(self):
        self.assertFalse(needs_approval_for_range_change(Decimal("0.05"), Decimal("0.1")))


if __name__ == "__main__":
    unittest.main()
