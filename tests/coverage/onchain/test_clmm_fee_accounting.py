import unittest
from decimal import Decimal

from onchain.dex.clmm.fee_accounting import fee_capture_efficiency, accrued_fees_from_growth


class TestFeeAccounting(unittest.TestCase):
    def test_efficiency_normal(self):
        r = fee_capture_efficiency(Decimal("50"), Decimal("10000"), Decimal("100"))
        self.assertEqual(r, Decimal("50") / Decimal("100"))

    def test_efficiency_zero_notional(self):
        self.assertEqual(fee_capture_efficiency(Decimal("50"), Decimal("0"), Decimal("100")), Decimal("0"))

    def test_efficiency_zero_fee_tier(self):
        self.assertEqual(fee_capture_efficiency(Decimal("50"), Decimal("10000"), Decimal("0")), Decimal("0"))

    def test_efficiency_zero_theoretical(self):
        # fee_tier>0 but notional so that theoretical computed; covered by normal above.
        self.assertEqual(fee_capture_efficiency(Decimal("0"), Decimal("10000"), Decimal("100")), Decimal("0"))

    def test_accrued(self):
        a, b = accrued_fees_from_growth(Decimal("10"), Decimal("0.2"), Decimal("0.3"))
        self.assertEqual(a, Decimal("2"))
        self.assertEqual(b, Decimal("3"))


if __name__ == "__main__":
    unittest.main()
