import unittest
from decimal import Decimal

from trading_system.research.lp.fee_vs_il_study import fee_vs_il_ratio


class TestFeeVsIlStudy(unittest.TestCase):
    def test_zero_il(self):
        self.assertEqual(fee_vs_il_ratio(Decimal("10"), Decimal("0")), Decimal("0"))

    def test_positive_il(self):
        self.assertEqual(fee_vs_il_ratio(Decimal("10"), Decimal("5")), Decimal("2"))

    def test_negative_il(self):
        self.assertEqual(fee_vs_il_ratio(Decimal("10"), Decimal("-5")), Decimal("2"))


if __name__ == "__main__":
    unittest.main()
