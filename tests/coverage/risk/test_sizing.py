import unittest
from decimal import Decimal

from trading_system.risk.sizing.service import (
    fixed_fractional,
    kelly_criterion,
    fixed_risk_usd,
    SizingResult,
)


class TestFixedFractional(unittest.TestCase):
    def test_zero_stop_loss(self):
        r = fixed_fractional(Decimal("1000"), Decimal("1"), Decimal("0"))
        self.assertEqual(r.suggested_size, Decimal("0"))
        self.assertEqual(r.reason, "stop_loss_pct must be > 0")

    def test_normal(self):
        r = fixed_fractional(Decimal("1000"), Decimal("1"), Decimal("2"))
        # risk_amount = 1000*1/100 = 10; size = 10/2 = 5
        self.assertEqual(r.suggested_size, Decimal("5"))
        self.assertEqual(r.max_size, Decimal("5"))


class TestKellyCriterion(unittest.TestCase):
    def test_zero_avg_loss(self):
        self.assertEqual(kelly_criterion(Decimal("0.6"), Decimal("10"), Decimal("0")), Decimal("0"))

    def test_normal_clamped(self):
        # b=2, p=0.6, q=0.4 -> kelly=(1.2-0.4)/2=0.4 ; clamped to 0.25
        k = kelly_criterion(Decimal("0.6"), Decimal("20"), Decimal("10"))
        self.assertEqual(k, Decimal("0.25"))

    def test_normal_negative_clamped(self):
        # b=1, p=0.4, q=0.6 -> kelly=(0.4-0.6)/1=-0.2 -> max(0,...) = 0
        k = kelly_criterion(Decimal("0.4"), Decimal("10"), Decimal("10"))
        self.assertEqual(k, Decimal("0"))


class TestFixedRiskUsd(unittest.TestCase):
    def test_zero_price(self):
        r = fixed_risk_usd(Decimal("1000"), Decimal("1"), Decimal("0"))
        self.assertEqual(r.suggested_size, Decimal("0"))
        self.assertEqual(r.max_size, Decimal("0"))

    def test_normal(self):
        r = fixed_risk_usd(Decimal("1000"), Decimal("1"), Decimal("10"))
        # risk_amount=10, size=10/10=1
        self.assertEqual(r.suggested_size, Decimal("1"))
        self.assertEqual(r.max_size, Decimal("1"))


if __name__ == "__main__":
    unittest.main()
