import unittest
from decimal import Decimal

from onchain.wallets.spend_policy import enforce_spend_policy


class TestSpendPolicy(unittest.TestCase):
    def test_ok(self):
        self.assertTrue(enforce_spend_policy(Decimal("10"), Decimal("50"), Decimal("100"), Decimal("20")))

    def test_exceed_action(self):
        self.assertFalse(enforce_spend_policy(Decimal("60"), Decimal("50"), Decimal("100"), Decimal("20")))

    def test_exceed_daily(self):
        self.assertFalse(enforce_spend_policy(Decimal("90"), Decimal("50"), Decimal("100"), Decimal("20")))

    def test_boundary(self):
        self.assertTrue(enforce_spend_policy(Decimal("50"), Decimal("50"), Decimal("100"), Decimal("50")))


if __name__ == "__main__":
    unittest.main()
