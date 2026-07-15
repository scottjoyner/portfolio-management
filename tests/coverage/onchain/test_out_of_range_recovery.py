import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.out_of_range_recovery import recovery_decision


class TestOutOfRangeRecovery(unittest.TestCase):
    def test_reposition(self):
        self.assertEqual(recovery_decision(Decimal("10"), Decimal("5"), Decimal("3")), "reposition")

    def test_hold(self):
        self.assertEqual(recovery_decision(Decimal("5"), Decimal("10"), Decimal("3")), "hold")

    def test_withdraw(self):
        self.assertEqual(recovery_decision(Decimal("5"), Decimal("3"), Decimal("10")), "withdraw")


if __name__ == "__main__":
    unittest.main()
