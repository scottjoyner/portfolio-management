import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.crash_liquidity_overlay import crash_overlay_size


class TestCrashLiquidityOverlay(unittest.TestCase):
    def test_size(self):
        self.assertEqual(crash_overlay_size(Decimal("1000"), Decimal("0.5")), Decimal("75"))

    def test_cap(self):
        self.assertEqual(crash_overlay_size(Decimal("1000"), Decimal("2")), Decimal("150"))

    def test_custom_cap(self):
        self.assertEqual(crash_overlay_size(Decimal("1000"), Decimal("1"), Decimal("0.1")), Decimal("100"))


if __name__ == "__main__":
    unittest.main()
