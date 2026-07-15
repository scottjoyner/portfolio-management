import unittest
from decimal import Decimal

from trading_system.risk.slippage.service import (
    estimate_slippage,
    slippage_adjusted_price,
    SlippageEstimate,
)


class TestEstimateSlippage(unittest.TestCase):
    def test_zero_liquidity(self):
        est = estimate_slippage(Decimal("100"), Decimal("0"), Decimal("10"))
        self.assertFalse(est.within_limits)
        self.assertEqual(est.reason, "zero liquidity")

    def test_within_limits(self):
        est = estimate_slippage(Decimal("10"), Decimal("10000"), Decimal("5"))
        self.assertIsInstance(est, SlippageEstimate)
        self.assertLessEqual(est.estimated_slippage_bps, est.max_slippage_bps)
        self.assertTrue(est.within_limits)

    def test_over_limits(self):
        # order_size / liquidity * 10000 + vol -> large
        est = estimate_slippage(Decimal("100000"), Decimal("1000"), Decimal("0"))
        self.assertGreater(est.estimated_slippage_bps, est.max_slippage_bps)
        self.assertFalse(est.within_limits)

    def test_default_volatility(self):
        est = estimate_slippage(Decimal("1"), Decimal("10000"))
        self.assertEqual(est.estimated_slippage_bps, Decimal("11"))


class TestSlippageAdjustedPrice(unittest.TestCase):
    def test_buy(self):
        p = slippage_adjusted_price(Decimal("100"), Decimal("100"), "buy")
        # 100 * (1 + 0.01) = 101
        self.assertEqual(p, Decimal("101"))

    def test_sell(self):
        p = slippage_adjusted_price(Decimal("100"), Decimal("100"), "sell")
        # 100 * (2 - 1.01) = 99
        self.assertEqual(p, Decimal("99"))

    def test_sell_default_side(self):
        p = slippage_adjusted_price(Decimal("100"), Decimal("0"), "anything")
        self.assertEqual(p, Decimal("100"))


if __name__ == "__main__":
    unittest.main()
