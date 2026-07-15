import unittest
from decimal import Decimal

from onchain.dex.amm.swap_estimator import estimate_action_slippage, estimate_swap
from onchain.dex.amm.cpmm_math import constant_product_out


class TestSwapEstimator(unittest.TestCase):
    def test_slippage_normal(self):
        self.assertEqual(estimate_action_slippage(Decimal("10"), Decimal("100")), Decimal("0.1"))

    def test_slippage_zero_reserve(self):
        self.assertEqual(estimate_action_slippage(Decimal("10"), Decimal("0")), Decimal("1"))

    def test_estimate_swap(self):
        res = estimate_swap(Decimal("100"), Decimal("1000"), Decimal("1000"), Decimal("30"))
        self.assertEqual(res["amount_out"], constant_product_out(Decimal("100"), Decimal("1000"), Decimal("1000"), Decimal("30")))
        self.assertEqual(res["slippage"], Decimal("0.1"))


if __name__ == "__main__":
    unittest.main()
