import unittest
from decimal import Decimal

from onchain.dex.clmm.inventory_model import estimate_position_delta, dollar_exposure, imbalance_ratio


class TestInventoryModel(unittest.TestCase):
    def test_estimate_delta(self):
        self.assertEqual(estimate_position_delta(Decimal("2"), Decimal("1"), Decimal("10")), Decimal("19"))

    def test_dollar_exposure(self):
        d = dollar_exposure(Decimal("2"), Decimal("1"), Decimal("10"))
        self.assertEqual(d["token0_usd"], Decimal("20"))
        self.assertEqual(d["token1_usd"], Decimal("1"))
        self.assertEqual(d["total_usd"], Decimal("21"))

    def test_imbalance_zero_total(self):
        self.assertEqual(imbalance_ratio(Decimal("0"), Decimal("0")), Decimal("0"))

    def test_imbalance(self):
        r = imbalance_ratio(Decimal("10"), Decimal("30"))
        self.assertEqual(r, (Decimal("30") - Decimal("10")) / Decimal("40"))


if __name__ == "__main__":
    unittest.main()
