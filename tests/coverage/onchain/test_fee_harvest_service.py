import unittest
from decimal import Decimal

from onchain.dex.fee_harvest.service import FeeHarvest, FeeHarvestService


class TestFeeHarvest(unittest.TestCase):
    def test_track_harvestable(self):
        svc = FeeHarvestService()
        svc.track("p1", Decimal("10"), Decimal("0"))
        p = svc._positions["p1"]
        self.assertTrue(p.harvestable)

    def test_track_not_harvestable(self):
        svc = FeeHarvestService()
        svc.track("p2", Decimal("0"), Decimal("0"))
        self.assertFalse(svc._positions["p2"].harvestable)

    def test_harvestable_positions(self):
        svc = FeeHarvestService()
        svc.track("p1", Decimal("10"), Decimal("0"))
        svc.track("p2", Decimal("0"), Decimal("0"))
        self.assertEqual(len(svc.harvestable_positions()), 1)


if __name__ == "__main__":
    unittest.main()
