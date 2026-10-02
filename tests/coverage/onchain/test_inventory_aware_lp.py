import unittest
from decimal import Decimal

from onchain.strategies.amm_lp.inventory_aware_lp import InventoryAwareLP, InventoryAwareLPConfig


class TestInventoryAwareLP(unittest.TestCase):
    def test_pause(self):
        s = InventoryAwareLP(InventoryAwareLPConfig())
        self.assertTrue(s.should_pause_accumulation(Decimal("0.5")))

    def test_no_pause(self):
        s = InventoryAwareLP(InventoryAwareLPConfig())
        self.assertFalse(s.should_pause_accumulation(Decimal("0.1")))

    def test_negative_skew_also_pauses(self):
        """Skew is symmetric: being over-weighted in *either* token is the risk.

        This expected no pause for -0.5 while expecting a pause for +0.5. LP
        inventory drifts either way, and a one-sided check lets the position run
        away in whichever direction the test happened not to cover.
        """
        s = InventoryAwareLP(InventoryAwareLPConfig())
        self.assertTrue(s.should_pause_accumulation(Decimal("-0.5")))
        self.assertTrue(s.should_pause_accumulation(Decimal("-1.0")))
        self.assertFalse(s.should_pause_accumulation(Decimal("-0.1")))


if __name__ == "__main__":
    unittest.main()
