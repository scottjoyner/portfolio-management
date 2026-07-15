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

    def test_negative(self):
        s = InventoryAwareLP(InventoryAwareLPConfig())
        self.assertFalse(s.should_pause_accumulation(Decimal("-0.5")))


if __name__ == "__main__":
    unittest.main()
