import unittest
from decimal import Decimal

from onchain.dex.lp_manager.service import LPPosition, LPManager


class TestLPManager(unittest.TestCase):
    def test_add_remove(self):
        m = LPManager()
        m.add_position(LPPosition(position_id="p1", chain="eth", protocol="uni", pool_address="0x1", amount_usd=Decimal("100")))
        self.assertEqual(m.total_liquidity_usd(), Decimal("100"))
        m.remove_position("p1")
        self.assertEqual(m.total_liquidity_usd(), Decimal("0"))
        m.remove_position("missing")

    def test_total(self):
        m = LPManager()
        m.add_position(LPPosition(position_id="p1", chain="eth", protocol="uni", pool_address="0x1", amount_usd=Decimal("100")))
        m.add_position(LPPosition(position_id="p2", chain="eth", protocol="uni", pool_address="0x2", amount_usd=Decimal("50")))
        self.assertEqual(m.total_liquidity_usd(), Decimal("150"))


if __name__ == "__main__":
    unittest.main()
