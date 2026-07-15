import unittest
from decimal import Decimal

from onchain.dex.amm.pool_state import PoolState


class TestPoolState(unittest.TestCase):
    def test_construct(self):
        ps = PoolState(reserve0=Decimal("1"), reserve1=Decimal("2"), fee_bps=Decimal("30"), block_number=100)
        self.assertEqual(ps.block_number, 100)


if __name__ == "__main__":
    unittest.main()
