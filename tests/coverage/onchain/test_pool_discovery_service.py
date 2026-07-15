import unittest
from decimal import Decimal

from onchain.dex.pool_discovery.service import PoolInfo, PoolDiscovery


class TestPoolDiscovery(unittest.TestCase):
    def test_register_and_find(self):
        d = PoolDiscovery()
        p = PoolInfo(address="0xABC", protocol="uni", token0="ETH", token1="USDC", fee_bps=30, liquidity_usd=Decimal("1000"))
        d.register_pool(p)
        # case-insensitive lookup, reversed order
        res = d.find_pools("USDC", "eth", "uni")
        self.assertEqual(res, [p])

    def test_find_no_protocol(self):
        d = PoolDiscovery()
        p = PoolInfo(address="0xABC", protocol="uni", token0="ETH", token1="USDC", fee_bps=30)
        d.register_pool(p)
        self.assertEqual(d.find_pools("eth", "usdc"), [p])

    def test_find_with_protocol(self):
        d = PoolDiscovery()
        p = PoolInfo(address="0xABC", protocol="uni", token0="ETH", token1="USDC", fee_bps=30)
        d.register_pool(p)
        self.assertEqual(d.find_pools("eth", "usdc", "uni"), [p])

    def test_no_match(self):
        d = PoolDiscovery()
        p = PoolInfo(address="0xABC", protocol="uni", token0="ETH", token1="USDC", fee_bps=30)
        d.register_pool(p)
        self.assertEqual(d.find_pools("eth", "dai", "uni"), [])
        self.assertEqual(d.find_pools("eth", "usdc", "curve"), [])


if __name__ == "__main__":
    unittest.main()
