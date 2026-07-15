from __future__ import annotations

from unittest import TestCase

from onchain.chains.base.adapter import BaseAdapter
from onchain.chains.ethereum.adapter import EthereumAdapter


class TestBaseAdapter(TestCase):
    def test_init(self):
        a = BaseAdapter(rpc_url="https://base.rpc")
        self.assertEqual(a.chain_id, 8453)
        self.assertEqual(a.rpc_url, "https://base.rpc")
        self.assertIsNone(a._w3)


class TestEthereumAdapter(TestCase):
    def test_init(self):
        a = EthereumAdapter(rpc_url="https://eth.rpc")
        self.assertEqual(a.chain_id, 1)
        self.assertEqual(a.rpc_url, "https://eth.rpc")
        self.assertIsNone(a._w3)
