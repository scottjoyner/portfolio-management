from __future__ import annotations

from unittest import TestCase

from onchain.contracts.abi_store.service import ABIEntry, ABIStore


class TestABIStore(TestCase):
    def test_store_and_get(self):
        store = ABIStore()
        entry = ABIEntry(address="0xABC", chain="base", abi=[{"x": 1}])
        store.store(entry)
        got = store.get("base", "0xabc")
        self.assertIs(got, entry)
        self.assertIsNone(store.get("base", "0xmissing"))
        self.assertTrue(store.has_abi("base", "0xABC"))
        self.assertFalse(store.has_abi("base", "0xmissing"))

    def test_store_overwrites(self):
        store = ABIStore()
        store.store(ABIEntry(address="0xABC", chain="base", abi=[{}]))
        store.store(ABIEntry(address="0xABC", chain="base", abi=[{"y": 2}], source="local"))
        self.assertEqual(store.get("base", "0xabc").source, "local")
