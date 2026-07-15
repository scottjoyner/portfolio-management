import unittest

from onchain.dex.adapters.service import DEXAdapter, DEXRegistry


class TestDEXAdapters(unittest.TestCase):
    def test_register_get(self):
        reg = DEXRegistry()
        a = DEXAdapter(name="uni", protocol="uniswap", supported_chains={"eth"})
        reg.register(a)
        self.assertIs(reg.get("uni"), a)
        self.assertIsNone(reg.get("missing"))

    def test_for_chain(self):
        reg = DEXRegistry()
        a = DEXAdapter(name="uni", protocol="uniswap", supported_chains={"eth", "arb"})
        reg.register(a)
        self.assertEqual(reg.for_chain("arb"), [a])
        self.assertEqual(reg.for_chain("sol"), [])


if __name__ == "__main__":
    unittest.main()
