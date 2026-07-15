import unittest

from onchain.wallets.gas_policy.service import GasPolicy, GasPolicyEngine


class TestGasPolicy(unittest.TestCase):
    def test_properties(self):
        p = GasPolicy(max_gas_price_gwei=100.0, max_priority_fee_gwei=2.0)
        self.assertEqual(p.max_gas_price_wei, 100_000_000_000)
        self.assertEqual(p.max_priority_fee_wei, 2_000_000_000)

    def test_default_policy(self):
        p = GasPolicy()
        self.assertEqual(p.max_gas_price_wei, 100_000_000_000)

    def test_engine(self):
        e = GasPolicyEngine()
        e.set_policy("eth", GasPolicy(max_gas_price_gwei=50.0))
        self.assertEqual(e.get_policy("eth").max_gas_price_gwei, 50.0)
        self.assertEqual(e.get_policy("arb").max_gas_price_gwei, 100.0)

    def test_clamp(self):
        e = GasPolicyEngine()
        self.assertEqual(e.clamp_gas_price("eth", 200), 100_000_000_000)

    def test_adjusted_limit(self):
        e = GasPolicyEngine()
        self.assertEqual(e.adjusted_gas_limit("eth", 100_000), 120_000)


if __name__ == "__main__":
    unittest.main()
