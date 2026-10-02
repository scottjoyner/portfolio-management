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

    def test_clamp_is_a_ceiling(self):
        """A suggestion above the policy cap is reduced, not raised to it.

        This passed 200 wei and expected the policy maximum back, which is the
        opposite of a clamp: it would have meant every low bid was raised to the
        cap. clamp_gas_price is min(suggested, max), so a low suggestion passes
        through and a high one is pulled down.
        """
        e = GasPolicyEngine()
        cap = e.get_policy("eth").max_gas_price_wei
        assert e.clamp_gas_price("eth", 200) == 200, "below the cap passes through"
        assert e.clamp_gas_price("eth", cap * 10) == cap, "above the cap is clamped down"

    def test_adjusted_limit(self):
        e = GasPolicyEngine()
        self.assertEqual(e.adjusted_gas_limit("eth", 100_000), 120_000)


if __name__ == "__main__":
    unittest.main()
