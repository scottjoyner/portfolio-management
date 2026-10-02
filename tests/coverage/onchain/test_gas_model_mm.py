import unittest
from decimal import Decimal

from onchain.simulation.gas_model_mm import estimate_action_gas, gas_cost_usd


class TestGasModelMM(unittest.TestCase):
    def test_known(self):
        # hops=0 to read the base cost; the default hops=1 adds 35k per hop.
        self.assertEqual(estimate_action_gas("add_liquidity", 0), 220000)
        self.assertEqual(estimate_action_gas("remove_liquidity", 0), 210000)
        self.assertEqual(estimate_action_gas("collect_fees", 0), 120000)
        self.assertEqual(estimate_action_gas("rebalance_position", 0), 330000)
        self.assertEqual(estimate_action_gas("deploy_and_hedge", 0), 420000)

    def test_hops_add_to_the_base(self):
        """Each hop adds 35k. These asserted the bare base, ignoring hops."""
        self.assertEqual(estimate_action_gas("add_liquidity", 1), 255000)
        self.assertEqual(estimate_action_gas("add_liquidity", 3), 325000)

    def test_unknown_default(self):
        self.assertEqual(estimate_action_gas("something_else", 0), 180000)

    def test_hops(self):
        self.assertEqual(estimate_action_gas("add_liquidity", 3), 220000 + 105000)

    def test_cost(self):
        c = gas_cost_usd(100000, Decimal("20"), Decimal("3000"))
        self.assertEqual(c, Decimal("100000") * Decimal("20") * Decimal("1e-9") * Decimal("3000"))


if __name__ == "__main__":
    unittest.main()
