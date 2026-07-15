import unittest

from onchain.models import ExecutionRoute, ActionType, SimulationResult
from onchain.simulation.call_static.harness import CallStaticHarness


def route():
    return ExecutionRoute(action_type=ActionType.SWAP, chain="eth", protocol="uni",
                          contracts_touched=["0xc1", "0xc2"], tokens_touched=["T1"])


class TestCallStaticHarness(unittest.TestCase):
    def test_success(self):
        h = CallStaticHarness()
        r = h.simulate(route(), 100.0, 90.0, 95.0)
        self.assertTrue(r.success)
        self.assertIsNotNone(r.simulation_hash)
        self.assertEqual(r.gas_used, 65000 + 2 * 25000)
        self.assertTrue(r.min_out_respected)

    def test_min_out_violation(self):
        h = CallStaticHarness()
        r = h.simulate(route(), 100.0, 90.0, 80.0)
        self.assertFalse(r.success)
        self.assertEqual(r.revert_reason, "MIN_OUT_VIOLATION")

    def test_zero_amount(self):
        h = CallStaticHarness()
        r = h.simulate(route(), 0.0, 90.0, 80.0)
        self.assertFalse(r.success)


if __name__ == "__main__":
    unittest.main()
