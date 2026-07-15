import unittest

from onchain.simulation.schemas import (
    ActionSimulationRequest,
    ActionSimulationResult,
    ActionProfitabilityReport,
    RouteFallbackPlan,
)


class TestSimulationSchemas(unittest.TestCase):
    def test_imports(self):
        self.assertIsNotNone(ActionSimulationRequest)
        self.assertIsNotNone(ActionSimulationResult)
        self.assertIsNotNone(ActionProfitabilityReport)
        self.assertIsNotNone(RouteFallbackPlan)


if __name__ == "__main__":
    unittest.main()
