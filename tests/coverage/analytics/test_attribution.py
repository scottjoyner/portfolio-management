import unittest
from decimal import Decimal

from analytics.attribution.service import AttributionResult, AttributionService


class TestAttributionService(unittest.TestCase):
    def test_compute_returns_result(self):
        svc = AttributionService()
        res = svc.compute("strat-1", [Decimal("0.1")], [Decimal("0.05")])
        self.assertIsInstance(res, AttributionResult)
        self.assertEqual(res.strategy_id, "strat-1")
        self.assertEqual(res.total_pnl, Decimal("0"))
        self.assertEqual(res.alpha_pnl, Decimal("0"))
        self.assertEqual(res.beta_pnl, Decimal("0"))
        self.assertEqual(res.fee_cost, Decimal("0"))

    def test_attribution_result_defaults(self):
        r = AttributionResult(strategy_id="x")
        self.assertEqual(r.total_pnl, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
