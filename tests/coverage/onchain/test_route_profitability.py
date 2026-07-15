import unittest
from decimal import Decimal

from onchain.dex.clmm.schemas import ActionSimulationResult, ActionProfitabilityReport
from onchain.simulation.route_profitability import evaluate_route_profitability, classify_route_fragility


def make_sim(net):
    return ActionSimulationResult(
        action_type="add_liquidity",
        contracts_touched=[],
        tokens_touched=[],
        approvals_required=[],
        estimated_gas=0,
        gas_cost_usd=Decimal("0"),
        slippage_bps=Decimal("0"),
        price_impact_bps=Decimal("0"),
        route_fragility_score=Decimal("0"),
        pool_liquidity_quality=Decimal("0"),
        contract_trust_score=Decimal("0"),
        token_safety_score=Decimal("0"),
        expected_gross_value_change_usd=Decimal("0"),
        expected_net_value_change_usd=net,
        worst_case_downside_usd=Decimal("0"),
        break_even_gas_usd=Decimal("0"),
        break_even_slippage_bps=Decimal("0"),
        confidence_score=Decimal("0"),
    )


class TestRouteProfitability(unittest.TestCase):
    def test_profitable(self):
        rep = evaluate_route_profitability(make_sim(Decimal("10")), Decimal("1"))
        self.assertIsInstance(rep, ActionProfitabilityReport)
        self.assertTrue(rep.is_profitable)

    def test_unprofitable(self):
        rep = evaluate_route_profitability(make_sim(Decimal("0.5")), Decimal("1"))
        self.assertFalse(rep.is_profitable)
        self.assertEqual(rep.rejection_reason, "expected net <= threshold")

    def test_fragility_fallback(self):
        f = classify_route_fragility(Decimal("0.8"), 100, False)
        # (1-0.8) + 100/10000 + 0.1 = 0.2 + 0.01 + 0.1 = 0.31
        self.assertEqual(f, Decimal("0.31"))

    def test_fragility_no_fallback(self):
        f = classify_route_fragility(Decimal("0.8"), 100, True)
        self.assertEqual(f, Decimal("0.21"))

    def test_fragility_clamped_high(self):
        f = classify_route_fragility(Decimal("-5"), 100000, False)
        self.assertEqual(f, Decimal("1"))

    def test_fragility_clamped_low(self):
        f = classify_route_fragility(Decimal("1"), 0, True)
        self.assertEqual(f, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
