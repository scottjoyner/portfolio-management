import unittest
from datetime import datetime
from decimal import Decimal

from onchain.dex.clmm.schemas import (
    ActionSimulationRequest,
    ActionSimulationResult,
    RouteFallbackPlan,
    RouteGraph,
    RouteStep,
)
from onchain.models import ActionType
from onchain.simulation.path_simulator_mm import (
    build_fallback_route,
    compare_primary_vs_fallback,
    simulate_action,
)


def make_request(with_route=True):
    route = None
    if with_route:
        steps = [RouteStep(venue="uni", contract="0xc", method="swap", token_in="T1", token_out="T2",
                           amount_in=Decimal("1"), min_amount_out=Decimal("0.9"))]
        route = RouteGraph(route_id="r1", steps=steps, quote_age_ms=500)
    return ActionSimulationRequest(
        action_type="add_liquidity",
        wallet="0xw",
        pool=None,
        route=route,
        amount_usd=Decimal("1000"),
        slippage_bps_limit=Decimal("50"),
        deadline_ts=datetime(2024, 1, 1),
    )


def make_result(net):
    return ActionSimulationResult(
        action_type="add_liquidity", contracts_touched=[], tokens_touched=[], approvals_required=[],
        estimated_gas=0, gas_cost_usd=Decimal("0"), slippage_bps=Decimal("0"),
        price_impact_bps=Decimal("0"), route_fragility_score=Decimal("0"),
        pool_liquidity_quality=Decimal("0"), contract_trust_score=Decimal("0"),
        token_safety_score=Decimal("0"), expected_gross_value_change_usd=Decimal("0"),
        expected_net_value_change_usd=net, worst_case_downside_usd=Decimal("0"),
        break_even_gas_usd=Decimal("0"), break_even_slippage_bps=Decimal("0"),
        confidence_score=Decimal("0"),
    )


class TestPathSimulatorMM(unittest.TestCase):
    def test_build_fallback(self):
        fb = build_fallback_route("reason")
        self.assertIsInstance(fb, RouteFallbackPlan)
        self.assertEqual(fb.expected_net_delta_usd, Decimal("-5"))

    def test_compare_none(self):
        p = make_result(Decimal("5"))
        self.assertIs(compare_primary_vs_fallback(p, None), p)

    def test_compare_primary(self):
        p = make_result(Decimal("5"))
        f = make_result(Decimal("3"))
        self.assertIs(compare_primary_vs_fallback(p, f), p)

    def test_compare_fallback(self):
        p = make_result(Decimal("3"))
        f = make_result(Decimal("5"))
        self.assertIs(compare_primary_vs_fallback(p, f), f)

    def test_simulate_with_route(self):
        r = simulate_action(make_request(with_route=True))
        self.assertIsInstance(r, ActionSimulationResult)
        self.assertEqual(r.contracts_touched, ["0xc"])
        self.assertEqual(r.tokens_touched, ["T1"])
        self.assertIsNotNone(r.fallback)

    def test_simulate_no_route(self):
        r = simulate_action(make_request(with_route=False))
        self.assertEqual(r.contracts_touched, [])
        self.assertEqual(r.tokens_touched, [])
        self.assertIsNone(r.fallback)


if __name__ == "__main__":
    unittest.main()
