from __future__ import annotations

from decimal import Decimal

from onchain.dex.clmm.schemas import (
    ActionSimulationRequest,
    ActionSimulationResult,
    RouteFallbackPlan,
)
from onchain.simulation.gas_model_mm import estimate_action_gas, gas_cost_usd
from onchain.simulation.route_profitability import classify_route_fragility
from onchain.simulation.slippage_model_mm import estimate_action_slippage


def build_fallback_route(reason: str) -> RouteFallbackPlan:
    return RouteFallbackPlan(reason=reason, fallback_route=None, expected_net_delta_usd=Decimal("-5"))


def compare_primary_vs_fallback(primary: ActionSimulationResult, fallback: ActionSimulationResult | None) -> ActionSimulationResult:
    if fallback is None or primary.expected_net_value_change_usd >= fallback.expected_net_value_change_usd:
        return primary
    return fallback


def simulate_action(action_request: ActionSimulationRequest) -> ActionSimulationResult:
    hops = len(action_request.route.steps) if action_request.route else 1
    gas = estimate_action_gas(action_request.action_type, hops)
    gas_usd = gas_cost_usd(gas, Decimal("20"), Decimal("3000"))
    slippage = estimate_action_slippage(action_request.amount_usd, Decimal("2_000_000"), Decimal("0.2"))
    gross = action_request.amount_usd * Decimal("0.003")
    net = gross - gas_usd - action_request.amount_usd * slippage
    fragility = classify_route_fragility(Decimal("0.8"), action_request.route.quote_age_ms if action_request.route else 0, False)
    return ActionSimulationResult(
        action_type=action_request.action_type,
        contracts_touched=[s.contract for s in action_request.route.steps] if action_request.route else [],
        tokens_touched=[s.token_in for s in action_request.route.steps] if action_request.route else [],
        approvals_required=["token_allowance"],
        estimated_gas=gas,
        gas_cost_usd=gas_usd,
        slippage_bps=slippage * Decimal("10000"),
        price_impact_bps=slippage * Decimal("7000"),
        route_fragility_score=fragility,
        pool_liquidity_quality=Decimal("0.8"),
        contract_trust_score=Decimal("0.95"),
        token_safety_score=Decimal("0.92"),
        expected_gross_value_change_usd=gross,
        expected_net_value_change_usd=net,
        worst_case_downside_usd=action_request.amount_usd * Decimal("0.02"),
        break_even_gas_usd=gross,
        break_even_slippage_bps=Decimal("25"),
        confidence_score=Decimal("0.75"),
        fallback=build_fallback_route("primary fragility elevated") if fragility > Decimal("0.6") else None,
    )
