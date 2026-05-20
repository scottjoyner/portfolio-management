from __future__ import annotations

from decimal import Decimal

from onchain.dex.clmm.schemas import ActionProfitabilityReport, ActionSimulationResult


def evaluate_route_profitability(sim: ActionSimulationResult, min_net_usd: Decimal) -> ActionProfitabilityReport:
    ok = sim.expected_net_value_change_usd > min_net_usd
    return ActionProfitabilityReport(
        opportunity_id=f"opp-{sim.action_type}",
        simulation=sim,
        is_profitable=ok,
        rejection_reason=None if ok else "expected net <= threshold",
    )


def classify_route_fragility(pool_quality: Decimal, quote_age_ms: int, fallback_available: bool) -> Decimal:
    fragility = (Decimal("1") - pool_quality) + Decimal(quote_age_ms) / Decimal("10000")
    if not fallback_available:
        fragility += Decimal("0.1")
    return min(Decimal("1"), max(Decimal("0"), fragility))
