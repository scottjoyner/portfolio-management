from __future__ import annotations

from decimal import Decimal

from .math import rebalance_distance
from .schemas import LPRebalanceDecision


def should_rebalance_position(
    price: Decimal,
    lower_price: Decimal,
    upper_price: Decimal,
    distance_threshold: Decimal,
    expected_cost_usd: Decimal,
    expected_edge_usd: Decimal,
) -> LPRebalanceDecision:
    distance = rebalance_distance(price, lower_price, upper_price) * Decimal("10000")
    should = distance < distance_threshold and expected_edge_usd > expected_cost_usd
    reason = "distance breach" if should else "insufficient edge"
    return LPRebalanceDecision(
        should_rebalance=should,
        reason=reason,
        distance_to_boundary_bps=max(Decimal("0"), distance),
        expected_rebalance_cost_usd=expected_cost_usd,
        expected_rebalance_edge_usd=expected_edge_usd,
    )
