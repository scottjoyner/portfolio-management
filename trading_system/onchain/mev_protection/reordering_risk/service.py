from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReorderingRisk:
    score: float = 0.0
    top_of_block_risk: float = 0.0
    sandwich_risk: float = 0.0
    cex_arb_risk: float = 0.0
    high_risk: bool = False


def assess_reordering_risk(tx_priority_fee: int, sandwich_risk_score: float, pool_liquidity_usd: float) -> ReorderingRisk:
    top_of_block = min(tx_priority_fee / 1e9 / 100, 1.0) if tx_priority_fee > 0 else 0
    liq_factor = min(1_000_000 / pool_liquidity_usd, 1.0) if pool_liquidity_usd > 0 else 1.0
    combined = (top_of_block * 0.3 + sandwich_risk_score * 0.5 + liq_factor * 0.2)
    return ReorderingRisk(
        score=combined,
        top_of_block_risk=top_of_block,
        sandwich_risk=sandwich_risk_score,
        high_risk=combined > 0.6,
    )
