from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class LiquidityInput:
    asset: str
    idle_balance: float
    working_balance: float
    depth_score: float
    spread_opportunity: float
    friction_score: float
    hedgeability: float
    risk_budget_available: float


@dataclass(slots=True)
class LiquidityScore:
    asset: str
    usefulness: float
    productivity: float
    transfer_necessity: float


class LiquidityOptimizer:
    """Simple deterministic liquidity scoring model for operator recommendations."""

    def score(self, item: LiquidityInput) -> LiquidityScore:
        total = max(item.idle_balance + item.working_balance, 1.0)
        idle_ratio = item.idle_balance / total
        working_ratio = item.working_balance / total

        usefulness = max(
            0.0,
            min(
                1.0,
                0.30 * working_ratio
                + 0.20 * item.depth_score
                + 0.15 * item.spread_opportunity
                + 0.15 * item.hedgeability
                + 0.20 * item.risk_budget_available,
            ),
        )
        productivity = max(0.0, min(1.0, usefulness - (0.35 * idle_ratio) - (0.25 * item.friction_score)))
        transfer_necessity = max(0.0, min(1.0, idle_ratio * 0.6 + (1 - productivity) * 0.4))

        return LiquidityScore(asset=item.asset, usefulness=round(usefulness, 4), productivity=round(productivity, 4), transfer_necessity=round(transfer_necessity, 4))

    def recommend_move_amount(self, item: LiquidityInput) -> float:
        score = self.score(item)
        return round(item.idle_balance * score.transfer_necessity, 2)
