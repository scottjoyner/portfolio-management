from __future__ import annotations

from dataclasses import dataclass


RISK_TIER_CAPS: dict[str, float] = {
    "TIER_0_CAPITAL_PRESERVATION": 0.40,
    "TIER_1_LOW_RISK": 0.30,
    "TIER_2_MODERATE_RISK": 0.20,
    "TIER_3_HIGH_RISK": 0.07,
    "TIER_4_EXPERT_HIGH_RISK": 0.02,
    "TIER_5_RESEARCH_ONLY": 0.01,
}


@dataclass(frozen=True)
class StrategyAllocationInput:
    strategy_id: str
    sleeve: str
    risk_tier: str
    requested_fraction: float
    quality_score: float
    drawdown: float
    live_safe: bool
    paper_ready: bool


@dataclass(frozen=True)
class AllocationDecision:
    strategy_id: str
    approved_fraction: float
    reason: str


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def allocate_capital(
    strategies: list[StrategyAllocationInput],
    *,
    locked_reserve_fraction: float,
    cash_buffer_fraction: float,
    hedge_reserve_fraction: float,
) -> list[AllocationDecision]:
    """Allocate deployable capital with tier caps, quality scaling, and safety downgrades."""
    if locked_reserve_fraction < 0 or cash_buffer_fraction < 0 or hedge_reserve_fraction < 0:
        raise ValueError("reserve fractions must be non-negative")

    deployable = 1.0 - (locked_reserve_fraction + cash_buffer_fraction + hedge_reserve_fraction)
    if deployable <= 0:
        raise ValueError("deployable capital must be positive")

    decisions: list[AllocationDecision] = []
    granted_total = 0.0
    for s in strategies:
        tier_cap = RISK_TIER_CAPS.get(s.risk_tier, 0.0)
        base = min(s.requested_fraction, tier_cap)

        # performance-aware throttle/ramp
        quality_scale = _clip(0.5 + (s.quality_score * 0.5), 0.1, 1.2)
        drawdown_scale = _clip(1.0 - s.drawdown, 0.0, 1.0)
        candidate = base * quality_scale * drawdown_scale

        # enforce operational safety posture
        reason = "allocated under tier cap"
        if not s.paper_ready:
            candidate *= 0.25
            reason = "paper-readiness throttle applied"
        if not s.live_safe:
            candidate *= 0.5
            reason = "live-safety throttle applied"
        if s.risk_tier in {"TIER_4_EXPERT_HIGH_RISK", "TIER_5_RESEARCH_ONLY"}:
            candidate = min(candidate, 0.02)
            reason = "expert/research hard cap applied"

        approved = max(0.0, candidate)
        granted_total += approved
        decisions.append(AllocationDecision(strategy_id=s.strategy_id, approved_fraction=approved, reason=reason))

    if granted_total == 0:
        return decisions

    # normalize to deployable budget
    if granted_total > deployable:
        scale = deployable / granted_total
        decisions = [
            AllocationDecision(
                strategy_id=d.strategy_id,
                approved_fraction=d.approved_fraction * scale,
                reason=f"{d.reason}; scaled to deployable budget",
            )
            for d in decisions
        ]
    return decisions
