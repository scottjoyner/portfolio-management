from __future__ import annotations

from decimal import Decimal


def classify_sequential_risk(onchain_leg_confidence: Decimal, hedge_leg_confidence: Decimal) -> str:
    joint = onchain_leg_confidence * hedge_leg_confidence
    if joint > Decimal("0.8"):
        return "LOW"
    if joint > Decimal("0.5"):
        return "MEDIUM"
    return "HIGH"


def semi_atomic_safety_score(onchain_leg_confidence: Decimal, hedge_leg_confidence: Decimal, unwind_ready: bool) -> Decimal:
    base = onchain_leg_confidence * hedge_leg_confidence
    return min(Decimal("1"), base + (Decimal("0.1") if unwind_ready else Decimal("0")))
