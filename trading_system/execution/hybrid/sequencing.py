from __future__ import annotations

from decimal import Decimal


def classify_hybrid_atomicity(onchain_confidence: Decimal, cex_confidence: Decimal) -> str:
    joint = onchain_confidence * cex_confidence
    if joint > Decimal("0.8"):
        return "SEMI_ATOMIC_STRONG"
    if joint > Decimal("0.5"):
        return "SEMI_ATOMIC_MODERATE"
    return "NON_ATOMIC"
