from __future__ import annotations

from decimal import Decimal


def token_allowed(risk_score: Decimal, min_score: Decimal = Decimal("0.75")) -> bool:
    return risk_score >= min_score
