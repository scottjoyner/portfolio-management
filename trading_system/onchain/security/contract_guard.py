from __future__ import annotations

from decimal import Decimal


def contract_allowed(trust_score: Decimal, min_score: Decimal = Decimal("0.8")) -> bool:
    return trust_score >= min_score
