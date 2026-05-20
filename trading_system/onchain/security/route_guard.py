from __future__ import annotations

from decimal import Decimal


def route_allowed(fragility: Decimal, trust: Decimal, max_fragility: Decimal = Decimal("0.7"), min_trust: Decimal = Decimal("0.7")) -> bool:
    return fragility <= max_fragility and trust >= min_trust
