from __future__ import annotations

from decimal import Decimal

from .math import active_range_fraction, position_inventory_mix


def classify_position_state(price: Decimal, lower: Decimal, upper: Decimal) -> str:
    if price < lower:
        return "BELOW_RANGE"
    if price > upper:
        return "ABOVE_RANGE"
    return "IN_RANGE"


def position_analytics(liquidity: Decimal, price: Decimal, lower: Decimal, upper: Decimal) -> dict[str, Decimal]:
    return {
        "active_range_fraction": active_range_fraction(price, lower, upper),
        **position_inventory_mix(liquidity, price, lower, upper),
    }
