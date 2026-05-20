from __future__ import annotations

from decimal import Decimal


def estimate_position_delta(token0_units: Decimal, token1_units: Decimal, spot: Decimal) -> Decimal:
    return token0_units * spot - token1_units


def dollar_exposure(token0_units: Decimal, token1_units: Decimal, spot: Decimal) -> dict[str, Decimal]:
    return {"token0_usd": token0_units * spot, "token1_usd": token1_units, "total_usd": token0_units * spot + token1_units}


def imbalance_ratio(token0_usd: Decimal, token1_usd: Decimal) -> Decimal:
    total = token0_usd + token1_usd
    if total == 0:
        return Decimal("0")
    return (token1_usd - token0_usd) / total
