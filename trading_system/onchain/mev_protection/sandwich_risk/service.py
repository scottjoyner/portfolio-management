from __future__ import annotations

from decimal import Decimal


def estimate_sandwich_loss(amount_usd: Decimal, pool_liquidity_usd: Decimal, typical_mev_bps: Decimal = Decimal("10")) -> Decimal:
    if pool_liquidity_usd <= Decimal("0"):
        return amount_usd
    impact = amount_usd / pool_liquidity_usd
    combined_bps = impact * Decimal("10000") + typical_mev_bps
    return amount_usd * combined_bps / Decimal("10000")


def sandwich_risk_score(amount_usd: Decimal, pool_liquidity_usd: Decimal) -> float:
    if pool_liquidity_usd <= Decimal("0"):
        return 1.0
    ratio = float(amount_usd / pool_liquidity_usd)
    return min(ratio * 100, 1.0)
