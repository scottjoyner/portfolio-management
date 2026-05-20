from __future__ import annotations

from decimal import Decimal


def estimate_action_slippage(amount_usd: Decimal, depth_usd: Decimal, volatility: Decimal) -> Decimal:
    if depth_usd <= 0:
        return Decimal("1")
    return (amount_usd / depth_usd) * (Decimal("1") + volatility)
