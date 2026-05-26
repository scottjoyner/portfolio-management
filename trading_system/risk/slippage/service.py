from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class SlippageEstimate:
    estimated_slippage_bps: Decimal
    max_slippage_bps: Decimal
    within_limits: bool
    reason: str = ""


def estimate_slippage(order_size: Decimal, pool_liquidity: Decimal, volatility_bps: Decimal = Decimal("10")) -> SlippageEstimate:
    if pool_liquidity <= Decimal("0"):
        return SlippageEstimate(Decimal("0"), Decimal("0"), False, "zero liquidity")

    slippage_ratio = order_size / pool_liquidity
    estimated_bps = slippage_ratio * Decimal("10000") + volatility_bps
    max_slippage = Decimal("100")
    return SlippageEstimate(
        estimated_slippage_bps=estimated_bps,
        max_slippage_bps=max_slippage,
        within_limits=estimated_bps <= max_slippage,
    )


def slippage_adjusted_price(base_price: Decimal, slippage_bps: Decimal, side: str) -> Decimal:
    factor = Decimal("1") + (slippage_bps / Decimal("10000"))
    if side == "buy":
        return base_price * factor
    return base_price * (Decimal("2") - factor)
