from __future__ import annotations

from decimal import Decimal


def fee_capture_efficiency(fees_usd: Decimal, notional_volume_usd: Decimal, fee_tier_bps: Decimal) -> Decimal:
    if notional_volume_usd <= 0 or fee_tier_bps <= 0:
        return Decimal("0")
    theoretical = notional_volume_usd * fee_tier_bps / Decimal("10000")
    return fees_usd / theoretical if theoretical > 0 else Decimal("0")


def accrued_fees_from_growth(liquidity: Decimal, fee_growth_delta0: Decimal, fee_growth_delta1: Decimal) -> tuple[Decimal, Decimal]:
    return liquidity * fee_growth_delta0, liquidity * fee_growth_delta1
