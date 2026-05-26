from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass
class QuoteResult:
    estimated_amount_out: Decimal
    price_impact_bps: Decimal
    fee_bps: int
    route: list[str]
    pool: str


def quote_swap(amount_in: Decimal, reserve_in: Decimal, reserve_out: Decimal, fee_bps: int = 30) -> QuoteResult:
    if reserve_in <= 0 or reserve_out <= 0:
        return QuoteResult(Decimal("0"), Decimal("10000"), fee_bps, [], "")

    fee = fee_bps / 10000
    amount_in_after_fee = amount_in * (Decimal("1") - Decimal(str(fee)))
    k = reserve_in * reserve_out
    new_reserve_in = reserve_in + amount_in_after_fee
    new_reserve_out = k / new_reserve_in
    amount_out = reserve_out - new_reserve_out

    price_impact = (amount_in / reserve_in) * Decimal("10000")
    return QuoteResult(
        estimated_amount_out=amount_out,
        price_impact_bps=price_impact,
        fee_bps=fee_bps,
        route=[],
        pool="",
    )
