from __future__ import annotations

from decimal import Decimal


def constant_product_out(amount_in: Decimal, reserve_in: Decimal, reserve_out: Decimal, fee_bps: Decimal) -> Decimal:
    net_in = amount_in * (Decimal("1") - fee_bps / Decimal("10000"))
    return (net_in * reserve_out) / (reserve_in + net_in)
