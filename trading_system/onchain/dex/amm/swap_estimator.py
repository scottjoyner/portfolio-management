from __future__ import annotations

from decimal import Decimal

from .cpmm_math import constant_product_out


def estimate_action_slippage(amount_in: Decimal, reserve_in: Decimal) -> Decimal:
    if reserve_in <= 0:
        return Decimal("1")
    return amount_in / reserve_in


def estimate_swap(amount_in: Decimal, reserve_in: Decimal, reserve_out: Decimal, fee_bps: Decimal) -> dict[str, Decimal]:
    out = constant_product_out(amount_in, reserve_in, reserve_out, fee_bps)
    return {"amount_out": out, "slippage": estimate_action_slippage(amount_in, reserve_in)}
