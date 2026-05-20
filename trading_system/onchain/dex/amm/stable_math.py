from __future__ import annotations

from decimal import Decimal


def stable_swap_out(amount_in: Decimal, reserve_in: Decimal, reserve_out: Decimal, amp: Decimal, fee_bps: Decimal) -> Decimal:
    # lightweight approximation
    invariant = reserve_in + reserve_out
    effective = amount_in * amp
    gross = effective * reserve_out / max(invariant, Decimal("1e-12"))
    return gross * (Decimal("1") - fee_bps / Decimal("10000"))
