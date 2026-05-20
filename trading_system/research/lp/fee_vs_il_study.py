from __future__ import annotations

from decimal import Decimal


def fee_vs_il_ratio(fees: Decimal, il: Decimal) -> Decimal:
    if il == 0:
        return Decimal("0")
    return fees / abs(il)
