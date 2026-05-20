from __future__ import annotations

from decimal import Decimal


def retreat_intensity(vol: Decimal, liquidity_drop_pct: Decimal) -> Decimal:
    return min(Decimal("1"), vol * Decimal("0.4") + liquidity_drop_pct / Decimal("100"))
