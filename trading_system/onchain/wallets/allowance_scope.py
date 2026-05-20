from __future__ import annotations

from decimal import Decimal


def minimized_allowance(required_amount: Decimal, buffer_pct: Decimal = Decimal("0.02")) -> Decimal:
    return required_amount * (Decimal("1") + buffer_pct)
