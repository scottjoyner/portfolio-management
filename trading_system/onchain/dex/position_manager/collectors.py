from __future__ import annotations

from decimal import Decimal


def collectable_fee_value(fee0: Decimal, fee1: Decimal, price: Decimal) -> Decimal:
    return fee0 * price + fee1
