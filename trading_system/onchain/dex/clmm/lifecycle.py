from __future__ import annotations

from decimal import Decimal


def material_range_change(old_lower: int, old_upper: int, new_lower: int, new_upper: int) -> Decimal:
    old_mid = Decimal(old_lower + old_upper) / Decimal("2")
    new_mid = Decimal(new_lower + new_upper) / Decimal("2")
    if old_mid == 0:
        return Decimal("1")
    return abs(new_mid - old_mid) / abs(old_mid)


def needs_approval_for_range_change(materiality: Decimal, threshold: Decimal) -> bool:
    return materiality > threshold
