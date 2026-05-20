from __future__ import annotations

from decimal import Decimal


def adverse_selection_trigger(toxic_flow_proxy: Decimal, threshold: Decimal = Decimal("0.7")) -> bool:
    return toxic_flow_proxy >= threshold
