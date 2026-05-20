from __future__ import annotations

from decimal import Decimal


def rotate_width(base_width_bps: Decimal, vol_regime: str) -> Decimal:
    if vol_regime == "CRISIS":
        return base_width_bps * Decimal("2.5")
    if vol_regime == "ELEVATED":
        return base_width_bps * Decimal("1.5")
    if vol_regime == "CALM":
        return base_width_bps * Decimal("0.8")
    return base_width_bps
