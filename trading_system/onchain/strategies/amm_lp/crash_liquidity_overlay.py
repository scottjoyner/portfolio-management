from __future__ import annotations

from decimal import Decimal


def crash_overlay_size(capital_usd: Decimal, shock_score: Decimal, cap_pct: Decimal = Decimal("0.15")) -> Decimal:
    return capital_usd * cap_pct * min(Decimal("1"), shock_score)
