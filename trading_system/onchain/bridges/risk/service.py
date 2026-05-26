from __future__ import annotations

from decimal import Decimal


def compute_bridge_risk(amount_usd: Decimal, bridge_count: int, chains_involved: list[str]) -> float:
    base_risk = 0.05 * bridge_count
    amount_factor = float(amount_usd) / 1_000_000 if amount_usd > 0 else 0
    return min(base_risk + amount_factor * 0.02, 1.0)
