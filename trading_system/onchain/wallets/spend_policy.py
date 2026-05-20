from __future__ import annotations

from decimal import Decimal


def enforce_spend_policy(amount_usd: Decimal, max_per_action_usd: Decimal, max_daily_usd: Decimal, daily_used_usd: Decimal) -> bool:
    return amount_usd <= max_per_action_usd and (daily_used_usd + amount_usd) <= max_daily_usd
