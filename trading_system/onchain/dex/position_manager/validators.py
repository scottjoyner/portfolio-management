from __future__ import annotations

from decimal import Decimal


def validate_action_limits(amount_usd: Decimal, max_action_usd: Decimal) -> None:
    if amount_usd > max_action_usd:
        raise ValueError("action exceeds max_action_usd")
