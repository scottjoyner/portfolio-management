from __future__ import annotations

from decimal import Decimal


def recovery_decision(immediate_reposition_edge: Decimal, hold_edge: Decimal, withdraw_edge: Decimal) -> str:
    options = {
        "reposition": immediate_reposition_edge,
        "hold": hold_edge,
        "withdraw": withdraw_edge,
    }
    return max(options, key=lambda option: options[option])
