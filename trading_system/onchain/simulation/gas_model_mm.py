from __future__ import annotations

from decimal import Decimal


def estimate_action_gas(action_type: str, hops: int = 1) -> int:
    base = {
        "add_liquidity": 220_000,
        "remove_liquidity": 210_000,
        "collect_fees": 120_000,
        "rebalance_position": 330_000,
        "deploy_and_hedge": 420_000,
    }.get(action_type, 180_000)
    return base + hops * 35_000


def gas_cost_usd(gas_units: int, gas_price_gwei: Decimal, native_usd: Decimal) -> Decimal:
    return Decimal(gas_units) * gas_price_gwei * Decimal("1e-9") * native_usd
