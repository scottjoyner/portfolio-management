from __future__ import annotations
from typing import List, Dict
from trading_system.core.models.domain import OrderIntent, CapitalBucketType, RiskMode

def calculate_rebalance(
    prices: Dict[str, float],
    portfolio_value_usd: float,
    product_weights: Dict[str, float],
    current_base: Dict[str, float],
    min_notional: float = 50.0
) -> List[OrderIntent]:
    """
    Calculates the necessary trades to reach target portfolio weights.
    Returns OrderIntent objects for the execution pipeline.
    """
    intents = []
    for product, w in product_weights.items():
        px = prices.get(product)
        if px is None or px <= 0:
            continue

        target_usd = max(0.0, w) * portfolio_value_usd
        target_base = target_usd / px
        cur_base = current_base.get(product, 0.0)

        diff_base = target_base - cur_base
        diff_usd = diff_base * px

        if abs(diff_usd) < min_notional:
            continue

        intents.append(OrderIntent(
            strategy_id="rebalance_auto",
            product_id=product,
            side="buy" if diff_base > 0 else "sell",
            order_type="market",
            size=abs(diff_base),
            bucket=CapitalBucketType.ACTIVE_TRADING,
            risk_mode=RiskMode.NORMAL,
            rationale=f"Automatic rebalance to weight {w}",
        ))
    return intents
