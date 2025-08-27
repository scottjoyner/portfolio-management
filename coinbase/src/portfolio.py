from __future__ import annotations
import math, typing as t

def dollars_to_base(usd_notional: float, price: float) -> float:
    if price <= 0: return 0.0
    return usd_notional / price

def rebalance_plan(
    prices: dict[str, float],
    portfolio_value_usd: float,
    product_weights: dict[str, float],
    current_base: dict[str, float],
    min_notional: float = 50.0,
) -> list[dict]:
    """
    Convert desired weights to buy/sell base sizes given current holdings and prices.
    Returns list of order intents: {side, product_id, base_size, quote_size}
    """
    intents = []
    for product, w in product_weights.items():
        px = prices.get(product)
        if px is None: continue
        target_usd = max(0.0, w) * portfolio_value_usd
        target_base = target_usd / px
        cur = current_base.get(product, 0.0)
        diff_base = target_base - cur
        diff_usd = diff_base * px
        if abs(diff_usd) < min_notional:
            continue
        intents.append({
            "product_id": product,
            "side": "buy" if diff_base > 0 else "sell",
            "base_size": abs(diff_base),
            "quote_size": abs(diff_usd)
        })
    return intents
