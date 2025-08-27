from __future__ import annotations
def rebalance_plan(prices: dict[str, float], portfolio_value_usd: float, product_weights: dict[str, float], current_base: dict[str, float], min_notional: float = 50.0) -> list[dict]:
    intents = []
    for product, w in product_weights.items():
        px = prices.get(product)
        if px is None: continue
        target_usd = max(0.0, w) * portfolio_value_usd
        target_base = target_usd / px if px>0 else 0.0
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
