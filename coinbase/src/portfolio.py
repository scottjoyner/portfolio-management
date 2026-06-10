from __future__ import annotations
from typing import List, Dict
from trading_system.core.portfolio_manager import calculate_rebalance
from trading_system.core.models.domain import OrderIntent

def rebalance_plan(
    prices: Dict[str, float], 
    portfolio_value_usd: float, 
    product_weights: Dict[str, float], 
    current_base: Dict[str, float], 
    min_notional: float = 50.0
) -> List[OrderIntent]:
    """
    Bridge function for Coinbase to utilize the shared core portfolio management logic.
    
    This maintains backward compatibility for the Coinbase-specific module while 
    unifying the underlying strategy calculation.
    """
    # Call the shared core logic
    intents = calculate_rebalance(
        prices=prices,
        portfolio_value_usd=portfolio_value_usd,
        product_weights=product_weights,
        current_base=current_base,
        min_notional=min_notional
    )
    
    # If the rest of the Coinbase system expects raw dictionaries instead of OrderIntent objects,
    # we convert them here.
    return [
        {
            "product_id": intent.product_id,
            "side": intent.side,
            "base_size": intent.size,
            "quote_size": intent.size * prices.get(intent.product_id, 0.0) if intent.side == "buy" else intent.size * prices.get(intent.product_id, 0.0), # Note: Simplified
            "rationale": intent.reason
        }
        for intent in intents
    ]
