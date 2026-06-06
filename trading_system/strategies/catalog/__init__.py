"""
Trading Strategy Catalog - Parallel Implementation Registry

This module provides centralized registration and discovery of all trading strategies.
Subagents implement strategies in parallel across categories:
- Trend-following (50+ implementations)
- Mean-reversion (40+ implementations)  
- Arbitrage (30+ implementations)
- Volatility (20+ implementations)
"""

from .registry import StrategyRegistry, register_strategy
from .catalog import load_registered_strategies

__all__ = ['StrategyRegistry', 'register_strategy', 'load_registered_strategies']

# Initialize global registry
_global_registry = None

def get_registry():
    """Get or initialize the global strategy registry."""
    global _global_registry
    if _global_registry is None:
        from .registry import StrategyRegistry
        _global_registry = StrategyRegistry()
    return _global_registry
