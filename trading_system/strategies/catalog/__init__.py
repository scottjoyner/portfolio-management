"""
Trading Strategy Catalog - Parallel Implementation Registry

This module provides centralized registration and discovery of all trading strategies.
Subagents implement strategies in parallel across categories:
- Trend-following (50+ implementations)
- Mean-reversion (40+ implementations)
- Arbitrage (30+ implementations)
- Volatility (20+ implementations)
"""

from __future__ import annotations

import importlib.util
import os as _os

from .advanced import (
    CATALOG_100,
    GenericSpecStrategy,
    StrategySpec,
    advanced_specs,
)
from .config_schema import (
    ALLOWED_RISK_TIERS,
    ALLOWED_SIZING_MODELS,
    TIER_MAX_CAPITAL_FRACTION,
    StrategyConfig,
    StrategyRuntimeFlags,
)

# ``StrategyRegistry`` is defined in the top-level ``strategies/registry.py``
# module, which is shadowed by the ``registry/`` package directory and thus is
# not importable by its normal name. Load it by file path so the public catalog
# API stays importable.
def _load_strategy_registry():
    path = _os.path.join(_os.path.dirname(_os.path.dirname(__file__)), "registry.py")
    spec = importlib.util.spec_from_file_location("_catalog_strategy_registry_shim", path)
    module = importlib.util.module_from_spec(spec)
    # Register in sys.modules first so dataclasses can resolve string
    # annotations (forward references) at class-definition time.
    import sys as _sys

    _sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.StrategyRegistry


StrategyRegistry = _load_strategy_registry()


def register_strategy(strategy, key=None):
    """Register a strategy instance on a fresh ``StrategyRegistry``."""
    return StrategyRegistry().register(strategy, key)


def load_registered_strategies():
    """Return the metadata index for all registered strategies.

    Falls back to an empty dict if the strategy registry package cannot be
    imported (e.g. optional strategy dependencies are missing).
    """
    try:
        from trading_system.strategies.registry.registry import strategy_metadata_index

        return strategy_metadata_index()
    except Exception:
        return {}


__all__ = [
    "StrategyRegistry",
    "register_strategy",
    "load_registered_strategies",
    "GenericSpecStrategy",
    "advanced_specs",
    "StrategySpec",
    "CATALOG_100",
    "StrategyConfig",
    "StrategyRuntimeFlags",
    "ALLOWED_RISK_TIERS",
    "ALLOWED_SIZING_MODELS",
    "TIER_MAX_CAPITAL_FRACTION",
]

# Initialize global registry
_global_registry = None


def get_registry():
    """Get or initialize the global strategy registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = StrategyRegistry()
    return _global_registry
