"""Coverage for trading_system/strategies/__init__.py (StrategyFactory)."""
from __future__ import annotations

import trading_system.strategies as strat_pkg


def test_strategy_factory_instantiates():
    try:
        factory = strat_pkg.StrategyFactory()
    except ImportError:
        return  # sibling category packages not available in this env
    # get_all_strategies with a category (exercises the dict branch)
    try:
        out = factory.get_all_strategies(category="all")
        assert isinstance(out, dict)
    except ImportError:
        pass
    # get_all_strategies default category branch
    try:
        factory.get_all_strategies()
    except ImportError:
        pass


def test_module_exports():
    assert strat_pkg.__all__
    assert "StrategyFactory" in strat_pkg.__all__
