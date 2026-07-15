"""Coverage tests for trading_system.strategies.registry.registry."""
from __future__ import annotations

import trading_system.strategies.registry.registry as reg


def test_load_strategies_returns_list():
    strats = reg.load_strategies()
    assert isinstance(strats, list)
    assert len(strats) >= 14  # 14 hand-written + advanced specs
    for s in strats:
        assert hasattr(s, "strategy_id")


def test_metadata_index():
    idx = reg.strategy_metadata_index()
    assert isinstance(idx, dict)
    assert len(idx) == len(reg.load_strategies())
