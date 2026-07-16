import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from trading_system.backtest_new_strategies import (  # noqa: E402
    _gen_synthetic,
    _load_target_strategies,
    run_walkforward,
)

EXPECTED_KEYS = {
    "strategy_id",
    "total_trades",
    "win_rate",
    "profit_factor",
    "total_return_pct",
    "max_drawdown_pct",
    "passed",
}


@pytest.fixture
def series():
    return _gen_synthetic(200, seed=7)


def test_run_walkforward_all_strategies(series):
    closes, highs, lows, volumes = series
    strategies = _load_target_strategies()
    assert len(strategies) == 12
    for s in strategies:
        metrics = run_walkforward(s, closes, highs, lows, volumes, product_id="BTC-USD")
        assert isinstance(metrics, dict)
        assert EXPECTED_KEYS.issubset(metrics.keys())
        assert isinstance(metrics["total_trades"], int)
        assert isinstance(metrics["win_rate"], float)
        assert isinstance(metrics["passed"], bool)


def test_run_walkforward_expected_keys(series):
    closes, highs, lows, volumes = series
    strategies = _load_target_strategies()
    metrics = run_walkforward(strategies[0], closes, highs, lows, volumes)
    assert EXPECTED_KEYS == set(metrics.keys())
