"""Generic driver for the remaining PART-1 target modules.

Instantiates each strategy class, feeds synthetic scenarios (rising/falling/
volatile/flat) and exercises position/exit helpers so entry+exit branches are
touched.  Per-module measurement uses --source so coverage is isolated.
"""
import importlib
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import pytest

from strat_helpers import discover_strategy_classes, drive_class, bars

MODULES = [
    "trading_system.strategies.trend.donchian_channel",
    "trading_system.strategies.trend.keltner_channel",
    "trading_system.strategies.trend.momentum_breakout",
    "trading_system.strategies.trend.volume_breakout",
    "trading_system.strategies.trend.vwap_momentum",
    "trading_system.strategies.mean_reversion.bollinger_mean_revert",
    "trading_system.strategies.mean_reversion.keltner_channel_range_bound",
    "trading_system.strategies.mean_reversion.rsi_mean_revert",
    "trading_system.strategies.mean_reversion.williams_r_mean_revert",
    "trading_system.strategies.mean_reversion.zscore_mean_reversion",
    "trading_system.strategies.mean_reversion.zscore_statistical_arb",
    "trading_system.strategies.market_making.order_book_imbalance",
    "trading_system.strategies.volatility.atr_breakout",
    "trading_system.strategies.volatility.volatility_targeting",
    "trading_system.strategies.grid_trading.bot",
    "trading_system.strategies.trend_following.bot",
]


@pytest.mark.parametrize("modname", MODULES)
def test_drive(modname):
    mod = importlib.import_module(modname)
    for cls in discover_strategy_classes(mod):
        res = drive_class(cls)
        assert res["instantiated"]
