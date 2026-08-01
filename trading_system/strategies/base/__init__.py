"""Base strategy classes and interfaces."""

from trading_system.strategies.base.interfaces import (
    Strategy,
    StrategyConfig,
    StrategyMetadata,
    StrategySignal,
)
from trading_system.strategies.base.simple import (
    BaseSignalStrategy,
    SimpleSignalModel,
)
from trading_system.strategies.base.legacy import (
    BaseStrategy,
    OHLCVBar,
    compute_ema,
    compute_sma,
    compute_z_score,
)

__all__ = [
    "BaseSignalStrategy",
    "SimpleSignalModel",
    "Strategy",
    "StrategyConfig",
    "StrategyMetadata",
    "StrategySignal",
    "BaseStrategy",
    "OHLCVBar",
    "compute_sma",
    "compute_ema",
    "compute_z_score",
]
