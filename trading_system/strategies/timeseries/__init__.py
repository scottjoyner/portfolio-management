"""Time-series / statistical novelty strategies (pure-Python).

Contains:
- HurstRegimeStrategy       (rescaled-range Hurst exponent regime)
- DFAAlphaRegimeStrategy    (detrended fluctuation analysis scaling exponent)
- SampleEntropyRegimeStrategy (approximate/sample entropy complexity regime)
"""
from trading_system.strategies.timeseries.hurst_regime import HurstRegimeStrategy
from trading_system.strategies.timeseries.dfa_alpha import DFAAlphaRegimeStrategy
from trading_system.strategies.timeseries.sample_entropy import (
    SampleEntropyRegimeStrategy,
)

__all__ = [
    "HurstRegimeStrategy",
    "DFAAlphaRegimeStrategy",
    "SampleEntropyRegimeStrategy",
]
