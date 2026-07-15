"""
Oracles Library - Multi-timeframe Technical Indicators and Signal Generators
===============================================================================

This module provides a comprehensive library of technical indicators and oracle signals:
- RSI family (RSI, Stochastic RSI, Williams %R)

All indicators are optimized for backtesting and produce standardized signal outputs.
"""

__all__ = [
    "RSIOracle",
    "StochasticRSIOracle",
    "WilliamsROracle",
]

# Import all oracle classes
from trading_system.oracles.rsi_family import (
    RSIOracle,
    StochasticRSIOracle,
    WilliamsROracle,
)
