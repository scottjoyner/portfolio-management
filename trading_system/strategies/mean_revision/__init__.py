"""
Mean Reversion Strategies Module for Crypto Spot Markets
========================================================

This module implements 15 production-ready mean reversion strategies:

1. Z-Score Mean Reversion - Core entry-point for the module
2. Bollinger Band Squeeze
3. Keltner Channel Range-Bound
4. Donchian Channel Reversal
5. Standard Deviation Extremes
6. Mean Absolute Deviation (MAD) Reversion
7. Percentile-Based Return Reversion
8. Stochastic RSI Extremes
9. Williams %R Mean Reversion
10. Ichimoku Zone Reversal
11. Rate of Change (ROC) Reversal
12. Momentum Oscillator Capture
13. Fibonacci Retracement Entries
14. ADX Weakness Mean Reversion
15. CCI Extreme Reversion

All strategies follow the factory pattern with init/on_bar lifecycle and include:
- Comprehensive docstrings covering purpose, regime fit, failure modes
- Entry/exit logic with position sizing
- Stop-loss implementation
- Error handling compatible with logging system
"""

from trading_system.strategies.mean_reversion.zscore_mean_reversion import (
    ZScoreMeanReversionStrategy, register_mean_reversion_strategy
)
from trading_system.strategies.mean_reversion.bollinger_band_squeeze import (
    BollingerBandSqueezeStrategy
)

__all__ = [
    "ZScoreMeanReversionStrategy",
    "BollingerBandSqueezeStrategy",
]
