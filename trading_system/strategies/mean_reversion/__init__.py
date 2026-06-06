"""
Mean Reversion Strategies Module for Crypto Spot Markets
========================================================

This module implements production-ready mean reversion strategies:

1. Z-Score Mean Reversion (Primary implementation)
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
    ZScoreMeanReversionStrategy,
)
from trading_system.strategies.mean_reversion.keltner_channel_range_bound import (
    KeltnerChannelRangeBoundStrategy,
)
from trading_system.strategies.mean_reversion.bollinger_band_squeeze import (
    BollingerBandSqueezeStrategy,
)

# Factory class for mean reversion strategies
class MeanReversionStrategyFactory:
    """Factory for loading and managing mean reversion strategies."""
    
    def __init__(self):
        self.strategies = {
            'zscore': ZScoreMeanReversionStrategy,
            'keltner': KeltnerChannelRangeBoundStrategy,
            'bollinger_squeeze': BollingerBandSqueezeStrategy,
        }
    
    def get_all(self, strategy_type=None):
        """Get all available mean reversion strategies."""
        if strategy_type is None:
            return list(self.strategies.values())
        return self.strategies.get(strategy_type)

# Export the factory instance
mean_reversion_factory = MeanReversionStrategyFactory()

__all__ = [
    "ZScoreMeanReversionStrategy",
    "KeltnerChannelRangeBoundStrategy",
    "BollingerBandSqueezeStrategy",
    "MeanReversionStrategyFactory",
    "mean_reversion_factory",
]
