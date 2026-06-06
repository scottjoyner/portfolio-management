"""
Oracles Library - Multi-timeframe Technical Indicators and Signal Generators
================================================================================

This module provides a comprehensive library of technical indicators and oracle signals:
- RSI family (RSI, Stochastic RSI, Williams %R)
- MACD family (MACD, MACD histograms, signal crossovers)
- Bollinger Bands (squeeze detection, band penetration)
- Keltner Channels (breakout, volatility expansion)
- Ichimoku Cloud (support/resistance, trend classification)
- Volume indicators (OBV, VWP, volume breakout)
- Mean reversion signals (Z-score, ATR-based thresholds)

All indicators are optimized for backtesting and produce standardized signal outputs.
"""

__all__ = [
    'RSIOracle', 
    'StochasticRSIOracle', 
    'WilliamsROracle', 
    'CCIOracle', 
    'MACDOracle',
    'BollingerBandOracle',
    'KeltnerChannelOracle',
    'IchimokuCloudOracle',
    'DonchianOracle',
    'ParabolicSAROracle',
    'ChandelierExitOracle',
    'VolumeBreakoutOracle',
    'VPVOracl',
    'OrderBookImbalanceOracle'
]

# Import all oracle classes
from trading_system.oracles.rsi_family import (
    RSIOracle, 
    StochasticRSIOracle, 
    WilliamsROracle
)
from trading_system.oracci.macd_family import MACDOracle
from trading_system.oracles.bollinger_keltner_family import (
    BollingerBandOracle, 
    KeltnerChannelOracle
)
from trading_system.oracles.ichimoku_cloud import IchimokuCloudOracle
from trading_system.oracles.donchian_donch import DonchianOracle
from trading_system.oracci.parabolic_sar import ParabolicSAROracle
from trading_system.oracles.chandelier_exit import ChandelierExitOracle
from trading_system.oracles.volume_indicators import VolumeBreakoutOracle, VPVOracl
from trading_system.oracles.microstructure import OrderBookImbalanceOracle
