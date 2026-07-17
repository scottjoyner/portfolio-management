"""Quality-C strategy package: 8 classic, edge-positive OHLCV strategies.

Each module exposes a single BaseSignalStrategy subclass. Designed to clear the
runtime BacktestVerdict gate on REAL data (win_rate>=50, sharpe>0.5,
profit_factor>1.20, total_return_pct>-10).
"""
from __future__ import annotations

from trading_system.strategies.quality_c.adx_weak_range_fade import (
    AdxWeakRangeFadeStrategy,
)
from trading_system.strategies.quality_c.atr_channel_reversion import (
    AtrChannelReversionStrategy,
)
from trading_system.strategies.quality_c.bollinger_double_touch import (
    BollingerDoubleTouchStrategy,
)
from trading_system.strategies.quality_c.connors_rsi2 import ConnorsRsi2Strategy
from trading_system.strategies.quality_c.keltner_reversion import (
    KeltnerReversionStrategy,
)
from trading_system.strategies.quality_c.price_channel_breakout import (
    PriceChannelBreakoutPullbackStrategy,
)
from trading_system.strategies.quality_c.stochastic_extreme import (
    StochasticExtremeReversionStrategy,
)
from trading_system.strategies.quality_c.volume_zscore import (
    VolumeZscoreReversionStrategy,
)

__all__ = [
    "ConnorsRsi2Strategy",
    "BollingerDoubleTouchStrategy",
    "KeltnerReversionStrategy",
    "AtrChannelReversionStrategy",
    "StochasticExtremeReversionStrategy",
    "VolumeZscoreReversionStrategy",
    "PriceChannelBreakoutPullbackStrategy",
    "AdxWeakRangeFadeStrategy",
]

QUALITY_C_STRATEGIES = [
    ConnorsRsi2Strategy(),
    BollingerDoubleTouchStrategy(),
    KeltnerReversionStrategy(),
    AtrChannelReversionStrategy(),
    StochasticExtremeReversionStrategy(),
    VolumeZscoreReversionStrategy(),
    PriceChannelBreakoutPullbackStrategy(),
    AdxWeakRangeFadeStrategy(),
]
