"""
Strategy Base Implementation

Provides reusable building blocks for common trading strategies.
Implementations are registered with StrategyManager/Registry.

Supported patterns:
- Mean reversion (z-score, Bollinger bands)
- Trend following (EMA crossovers, moving averages)
- Breakout (pivot points, volume breakout)
- Seasonal (calendar-based rules)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class OHLCVBar:
    """OHLCV bar with timestamp."""
    
    timestamp: int  # Unix timestamp or exchange-specific ID
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    
    def __post_init__(self) -> None:
        if self.open is not None:
            assert (self.high or 0) >= (self.low or 0), "high must be >= low"


def compute_sma(data: list[OHLCVBar], period: int) -> list[float]:
    """Compute Simple Moving Average."""
    
    if not data or len(data) < period:
        return []
    
    sma = []
    for i in range(len(data)):
        start_idx = max(0, i - period + 1)
        window = data[start_idx:i + 1]
        prices = [b.close for b in window if b.close is not None]
        
        if not prices:
            sma.append(None)
        else:
            sma.append(sum(prices) / len(prices))
    
    return sma


def compute_ema(data: list[OHLCVBar], period: int) -> list[float]:
    """Compute Exponential Moving Average."""
    
    if not data or len(data) < period:
        return []
    
    ema = []
    multiplier = 2 / (period + 1)
    
    # Start with SMA for initialization
    window = data[:min(period, len(data))]
    first_sma = sum(b.close for b in window if b.close is not None) / max(len(window), 1)
    ema.append(first_sma)
    
    for bar in data[period:]:
        last_ema = ema[-1]
        price = bar.close or last_ema
        
        ema.append((price - last_ema) * multiplier + last_ema)
    
    return ema


def compute_z_score(data: list[float]) -> list[float]:
    """Compute z-scores from price data."""
    
    if not data or len(data) < 2:
        return []
    
    mean = sum(data) / len(data)
    variance = sum((x - mean) ** 2 for x in data) / len(data)
    std = math.sqrt(variance)
    
    if std == 0:
        return [0.0] * len(data)
    
    return [(x - mean) / std for x in data]


class BaseStrategy:
    """
    Abstract base class for all trading strategies.
    
    Strategies must implement:
    - setup(ohlcv_data): Initialize with historical data
    - on_bar(bar): Return (signal, entry_price) or (None, None)
    
    Example:
        class MyTrendStrategy(BaseStrategy):
            def setup(self, ohlvc_data):
                self.sma = compute_sma(ohlvc_data, 20)
            
            def on_bar(self, bar):
                if len(bar.close) > 0 and bar.close[-1] > self.sma[-1]:
                    return True, bar.close[-1]
                return None, None
    
    Then register with:
        from strategies.registry import StrategyManager
        
        manager = StrategyManager(session)
        manager.register(MyTrendStrategy(), key="my_trend_strategy")
    """
    
    def setup(self, ohlcv_data: list[OHLCVBar]) -> None:
        """
        Initialize strategy with OHLCV historical data.
        
        Args:
            ohlcv_data: List of OHLCV bars (chronological order assumed)
        """
        pass
    
    def on_bar(self, bar: OHLCVBar) -> tuple[Optional[bool], Optional[float]]:
        """
        Generate signal based on latest bar.
        
        Args:
            bar: Latest OHLCV bar
        
        Returns:
            Tuple of (signal, entry_price) or (None, None)
            - signal: True = buy, False = sell, None = no signal
            - entry_price: Suggested entry price if signal triggered
        
        Example:
            # Mean reversion strategy
            if len(self.z_scores) < 20:
                return None, None
            
            current_z = self.z_scores[-1]
            
            if current_z < -2.0:  # Buy at oversold
                return True, bar.close
            elif current_z > 2.0:  # Sell at overbought
                return False, bar.close
            
            return None, None
        """
        
        return None, None
    
    def is_position_open(self) -> bool:
        """Check if strategy has active position."""
        
        return getattr(self, '_position_size', 0) != 0
    
    def close_position(self) -> None:
        """Close existing position."""
        
        self._position_size = 0
