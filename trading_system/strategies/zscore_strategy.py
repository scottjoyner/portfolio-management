"""
Z-Score Mean Reversion Strategy Implementation (Corrected)

Statistical mean reversion strategy based on z-score bands.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import List, Tuple, Optional

try:
    from trading_system.strategies.base import OHLCVBar
except ImportError:
    @dataclass
    class OHLCVBar:
        timestamp: int
        open: Optional[float] = None
        high: Optional[float] = None
        low: Optional[float] = None
        close: Optional[float] = None
        volume: Optional[float] = None


@dataclass 
class StrategyConfig:
    """Strategy configuration parameters."""
    
    lookback_bars: int = 60
    z_buy_threshold: float = -2.5
    z_sell_threshold: float = 2.5
    trailing_stop_pct: float = 0.10


@dataclass 
class Position:
    """Track strategy position state."""
    
    entry_price: float
    quantity: int = 0
    max_unrealized_pnl_reached: float = 0.0
    
    def calculate_realized_pnl(self, current_price: float) -> float:
        return (current_price - self.entry_price) * self.quantity


class ZScoreMeanReversionStrategy:
    """
    Z-Score Mean Reversion Strategy.
    
    Example usage:
        strategy = ZScoreMeanReversionStrategy(
            lookback_bars=60,
            z_buy_threshold=-2.5,
            z_sell_threshold=2.5,
            trailing_stop_pct=0.10
        )
        
        close_prices = [45000, 45100, 45050, ...]
        strategy.setup(close_prices)
        
        signal, entry_price = strategy.on_bar(latest_close)
    """
    
    def __init__(self):
        self.config: StrategyConfig = StrategyConfig()
        self.close_prices: List[float] = []
        self.position: Optional[Position] = None
        self.max_unrealized_pnl_reached: float = 0.0
    
    def setup(self, ohlcv_data: Sequence[OHLCVBar]) -> None:
        """Initialize strategy with OHLCV data."""
        
        if not ohlcv_data or len(ohlcv_data) < self.config.lookback_bars:
            raise ValueError(
                f"Need at least {self.config.lookback_bars} bars for z-score calculation"
            )
        
        # Extract close prices
        self.close_prices = [bar.close for bar in ohlcv_data if bar.close is not None]
        self.position = None
    
    def on_bar(self, bar: OHLCVBar) -> Tuple[Optional[bool], Optional[float]]:
        """Generate buy/sell signal based on z-score."""
        
        if not self.close_prices:
            return None, None
        
        current_close = bar.close
        
        if current_close is None:
            return None, None
        
        z_score = self._calculate_z_score()
        
        # Buy when oversold
        if z_score <= self.config.z_buy_threshold:
            self.position = Position(entry_price=current_close)
            return True, current_close
        
        # Sell when overbought
        elif z_score >= self.config.z_sell_threshold and self.position:
            realized_pnl = self.position.calculate_realized_pnl(current_close)
            self.position.quantity = 0
            return False, current_close
        
        # Check trailing stop
        elif self.position:
            unrealized_pnl = self.position.calculate_realized_pnl(current_close)
            
            self.max_unrealized_pnl_reached = max(
                self.max_unrealized_pnl_reached,
                unrealized_pnl
            )
            
            if unrealized_pnl < (self.max_unrealized_pnl_reached * (1 - self.config.trailing_stop_pct)):
                self.position.quantity = 0
                return False, current_close
        
        return None, None
    
    def _calculate_z_score(self) -> float:
        """Calculate z-score for latest price."""
        
        if len(self.close_prices) < 10:
            return 0.0
        
        prices = self.close_prices[-self.config.lookback_bars:]
        mean = sum(prices) / len(prices)
        
        variance = sum((p - mean) ** 2 for p in prices) / (len(prices) - 1)
        std = math.sqrt(variance)
        
        if std == 0:
            return 0.0
        
        latest_price = self.close_prices[-1]
        z_score = (latest_price - mean) / std
        
        return z_score
