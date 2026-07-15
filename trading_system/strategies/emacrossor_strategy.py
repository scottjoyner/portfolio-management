"""
EMA Crossover Strategy Implementation (Standalone Version)

Trend-following strategy that buys on fast-slow EMA crossover and sells on crossover back.
This version is standalone and can be loaded via the strategy registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class StrategyConfig:
    """Strategy configuration parameters."""
    
    fast_period: int = 9
    slow_period: int = 21
    stop_loss_pct: float = 0.05


@dataclass
class Position:
    """Track strategy position state."""
    
    entry_price: float
    quantity: int = 0
    unrealized_pnl_cents: float = 0.0
    
    def mark_close(self, last_price: float) -> float:
        """Mark position as closed and return realized PnL."""
        pnl = (last_price - self.entry_price) * self.quantity
        self.quantity = 0
        self.unrealized_pnl_cents = 0.0
        return pnl


def compute_ema(data: list[float], period: int) -> list[float]:
    """Compute Exponential Moving Average from price data."""
    
    if len(data) < period:
        return []
    
    # Use simple SMA for EMA initialization (warmup)
    first_n = min(period, len(data))
    ema = sum(data[:first_n]) / first_n
    
    multiplier = 2.0 / (period + 1)
    
    for i in range(first_n, len(data)):
        last_ema = ema
        price = data[i]
        ema = (price - last_ema) * multiplier + last_ema
    
    return [ema]


class EMACrossoverStrategy:
    """
    Exponential Moving Average Crossover Strategy.
    
    Implements the classic "Golden Cross / Death Cross" approach:
    - Buys when fast-EMA crosses above slow-EMA (golden cross)
    - Sells when fast-EMA crosses below slow-EMA (death cross)
    - Applies stop-loss for risk management
    
    Usage Example:
        strategy = EMACrossoverStrategy(
            fast_period=9,      # Fast EMA period
            slow_period=21,     # Slow EMA period  
            stop_loss_pct=0.05  # 5% stop-loss
        )
        
        # Initialize with data (list of close prices)
        close_prices = [45000, 45100, 45050, ...]  # Your OHLCV closes
        strategy.setup(close_prices)
        
        # Generate signal
        signal, entry_price = strategy.on_bar(last_close_price)
    """
    
    def __init__(self):
        self.config: StrategyConfig = StrategyConfig()
        self.ema_fast: list[float] = []
        self.ema_slow: list[float] = []
        self.last_crossed_above: bool = False
    
    def setup(self, close_prices: list[float]) -> None:
        """Initialize strategy with OHLCV close prices."""
        
        if not close_prices or len(close_prices) < 30:
            raise ValueError("Need at least 30 bars for EMA crossover")
        
        self.ema_fast = compute_ema(close_prices, self.config.fast_period)
        self.ema_slow = compute_ema(close_prices, self.config.slow_period)
    
    def on_bar(self, close_price: float) -> tuple[Optional[bool], Optional[float]]:
        """Generate buy/sell signal based on latest bar."""
        
        if not self.ema_fast or not self.ema_slow:
            return None, None
        
        current_crossed_above = self.ema_fast[-1] > self.ema_slow[-1]
        
        # Detect crossover (signal transition)
        if self.last_crossed_above != current_crossed_above:
            signal = current_crossed_above
            self.last_crossed_above = current_crossed_above
            
            return signal, close_price
        
        return None, None
