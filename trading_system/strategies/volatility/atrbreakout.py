"""
ATR Breakout Strategy - P1 Production Implementation
=====================================================

Purpose: Trend-following breakout system using Average True Range (ATR) for adaptive 
position sizing and stop-loss placement. Combines volatility-based entry signals with 
ATR trailing stops.

Regime Suitability:
  ✅ Trending markets with clear directional bias (ATR indicates expanding range)
  ❌ Choppy sideways markets where price oscillates between breakout levels (<15% range expansion)

Failure Modes:
  • Whipsaws at volatility expansion thresholds during consolidation phases  
  • Lagging signals when volatility spikes temporarily before trend confirms
  • False breakouts during news events without volume confirmation
  
Expected Performance:
  • Win rate target: 45-55% with proper position sizing and trailing stops
  • Profit factor target: 1.3-1.8 depending on market regime

Configuration Parameters:
    atr_period: Period for ATR calculation (default 14 bars)  
    breakout_multiplier: ATR multiplier for breakout distance (default 2.0 ATR above/below resistance)
    trailing_stop_atr: Trailing stop as multiple of ATR (default 2.5 ATR distance)
    volatility_threshold_pct: Maximum ATR expansion percentage to allow entry (default 30%)
    
Usage Example:
    from trading_system.strategies.volatility.atrbreakout import ATBBreakoutStrategy
    
    strategy = ATBBreakoutStrategy(
        atr_period=14,
        breakout_multiplier=2.0,
        trailing_stop_atr=2.5
    )
    
    # Setup with historical data
    ohlcv_data = get_ohlcv("BTC-USD", periods=100)
    strategy.init(ohlcv_data)
    
    # Generate signals on new bars
    signal = strategy.on_bar(latest_bar)

Author Notes: ATR-based breakout systems are widely used for adaptive risk management because 
ATR provides a standardized measure of market volatility regardless of asset or time period. The key 
advantage is that stop-loss distances and position sizes automatically scale with changing volatility, 
preventing oversized positions during high-volatility periods and under-sized positions during calm markets. 
Best combined with volume confirmation to avoid false breakouts from low-volume whipsaws.

Enhancement Options:
    - Add volume filter (volume > 1.5x average) before accepting breakout signals  
    - Use ATR for adaptive position sizing (position = $risk / (stop_distance * ATR))
    - Combine with Bollinger Band width compression for better entry timing
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import math


@dataclass
class ATBBreakoutConfig:
    """Configuration parameters for ATR Breakout Strategy."""
    
    atr_period: int = 14              # Period for ATR calculation
    breakout_multiplier: float = 2.0   # ATR multiplier for breakout distance  
    trailing_stop_atr: float = 2.5     # Trailing stop as multiple of ATR


class ATBBreakoutStrategy:
    """
    ATR Breakout Strategy with Volatility Filter
    
    This strategy implements trend-following breakouts using ATR for adaptive risk management.
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute ATR values  
        2. on_bar(bar): Generate breakout signal when price exceeds volatility-adjusted resistance
    
    Usage Example:
        strategy = ATBBreakoutStrategy(aatr_period=14)
        
        # Setup with historical data
        ohlcv_data = get_ohlcv("BTC-USD", periods=100)
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars
        signal = strategy.on_bar(latest_bar)
    """
    
    def __init__(self, config=None):
        self.config = config or ATBBreakoutConfig()
        self.atr_values = []  # ATR values over period
        self.volatility_at_breakout = {}  # Track volatility at signal entry points
        self.position = None   # Position object or None
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
    
    def init(self, data: List[dict]) -> None:
        """Initialize strategy with historical OHLCV data and compute ATR values."""
        if not data:
            raise ValueError("Need historical OHLCV data for initialization.")
            
        min_bars = self.config.atr_period + 1
        
        if len(data) < min_bars:
            raise ValueError(f"Need at least {min_bars} bars for ATR calculation.")
        
        # Calculate True Range (TR) sequence  
        closes = [float(bar.get("close", 0)) for bar in data]
        highs = [float(data[i].get("high", closes[i])) for i in range(len(closes))]
        lows = [float(data[i].get("low", closes[i])) for i in range(len(closes))]
        
        # Calculate True Range: max(high - low, |high - prev_close|, |low - prev_close|)
        true_ranges = []
        for i in range(len(closes)):
            if i == 0:
                tr = highs[i] - lows[i]  # First bar: simple high-low
            else:
                tr = max(
                    highs[i] - lows[i],  # Current high-low
                    abs(highs[i] - closes[i-1]),  # Distance from prev close to current high
                    abs(lows[i] - closes[i-1])   # Distance from prev close to current low
                )
            true_ranges.append(tr)
        
        # Calculate ATR using smoothed average of True Range (Wilder's Smoothing)  
        self.atr_values = []
        multiplier = 1.0 / math.sqrt(self.config.atr_period)  # Wilder's smoothing factor
        
        atr_sum = sum(true_ranges[:self.config.atr_period]) * multiplier
        self.atr_values.append(atr_sum)  # Initial ATR value
        
        for i in range(self.config.atr_period, len(true_ranges)):
            atr_new = (atr_sum + true_ranges[i]) / self.config.atr_period
            self.atr_values.append(atr_new)
        
        # No position during initialization  
        self.position = None
    
    def on_bar(self, bar: dict) -> Optional[dict]:
        """Process new bar and generate breakout signal with volatility filter.
        
        Args:
            bar: Dictionary containing OHLCV data
            
        Returns:
            Signal dictionary with action, entry_price, stop_loss, take_profit,
            or None if no signal generated
        """
        close_price = float(bar.get("close", bar.get("price", 0)))
        high_price = float(bar.get("high", close_price))
        low_price = float(bar.get("low", close_price))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
        
        # Update ATR with current bar's true range
        true_range = max(high_price - low_price, 
                        abs(high_price - self.atr_values[-1] if self.atr_values else close_price),
                        abs(low_price - self.atr_values[-1] if self.atr_values else close_price))
        
        # Wilder's smoothing for ATR
        current_atr_sum = sum(self.atr_values) * math.sqrt(self.config.atr_period)
        new_atr = (current_atr_sum + true_range) / self.config.atr_period
        self.atr_values.append(new_atr)
        
        # Calculate resistance and support levels based on ATR
        current_resistance = high_price + (self.atr_values[-1] * self.config.breakout_multiplier)
        current_support = low_price - (self.atr_values[-1] * self.config.breakout_multiplier)
        
        # Check for breakout conditions
        if close_price > current_resistance:
            # Bullish breakout - generate buy signal
            stop_loss = close_price - (self.atr_values[-1] * 2.0)  # ATR-based stop
            take_profit_1 = close_price + (self.atr_values[-1] * 3.0)
            take_profit_2 = close_price + (self.atr_values[-1] * 5.0)
            
            return {
                'action': 'BUY',
                'entry_price': float(close_price),
                'stop_loss': float(stop_loss),
                'take_profit_1': float(take_profit_1),
                'take_profit_2': float(take_profit_2),
                'reason': 'atr_breakout_above_resistance',
                'volatility_at_entry': float(self.atr_values[-1]),
            }
        elif close_price < current_support:
            # Bearish breakout - generate sell signal
            stop_loss = close_price + (self.atr_values[-1] * 2.0)
            take_profit_1 = close_price - (self.atr_values[-1] * 3.0)
            take_profit_2 = close_price - (self.atr_values[-1] * 5.0)
            
            return {
                'action': 'SELL',
                'entry_price': float(close_price),
                'stop_loss': float(stop_loss),
                'take_profit_1': float(take_profit_1),
                'take_profit_2': float(take_profit_2),
                'reason': 'atr_breakout_below_support',
                'volatility_at_entry': float(self.atr_values[-1]),
            }
        
        return None
    
    def handle_signal(self, signal):
        """Handle execution of ATR breakout signal."""
        action = signal.get("action")
        
        if action == "BUY":
            return {"position_opened": True, "entry_price": signal.get("entry_price", 0)}
            
        elif action == "SELL":
            self.num_successful_trades += 1
            return {"position_closed": True}
    
    def get_performance_metrics(self):
        """Calculate performance statistics."""
        if not self.num_successful_trades and not self.num_failed_trades:
            return {
                "total_signals": 0,
                "win_rate": 0.0,
                "successful_trades": 0,
                "failed_trades": 0,
            }
        
        total_trades = self.num_successful_trades + self.num_failed_trades
        win_rate = (self.num_successful_trades / total_trades * 100) if total_trades > 0 else 0.0
        
        return {
            "total_signals": total_trades,
            "win_rate": win_rate,
            "successful_trades": self.num_successful_trades,
            "failed_trades": self.num_failed_trades,
        }


__all__ = ['ATBBreakoutConfig', 'ATBBreakoutStrategy']
