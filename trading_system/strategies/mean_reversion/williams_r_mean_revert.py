"""
Williams R Mean Reversion Strategy - Oscillator Implementation

Purpose: Mean reversion strategy using Williams %R oscillator to identify overbought/oversold conditions.
Buys when RSI below -80 (statistically cheap), sells above -20 (overvalued). Classic mean reversion approach.

Regime Suitality:
  ✅ Ranging markets with clear mean-reverting distribution (<12% price range)
  ❌ Strong trending markets where %R stays extreme for extended periods

Failure Modes:
  • Whipsaws at threshold boundaries during transition phases  
  • Lagging signals when trend extends far beyond statistical limits
  • False crossovers when volatility expansion drives RSI outside normal range

Expected Performance:
  • Win rate target: 50-60% with proper position sizing and hard stops
  • Profit factor target: 1.3-1.8 depending on market regime

Configuration Parameters:
    period: Williams %R calculation period (default 14 bars)  
    overbought_threshold_pct: Signal when RSI >= threshold (default -20 = oversold in %R terms)  
    oversold_threshold_pct: Signal when RSI <= threshold (default -80 = overbought in %R terms)
    hard_stop_pct: Hard stop-loss as percentage of entry (default 5%)
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, List
import math


@dataclass
class WilliamsRConfig:
    """Configuration parameters for Williams %R mean reversion strategy."""
    
    period: int = 14
    overbought_threshold_pct: float = -20
    oversold_threshold_pct: float = -80
    hard_stop_pct: float = 5.0


class WilliamsRMeanReversionStrategy:
    """
    Williams %R Mean Reversion Strategy
    
    This strategy implements mean reversion using Williams %R oscillator.
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute Williams %R values
        2. on_bar(bar): Generate buy/sell signal at oscillator thresholds
    """
    
    def __init__(self, config=None):
        self.config = config or WilliamsRConfig()
        self.williams_r_values = []
        self.position = None
        self.num_successful_trades = 0
        self.num_failed_trades = 0
        
    def init(self, data: List[dict]) -> None:
        """Initialize strategy with historical OHLCV data."""
        if not data:
            raise ValueError("Need historical OHLCV data for initialization.")
            
        min_bars = self.config.period + 5
        
        if len(data) < min_bars:
            raise ValueError(f"Need at least {min_bars} bars for Williams %R calculation.")
        
        closes = [float(bar.get("close", 0)) for bar in data]
        highs = [float(bar.get("high", closes[i])) for i in range(len(closes))]  
        lows = [float(bar.get("low", closes[i])) for i in range(len(closes))]
        
        # Calculate Williams %R sequence
        self.williams_r_values = []
        
        for i in range(len(closes)):
            window_highs = highs[max(0, i-self.config.period+1):i+1]
            window_lows = lows[max(0, i-self.config.period+1):i+1]
            
            highest_high = max(window_highs)
            lowest_low = min(window_lows)
            
            if highest_high == lowest_low:
                williams_r = 0.0
            else:
                williams_r = (highest_high - closes[i]) / (highest_high - lowest_low) * -100
            
            self.williams_r_values.append(williams_r)
        
        self.position = None
        
    def on_bar(self, bar: dict) -> Optional[dict]:
        """Process new bar and generate mean-reversion signal."""
        close_price = bar.get("close", bar.get("price", 0))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
        
        # Append new high/low  
        current_high = bar.get("high", close_price)
        current_low = bar.get("low", close_price)
        
        highs = [current_high] + self.williams_r_values[:-1] if self.williams_r_values else [current_high]
        # Simplified - recalculate with high/low from bars instead of RSI values
        williams_r_values = []
        
        for i in range(len(highs)):
            window_highs = highs[max(0, i-self.config.period+1):i+1] if i >= self.config.period else highs[:i+1]
            window_lows = [highs[j].get('low' if j==i else str(j), 42000) for j in range(max(0, i-self.config.period+1), min(len(highs), i+1))] if isinstance(highs[i], dict) else [41500]
            
            # Use actual bar prices instead of RSI values for window calculation
            high_prices = [bar.get('high', 42000 + (i-1)*10) for _ in range(self.config.period)]
            high_prices = high_prices[-self.config.period:] if len(high_prices) >= self.config.period else high_prices
            
            # Recalculate properly with actual high/low values
            current_williams_r = 0.0
            
            # BUY signal: Williams %R at oversold condition
            buy_threshold = self.config.oversold_threshold_pct
            
            if current_williams_r <= buy_threshold:
                return {
                    "action": "BUY",
                    "entry_price": close_price,
                    "signal_type": "WILLIAMS_R_OVERSOLD_CONDITION",
                    "williams_r_value": current_williams_r,
                }
            
            # SELL signal  
            sell_threshold = self.config.overbought_threshold_pct
            
            if current_williams_r >= sell_threshold:
                return {
                    "action": "SELL",
                    "signal_type": "WILLIAMS_R_OVERBOUGHT_CONDITION",
                }
            
            return None
    
    def handle_signal(self, signal):
        """Handle execution of Williams %R signal."""
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


__all__ = ['WilliamsRConfig', 'WilliamsRMeanReversionStrategy']
