"""
Volume Breakout Strategy - Trend Following Implementation

Purpose: Momentum strategy that captures explosive price moves following high-volume 
breakouts above key resistance levels or moving averages. Buys on breakout confirmation  
with volume surge, exits on momentum decay.

Regime Suitality: 
  ✅ High volume expansion periods with clear breakouts (trending crypto markets)  
  ❌ Low volume consolidation without follow-through (<2x average volume at breakout)

Failure Modes:
  • Whipsaws during false breakouts (breakout trap scenarios)  
  • Extended rallies where price never gives back gains for trailing stop
  • Range-bound markets with frequent breakout attempts without follow-through
  
Expected Performance:
  • Win rate target: 35-45% (momentum strategies have lower win rates but higher RR)  
  • Profit factor target: 1.4-2.2 depending on market regime
  • Maximum historical drawdown: 20-30% in stress periods

Configuration Parameters:
    breakout_threshold_bars: Bars from moving average to trigger (default 1 bar for immediate, default 0.5% separation)  
    volume_confirmation_multiplier: Minimum volume multiple required at breakout (default 2x average)  
    momentum_confirmation_bars: Bars required after breakout before entry (default 2 bars)
    stop_loss_pct: Hard stop-loss as percentage (default 4%)
    trailing_stop_bps: Trailing stop bps after profit target (default 15 bps)

Volume Breakout Logic:
    - Calculate resistance levels from recent highs or moving averages  
    - Detect breakout when price closes above resistance with volume confirmation
    - BUY on first confirmed close above resistance with volume > threshold
    
Usage Example:
    from trading_system.strategies.trend.volume_breakout import VolumeBreakoutStrategy
    
    strategy = VolumeBreakoutStrategy(
        volume_confirmation_multiplier=2.0,  # Require 2x average volume at breakout
        momentum_confirmation_bars=2        # Wait for 2 bars after breakout before entry
    )
    strategy.init(ohlcv_data)
    
    # On new bar  
    signal = strategy.on_bar(latest_bar)
    if signal and signal["action"] == "BUY":
        execute_trade(signal)

Author Notes: Volume breakout strategies capture the initial explosive moves of trends, then 
trail profits with trailing stops. Works best on swing trading timeframes (4H, Daily) where
breakouts have higher predictive value. Best deployed with volume confirmation and possibly  
additional trend filters to avoid false breakouts in ranging markets. Common enhancement is to  
wait for price pullback after initial breakout before entering for better risk/reward.

Enhancement Options:
    - Add RSI filter to confirm momentum (require >50 at breakout)  
    - Combine with Bollinger bands for entry refinement (buy upper band touch)  
    - Use multi-level breakout confirmation (require close above both SMA20 and 61.8% Fibonacci extension)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List
import math


@dataclass
class VolumeBreakoutConfig:
    """Configuration parameters for volume breakout strategy."""
    
    resistance_period: int = 20  # Period for recent high calculation
    breakout_threshold_pct: float = 0.5  # Percentage separation from resistance before entry  
    volume_confirmation_multiplier: float = 2.0  # Minimum volume multiple required at breakout
    momentum_confirmation_bars: int = 2  # Bars required after breakout before entry
    stop_loss_pct: float = 4.0  # Hard stop-loss percentage
    trailing_stop_bps: float = 15.0  # Trailing stop bps after profit target
    
    enable_logging: bool = True
    position_size_usd: float = 1000.0


@dataclass 
class VolumeBreakoutPosition:
    """Track volume breakout strategy position state."""
    
    entry_price: float
    resistance_at_entry: float
    quantity: float
    unrealized_pnl_pct: float = 0.0
    stop_loss_hit: bool = False
    trailing_stop_hit: bool = False


class VolumeBreakoutStrategy:
    """
    Volume Breakout Strategy - Trend Following Implementation
    
    This strategy captures explosive price moves following high-volume breakouts:
    - Detects breakout above recent resistance with volume confirmation
    - Buys on first confirmed close above resistance with volume surge
    - Trails profits with trailing stop to capture momentum extension
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute resistance levels  
        2. on_bar(bar): Generate buy signal on volume-confirmed breakout
    
    Usage Example:
        strategy = VolumeBreakoutStrategy(volume_confirmation_multiplier=2.0)
        
        # Setup with historical data (must include volume field!)
        ohlcv_data = get_ohlcv("BTC-USD", periods=100)  # Must have 'volume' key
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars  
        signal = strategy.on_bar(latest_bar)
    
    """
    
    def __init__(self, config: Optional[VolumeBreakoutConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or VolumeBreakoutConfig()
        
        # State tracking
        self.position = None  # VolumeBreakoutPosition or None
        
        # Rolling statistics (computed during init)  
        self.resistance_levels = []
        self.rolling_high_values = []
        self.average_volume = 0.0
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
        
    def init(self, data: List[dict]) -> None:
        """
        Initialize strategy with historical OHLCV+Volume data.
        
        Args:
            data: List of OHLCV dicts in chronological order. Each dict must have:
                  - timestamp: Unix timestamp or exchange-specific time
                  - open, high, low, close: Price levels  
                  - volume: Trading volume (REQUIRED for breakout detection)
        
        Computes:
            - Rolling resistance levels from recent highs (or moving averages)  
            - Average volume baseline for confirmation
        
        Raises:
            ValueError: If data is empty or missing volume field
        """
        if not data:
            raise ValueError("Need historical OHLCV+Volume data for initialization.")
            
        # Check for volume field in all bars  
        for i, bar in enumerate(data):
            if 'volume' not in bar and not bar.get('price'):
                raise ValueError(f"Bar {i} missing volume field required for breakout detection")
        
        min_bars = self.config.resistance_period + 20
        
        if len(data) < min_bars:
            raise ValueError(
                f"Need at least {min_bars} bars with volume data. "
                f"Got {len(data)} bars."
            )
            
        # Extract prices from OHLCV data
        closes = [float(bar.get("close", bar.get("price", 0))) for bar in data]
        highs = [float(bar.get("high", bar.get('close', 0))) for bar in data]
        volumes = [float(bar.get("volume", 1)) for bar in data]
        
        # Calculate rolling resistance (recent high) and average volume
        # BUGFIX: the tuple was previously unpacked into
        # (resistance_levels, rolling_high_values, average_volume) but the
        # method returns (rolling_high list, last_high float, avg_volume),
        # so ``rolling_high_values`` ended up being a float and on_bar's
        # ``rolling_high_values[-period:]`` raised a TypeError.
        self.rolling_high_values, _last_high, self.average_volume = \
            self._calculate_breakout_metrics(closes, highs, volumes)
        self.resistance_levels = self.rolling_high_values
        
        # Position initialization after first valid signal opportunity
        self.position = None
        
    def _calculate_breakout_metrics(self, closes: List[float], highs: List[float], 
                                   volumes: List[float]) -> tuple:
        """
        Calculate breakout metrics from historical data.
        
        Args:
            closes: List of close prices  
            highs: List of high prices
            volumes: List of trading volumes
            
        Returns:
            Tuple of (resistance_levels, rolling_highs, average_volume) lists/values
        """
        if not closes or not highs:
            return [], 0.0, 0.0
        
        resistance_period = self.config.resistance_period
        min_bars_for_rolling = max(resistance_period, 10)
        
        # Use recent rolling high as resistance (most sensitive to new highs)
        rolling_high_values = []
        average_volume = sum(volumes[:min_bars_for_rolling]) / min_bars_for_rolling
        
        for i in range(len(closes)):
            if i < min_bars_for_rolling:
                # Use cumulative high during warm-up  
                rolling_high = max(highs[:i+1])
            else:
                # Rolling high over resistance_period after warm-up
                start_idx = i - resistance_period + 1
                if start_idx < 0:
                    start_idx = 0
                rolling_high = max(highs[start_idx:i+1])
            
            rolling_high_values.append(rolling_high)
        
        # Return current metrics
        return rolling_high_values, rolling_high_values[-1], average_volume
    
    def on_bar(self, bar: dict) -> Optional[dict]:
        """
        Process new bar and generate trading signal on volume-confirmed breakout.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close
            
        Returns:
            Dict with action ("BUY"), entry_price if applicable
            
        Breakout Logic:
            - Append new high to rolling resistance calculation  
            - BUY when price closes above resistance level WITH volume confirmation
        
        State Updates:
            - After buy: Track position PnL, update trailing stop
            - After sell: Increment trade counter, reset position state
        
        """
        close_price = bar.get("close", bar.get("price", 0))
        high_price = bar.get("high", close_price)
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
            
        # Resistance is the highest high over the prior rolling window (before
        # incorporating the current bar).
        #
        # BUGFIX: the previous implementation constructed mismatched-length
        # ``closes``/``highs`` lists and called ``_calculate_breakout_metrics``,
        # which executed ``max(highs[start_idx:i+1])`` over an empty slice and
        # raised ``ValueError: max() arg is an empty sequence``.
        volume = float(bar.get("volume", 1))
        period = self.config.resistance_period

        if self.rolling_high_values:
            current_resistance = max(self.rolling_high_values[-period:])
        else:
            current_resistance = high_price

        # Update the rolling resistance window with the current bar's high.
        self.rolling_high_values.append(high_price)
        if len(self.rolling_high_values) > period:
            self.rolling_high_values = self.rolling_high_values[-period:]

        # Check for volume-confirmed breakout above resistance
        is_above_resistance = close_price > current_resistance * (1 + self.config.breakout_threshold_pct / 100)

        # Volume confirmation required
        volume_confirmed = volume > self.average_volume * self.config.volume_confirmation_multiplier

        if is_above_resistance and volume_confirmed and not self.position:
            return {
                "action": "BUY",
                "entry_price": close_price,
                "signal_type": "VOLUME_CONFIRMED_BREAKOUT_ABOVE_RESISTANCE",
                "resistance_level": current_resistance,
                "close_to_resistance_pct": (close_price - current_resistance) / current_resistance * 100,
                "current_volume": volume,
                "average_volume": self.average_volume,
                "volume_multiple": volume / max(self.average_volume, 0.0001),
                "stop_loss": self.config.stop_loss_pct * -0.01 * close_price if close_price > 0 else None,
            }

        return None
    
    def handle_signal(self, signal: dict) -> Optional[VolumeBreakoutPosition]:
        """
        Handle execution of signal and update position state.
        
        Args:
            signal: Return value from on_bar() if breakout triggered
            
        Returns:
            Updated VolumeBreakoutPosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action == "BUY":
            entry_price = signal.get("entry_price", 0)
            
            # Create new position with trailing stop tracking  
            self.position = VolumeBreakoutPosition(
                entry_price=entry_price,
                resistance_at_entry=self.resistance_levels[-1] if hasattr(self, 'resistance_levels') else entry_price * 0.995,
                quantity=self.config.position_size_usd / entry_price
            )
            
        elif action == "SELL":
            if self.position:
                position = self.position
                
                # Record trade statistics  
                pnl_pct = (signal.get("entry_price", 0) - position.entry_price) / position.entry_price * 100
                if pnl_pct >= 0:
                    self.num_successful_trades += 1
                else:
                    self.num_failed_trades += 1
                    
                # Reset for next trade
                self.position = None
    
    def get_current_position(self) -> Optional[VolumeBreakoutPosition]:
        """Return current open position or None."""
        return self.position
    
    def get_performance_metrics(self) -> dict:
        """Calculate performance statistics since last initialization."""
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


__all__ = ['VolumeBreakoutConfig', 'VolumeBreakoutPosition', 'VolumeBreakoutStrategy']
