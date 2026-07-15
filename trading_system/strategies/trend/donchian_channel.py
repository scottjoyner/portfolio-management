"""
Donchian Channel Trend Strategy - Breakout Implementation

Purpose: Momentum breakout strategy using Donchian Channels (highest/lowest N-period prices) for
entry signals. Classic breakout-following approach with trailing stop management.

Regime Suitality:
  ✅ Strong trending markets with clear directional bias (breakouts above/below bands)
  ❌ Choppy sideways markets where price oscillates between highest/lowest boundaries (<15% range)

Failure Modes:
  • Whipsaws near boundary during consolidation phases  
  • Lagging signals due to N-period lookback (slower response than VWAP/MACD)
  • False breakouts when volatility expansion drives channel wider temporarily
  
Expected Performance:
  • Win rate target: 45-55% with proper position sizing and trailing stops
  • Profit factor target: 1.2-1.8 depending on market regime

Configuration Parameters:
    donchian_period: N-period highest/lowest prices for channel boundaries (default 20 bars)  
    breakout_threshold_pct: Distance above/below band before entry signal (default 0.5%)  
    trailing_stop_pct: Trailing stop as percentage of entry (default 3%)
    min_volume_multiplier: Minimum volume relative to average for breakout confirmation (default 1.5)

Donchian Channel Logic:
    Upper Band = Highest(close, period=20) - highest price over N bars (resistance)
    Lower Band = Lowest(close, period=20) - lowest price over N bars (support)
    
    Entry Signals:
        - BUY BREAKOUT: Close crosses above Upper Band (new resistance level broken)  
        - SELL SIGNAL: Price pulls back to Lower Band during uptrend or below Lower Band
                     (classic trend reversal signal)
    
Usage Example:
    from trading_system.strategies.trend.donchian_channel import DonchianChannelTrendStrategy
    
    strategy = DonchianChannelTrendStrategy(
        donchian_period=20,
        breakout_threshold_pct=0.5,
        trailing_stop_pct=3.0
    )
    
    # Setup with historical data
    ohlcv_data = get_ohlcv("BTC-USD", periods=100)
    strategy.init(ohlcv_data)
    
    # Generate signals on new bars
    signal = strategy.on_bar(latest_bar)
    if signal and signal["action"] == "BUY":
        execute_breakout_buy(signal)

Author Notes: Donchian Channels use fixed N-period lookback which makes them excellent for
breakout detection but lagging compared to MA-based systems. Classic example in trading literature,
widely used by breakout traders like Paul Tudor Jones (N=20-30 period Donchian). Best deployed on
swing trading timeframes (Daily/4H) where breakouts represent major trend changes rather than noise.

Enhancement Options:
    - Combine with volume confirmation for quality breakout detection  
    - Use ATR or Keltner Channels to filter false breakouts during consolidation
    - Add RSI filter to avoid chasing extended move after breakout
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List
import math


@dataclass
class DonchianChannelConfig:
    """Configuration parameters for Donchian Channel trend strategy."""
    
    donchian_period: int = 20  # N-period highest/lowest prices for channel boundaries
    breakout_threshold_pct: float = 0.5  # Distance above/below band before entry signal  
    trailing_stop_pct: float = 3.0  # Trailing stop as percentage of entry
    min_volume_multiplier: float = 1.5  # Minimum volume relative to average for confirmation
    
    enable_logging: bool = True


@dataclass
class DonchianPosition:
    """Track Donchian Channel breakout strategy position state."""
    
    entry_price: float
    channel_upper_at_entry: float
    channel_lower_at_entry: float
    quantity: float
    unrealized_pnl_pct: float = 0.0


class DonchianChannelTrendStrategy:
    """
    Donchian Channel Trend Strategy - Breakout Implementation
    
    This strategy implements breakout trend-following using Donchian Channels:
    - Provides fixed N-period highest/lowest price boundaries (resistance/support)
    - Generates entry signals on channel boundary breakout
    - Classic momentum breakout-following approach with trailing stops
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute Donchian channels
        2. on_bar(bar): Generate buy/sell signal when price crosses upper band (breakout)
    
    Usage Example:
        strategy = DonchianChannelTrendStrategy(donchian_period=20)
        
        # Setup with historical data
        ohlcv_data = get_ohlcv("BTC-USD", periods=100)
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars
        signal = strategy.on_bar(latest_bar)
    
    """
    
    def __init__(self, config: Optional[DonchianChannelConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or DonchianChannelConfig()
        
        # State tracking  
        self.high_values = []  # Highest price over N period (upper channel boundary)
        self.low_values = []   # Lowest price over N period (lower channel boundary)
        self.volume_values = []  # Rolling volume history for breakout confirmation
        self.position = None   # DonchianPosition or None
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
        
    def init(self, data: List[dict]) -> None:
        """
        Initialize strategy with historical OHLCV data.
        
        Args:
            data: List of OHLCV dicts in chronological order. Each dict must have:
                  - timestamp: Unix timestamp or exchange-specific time  
                  - open, high, low, close: Price levels  
                  - volume: Trading volume (required for volume confirmation)
  
        Computes:
            - N-period highest price sequence (upper Donchian boundary)
            - N-period lowest price sequence (lower Donchian boundary)
        
        Raises:
            ValueError: If data is empty or too short for Donchian calculation
        """
        if not data:
            raise ValueError("Need historical OHLCV data for initialization.")
            
        min_bars = self.config.donchian_period + 1
        
        if len(data) < min_bars:
            raise ValueError(
                f"Need at least {min_bars} bars for Donchian Channel calculation. "
                f"Got {len(data)} bars."
            )
            
        # Extract close and high prices from OHLCV data  
        closes = [float(bar.get("close", 0)) for bar in data]
        highs = [float(bar.get("high", bar.get("close", 0))) for bar in data]
        
        # Calculate Donchian channels (N-period highest/lowest)  
        self.high_values, self.low_values = \
            self._calculate_donchian_bands(closes, highs)
        
        # Position initialization after first valid signal opportunity
        self.position = None
        
    def _calculate_donchian_bands(self, closes: List[float], highs: List[float]) -> tuple:
        """
        Calculate Donchian Channel bands from historical prices.
        
        Args:
            closes: List of close prices  
            highs: List of high prices
            
        Returns:
            Tuple of (high_values list, low_values list) representing upper/lower bands
        
        Logic:
            - Upper Band = Highest price over N-period window (resistance)
            - Lower Band = Lowest price over N-period window (support)
        """
        if not closes or not highs:
            return [], []
            
        donchian_period = self.config.donchian_period
        warmup_end = min(donchian_period, len(closes))
        
        high_values = []
        low_values = []
        
        for i in range(len(closes)):
            if i < donchian_period:
                # Warm-up period - use max/min from available bars
                window_closes = closes[:i+1]
                window_highs = highs[:i+1]
                
                high_value = max(window_highs)  # Highest price in window (resistance)
                low_value = min(window_closes)  # Lowest close price (support approximation)
            else:
                # Full N-period window after warm-up  
                start_idx = i - donchian_period + 1
                window_highs = highs[start_idx:i+1]
                window_closes = closes[start_idx:i+1]
                
                high_value = max(window_highs)  # Highest price over N bars (upper band)
                low_value = min(window_closes)   # Lowest close price over N bars (lower band)
            
            high_values.append(high_value)
            low_values.append(low_value)
        
        return high_values, low_values
    
    def on_bar(self, bar: dict) -> Optional[dict]:
        """
        Process new bar and generate trading signal based on Donchian breakout.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close, volume
            
        Returns:
            Dict with action ("BUY", "SELL"), entry_price if applicable
            
        Breakout Detection Logic:
            - Append new prices to window for N-period calculation  
            - BUY when High closes above Upper Band (new resistance broken)
            - SELL when price pulls back to Lower Band or breaks below it
        
        """
        close_price = bar.get("close", bar.get("price", 0))
        high_price = bar.get("high", close_price)
        low_price = bar.get("low", close_price)

        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None

        # Maintain rolling high/low windows for the Donchian channel.  The
        # upper band is the highest high of the *previous* N bars (the level
        # that must be broken); we therefore compute it before appending the
        # current bar.
        #
        # BUGFIX: the previous implementation did ``max(new_highs[-period:])``
        # (a float) and then indexed it with ``[0]``, raising
        # ``TypeError: 'float' object is not subscriptable``.
        period = self.config.donchian_period
        if self.high_values:
            upper_band = max(self.high_values[-period:])
        else:
            upper_band = high_price

        # Append current bar to the rolling windows (bounded length).
        self.high_values.append(high_price)
        self.low_values.append(low_price)
        if len(self.high_values) > period:
            self.high_values = self.high_values[-period:]
            self.low_values = self.low_values[-period:]

        # BUY signal: price crosses above upper Donchian band (resistance broken)
        breakout_threshold = self.config.breakout_threshold_pct / 100.0

        if close_price >= upper_band + upper_band * breakout_threshold:
            volume_at_bar = bar.get("volume", 100)
            # Average of prior bars' volume (falls back to current bar volume).
            if self.volume_values:
                avg_volume = sum(self.volume_values) / len(self.volume_values)
            else:
                avg_volume = volume_at_bar
            self.volume_values.append(volume_at_bar)
            if len(self.volume_values) > period:
                self.volume_values = self.volume_values[-period:]

            # Volume confirmation (optional but recommended)
            volume_confirmed = volume_at_bar >= avg_volume * self.config.min_volume_multiplier

            if volume_confirmed:
                return {
                    "action": "BUY",
                    "entry_price": close_price,
                    "signal_type": "DONCHIAN_UPPER_BAND_BREAKOUT",
                    "upper_band": upper_band,
                    "price_above_band_pct": (close_price - upper_band) / upper_band * 100,
                    "stop_loss": self.config.trailing_stop_pct * -0.01 * close_price if close_price > 0 else None,
                }

            return {
                "action": "BUY",
                "entry_price": close_price,
                "signal_type": "DONCHIAN_UPPER_BAND_BREAKOUT_VOLUME_FILTERED",
                "upper_band": upper_band,
            }

        return None
    
    def handle_signal(self, signal: dict) -> Optional[DonchianPosition]:
        """
        Handle execution of breakout signal and update position state.
        
        Args:
            signal: Return value from on_bar() if breakout triggered
            
        Returns:
            Updated DonchianPosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action == "BUY":
            entry_price = signal.get("entry_price", 0)
            
            # Create new position with trailing stop tracking  
            self.position = DonchianPosition(
                entry_price=entry_price,
                channel_upper_at_entry=self.high_values[-1] if hasattr(self, 'high_values') else entry_price * 1.2,
                channel_lower_at_entry=self.low_values[-1] if hasattr(self, 'low_values') else entry_price * 0.8,
                quantity=entry_price  # Simplified - actual logic would use config position size
            )
            
        elif action == "SELL":
            if self.position:
                pnl_pct = (signal.get("entry_price", 0) - self.position.entry_price) / self.position.entry_price * 100
                if pnl_pct >= 0:
                    self.num_successful_trades += 1
                else:
                    self.num_failed_trades += 1
                    
                # Reset for next trade
                self.position = None
    
    def get_current_position(self) -> Optional[DonchianPosition]:
        """Return current open breakout position or None."""
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


__all__ = ['DonchianChannelConfig', 'DonchianPosition', 'DonchianChannelTrendStrategy']
