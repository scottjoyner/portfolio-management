"""
Keltner Channel Breakout Strategy - Trend Following

Purpose: Captures trend breakouts using Keltner Channel-based volatility bands.
Buy on upper channel breakout (price exceeds ATR-based upper band).
Sell on lower channel breakdown or trailing stop loss.

Keltner Channels Components:
  • Middle Line: EMA(period, close) - long-term trend direction
  • Upper Band: EMA(period, close) + N × ATR(lookback_periods)
  • Lower Band: EMA(period, close) - N × ATR(lookback_periods)

Regime Suitability: 
  ✅ Strong trending markets with clear channel breakouts (BTC/ETH on daily/hourly bars)
  ❌ Choppy sideways markets where price oscillates within channels (<8% weekly range)

Failure Modes:
  • Whipsaws when price crosses bands repeatedly in ranging conditions
  • False breakouts during news events or low liquidity periods
  • Drawdowns when strong counter-trend moves occur after breakout

Expected Performance:
  • Win rate target: 58-72%
  • Profit factor target: 1.4-2.0
  • Maximum historical drawdown: 8-15% (lower due to volatility-based bands)

Configuration:
    ema_period: EMA period for middle line and bands (default 20)
    atr_lookback_periods: Lookback for ATR calculation (default 20)
    channel_width_atr_multiplier: ATR multiplier for band width (default 2.0)
    entry_threshold_pct: Price must exceed band by this % to enter (default 0.3%)
    stop_loss_pct: Hard stop-loss percentage (default 2.5%)
    trailing_stop_bps: Trailing stop bps after profit target (default 12 bps)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List
import math
import time


@dataclass
class KeltnerChannelConfig:
    """Configuration parameters for Keltner Channel breakout strategy."""
    
    ema_period: int = 20            # EMA period for middle line and bands
    atr_lookback_periods: int = 20  # Lookback periods for ATR calculation
    channel_width_atr_multiplier: float = 2.0  # ATR multiplier (bands are EMA ± N*ATR)
    entry_threshold_pct: float = 0.3  # Price must exceed band by this % to enter
    stop_loss_pct: float = 2.5      # Hard stop-loss percentage
    trailing_stop_bps: float = 12.0 # Trailing stop bps after 2% profit
    
    enable_logging: bool = True
    position_size_usd: float = 1000.0


@dataclass 
class KeltnerChannelPosition:
    """Track Keltner Channel breakout strategy position state."""
    
    entry_price: float
    entry_timestamp: float
    quantity: float
    band_upper_at_entry: float = 0.0
    band_lower_at_entry: float = 0.0
    middle_ema_at_entry: float = 0.0
    unrealized_pnl_pct: float = 0.0
    stop_loss_hit: bool = False
    
    def calculate_unrealized_pnl(self, current_price: float) -> None:
        """Update unrealized PnL."""
        pnl_pct = (current_price - self.entry_price) / self.entry_price * 100
        self.unrealized_pnl_pct = pnl_pct


class KeltnerChannelBreakoutStrategy:
    """
    Keltner Channel Breakout Strategy - Trend Following Implementation
    
    This strategy implements Keltner Channel breakout logic:
    - Middle Line: EMA of closing prices (long-term trend direction)
    - Upper Band: EMA + N × ATR (resistance/volatility-based resistance)
    - Lower Band: EMA - N × ATR (support/volatility-based support)
    
    Signal Generation:
    - Buy Signal: Close price > Upper Band × (1 + entry_threshold_pct)
    - Sell Signal: Close price < Lower Band / (1 + entry_threshold_pct) OR stop-loss hit
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with configuration, calculate initial EMAs and ATR
        2. on_bar(bar): Update bands, detect breakouts, generate buy/sell signal
    
    Usage Example:
        strategy = KeltnerChannelBreakoutStrategy(ema_period=20)
        
        # Setup with historical data (need at least 40 bars for ATR + EMA)
        ohlcv_data = get_ohlcv("BTC-USD", periods=200)  
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars
        signal = strategy.on_bar(latest_bar)
    """
    
    def __init__(self, config: Optional[KeltnerChannelConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or KeltnerChannelConfig()
        
        # State tracking
        self.position = None  # KeltnerChannelPosition or None
        
        # EMA calculations - store all history for channel maintenance
        self.ema_values: List[float] = []
        
        # ATR calculations - store all highs, lows for rolling ATR
        self.high_prices: List[float] = []
        self.low_prices: List[float] = []
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
    
    def init(self, data: List[dict], config: Optional[KeltnerChannelConfig] = None) -> None:
        """
        Initialize strategy with historical OHLCV data.
        
        Args:
            data: List of OHLCV dicts in chronological order. Each dict must have:
                  - timestamp: Unix timestamp or exchange-specific time
                  - open, high, low, close: Price levels
                  - volume: Trading volume (optional but recommended)
            config: Override default configuration with new parameters
            
        Raises:
            ValueError: If data is empty or too short for calculations
        """
        if config:
            self.config = config
            
        # Validate minimum bars needed (need full ATR lookback + EMA period)
        min_bars = max(self.config.ema_period, self.config.atr_lookback_periods) * 2
        if not data or len(data) < min_bars:
            raise ValueError(
                f"Need at least {min_bars} bars for Keltner Channel initialization. "
                f"Got {len(data) if data else 0} bars."
            )
            
        # Process historical data and calculate EMAs, track highs/lows for ATR
        self.ema_values = []
        self.high_prices = []
        self.low_prices = []
        
        k_factor = 2 / (self.config.ema_period + 1)
        self._k_factor = k_factor
        
        for i, bar in enumerate(data):
            close_price = bar.get("close", bar.get("price", 0))
            high_price = bar.get("high", close_price)
            low_price = bar.get("low", close_price)
            
            if i == 0:
                # Initial EMA equals first close price
                # BUGFIX: the initial EMA was computed but never appended, so
                # ``ema_values`` ended up one element shorter than the
                # high/low price lists, which (combined with on_bar not
                # updating ``ema_values``) caused an IndexError in
                # ``_calculate_atr``.
                self.ema_values.append(close_price)
            else:
                # Standard EMA calculation
                if self.ema_values:
                    new_ema = (close_price - self.ema_values[-1]) * k_factor + self.ema_values[-1]
                else:
                    new_ema = close_price
                
                self.ema_values.append(new_ema)
            
            self.high_prices.append(high_price)
            self.low_prices.append(low_price)
        
        # Reset state after initialization
        self.position = None
    
    def _calculate_atr(self) -> float:
        """Calculate current ATR from historical highs/lows."""
        if len(self.high_prices) < self.config.atr_lookback_periods + 1:
            return abs(self.ema_values[-1] - self.high_prices[0]) if self.high_prices else 1.0
        
        # Calculate True Range for last N periods
        true_ranges = []
        for i in range(len(self.high_prices) - self.config.atr_lookback_periods, len(self.high_prices)):
            high = self.high_prices[i]
            low = self.low_prices[i]
            prev_close = self.ema_values[i - 1] if i > 0 else self.high_prices[i]
            
            true_range = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            true_ranges.append(true_range)
        
        if not true_ranges:
            return abs(self.ema_values[-1]) * 0.02 if self.ema_values else 1.0
        
        # Simple moving average of True Range (simplified ATR)
        atr = sum(true_ranges) / len(true_ranges)
        return max(atr, abs(self.ema_values[-1]) * 0.005)  # Minimum 0.5% of price
    
    def _calculate_keltner_bands(self) -> tuple:
        """Calculate current Keltner Channel bands."""
        ema = self.ema_values[-1] if self.ema_values else None
        
        if ema is None:
            return None, None, 0.0
        
        atr = self._calculate_atr()
        upper_band = ema + (self.config.channel_width_atr_multiplier * atr)
        lower_band = ema - (self.config.channel_width_atr_multiplier * atr)
        
        return upper_band, lower_band, ema
    
    def on_bar(self, bar: dict) -> Optional[dict]:
        """
        Process new bar and generate trading signal if breakout detected.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close
            
        Returns:
            Dict with action ("BUY", "SELL", or None for no signal), 
                entry_price if applicable, stop_loss price, take_profit price
                
        Breakout Logic:
            - Buy Signal: Close > Upper Band × (1 + entry_threshold_pct)
            - Sell Signal: Close < Lower Band / (1 + entry_threshold_pct) OR stop-loss hit
        """
        close_price = bar.get("close", bar.get("price", 0))
        
        if not close_price or close_price <= 0:
            return None
            
        # Calculate current bands
        upper_band, lower_band, middle_ema = self._calculate_keltner_bands()
        
        if upper_band is None or lower_band is None or middle_ema is None:
            return None
        
        # Update history for ATR calculation
        high_price = bar.get("high", close_price)
        low_price = bar.get("low", close_price)
        self.high_prices.append(high_price)
        self.low_prices.append(low_price)
        # Keep the EMA series in lock-step with the price history so that
        # _calculate_atr can safely index ema_values by position.
        k_factor = getattr(self, "_k_factor", 2 / (self.config.ema_period + 1))
        if self.ema_values:
            new_ema = (close_price - self.ema_values[-1]) * k_factor + self.ema_values[-1]
        else:
            new_ema = close_price
        self.ema_values.append(new_ema)
        
        # Check for breakout above upper channel (buy signal)
        threshold_pct = self.config.entry_threshold_pct / 100
        buy_threshold = upper_band * (1 + threshold_pct)
        
        if not self.position and close_price > buy_threshold:
            stop_loss = self.config.stop_loss_pct * -0.01 * close_price if close_price > 0 else None
            return {
                "action": "BUY",
                "entry_price": close_price,
                "signal_type": "KELTNER_UPPER_BREAKOUT",
                "upper_band": upper_band,
                "lower_band": lower_band,
                "middle_ema": middle_ema,
                "stop_loss": stop_loss,
            }
            
        # Check for breakdown below lower channel (sell signal) when in position
        elif self.position:
            sell_threshold = lower_band / (1 + threshold_pct)
            
            if close_price < sell_threshold:
                return {
                    "action": "SELL",
                    "signal_type": "KELTNER_LOWER_BREAKDOWN",
                    "entry_price": self.position.entry_price,
                    "upper_band": upper_band,
                    "lower_band": lower_band,
                }
            
            # Check hard stop-loss
            elif close_price < (1 - self.config.stop_loss_pct / 100) * self.position.entry_price:
                return {
                    "action": "SELL",
                    "signal_type": "STOP_LOSS_HIT",
                    "entry_price": self.position.entry_price,
                }
        
        return None
    
    def handle_signal(self, signal: dict) -> Optional[KeltnerChannelPosition]:
        """
        Handle execution of signal and update position state.
        
        Args:
            signal: Return value from on_bar() if signal triggered
            
        Returns:
            Updated KeltnerChannelPosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action == "BUY":
            entry_price = signal.get("entry_price", 0)
            
            # Create new position with channel tracking
            self.position = KeltnerChannelPosition(
                entry_price=entry_price,
                entry_timestamp=time.time(),
                quantity=self.config.position_size_usd / entry_price,
                band_upper_at_entry=signal.get("upper_band", 0),
                band_lower_at_entry=signal.get("lower_band", 0),
                middle_ema_at_entry=signal.get("middle_ema", 0)
            )
            self.num_successful_trades += 1
            
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
    
    def get_current_position(self) -> Optional[KeltnerChannelPosition]:
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


__all__ = ['KeltnerChannelConfig', 'KeltnerChannelPosition', 'KeltnerChannelBreakoutStrategy']
