"""
Simple Momentum Breakout Strategy (Trend Following)

Purpose: Captures trending price movements by identifying breakouts from N-period lookback window.
Entry triggers when current price exceeds maximum of lookback period (resistance breakout).
Exit triggers when price drops below minimum (trend exhaustion).

Regime Suitability: Strong trending markets, works best with high-volume assets like BTC/ETH.
Failure Modes: Whipsaws in ranging markets, false breakouts due to low liquidity events.
Expected Holding Horizon: 1-5 days per trade typical for daily bars.

Configuration:
    lookback_periods: Number of bars for resistance/support calculation (default 20)
    entry_threshold_pct: Price must exceed resistance by this % to enter (default 0.5%)
    stop_loss_pct: Hard stop-loss as percentage of entry price (default 3%)
    trailing_stop_bps: Trailing stop basis points after profit target reached (default 10 bps)

Author Notes: Classic Donchian channel breakout logic. Best combined with volume confirmation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, List
from datetime import datetime
import math


@dataclass
class SimpleMomentumBreakoutConfig:
    """Configuration parameters for momentum breakout strategy."""
    
    lookback_periods: int = 20  # Resistance/support lookback window
    entry_threshold_pct: float = 0.5  # Price must exceed resistance by this %
    stop_loss_pct: float = 3.0  # Hard stop-loss percentage
    trailing_stop_bps: float = 10.0  # Trailing stop bps after 2% profit
    
    enable_logging: bool = True
    position_size_usd: float = 1000.0


@dataclass 
class MomentumPosition:
    """Track breakout strategy position state."""
    
    entry_price: float
    entry_timestamp: float
    quantity: float
    unrealized_pnl_pct: float = 0.0
    stop_loss_hit: bool = False
    trailing_stop_hit: bool = False
    
    def calculate_unrealized_pnl(self, current_price: float) -> None:
        """Update unrealized PnL."""
        pnl_pct = (current_price - self.entry_price) / self.entry_price * 100
        self.unrealized_pnl_pct = pnl_pct
        
    def check_trailing_stop(self, current_price: float) -> Optional[float]:
        """Check if trailing stop triggered. Returns exit signal or None."""
        if not self.trailing_stop_hit:
            return None
            
        profit_target = 2.0  # Base 2% to start trailing
        trailing_exit_pct = max(profit_target, (1 + self.unrealized_pnl_pct / 100) * (1 - self.trailing_stop_bps / 10000) - 1) * 100
        
        if current_price < self.entry_price * (1 + trailing_exit_pct / 100):
            return "CLOSE"
            
        return None


class SimpleMomentumBreakoutStrategy:
    """
    Simple Momentum Breakout Strategy - Trend Following Implementation
    
    This strategy implements classic breakout trading logic:
    - Buys when price breaks above lookback maximum (resistance)
    - Sells when price drops below lookback minimum (support) or stop-loss triggers
    - Applies trailing stops to lock in profits after momentum confirms
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with configuration and state
        2. on_bar(bar): Analyze new bar, generate buy/sell signal if breakout detected
    
    Usage Example:
        strategy = SimpleMomentumBreakoutStrategy(lookback_periods=20)
        
        # Setup with historical data
        ohlcv_data = get_ohlcv("BTC-USD", periods=100)  # List of {timestamp, open, high, low, close}
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars
        signal = strategy.on_bar(latest_bar)
    
    """
    
    def __init__(self, config: Optional[SimpleMomentumBreakoutConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or SimpleMomentumBreakoutConfig()
        
        # State tracking
        self.position = None  # MomentumPosition or None
        self.lookback_high = None
        self.lookback_low = None
        self.entry_timestamp = None
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
        
    def init(self, data: List[dict], config: Optional[SimpleMomentumBreakoutConfig] = None) -> None:
        """
        Initialize strategy with historical OHLCV data.
        
        Args:
            data: List of OHLCV dicts in chronological order. Each dict must have:
                  - timestamp: Unix timestamp or exchange-specific time
                  - open, high, low, close: Price levels
                  - volume: Trading volume (optional but recommended)
            config: Override default configuration with new parameters
            
        Raises:
            ValueError: If data is empty or too short for lookback calculation
        """
        if config:
            self.config = config
            
        if not data or len(data) < self.config.lookback_periods:
            raise ValueError(
                f"Need at least {self.config.lookback_periods} bars for initialization. "
                f"Got {len(data) if data else 0} bars."
            )
            
        # Extract prices from OHLCV data
        self.high_prices = [bar.get("high", bar.get("price", 0)) for bar in data]
        self.low_prices = [bar.get("low", bar.get("price", 0)) for bar in data]
        
        # Calculate initial lookback high/low from full history (conservative)
        if self.high_prices:
            self.lookback_high = max(self.high_prices[:self.config.lookback_periods])
            self.lookback_low = min(self.low_prices[:self.config.lookback_periods])
            
        self.position = None
        self.entry_timestamp = None
        
    def on_bar(self, bar: dict) -> Optional[dict]:
        """
        Process new bar and generate trading signal if breakout detected.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close
            
        Returns:
            Dict with action ("BUY", "SELL", or None for no signal), 
                entry_price if applicable, stop_loss price, take_profit price
            
        Breakout Logic:
            - Buy signal: Close > lookback_high * (1 + entry_threshold)
            - Sell signal: Close < lookback_low / (1 + entry_threshold) OR stop-loss hit
            
        State Updates:
            - After buy: Track unrealized PnL, calculate new lookback high/low
            - After sell: Increment trade counter, reset position state
        """
        close_price = bar.get("close", bar.get("price", 0))
        
        if not close_price or close_price <= 0:
            return None
            
        # Calculate adjusted threshold price
        threshold_pct = self.config.entry_threshold_pct / 100
        
        # Check for breakout above resistance (buy signal)
        if not self.position:
            buy_threshold = self.lookback_high * (1 + threshold_pct)
            
            if close_price > buy_threshold:
                # Execute breakout entry
                return {
                    "action": "BUY",
                    "entry_price": close_price,
                    "signal_type": "BREAKOUT_ABOVE_RESISTANCE",
                    "stop_loss": self.config.stop_loss_pct * -0.01 * close_price if close_price > 0 else None,
                    "lookback_high": self.lookback_high,
                }
                
        # Check for breakout below support (sell signal) when in position
        elif self.position:
            sell_threshold = self.lookback_low / (1 + threshold_pct)
            
            if close_price < sell_threshold:
                return {
                    "action": "SELL",
                    "signal_type": "BREAKOUT_BELOW_SUPPORT",
                    "entry_price": self.position.entry_price,
                }
                
        # Check trailing stop after reaching profit target
        elif self.position and self.config.trailing_stop_bps > 0:
            pnl_exit = self._check_trailing_stop()
            
            if pnl_exit:
                return {
                    "action": "SELL", 
                    "signal_type": "TRAILING_STOP_EXIT"
                }
        
        # Check hard stop-loss
        elif self.position and close_price < (1 - self.config.stop_loss_pct / 100) * self.position.entry_price:
            return {
                "action": "SELL",
                "signal_type": "STOP_LOSS_HIT",
            }
            
        return None
    
    def _check_trailing_stop(self) -> Optional[dict]:
        """Check if trailing stop should trigger exit."""
        if not self.position or self.config.trailing_stop_bps <= 0:
            return None
            
        position = self.position
        current_pnl_pct = position.unrealized_pnl_pct
        
        # Start trailing after 2% profit
        if current_pnl_pct > 2.0:
            trail_exit_pct = max(2.0, (1 + current_pnl_pct / 100) * (1 - self.config.trailing_stop_bps / 10000) - 1) * 100
            
            if current_pnl_pct < trail_exit_pct:
                return {"exit_reason": "TRAILING_STOP_TRIGGERED"}
                
        return None
    
    def handle_signal(self, signal: dict) -> Optional[MomentumPosition]:
        """
        Handle execution of signal and update position state.
        
        Args:
            signal: Return value from on_bar() if signal triggered
            
        Returns:
            Updated MomentumPosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action == "BUY":
            entry_price = signal.get("entry_price", 0)
            
            # Create new position with stop-loss and trailing stop tracking
            self.position = MomentumPosition(
                entry_price=entry_price,
                entry_timestamp=time.time()
            )
            
            # Update lookback after breakout (conservative expansion)
            if hasattr(self, 'lookback_high'):
                self.lookback_high = max(self.high_prices[-1] if self.high_prices else 0, entry_price * 1.01)
                self.lookback_low = min(self.low_prices[-1] if self.low_prices else 0, entry_price * 0.99)
                
            self.num_successful_trades += 1
            
        elif action == "SELL":
            if self.position:
                position = self.position
                position.calculate_unrealized_pnl(0)  # Close price unknown until execution
                
                # Record trade statistics  
                pnl_pct = (signal.get("entry_price", 0) - position.entry_price) / position.entry_price * 100
                if pnl_pct >= 0:
                    self.num_successful_trades += 1
                else:
                    self.num_failed_trades += 1
                    
                # Reset for next trade
                self.position = None
    
    def get_current_position(self) -> Optional[MomentumPosition]:
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


__all__ = ['SimpleMomentumBreakoutConfig', 'MomentumPosition', 'SimpleMomentumBreakoutStrategy']
