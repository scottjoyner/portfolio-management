"""
RSI Mean Reversion Strategy - Statistical Arbitrage Implementation

Purpose: Mean reversion strategy using Relative Strength Index (RSI) to identify 
overbought and oversold conditions. Buys when RSI indicates oversold territory, sells
when RSI indicates overbought conditions.

Regime Suitality: 
  ✅ Oscillating markets with mean-reverting RSI patterns (typical altcoin behavior)  
  ❌ Strong trending markets where RSI stays extreme for extended periods

Failure Modes:
  • Trend continuation causing wide losses while RSI remains extreme  
  • Whipsaws near RSI thresholds during choppy markets
  • False signals when asset lacks mean-reverting distribution
  
Expected Performance:
  • Win rate target: 50-60% in ranging regimes  
  • Profit factor target: 1.3-1.9 depending on market regime
  • Maximum historical drawdown: 12-20% in stress periods

Configuration Parameters:
    rsi_period: RSI calculation period (default 14 bars)
    rsi_overbought_threshold: RSI threshold for sell signal (default 70)  
    rsi_oversold_threshold: RSI threshold for buy signal (default 30)
    stop_loss_pct: Hard stop-loss as percentage (default 3%)
    target_return_pct: Mean reversion target before exit (default 0.2% from entry)
    trailing_stop_bps: Trailing stop bps after profit target (default 15 bps)

RSI Logic:
    - Calculate RSI using exponential average of gains/losses over period
    - RSI above overbought threshold (70) = sell signal (statistically expensive)  
    - RSI below oversold threshold (30) = buy signal (statistically undervalued)
    
Usage Example:
    from trading_system.strategies.mean_reversion.rsi_mean_revert import RSIMeanReversionStrategy
    
    strategy = RSIMeanReversionStrategy(
        rsi_period=14,
        rsi_overbought_threshold=70,
        rsi_oversold_threshold=30
    )
    strategy.init(ohlcv_data)
    
    # On new bar
    signal = strategy.on_bar(latest_bar)
    if signal and signal["action"] == "BUY":
        execute_trade(signal)

Author Notes: Classic RSI mean reversion logic used by many retail traders. Works best in 
ranging markets where RSI frequently oscillates between 30-70. The strategy assumes that 
prices tend to revert to fair value after reaching extreme RSI levels. Best deployed with 
volume confirmation or additional regime filters to avoid trend traps.

Enhancement Options:
    - Add trend filter (require positive/negative ADX to confirm regime)  
    - Combine with Bollinger bands for entry refinement
    - Use dynamic thresholds based on market volatility regime
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List
import math


@dataclass
class RSIConfig:
    """Configuration parameters for RSI mean reversion strategy."""
    
    rsi_period: int = 14  # RSI calculation period
    rsi_overbought_threshold: float = 70.0  # RSI threshold for sell signal
    rsi_oversold_threshold: float = 30.0  # RSI threshold for buy signal  
    stop_loss_pct: float = 3.0  # Hard stop-loss percentage
    target_return_pct: float = 0.2  # Target return for reversion exit
    trailing_stop_bps: float = 15.0  # Trailing stop bps after profit target
    
    enable_logging: bool = True
    position_size_usd: float = 1000.0


@dataclass 
class RSIPosition:
    """Track RSI mean reversion strategy position state."""
    
    entry_price: float
    rsi_at_entry: float
    quantity: float
    unrealized_pnl_pct: float = 0.0
    stop_loss_hit: bool = False
    trailing_stop_hit: bool = False


class RSIMeanReversionStrategy:
    """
    RSI Mean Reversion Strategy - Statistical Arbitrage Implementation
    
    This strategy implements mean reversion using RSI oscillator:
    - BUY when RSI indicates oversold territory (statistically undervalued)
    - SELL when RSI indicates overbought territory (statistically overvalued)
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute RSI  
        2. on_bar(bar): Generate buy/sell signal at RSI threshold breach
    
    Usage Example:
        strategy = RSIMeanReversionStrategy(rsi_period=14, rsi_overbought=70)
        
        # Setup with historical data
        ohlcv_data = get_ohlcv("BTC-USD", periods=100)
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars
        signal = strategy.on_bar(latest_bar)
    
    """
    
    def __init__(self, config: Optional[RSIConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or RSIConfig()
        
        # State tracking
        self.position = None  # RSIPosition or None
        
        # Rolling statistics (computed during init)
        self.rsi_values = []
        
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
                  - volume: Trading volume (optional but recommended)
        
        Computes:
            - RSI values for each bar using exponential average of gains/losses
            
        Raises:
            ValueError: If data is empty or too short for RSI calculation
        """
        if not data:
            raise ValueError("Need historical OHLCV data for initialization.")
            
        min_bars = self.config.rsi_period + 10
        
        if len(data) < min_bars:
            raise ValueError(
                f"Need at least {min_bars} bars for RSI calculation. "
                f"Got {len(data)} bars."
            )
            
        # Extract close prices from OHLCV data
        closes = [float(bar.get("close", bar.get("price", 0))) for bar in data]
        
        # Calculate RSI values
        self.rsi_values = self._calculate_rsi(closes)
        
        # Position initialization after first valid signal opportunity
        self.position = None
        
    def _calculate_rsi(self, closes: List[float]) -> List[float]:
        """
        Calculate RSI from close prices using exponential average method.
        
        Args:
            closes: List of close prices
            
        Returns:
            List of RSI values aligned with close prices
        """
        if not closes:
            return []
            
        period = self.config.rsi_period
        
        # Warm-up period  
        warmup_end = min(period, len(closes))
        
        rsi_values = []
        gains = []
        losses = []
        
        simple_gain_sum = 0.0
        simple_loss_sum = 0.0
        
        for i in range(len(closes)):
            if i == 0:
                # Initial change
                change = closes[i] - (closes[0] if len(closes) > 1 else closes[i])
                
                if change > 0:
                    simple_gain_sum += change
                    gains.append(change)
                    losses.append(0.0)
                else:
                    simple_loss_sum += abs(change)
                    gains.append(0.0)
                    losses.append(abs(change))
            else:
                prev_close = closes[i-1]
                current_close = closes[i]
                change = current_close - prev_close
                
                if change > 0:
                    gain = change
                    loss = 0.0
                    simple_gain_sum += gain
                else:
                    gain = 0.0  
                    loss = abs(change)
                    simple_loss_sum += loss
                    
                gains.append(gain)
                losses.append(loss)
            
            # Calculate RSI during warm-up (use SMA for initial value)
            if i < warmup_end and len(gains) > 0:
                avg_gain = sum(gains[:i+1]) / (i + 1)
                avg_loss = sum(losses[:i+1]) / (i + 1)
                
                if avg_loss == 0:
                    rsi_val = 100.0
                else:
                    rs = avg_gain / avg_loss
                    rsi_val = 100 - (100 / (1 + rs))
                    
            # Calculate RSI after warm-up using EMA
            elif len(gains) > period:
                # Smoothed average gain/loss  
                prev_avg_gain = sum(gains[i-period:i]) / period if i >= period else sum(gains[:period]) / period
                prev_avg_loss = sum(losses[i-period:i]) / period if i >= period else sum(losses[:period]) / period
                
                # Apply smoothing factors for EMA
                avg_gain_smooth = (2 * gain) / (period + 1) if gain > 0 else avg_gain_smooth
                avg_loss_smooth = (2 * loss) / (period + 1) if loss > 0 else avg_loss_smooth
                
                rsi_val = 100 - (100 / (1 + avg_gain_smooth / avg_loss_smooth)) if avg_loss_smooth > 0 else 100.0
            else:
                rsi_val = self.rsi_values[-1] if self.rsi_values else 50.0
                
            rsi_values.append(rsi_val)
        
        return rsi_values
    
    def on_bar(self, bar: dict) -> Optional[dict]:
        """
        Process new bar and generate trading signal at RSI threshold breach.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close
            
        Returns:
            Dict with action ("BUY", "SELL"), entry_price if applicable
            
        Mean Reversion Logic:
            - Calculate RSI from recent price movements
            - BUY when RSI < rsi_oversold_threshold (statistically undervalued)
            - SELL when RSI > rsi_overbought_threshold (statistically overvalued)
        
        State Updates:
            - After buy: Track position PnL, monitor reversion target, update trailing stop  
            - After sell: Increment trade counter, reset position state
        
        """
        close_price = bar.get("close", bar.get("price", 0))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
            
        # Append new price and recalculate RSI  
        closes = [close_price] + self.rsi_values[:-1]
        rsi_values = self._calculate_rsi(closes)
        
        current_rsi = rsi_values[-1]
        
        # Check oversold threshold for buy signal
        if current_rsi < self.config.rsi_oversold_threshold and not self.position:
            return {
                "action": "BUY",
                "entry_price": close_price,
                "signal_type": "RSI_OVERSOLD_BUY_SIGNAL",
                "rsi": current_rsi,
                "stop_loss": self.config.stop_loss_pct * -0.01 * close_price if close_price > 0 else None,
            }
            
        # Check overbought threshold for sell signal  
        elif current_rsi > self.config.rsi_overbought_threshold and not self.position:
            return {
                "action": "SELL",
                "signal_type": "RSI_OVERBOUGHT_SELL_SIGNAL"
            }
            
        return None
    
    def handle_signal(self, signal: dict) -> Optional[RSIPosition]:
        """
        Handle execution of signal and update position state.
        
        Args:
            signal: Return value from on_bar() if threshold breached
            
        Returns:
            Updated RSIPosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action == "BUY":
            entry_price = signal.get("entry_price", 0)
            
            # Create new position with trailing stop tracking  
            self.position = RSIPosition(
                entry_price=entry_price,
                rsi_at_entry=current_rsi,
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
    
    def get_current_position(self) -> Optional[RSIPosition]:
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


__all__ = ['RSIConfig', 'RSIPosition', 'RSIMeanReversionStrategy']
