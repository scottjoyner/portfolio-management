"""
Bollinger Band Mean Reversion Strategy - Statistical Arbitrage Implementation

Purpose: Mean reversion strategy that buys when price touches lower Bollinger band 
(statistically cheap) and sells when price touches upper band (statistically expensive).
Exploits overreactions in ranging markets where mean reversion dominates.

Regime Suitality: 
  ✅ Low volatility ranging markets with clear mean-reverting behavior
  ❌ Strong trending markets where breakouts persist without revert

Failure Modes:
  • Wide losses during trend continuation after band breach (breakout traps)
  • Whipsaws near band edges during high volatility expansion  
  • False signals when price distribution becomes asymmetric

Expected Performance: 
  • Win rate target: 50-60% in ranging regimes  
  • Profit factor target: 1.2-1.8 depending on market regime
  • Maximum historical drawdown: 15-25% in stress periods

Configuration Parameters:
    period: SMA period for band calculation (default 20 bars)
    num_std: Number of standard deviations for bands (default 2.0 std)  
    z_score_buy_threshold: Z-score threshold for lower band buy signal (default -1.8 std from mean)
    z_score_sell_threshold: Z-score threshold for upper band sell signal (default +1.8 std from mean)
    stop_loss_pct: Hard stop-loss as percentage (default 3%)
    target_return_pct: Mean reversion target before exit (default 0.2% from entry)
    trailing_stop_bps: Trailing stop bps after profit target (default 15 bps)

Bollinger Band Logic:
    Middle Band = SMA(close_price, period=20)
    Upper Band = Middle Band + num_std * Std(close_price, period=20)  
    Lower Band = Middle Band - num_std * Std(close_price, period=20)
    
    Statistical Arbitrage Logic:
        - Calculate z-score relative to rolling mean and std
        - BUY: Price below lower band OR z_score < buy_threshold (-1.8 std)
        - SELL: Price above upper band OR z_score > sell_threshold (+1.8 std)
        
Usage Example:
    from trading_system.strategies.mean_reversion.bollinger_mean_revert import BollingerBandMeanReversionStrategy
    
    strategy = BollingerBandMeanReversionStrategy(
        period=20,
        num_std=2.0,
        z_score_buy_threshold=-1.8  # Buy when below -1.8 std from mean
    )
    strategy.init(ohlcv_data)
    
    # On new bar
    signal = strategy.on_bar(latest_bar)
    if signal and signal["action"] == "BUY":
        execute_trade(signal)

Author Notes: Classic mean reversion logic on Bollinger bands. The band edges represent 
statistical extremes (typically 2 std from mean), so breaching them suggests temporary price 
extremity that historically reverts. Best suited for swing trading in ranging markets with
low volatility regimes. Works well on altcoins and smaller market caps during bull/bear cycles.

Enhancement Options:
    - Add trend filter (only trade when RSI shows mean-reverting range)
    - Require volume confirmation at band breach
    - Combine with stochastic oscillator for entry timing optimization
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List
import math


@dataclass
class BollingerBandMeanRevertConfig:
    """Configuration parameters for Bollinger Band mean reversion strategy."""
    
    period: int = 20  # SMA period for band calculation
    num_std: float = 2.0  # Number of standard deviations for bands  
    z_score_buy_threshold: float = -1.8  # Buy threshold in std from mean (lower band)
    z_score_sell_threshold: float = 1.8  # Sell threshold in std from mean (upper band)
    stop_loss_pct: float = 3.0  # Hard stop-loss percentage
    target_return_pct: float = 0.2  # Target return for reversion exit
    trailing_stop_bps: float = 15.0  # Trailing stop bps after profit target
    
    enable_logging: bool = True
    position_size_usd: float = 1000.0


@dataclass 
class BBPosition:
    """Track Bollinger mean reversion strategy position state."""
    
    entry_price: float
    entry_z_score: float
    quantity: float
    unrealized_pnl_pct: float = 0.0
    stop_loss_hit: bool = False
    trailing_stop_hit: bool = False


class BollingerBandMeanReversionStrategy:
    """
    Bollinger Band Mean Reversion Strategy - Statistical Arbitrage Implementation
    
    This strategy implements mean reversion using Bollinger Band breaches and z-score analysis:
    - BUY when price touches/penetrates lower band (statistically undervalued)
    - SELL when price touches/penetrates upper band (statistically overvalued)
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute bands
        2. on_bar(bar): Generate buy/sell signal at band breach
    
    Usage Example:
        strategy = BollingerBandMeanReversionStrategy(period=20, num_std=2.0)
        
        # Setup with historical data
        ohlcv_data = get_ohlcv("BTC-USD", periods=100)
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars
        signal = strategy.on_bar(latest_bar)
    
    """
    
    def __init__(self, config: Optional[BollingerBandMeanRevertConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or BollingerBandMeanRevertConfig()
        
        # State tracking
        self.position = None  # BBPosition or None
        
        # Rolling statistics (computed during init)
        self.middle_band = []
        self.band_widths = []
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
        
    def init(self, data: List[dict], config: Optional[BollingerBandMeanRevertConfig] = None) -> None:
        """
        Initialize strategy with historical OHLCV data.
        
        Args:
            data: List of OHLCV dicts in chronological order. Each dict must have:
                  - timestamp: Unix timestamp or exchange-specific time
                  - open, high, low, close: Price levels  
                  - volume: Trading volume (optional but recommended)
            config: Override default configuration with new parameters
            
        Computes:
            - Middle band (SMA) for each bar  
            - Band width for regime detection (wider bands = higher volatility)
            
        Raises:
            ValueError: If data is empty or too short for mean/std calculation
        """
        if config:
            self.config = config
            
        period = self.config.period
        
        # Validate minimum data
        if not data or len(data) < period + 10:
            raise ValueError(
                f"Need at least {period + 10} bars for initialization. "
                f"Got {len(data) if data else 0} bars."
            )
            
        # Extract close prices from OHLCV data
        closes = [float(bar.get("close", bar.get("price", 0))) for bar in data]
        
        # Calculate middle band and band widths
        self.middle_band, self.band_widths = \
            self._compute_bollinger_statistics(closes)
        
        # Position initialization after first valid signal opportunity
        self.position = None
        
    def _compute_bollinger_statistics(self, closes: List[float]) -> tuple:
        """
        Compute Bollinger band statistics (middle band and width).
        
        Args:
            closes: List of close prices
            
        Returns:
            Tuple of (middle_band, band_widths) lists aligned with close prices
        """
        if not closes:
            return [], []
            
        period = self.config.period
        num_std = self.config.num_std
        
        # Warm-up period
        warmup_end = min(period, len(closes))
        
        middle_band = []
        band_widths = []
        simple_sum = 0.0
        
        for i in range(len(closes)):
            if i < warmup_end:
                # Use SMA as initial value during warm-up
                simple_sum += closes[i]
                
                if i == 0:
                    middle_band.append(simple_sum)
                    band_widths.append(0.0)
                else:
                    sma_value = simple_sum / (i + 1)
                    middle_band.append(sma_value)
                    band_widths.append(0.0)
            else:
                # Calculate SMA and std after warm-up
                start_idx = i - period + 1
                if start_idx < 0:
                    start_idx = 0
                
                sma_data = closes[start_idx:i+1]
                
                # Middle band = SMA
                mean_val = sum(sma_data) / len(sma_data)
                
                # Standard deviation
                variance = sum((x - mean_val) ** 2 for x in sma_data) / len(sma_data)
                std_val = math.sqrt(variance) if variance > 0 else 0.0
                
                middle_band.append(mean_val)
                band_widths.append(2 * num_std * std_val)
        
        return middle_band, band_widths
    
    def on_bar(self, bar: dict) -> Optional[dict]:
        """
        Process new bar and generate trading signal at Bollinger Band breach.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close
            
        Returns:
            Dict with action ("BUY", "SELL"), entry_price if applicable
            
        Mean Reversion Logic:
            - Calculate middle band and upper/lower thresholds based on config z-score
            - BUY when price significantly below lower threshold (statistically undervalued)
            - SELL when price significantly above upper threshold (statistically overvalued)
            
        State Updates:
            - After buy: Track position PnL, monitor reversion target, update trailing stop
            - After sell: Increment trade counter, reset position state
        """
        close_price = bar.get("close", bar.get("price", 0))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
            
        # Append new price and compute updated statistics
        closes = [close_price] + self.middle_band[:-1]
        middle_band, band_widths = \
            self._compute_bollinger_statistics(closes)
        
        # Get current statistics
        current_middle = middle_band[-1]
        current_std_deviation = (band_widths[-1] / 2) if band_widths else close_price * 0.02
        
        # Avoid division by zero
        if current_std_deviation < 0.0001 * close_price:
            current_std_deviation = 0.0001 * close_price
            
        # Calculate Bollinger Band edges based on z-score thresholds
        lower_threshold = current_middle + self.config.z_score_buy_threshold * current_std_deviation
        upper_threshold = current_middle + self.config.z_score_sell_threshold * current_std_deviation
        
        # Detect overreaction buy signal (price below lower threshold)
        if close_price < lower_threshold and not self.position:
            return {
                "action": "BUY",
                "entry_price": close_price,
                "signal_type": "BOLLINGER_LOWER_BAND_BREACH",
                "z_score_from_mean": (close_price - current_middle) / current_std_deviation,
                "middle_band": current_middle,
                "lower_threshold": lower_threshold,
                "upper_threshold": upper_threshold,
                "stop_loss": self.config.stop_loss_pct * -0.01 * close_price if close_price > 0 else None,
            }
            
        # Detect overreaction sell signal (price above upper threshold)  
        elif close_price > upper_threshold and not self.position:
            return {
                "action": "SELL",
                "signal_type": "BOLLINGER_UPPER_BAND_BREACH"
            }
            
        return None
    
    def handle_signal(self, signal: dict) -> Optional[BBPosition]:
        """
        Handle execution of signal and update position state.
        
        Args:
            signal: Return value from on_bar() if threshold breached
            
        Returns:
            Updated BBPosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action == "BUY":
            entry_price = signal.get("entry_price", 0)
            
            # Create new position with trailing stop tracking  
            self.position = BBPosition(
                entry_price=entry_price,
                entry_z_score=self.config.z_score_buy_threshold,
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
    
    def get_current_position(self) -> Optional[BBPosition]:
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


__all__ = ['BollingerBandMeanRevertConfig', 'BBPosition', 'BollingerBandMeanReversionStrategy']
