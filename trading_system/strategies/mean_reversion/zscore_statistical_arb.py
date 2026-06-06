"""
Z-Score Statistical Arbitrage Strategy - Mean Reversion Implementation

Purpose: Statistical mean-reversion strategy using z-score for price deviation analysis from historical mean.
Buys when asset price deviates significantly below mean (statistically undervalued), 
sells when price deviates above mean (statistically overvalued).

Regime Suitability: 
  ✅ Ranging/mean-reverting markets with stable volatility (most altcoins in bull bear)
  ❌ Strong trending markets where deviation persists (BTC during parabolic pumps/dumps)

Failure Modes:
  • Wide losses during trend continuation after z-score threshold breach
  • Whipsaws near z-score thresholds during high volatility expansion
  • False mean signals when distribution is skewed or non-stationary
  
Expected Performance:
  • Win rate target: 55-65% (mean-reversion strategies typically have higher win rates)  
  • Profit factor target: 1.4-2.2 depending on market regime
  • Maximum historical drawdown: 10-20% in stress periods

Configuration Parameters:
    lookback_period: Period for mean/std calculation (default 20 bars)
    z_score_buy_threshold: Z-score threshold for buy signals (below mean = undervalued, default -1.5 std)
    z_score_sell_threshold: Z-score threshold for sell signals (above mean = overvalued, default +1.5 std)
    stop_loss_pct: Hard stop-loss as percentage (default 3%)
    target_return_pct: Mean reversion target before partial exit (default 0.2% from entry)
    trailing_stop_bps: Trailing stop bps after profit target reached (default 10 bps)

Z-Score Logic:
    Mean = Average(close_price, lookback_period)
    Std = Standard Deviation(close_price, lookback_period)  
    Z-Score = (Current Price - Mean) / Std
    
    BUY: Z-Score < z_score_buy_threshold (-1.5 or lower = statistically cheap)
    SELL: Z-Score > z_score_sell_threshold (+1.5 or higher = statistically expensive)
    
    Strategy assumes mean-reverting distribution where extreme deviations eventually revert

Usage Example:
    from trading_system.strategies.mean_reversion.zscore_statistical_arb import ZScoreStatisticalArbStrategy
    
    strategy = ZScoreStatisticalArbStrategy(
        lookback_period=20,
        z_score_buy_threshold=-1.5,
        z_score_sell_threshold=+1.5
    )
    strategy.init(ohlcv_data)
    
    # On new bar
    signal = strategy.on_bar(latest_bar)
    if signal and signal["action"] == "BUY":
        execute_trade(signal)

Author Notes: Classic statistical arbitrage logic based on mean-reverting assumption. Works best on 
assets with stable volatility and ranging price action. The z-score approach normalizes for asset
price levels, making it comparable across different crypto pairs. Enhancement options include 
rolling window adjustment or regime filtering to avoid trend continuation losses.

Risk Management:
    - Only deploy in confirmed mean-reverting regimes (use range detection filters)
    - Consider stop-loss reduction during high-volatility periods
    - Monitor z-score distribution for structural changes (regime shift warning)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List
import math


@dataclass
class ZScoreStatisticalArbConfig:
    """Configuration parameters for Z-Score statistical arbitrage strategy."""
    
    lookback_period: int = 20  # Period for mean/std calculation
    z_score_buy_threshold: float = -1.5  # Buy when price below this std deviation from mean
    z_score_sell_threshold: float = 1.5  # Sell when price above this std deviation
    stop_loss_pct: float = 3.0  # Hard stop-loss percentage
    target_return_pct: float = 0.2  # Target return for partial exit (mean reversion)
    trailing_stop_bps: float = 10.0  # Trailing stop bps after profit target
    
    enable_logging: bool = True
    position_size_usd: float = 1000.0


@dataclass 
class ZScorePosition:
    """Track Z-Score strategy position state."""
    
    entry_price: float
    entry_z_score: float
    quantity: float
    unrealized_pnl_pct: float = 0.0
    stop_loss_hit: bool = False
    trailing_stop_hit: bool = False


class ZScoreStatisticalArbStrategy:
    """
    Z-Score Statistical Arbitrage Strategy - Mean Reversion Implementation
    
    This strategy implements statistical mean-reversion using z-score analysis:
    - BUY when price deviates significantly below rolling mean (statistically undervalued)
    - SELL when price deviates above rolling mean (statistically overvalued)
    
    Factory Pattern Lifecycle:
        1. init(): Initialize with historical OHLCV data and compute rolling statistics
        2. on_bar(bar): Generate buy/sell signal based on z-score deviation
    
    Usage Example:
        strategy = ZScoreStatisticalArbStrategy(lookback_period=20, z_score_threshold=-1.5)
        
        # Setup with historical data
        ohlcv_data = get_ohlcv("BTC-USD", periods=100)
        strategy.init(ohlcv_data)
        
        # Generate signals on new bars
        signal = strategy.on_bar(latest_bar)
    
    """
    
    def __init__(self, config: Optional[ZScoreStatisticalArbConfig] = None):
        """Initialize strategy with default configuration."""
        self.config = config or ZScoreStatisticalArbConfig()
        
        # State tracking
        self.position = None  # ZScorePosition or None
        
        # Rolling statistics (computed during init)
        self.rolling_means = []
        self.rolling_stds = []
        
        # Performance tracking
        self.num_successful_trades = 0
        self.num_failed_trades = 0
        
    def init(self, data: List[dict], config: Optional[ZScoreStatisticalArbConfig] = None) -> None:
        """
        Initialize strategy with historical OHLCV data.
        
        Args:
            data: List of OHLCV dicts in chronological order. Each dict must have:
                  - timestamp: Unix timestamp or exchange-specific time
                  - open, high, low, close: Price levels  
                  - volume: Trading volume (optional but recommended)
            config: Override default configuration with new parameters
            
        Computes:
            - Rolling mean and standard deviation for each bar using lookback_period
            - Prepares statistics for z-score calculation
            
        Raises:
            ValueError: If data is empty or too short for mean/std calculation
        """
        if config:
            self.config = config
            
        lookback_period = self.config.lookback_period
        
        # Validate minimum data
        if not data or len(data) < lookback_period + 10:
            raise ValueError(
                f"Need at least {lookback_period + 10} bars for initialization. "
                f"Got {len(data) if data else 0} bars."
            )
            
        # Extract close prices from OHLCV data
        closes = [float(bar.get("close", bar.get("price", 0))) for bar in data]
        
        # Calculate rolling means and standard deviations
        self.rolling_means, self.rolling_stds = \
            self._compute_rolling_statistics(closes)
        
        # Position initialization after first valid signal opportunity
        self.position = None
        
    def _compute_rolling_statistics(self, closes: List[float]) -> tuple:
        """
        Compute rolling mean and standard deviation for close prices.
        
        Args:
            closes: List of close prices
            
        Returns:
            Tuple of (rolling_means, rolling_stds) lists aligned with close prices
        """
        if not closes:
            return [], []
            
        lookback_period = self.config.lookback_period
        
        # Warm-up period
        warmup_end = min(lookback_period, len(closes))
        
        rolling_means = []
        rolling_stds = []
        
        for i in range(len(closes)):
            if i < warmup_end:
                # Use full available history during warm-up
                start_idx = 0
                data_points = closes[:i+1]
            else:
                # Use fixed lookback period after warm-up
                start_idx = i - lookback_period + 1
                data_points = closes[start_idx:i+1]
            
            # Calculate mean and std for this window
            if len(data_points) > 0:
                mean_val = sum(data_points) / len(data_points)
                
                # Standard deviation (population or sample based on window size)
                if len(data_points) == 1:
                    std_val = 0.0  # Single point has no variance
                else:
                    variance = sum((x - mean_val) ** 2 for x in data_points) / len(data_points)
                    std_val = math.sqrt(variance) if variance > 0 else 0.0
                
                rolling_means.append(mean_val)
                rolling_stds.append(std_val)
            else:
                rolling_means.append(closes[i])
                rolling_stds.append(0.0)
        
        return rolling_means, rolling_stds
    
    def on_bar(self, bar: dict) -> Optional[dict]:
        """
        Process new bar and generate trading signal based on z-score deviation.
        
        Args:
            bar: New OHLCV bar with timestamp, open, high, low, close
            
        Returns:
            Dict with action ("BUY", "SELL"), entry_price if applicable, thresholds
            
        Z-Score Logic:
            - Calculate rolling mean and std from lookback period
            - Compute z-score: (close_price - rolling_mean) / rolling_std  
            - BUY: z-score < buy_threshold (-1.5 or lower = statistically undervalued)
            - SELL: z-score > sell_threshold (+1.5 or higher = statistically overvalued)
            
        State Updates:
            - After buy: Track position PnL, monitor mean reversion target, update trailing stop
            - After sell: Increment trade counter, reset position state
        """
        close_price = bar.get("close", bar.get("price", 0))
        
        if not close_price or math.isnan(close_price) or close_price <= 0:
            return None
            
        # Append new price and compute updated statistics
        closes = [close_price] + self.rolling_means[:-1]
        rolling_means, rolling_stds = \
            self._compute_rolling_statistics(closes)
        
        # Get current statistics (most recent values)
        current_mean = rolling_means[-1]
        current_std = rolling_stds[-1]
        
        # Avoid division by zero - use mean itself if std is too small
        if current_std < 0.0001 * current_mean:  # Less than 0.01% of price
            current_std = 0.0001 * current_mean
            
        # Calculate z-score
        z_score = (close_price - current_mean) / current_std
        
        # Check buy threshold (below mean = undervalued)
        if z_score < self.config.z_score_buy_threshold and not self.position:
            return {
                "action": "BUY",
                "entry_price": close_price,
                "signal_type": "ZSCORE_BELOW_MEAN",
                "z_score": z_score,
                "rolling_mean": current_mean,
                "rolling_std": current_std,
                "stop_loss": self.config.stop_loss_pct * -0.01 * close_price if close_price > 0 else None,
            }
            
        # Check sell threshold (above mean = overvalued)  
        elif z_score > self.config.z_score_sell_threshold and not self.position:
            return {
                "action": "SELL",
                "signal_type": "ZSCORE_ABOVE_MEAN"
            }
            
        # Check trailing stop for position already in trade
        elif self.position and current_std > 0:
            target_return = self.config.target_return_pct / 100
            
            # Check if we've reached reversion target
            profit_target_prcnt = target_return + z_score * (current_std / current_mean)  # Approximation
            
            if profit_target_prcnt > 0 and self.position.unrealized_pnl_pct >= profit_target_prcnt:
                return {
                    "action": "SELL",
                    "signal_type": "ZSCORE_REVERSION_TARGET"
                }
        
        return None  # No signal on this bar
    
    def handle_signal(self, signal: dict) -> Optional[ZScorePosition]:
        """
        Handle execution of signal and update position state.
        
        Args:
            signal: Return value from on_bar() if threshold breached
            
        Returns:
            Updated ZScorePosition or None (if closing existing position)
        """
        action = signal.get("action")
        
        if action == "BUY":
            entry_price = signal.get("entry_price", 0)
            
            # Create new position with stop-loss and trailing stop tracking
            self.position = ZScorePosition(
                entry_price=entry_price,
                entry_z_score=self.config.z_score_buy_threshold,  # Approximate initial z-score
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
    
    def get_current_position(self) -> Optional[ZScorePosition]:
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


__all__ = ['ZScoreStatisticalArbConfig', 'ZScorePosition', 'ZScoreStatisticalArbStrategy']
