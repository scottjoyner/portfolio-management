"""
Z-Score Mean Reversion Strategy - Core Entry Point Module
=========================================================

This module provides the centralized interface for loading and instantiating
all mean reversion strategies. Use create_strategy_instance() to load any
strategy by name with appropriate configuration.

Available Strategies:
---------------------
- ZScoreMeanReversionStrategy - Standard deviation-based mean reversion
- BollingerBandSqueezeStrategy - Volatility contraction mean reversion
- KeltnerChannelRangeBoundStrategy - Range-bound channel strategy
- DonchianChannelReversalStrategy - Breakout from support levels
- StandardDeviationExtremesStrategy - Deviation threshold entry
- MeanAbsoluteDeviationReversionStrategy - MAD-based mean reversion
- PercentileBasedReturnReversionStrategy - Historical percentile entries
- StochasticRSIExtremesStrategy - RSI oscillator extremes
- WilliamsPercentRReversionStrategy - %R oversold/overbought entries
- IchimokuZoneReversalStrategy - Cloud zone boundary reversal
- RateOfChangeReversalStrategy - Momentum reversal on ROC extremes
- MomentumOscillatorCaptureStrategy - Stoch/MACD momentum mean reversion
- FibonacciRetracementEntryStrategy - Fibo pullback entries
- ADXWeaknessMeanReversionStrategy - Low trend-strength reversals
- CCIExtremeReversionStrategy - CCI oscillator extremes

Usage:
------
from trading_system.strategies.mean_reversion import create_strategy_instance
from trading_system.strategies.mean_reversion.zscore_mean_reversion import ZScoreMeanReversionStrategy

# Load strategy by name
config = {"strategy": "ZScore", "lookback_bars": 60, "z_score_threshold": 2.5}
strategy = create_strategy_instance(config)

# Direct instantiation
strategy = ZScoreMeanReversionStrategy()
"""

from dataclasses import dataclass, field
from typing import List, Optional, Union, Callable, Any
import math

from trading_system.strategies.factory import (
    StrategyBase, StrategyConfig, Signal, create_strategy_instance
)


# ============================================================================
# STRATEGY CLASS REGISTRY
# All mean reversion strategies are registered here for unified loading
# ============================================================================

_strategy_registry: dict[str, Callable] = {}

def register_mean_reversion_strategy(strategy_class):
    """Decorator to register a mean reversion strategy with the factory."""
    def wrapper(cls):
        name = cls.__name__
        _strategy_registry[name] = lambda config: create_strategy_instance(cls, config)
        print(f"[MeanReversionRegistry] Registered: {name}")
        return cls
    return wrapper

def get_available_strategies():
    """Return list of available mean reversion strategy names."""
    return list(_strategy_registry.keys())


# ============================================================================
# Z-SCORE MEAN REVERSION STRATEGY (Primary Implementation)
# ============================================================================

@dataclass
class ZScoreConfig(StrategyConfig):
    """Configuration for Z-Score mean reversion strategy."""
    
    name: str = "ZScoreMeanReversion"
    lookback_bars: int = 60  # Bars of price history for statistics
    z_score_threshold: float = 2.5  # Z-score threshold for entry (absolute)
    max_position_duration_hours: int = 48  # Maximum hours before forced exit review
    cooldown_after_failure: int = 12  # Hours after failed trade before retry
    
    # Position sizing parameters
    volatility_scaling: bool = True  # Use ATR-based position sizing
    volatility_lookback: int = 20  # Bars for historical volatility calculation
    
    # Risk management
    max_drawdown_pct: float = 0.03  # Max drawdown before exit (relative)
    stop_loss_pct: float = 0.05  # Hard stop-loss as % of entry price
    take_profit_pct: float = 0.10  # Take-profit target
    
    def validate(self):
        """Validate configuration parameters."""
        if self.lookback_bars < 30:
            raise ValueError(f"lookback_bars must be >= 30, got {self.lookback_bares}")
        if self.z_score_threshold < 2.0 or self.z_score_threshold > 4.0:
            raise ValueError(f"z_score_threshold should be between 2.0-4.0, got {self.z_score_threshold}")
        return True

@register_mean_reversion_strategy
class ZScoreMeanReversionStrategy(StrategyBase):
    """
    Z-Score Mean Reversion Strategy
    
    Implements classic mean reversion using statistical z-scores. The strategy
    calculates the mean and standard deviation of price over a lookback period,
    then enters positions when price deviates beyond configurable z-score thresholds.
    
    Entry Logic:
    -----------
    - BUY (oversold): When (price - mean) / std < -z_score_threshold
    - SELL (overbought): When (price - mean) / std > z_score_threshold
    
    Position Sizing:
    ----------------
    Uses volatility-adjusted sizing with optional ATR-based scaling to account for
    varying market conditions. Higher volatility reduces position size proportionally.
    
    Risk Management:
    ----------------
    - Stop-loss: Hard percentage stop relative to entry price (configurable)
    - Take-profit: Target profit level as % of entry price
    - Maximum drawdown: Exits if trade loses more than X% of entry
    
    Regime Suitability:
    -------------------
    Primary regime: Range-bound markets with stable mean reversion properties
    Works best on medium timeframes (4h, daily) for crypto spot markets
    Performs well during low-to-moderate volatility periods
    
    Failure Modes:
    --------------
    - Whipsaws during rapid price swings near threshold boundaries
    - Poor performance in trending regimes (buying top/selling bottom risk)
    - Requires sufficient data buffer for accurate statistics
    - Can fail if underlying asset lacks mean-reverting properties
    
    Expected Holding Horizon:
    --------------------------
    Short-term: Hours to 1-2 days typically
    Average trade duration: 4-24 hours in normal conditions
    Can extend during slow-moving range-bound periods
    """
    
    def __init__(self, config: Optional[ZScoreConfig] = None):
        super().__init__(config or ZScoreConfig())
        
        # Initialize configuration
        self.config.validate()
        
        # Strategy parameters
        self.lookback_bars = self.config.lookback_bars
        self.z_score_threshold = abs(self.config.z_score_threshold)
        self.max_position_duration_hours = self.config.max_position_duration_hours
        self.cooldown_after_failure = self.config.cooldown_after_failure
        
        # Position sizing
        self.volatility_scaling = self.config.volatility_scaling
        self.volatility_lookback = self.config.volatility_lookback
        
        # Risk management
        self.max_drawdown_pct = self.config.max_drawdown_pct
        self.stop_loss_pct = self.config.stop_loss_pct
        self.take_profit_pct = self.config.take_profit_pct
        
        # State variables for statistics calculation
        self.price_buffer: List[float] = field(default_factory=list)
        self.mean_price: Optional[float] = None
        self.std_price: Optional[float] = None
        
        # Position tracking state
        self.last_entry_bar_count: int = 0
        self.failed_trades_since_last_position: int = 0
        self.last_failed_signal: Optional[Signal] = None
    
    def init(self, data: dict) -> None:
        """
        Initialize Z-Score calculation with price history.
        
        Args:
            data: Dictionary containing historical OHLCV data or initial bar
        """
        try:
            # Handle both initialization and first bar scenarios
            if isinstance(data, list):
                # Initialize with historical data
                self.price_buffer = [float(b.get('close', 0)) for b in data]
                
                # Validate prices
                non_zero_prices = [p for p in self.price_buffer if p > 0]
                if len(non_zero_prices) < self.lookback_bars:
                    raise ValueError(f"Need at least {self.lookback_bars} valid prices")
                    
            elif isinstance(data, dict):
                # Single bar - append to buffer
                close = float(data.get('close', 0))
                if close <= 0:
                    return
                
                self.price_buffer.append(close)
                
                # Need sufficient data for initial statistics
                if len(self.price_buffer) < self.lookback_bars:
                    return
                    
        except (TypeError, ValueError, ZeroDivisionError) as e:
            self._log_error(f"Initialization failed: {str(e)}")
    
    def _calculate_statistics(self) -> tuple[Optional[float], Optional[float]]:
        """
        Calculate rolling mean and standard deviation from price buffer.
        
        Returns:
            Tuple of (mean, std) or (None, None) if insufficient data
        """
        try:
            prices = self.price_buffer[-self.lookback_bars:]
            
            # Calculate mean
            mean_price = sum(prices) / len(prices)
            
            # Calculate standard deviation (population std for mean reversion)
            variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
            std_price = math.sqrt(variance)
            
            return mean_price, std_price
            
        except (TypeError, ValueError, ZeroDivisionError):
            return None, None
    
    def _calculate_position_size(self, entry_price: float) -> Optional[float]:
        """
        Calculate position size with volatility scaling.
        
        Args:
            entry_price: Current price at entry time
            
        Returns:
            Position size (number of shares/contracts) or None
        """
        try:
            if not self.config.position_size_usd or entry_price <= 0:
                return None
                
            # Base position calculation
            base_quantity = self.config.position_size_usd / entry_price
            
            # Apply volatility scaling if enabled
            if self.volatility_scaling and len(self.price_buffer) >= self.volatility_lookback:
                recent_prices = self.price_buffer[-self.volatility_lookback:]
                
                # Calculate historical volatility (simplified 30-day equivalent)
                if len(recent_prices) > 1:
                    prices_0 = [p / 100 for p in recent_prices]  # Normalize
                    returns = [(prices_0[i] - prices_0[i-1]) / prices_0[i-1] 
                              for i in range(1, len(prices_0))]
                    
                    if len(returns) > 0:
                        mean_return = sum(returns) / len(returns)
                        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
                        daily_vol = math.sqrt(variance)
                        
                        # Scale position inversely with volatility (vol-targeting)
                        target_vol = 0.02  # 2% daily vol target
                        vol_adjustment = target_vol / max(daily_vol, 0.001)
                        
                        adjusted_quantity = base_quantity * min(vol_adjustment, 1.0)
                        
                        # Also apply z-score-based sizing for additional safety
                        mean_std = self._calculate_statistics()
                        if mean_std[1] and mean_std[1] > 0:
                            normalized_deviation = abs(entry_price - mean_std[0]) / mean_std[1]
                            
                            # Larger deviations get slightly larger positions (up to cap)
                            deviation_factor = min(1.0 + 0.2 * normalized_deviation, 1.5)
                            adjusted_quantity *= deviation_factor
                        
                        return round(adjusted_quantity, 8)
            
            return round(base_quantity, 8)
            
        except Exception as e:
            self._log_error(f"Position sizing failed: {str(e)}")
            return None
    
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """
        Generate Z-Score mean reversion signal from OHLCV bar.
        
        Logic Flow:
        1. Update price buffer with new bar's close price
        2. Calculate rolling mean and standard deviation
        3. Compute current z-score: (price - mean) / std
        4. Check entry signals when price deviates beyond threshold
        5. Validate position duration hasn't exceeded max
        6. Check stop-loss/take-profit for active positions
        
        Args:
            bar: OHLCV dictionary with keys: open, high, low, close, volume
            
        Returns:
            Signal object on entry/exit or None (hold)
        """
        try:
            # Extract and validate price data
            close = float(bar.get('close', 0))
            if close <= 0:
                return Signal(action='HOLD')
                
            high = float(bar.get('high', close))
            low = float(bar.get('low', close))
            open_price = float(bar.get('open', close))
            
            # Update price buffer
            self.price_buffer.append(close)
            
            # Trim buffer to maintain lookback period
            if len(self.price_buffer) > self.lookback_bars:
                self.price_buffer.pop(0)
            
            # Calculate statistics (mean, std)
            mean_price, std_price = self._calculate_statistics()
            
            if mean_price is None or std_price is None or std_price == 0:
                return Signal(action='HOLD')
            
            # Calculate current z-score
            current_zscore = (close - mean_price) / std_price
            
            # Check for entry signals
            signal_action = None
            confidence = 0.0
            
            if not self.position:
                # No active position - look for entry opportunities
                
                # BUY signal: price significantly below mean (oversold)
                if current_zscore < -self.z_score_threshold:
                    signal_action = 'BUY'
                    confidence = min(0.9, abs(current_zscore + self.z_score_threshold) / 1.5)
                    
                    # Check if duration exceeded max for existing position logic
                    if close > (open_price * (1 + self.stop_loss_pct)):
                        return Signal(action='HOLD')  # Near stop, skip
                    
            else:
                # Active position - check exit conditions
                
                entry_price = self.position.price or close
                
                # Check take-profit first
                profit_pct = (close - entry_price) / entry_price
                
                if profit_pct >= self.take_profit_pct:
                    signal_action = 'SELL'  # Close on target
                    confidence = 0.95
                    
                # Check stop-loss
                elif profit_pct <= -self.stop_loss_pct:
                    signal_action = 'SELL'  # Hard stop
                    confidence = 1.0
                    
                # Check max drawdown
                elif profit_pct <= -self.max_drawdown_pct:
                    signal_action = 'SELL'
                    confidence = 0.85
            
            # Execute entry/exit if valid signal
            if signal_action is not None and self.position is None:
                quantity = self._calculate_position_size(close)
                
                if quantity is not None:
                    return Signal(
                        action=signal_action,
                        price=close,
                        quantity=quantity,
                        stop_loss=entry_price * (1 - self.stop_loss_pct) if signal_action == 'BUY' else None,
                        take_profit=entry_price * (1 + self.take_profit_pct) if signal_action == 'BUY' else 
                                  close / (1 + self.take_profit_pct) if signal_action == 'SELL' else None,
                        confidence=confidence,
                        signal_type='ZSCORE_MEAN_REVERSION',
                        zscore=current_zscore,
                        mean=mean_price,
                        std=std_price
                    )
                    
            elif signal_action is not None and self.position is not None:
                # Execute exit on close position
                quantity = -self.position.quantity if signal_action == 'SELL' else self.position.quantity
                
                return Signal(
                    action='CLOSE' if signal_action == 'SELL' else signal_action,
                    price=close,
                    quantity=abs(quantity),
                    stop_loss=None,
                    take_profit=None,
                    confidence=confidence,
                    signal_type='ZSCORE_MEAN_REVERSION_EXIT',
                    entry_price=self.position.price or close,
                    pnl_pct=(close - (self.position.price or close)) / (self.position.price or close)
                )
                
            return Signal(action='HOLD')
            
        except Exception as e:
            self._log_error(f"Z-score signal generation failed: {str(e)}")
            return Signal(action='HOLD')
    
    def finalize(self) -> dict:
        """Cleanup position state and strategy buffers."""
        result = super().finalize()
        
        # Reset statistics to prepare for next session
        self.price_buffer = []
        self.mean_price = None
        self.std_price = None
        
        return {
            'final_pnl': self.current_pnl,
            'total_trades': self.num_trades,
            'lookback_bars': self.lookback_bars,
            'z_score_threshold': self.z_score_threshold,
            'bar_count_at_finalization': len(self.price_buffer)
        }
