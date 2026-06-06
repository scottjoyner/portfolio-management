"""
MACD Signal Crossover Strategy - Trend Following Implementation

Purpose:
--------
Identifies trend changes using Moving Average Convergence Divergence (MACD) 
indicator signals. This classic momentum indicator combines two EMAs and tracks
their convergence/divergence to generate crossover buy/sell signals.

Regime Suitability:
-------------------
- Primary regime: Trending markets with clear directional moves
- Works well on medium timeframes (4h, daily)
- Good for catching sustained trends in crypto spot markets
- Less effective in choppy/range-bound markets (false crossovers frequent)

Failure Modes:
--------------
- Whipsaws during consolidation periods
- Lag-induced false signals at trend tops/bottoms
- Poor performance in low-volatility regimes
- Requires sufficient data buffer for indicator calculation

Expected Holding Horizon:
--------------------------
- Medium-term: 3-10 days typically
- Can extend to weeks in strong trending conditions
- Average trade duration: 4-7 days in normal markets
"""
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
import math

# Import from factory base
from trading_system.strategies.factory import StrategyBase, StrategyConfig, Signal


@dataclass
class MACDConfig(StrategyConfig):
    """Configuration for MACD crossover strategy."""
    
    name: str = "MACDCrossover"
    fast_ema_period: int = 12  # Fast EMA period for MACD
    slow_ema_period: int = 26   # Slow EMA period for MACD 
    signal_ema_period: int = 9  # Signal line (EMA of MACD histogram)
    threshold_pct: float = 0.5  # % move to exit crossover trades


class MACDCrossoverStrategy(StrategyBase):
    """
    MACD Signal Crossover Strategy
    
    Implements trend following using the MACD oscillator which combines 
    exponential moving averages. Generates signals when the MACD line crosses
    above/below its signal line, riding momentum in trending crypto markets.
    
    Key Features:
    - Standard MACD indicator (12/26/9 parameters)
    - Tracks histogram for momentum confirmation
    - Exit on opposite crossover or trailing threshold
    """
    
    def __init__(self, config: Optional[MACDConfig] = None):
        super().__init__(config or MACDConfig())
        self.fast_period = self.config.fast_ema_period
        self.slow_period = self.config.slow_ema_period
        self.signal_period = self.config.signal_ema_period
        self.exit_threshold_pct = self.config.threshold_pct
        
        # State variables for MACD calculation
        self.macd_values: List[float] = field(default_factory=list)
        self.signal_line_values: List[float] = field(default_factory=list)
        self.histogram_values: List[float] = field(default_factory=list)
        
        # EMA calculations need buffers
        self.fast_ema_buffer: List[float] = field(default_factory=lambda: [0.0] * 150)
        self.slow_ema_buffer: List[float] = field(default_factory=lambda: [0.0] * 150)
        
    def init(self, data: dict) -> None:
        """
        Initialize MACD calculation with price history.
        
        Requires sufficient OHLCV data to populate EMA buffers 
        for accurate indicator calculation on first bar.
        """
        # Validate configuration
        if self.fast_period < 5:
            raise ValueError(f"MACD fast_ema_period must be >= 5, got {self.fast_period}")
        if self.slow_period <= self.fast_period:
            raise ValueError(f"slow_ema_period ({self.slow_period}) must be > fast_ema_period ({self.fast_period})")
        if self.signal_period < 3:
            raise ValueError(f"signal_ema_period must be >= 3, got {self.signal_period}")
            
    def _calculate_ema(self, prices: List[float], period: int, buffer: List[float]) -> Optional[float]:
        """Calculate EMA given price history and buffer."""
        try:
            if len(prices) < 2:
                return None
                
            # Calculate multiplier for EMA
            mult = 100 / (period + 100)
            
            # Initialize with simple moving average of first 'period' bars
            sma_sum = sum(prices[:period]) if len(prices) >= period else sum(prices)
            ema = sma_sum / period
            
            # Update EMA with remaining prices
            for price in prices[period:]:
                ema = (price * mult) + (ema * (1 - mult))
            
            return ema
                
        except (TypeError, ValueError, ZeroDivisionError):
            return None
    
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """
        Generate MACD crossover signal from new OHLCV bar.
        
        Logic Flow:
        1. Calculate fast and slow EMAs from price history
        2. Compute MACD line (fast - slow EMA)
        3. Calculate signal line (EMA of MACD histogram)
        4. Detect crossover signals (MACD crosses signal line)
        5. Check exit conditions for active positions
        
        Args:
            bar: OHLCV dictionary with keys: open, high, low, close, volume
            
        Returns:
            Signal object on crossover or None (hold)
        """
        try:
            # Extract price data
            close = float(bar.get('close', 0))
            if close <= 0:
                return Signal(action='HOLD')
            
            except (TypeError, ValueError):
                return Signal(action='HOLD')
            
            # Append new price to buffers and trim old values
            self.fast_ema_buffer[-1] = close
            if len(self.fast_ema_buffer) < 20:
                self.fast_ema_buffer.append(close)
            else:
                self.fast_ema_buffer.pop(0)
                
            self.slow_ema_buffer[-1] = close
            if len(self.slow_ema_buffer) < 40:
                self.slow_ema_buffer.append(close)
            else:
                self.slow_ema_buffer.pop(0)
            
            # Calculate fast and slow EMAs (using buffered prices)
            fast_ema = self._calculate_ema(self.fast_ema_buffer, self.fast_period, [])
            slow_ema = self._calculate_ema(self.slow_ema_buffer, self.slow_period, [])
            
            if fast_ema is None or slow_ema is None:
                return Signal(action='HOLD')
            
            # Calculate MACD line and histogram
            macd_line = fast_ema - slow_ema
            
            # Calculate MACD histogram (if we have history)
            if len(self.histogram_values) > 0:
                prev_macd = self.histogram_values[-1]
            else:
                prev_macd = 0.0
                
            histogram = macd_line - prev_macd
            
            # Update values to lists
            self.macd_values.append(macd_line)
            if len(self.macd_values) < self.signal_period + 1:
                self.signal_line_values.append(0.0)
            else:
                self.signal_line_values[-1] = macd_line
            
            if len(self.signal_line_values) < self.signal_period:
                return Signal(action='HOLD')  # Need full signal line history
            
            # Calculate signal line (EMA of histogram)
            hist_sum = sum(self.macd_values[0:self.signal_period]) / self.signal_period
            signal_line = hist_sum
            
            if len(self.signal_values) < self.signal_period:
                return Signal(action='HOLD')
                
            current_macd = macd_line
            current_signal = signal_line
            
            # Check for crossover signals
            signal_action, new_position = None, None
            confidence = 0.0
            
            if not self.position:
                # Long entry: MACD crosses above signal line (bullish)
                if current_macd > current_signal and prev_macd <= prev_signal:
                    signal_action = 'BUY'
                    confidence = min(0.9, abs(current_macd - current_signal) / 0.1)
                # Short entry: MACD crosses below signal line (bearish)
                elif current_macd < current_signal and prev_macd >= prev_signal:
                    signal_action = 'SELL'
                    confidence = min(0.9, abs(current_macd - current_signal) / 0.1)
            else:
                # Check exit conditions for active position
                if self.position.action == 'BUY':
                    # Exit on bearish crossover or trailing threshold
                    if current_macd < current_signal and prev_macd >= prev_signal:
                        signal_action = 'SELL'
                        confidence = 0.85  # Strong confirmation required
                    # Check trailing threshold exit
                    elif self._check_trailing_exit(close, macd_line):
                        signal_action = 'SELL'
                        confidence = 0.7
                
                else:  # position.action == 'SELL'
                    if current_macd > current_signal and prev_macd <= prev_signal:
                        signal_action = 'BUY'
                        confidence = 0.85
                    elif self._check_trailing_exit(close, macd_line):
                        signal_action = 'BUY'
                        confidence = 0.7
            
            if signal_action is not None:
                return Signal(
                    action=signal_action,
                    price=close,
                    quantity=self.position.quantity if self.position else self._calculate_quantity(close),
                    stop_loss=None,
                    take_profit=None,
                    confidence=confidence,
                    signal_type='MACD_CROSSOVER'
                )
            
            # No crossover - hold position if exists
            return Signal(action='HOLD')
        
        except Exception as e:
            self._log_error(f"MACD calculation failed: {str(e)}")
            return Signal(action='HOLD')
    
    def _check_trailing_exit(self, current_price: float, indicator_value: float) -> bool:
        """Check if indicator has crossed trailing exit threshold."""
        try:
            if self.position.quantity and indicator_value > 0:
                # Exit if indicator drops X% from recent high (trailing behavior)
                recent_max = max([self.macd_values[i] for i in range(max(0, len(self.macd_values)-10), len(self.macd_values))])
                if current_price * indicator_value < recent_max * (1 - self.exit_threshold_pct / 100):
                    return True
        except Exception:
            pass
        return False
    
    def _calculate_quantity(self, price: float) -> Optional[float]:
        """Calculate position quantity based on config allocation."""
        try:
            if self.config.position_size_usd > 0 and price > 0:
                quantity = self.config.position_size_usd / price
                return round(quantity, 8)
        except (TypeError, ValueError):
            pass
        return None
    
    def get_name(self) -> str:
        """Return strategy name for logging/identification."""
        return self.config.name
    
    def finalize(self) -> dict:
        """Close position and cleanup MACD state."""
        if self.position:
            close_qty = self._calculate_quantity(float(self.position.price or 0))
            if close_qty:
                self.position.quantity = round(-close_qty, 8)
        
        return {
            'final_pnl': self.current_pnl,
            'total_trades': self.num_trades,
            'fast_period': self.fast_period,
            'slow_period': self.slow_period,
            'signal_line_period': self.signal_period
        }
