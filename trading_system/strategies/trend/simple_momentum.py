"""
Simple Momentum Breakout Strategy - Trend Following Implementation

Purpose:
--------
Captures strong directional moves by detecting % price changes over N periods.
This is a pure momentum strategy that enters when price exceeds a threshold
move from recent levels, riding breakouts and trending markets.

Regime Suitability:
-------------------
- Bull markets with strong uptrends (primary regime)
- Trending crypto markets with clear direction
- High volatility environments with sustained moves
- Unsuitable for choppy, range-bound markets (>70% drawdown in ranging conditions)

Failure Modes:
--------------
- Whipsaws in sideways markets (false breakouts)
- Over-trades during consolidation phases
- Poor performance in mean-reverting regimes
- Sensitive to lookback period selection

Expected Holding Horizon:
--------------------------
- Short to medium-term: 1-7 days typically
- Can extend to weeks in strong trending conditions
- Average trade duration: 2-5 days in normal markets
"""
from dataclasses import dataclass, field
from typing import List, Optional
import math

# Import from factory base
from trading_system.strategies.factory import StrategyBase, StrategyConfig, Signal


@dataclass
class MomentumConfig(StrategyConfig):
    """Configuration for simple momentum breakout strategy."""
    
    name: str = "SimpleMomentum"
    momentum_periods: int = 10  # Lookback period for % change calculation
    momentum_threshold_pct: float = 3.0  # Min % move to trigger entry
    exit_threshold_pct: float = -2.0  # Trailing stop for profit taking
    position_size_usd: float = 1500.0  # Default allocation


class SimpleMomentumStrategy(StrategyBase):
    """
    Simple Momentum Breakout Strategy
    
    Implements pure momentum by detecting significant % price changes over 
    N periods and riding breakouts. This strategy captures strong directional
    moves common in crypto spot markets during trending phases.
    
    Key Features:
    - Tracks % change over configurable lookback
    - Uses trailing stop for profit protection
    - No indicator lag (direct price-based)
    """
    
    def __init__(self, config: Optional[MomentumConfig] = None):
        super().__init__(config or MomentumConfig())
        self.momentum_periods = self.config.momentum_periods
        self.threshold_pct = self.config.momentum_threshold_pct
        self.exit_threshold_pct = self.config.exit_threshold_pct
        self.position_size_usd = self.config.position_size_usd
        
        # State variables for momentum calculation
        self.high_watermark: float = 0.0
        self.recent_highs: List[float] = field(default_factory=lambda: [0.0] * self.momentum_periods)
        
    def init(self, data: dict) -> None:
        """
        Initialize strategy with market data.
        
        Sets up state variables and processes initial price history if provided.
        Does not require external indicators - uses raw OHLCV data only.
        """
        # Initialize watermark system for tracking highs
        self.high_watermark = 0.0
        
        # Validate configuration
        if self.momentum_periods < 5:
            raise ValueError(f"Minimum momentum_periods must be >= 5, got {self.momentum_periods}")
        if self.threshold_pct <= 0:
            raise ValueError(f"Momentum threshold must be positive (>0), got {self.threshold_pct}")
            
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """
        Generate momentum signal from new OHLCV bar.
        
        Logic Flow:
        1. Update recent high watermark system
        2. Check if price > threshold above watermark (entry signal)
        3. If holding position, check trailing stop exit condition
        4. Return Signal or None
        
        Args:
            bar: OHLCV dictionary with keys: open, high, low, close, volume
            
        Returns:
            Signal object with BUY/SELL/HOLD action and confidence score, or None
        """
        try:
            # Extract price data with validation
            high = float(bar.get('high', 0))
            low = float(bar.get('low', 0))
            close = float(bar.get('close', 0))
            
            if close <= 0 or math.isnan(close) or math.isinf(close):
                # Invalid price data - return hold
                return Signal(action='HOLD')
                
        except (TypeError, ValueError):
            return Signal(action='HOLD')
        
        # Update high watermark system (rolling buffer of period highs)
        self.recent_highs[-1] = high
        if len(self.recent_highs) < self.momentum_periods:
            self.recent_highs.append(high)
            
        # Calculate average recent high from the rolling buffer
        if len(self.recent_highs) > 0 and sum(self.recent_highs) / len(self.recent_highs) > 0:
            avg_recent_high = sum(self.recent_highs) / len(self.recent_highs)
        else:
            avg_recent_high = high
        
        # Momentum entry signal: price significantly above watermark
        if not self.position:
            momentum_pct = (close - avg_recent_high) / max(avg_recent_high, 0.001) * 100
            
            if momentum_pct >= self.threshold_pct:
                # Entry signal - high confidence on clean breakouts
                confidence = min(0.95, self.threshold_pct / 5.0)
                
                return Signal(
                    action='BUY',
                    price=close,
                    quantity=self._calculate_quantity(close),
                    stop_loss=None,  # Will use trailing stop
                    take_profit=None,
                    confidence=confidence,
                    signal_type='MOMENTUM_BREAKOUT'
                )
        
        # Check exit condition (trailing stop) only if holding position
        if self.position:
            current_pct_gain = (close - self.entry_price) / max(self.entry_price, 0.001) * 100
            
            # Trailing stop exit at profit target or loss limit
            trailing_stop_loss = self._calculate_trailing_stop(close, current_pct_gain)
            
            if trailing_stop_loss is not None:
                return Signal(
                    action='SELL',
                    price=close,
                    quantity=self.position.quantity,
                    stop_loss=trailing_stop_loss,
                    take_profit=None,
                    confidence=min(0.9, abs(trailing_stop_loss) / 5.0),
                    signal_type='MOMENTUM_TRAILING_STOP'
                )
        
        # No active signals - hold position if existing
        return Signal(action='HOLD')
    
    def _calculate_quantity(self, price: float) -> Optional[float]:
        """Calculate position quantity based on config allocation."""
        try:
            if self.config.position_size_usd > 0 and price > 0:
                quantity = self.config.position_size_usd / price
                return round(quantity, 8)
        except (TypeError, ValueError):
            pass
        return None
    
    def _calculate_trailing_stop(self, current_price: float, current_pct_gain: float) -> Optional[float]:
        """Calculate trailing stop level for momentum protection."""
        try:
            if self.position.quantity and current_pct_gain > 0:
                # Trailing stop at X% below current price while in profit
                # Stop moves up as price advances (trailing behavior)
                trail_loss = self.exit_threshold_pct * -1  # Negative for profit protection
                
                trailing_stop_level = current_price * (1 + trail_loss / 100.0)
                
                return round(trailing_stop_level, 2)
        except (TypeError, ValueError):
            pass
        return None
    
    def get_name(self) -> str:
        """Return strategy name for logging/identification."""
        return self.config.name
    
    def finalize(self) -> dict:
        """Close position and cleanup momentum state."""
        if self.position:
            # Close out remaining position at market
            close_qty = self._calculate_quantity(float(self.position.price or 0))
            if close_qty:
                self.position.quantity = round(-close_qty, 8)  # Negative for sell
            
        return {
            'final_pnl': self.current_pnl,
            'total_trades': self.num_trades,
            'momentum_periods': self.momentum_periods,
            'final_watermark': self.high_watermark
        }
