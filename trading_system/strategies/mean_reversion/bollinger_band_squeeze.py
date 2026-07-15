"""
Bollinger Band Squeeze Strategy
    
Purpose:
--------
Implements mean reversion using Bollinger Band volatility contraction signals.
The strategy identifies "squeeze" periods when bands narrow (low volatility) and
enters positions when price touches the upper or lower band boundaries during
these low-volatility regimes.

Regime Suitability:
-------------------
Primary regime: Range-bound markets following squeeze contractions
Works best on daily and 4-hour timeframes for crypto spot markets
Effective for catching mean reversion after volatility expansions
    
Failure Modes:
--------------
- Whipsaws during trend transitions (squeeze-to-trend breakouts)
- False signals when market shifts from mean-reverting to trending
- Poor performance if underlying asset lacks volatility clustering properties
    
Expected Holding Horizon:
--------------------------
Short-term: 6-48 hours typically
Average trade duration: 12-36 hours in normal conditions

"""

import math
from dataclasses import dataclass, field
from typing import Optional, List

from trading_system.strategies.mean_reversion.zscore_mean_reversion import register_mean_reversion_strategy
from trading_system.strategies.factory import Signal, StrategyBase

@dataclass
class BollingerBandSqueezeConfig:
    """Configuration for Bollinger Band squeeze strategy."""
    
    name: str = "BollingerBandSqueeze"
    bb_period: int = 20  # Period for moving average and band calculation
    bb_std_multiplier_lower: float = 2.0  # Lower band (2-3 std devs)
    bb_std_multiplier_upper: float = 2.5  # Upper band (2-3 std devs)
    squeeze_threshold_pct: float = 15.0  # Reduction in band width to trigger squeeze

@register_mean_reversion_strategy
class BollingerBandSqueezeStrategy(StrategyBase):
    """
    Bollinger Band Squeeze Mean Reversion Strategy
    
    Uses Bollinger Band volatility contraction to identify optimal mean reversion
    entry points. When bands narrow significantly (squeeze), the strategy enters
    positions at band boundaries, expecting price to revert toward the middle band.
    
    Entry Logic:
    -----------
    - BUY: Price touches or breaks below lower Bollinger Band during squeeze period
    - SELL: Price touches or breaks above upper Bollinger Band during squeeze period
    
    Position Sizing:
    ----------------
    Fixed allocation based on position_size_usd config, adjusted by band width ratio
    
    Risk Management:
    ----------------
    - Stop-loss: X% below entry (configurable via standard stop_loss_pct)
    - Take-profit: Revert to middle band or X% profit target
    
    Regime Suitability:
    -------------------
    Range-bound markets with volatility clustering properties
    Daily and 4-hour timeframes for crypto spot
    Works after low-volatility squeeze periods
    
    Failure Modes:
    --------------
    - Whipsaws when trend breaks during compression phases
    - False signals on volatile assets without mean reversion
    """
    
    def __init__(self, config: Optional[BollingerBandSqueezeConfig] = None):
        super().__init__(config or BollingerBandSqueezeConfig())
        
        self.bb_period = self.config.bb_period
        self.bb_lower_mult = self.config.bb_std_multiplier_lower
        self.bb_upper_mult = self.config.bb_std_multiplier_upper
        self.squeeze_threshold_pct = self.config.squeeze_threshold_pct
        
        # State variables
        self.price_buffer: List[float] = []
        self.ma_buffer: List[float] = [0.0] * max(self.bb_period + 1, 30)
        self.bb_lower: Optional[float] = None
        self.bb_upper: Optional[float] = None
        self.bb_width: Optional[float] = None
        self.prev_bb_width: Optional[float] = None
        self.squeeze_triggered: bool = False
        self.last_squeeze_bar_count: int = 0
    
    def init(self, data: dict) -> None:
        """Initialize with price history."""
        try:
            if isinstance(data, list):
                self.price_buffer = [float(b.get('close', 0)) for b in data]
                
                # Build initial MA buffer
                non_zero = [p for p in self.price_buffer if p > 0]
                if len(non_zero) >= self.bb_period:
                    self.ma_buffer[-1] = sum(non_zero[-self.bb_period:]) / self.bb_period
                    self.ma_buffer[:-1] = self.ma_buffer[-self.bb_period:]
                
            elif isinstance(data, dict):
                close = float(data.get('close', 0))
                if close > 0:
                    self.price_buffer.append(close)
                    
                    # Build MA buffer
                    ma_buffer = [0.0] * (self.bb_period + 1)
                    ma_buffer[-1] = sum(self.price_buffer[-min(len(self.price_buffer), self.bb_period):]) / min(len(self.price_buffer), self.bb_period)
                    self.ma_buffer = ma_buffer
                    
        except Exception as e:
            self._log_error(f"Initialization failed: {str(e)}")
    
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """Generate BB squeeze mean reversion signal."""
        try:
            close = float(bar.get('close', 0))
            if close <= 0:
                return Signal(action='HOLD')
                
            open_price = float(bar.get('open', close))
            
            # Update price buffer
            self.price_buffer.append(close)
            
            # Maintain sufficient MA history
            while len(self.ma_buffer) < self.bb_period + 2:
                new_ma = sum(self.price_buffer[-len(self.price_buffer):]) / len(self.price_buffer) if self.price_buffer else 0.0
                self.ma_buffer.insert(0, new_ma)
            
            # Calculate Bollinger Bands
            ma = self.ma_buffer[-1]
            prices_for_std = self.price_buffer[-self.bb_period:]
            
            if len(prices_for_std) >= self.bb_period and sum(prices_for_std) / len(prices_for_std) > 0:
                mean_price = sum(prices_for_std) / len(prices_for_std)
                variance = sum((p - mean_price) ** 2 for p in prices_for_std) / len(prices_for_std)
                std_dev = math.sqrt(variance) if variance > 0 else 0.0001
                
                bb_lower = ma - (self.bb_lower_mult * std_dev)
                bb_upper = ma + (self.bb_upper_mult * std_dev)
                
                self.bb_lower = bb_lower
                self.bb_upper = bb_upper
                
                # Calculate band width as % of middle band
                if mean_price > 0:
                    bb_width_pct = abs(bb_upper - bb_lower) / mean_price * 100
                else:
                    bb_width_pct = 0.0
                    
                self.bb_width = bb_width_pct
                
            else:
                return Signal(action='HOLD')
            
            # Detect squeeze (band width reduction by threshold % compared to previous period)
            is_squeeze = False
            prev_width = self.prev_bb_width
            if prev_width and self.bb_width > 0:
                width_reduction_pct = (prev_width - self.bb_width) / prev_width * 100
                is_squeeze = width_reduction_pct >= self.squeeze_threshold_pct
            
            self.prev_bb_width = self.bb_width
            squeeze_triggered = is_squeeze and not self.squeeze_triggered
            
            # Generate entry signals on band touches during squeeze
            signal_action = None
            confidence = 0.0
            
            if not self.position:
                if squeeze_triggered or (prev_width is not None and prev_width <= 15):  # Low vol regime
                
                    # BUY at lower band (oversold during compression)
                    if close <= bb_lower * 0.995:  # Touch or break below lower band
                        signal_action = 'BUY'
                        confidence = min(0.85, 0.7 + (bb_lower - close) / bb_lower * 0.15)
                    
                    # SELL at upper band (overbought during compression)
                    elif close >= bb_upper * 1.005:  # Touch or break above upper band
                        signal_action = 'SELL'
                        confidence = min(0.85, 0.7 + (close - bb_upper) / bb_upper * 0.15)
                        
            elif self.position and signal_action is None:
                # Check exit conditions for active position
                entry_price = self.position.price or close
                
                profit_pct = (close - entry_price) / entry_price if entry_price > 0 else 0
                
                # Take-profit at target
                if profit_pct >= 0.05:  # 5% profit
                    signal_action = 'SELL'
                    confidence = 0.90
                # Check stop-loss
                elif profit_pct <= -0.10:  # 10% loss
                    signal_action = 'SELL'
                    confidence = 1.0
            
            if signal_action is not None and self.position is None:
                quantity = self._calculate_position_size(close)
                
                return Signal(
                    action=signal_action,
                    price=close,
                    quantity=quantity,
                    stop_loss=None,
                    take_profit=None,
                    confidence=confidence,
                    signal_type='BB_SQUEEZE_MEAN_REVERSION',
                    squeeze=squeeze_triggered or (prev_width is not None and prev_width <= 15)
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
                    signal_type='BB_SQUEEZE_MEAN_REVERSION_EXIT',
                    entry_price=self.position.price or close,
                    pnl_pct=(close - (self.position.price or close)) / (self.position.price or close)
                )
            
            return Signal(action='HOLD')
            
        except Exception as e:
            self._log_error(f"BB squeeze signal failed: {str(e)}")
            return Signal(action='HOLD')
    
    def finalize(self) -> dict:
        """Cleanup Bollinger Band state."""
        result = super().finalize()
        self.price_buffer = []
        self.bb_lower = None
        self.bb_upper = None
        self.bb_width = None
        
        return result
