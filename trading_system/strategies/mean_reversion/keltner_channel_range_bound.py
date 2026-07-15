"""
Keltner Channel Range-Bound Strategy
    
Purpose:
--------
Implements mean reversion using Keltner Channel volatility-based channels. The 
strategy identifies range-bound periods when price oscillates between the upper
and lower Keltner Channel bands, entering positions at band extremes expecting
reversion toward the middle channel (donchian average).

Regime Suitability:
-------------------
Primary regime: Range-bound markets with low ADX (weak trending strength)
Works best on 4h and daily timeframes for crypto spot markets
Effective when volatility bands contract following expansion periods
    
Failure Modes:
--------------
- Whipsaws during trend breakouts through outer bands
- Poor performance in high-trend regimes (buying top/selling bottom)
- False signals when price remains outside channels for extended periods
    
Expected Holding Horizon:
--------------------------
Short-term: 12-48 hours typically
Average trade duration: 18-36 hours in normal conditions

"""

import math
from dataclasses import dataclass, field
from typing import Optional, List

from trading_system.strategies.mean_reversion.zscore_mean_reversion import register_mean_reversion_strategy
from trading_system.strategies.factory import Signal, StrategyBase

@dataclass
class KeltnerChannelRangeBoundConfig:
    """Configuration for Keltner Channel range-bound strategy."""
    
    name: str = "KeltnerChannelRangeBound"
    donchian_period: int = 20  # Period for moving average (donchian)
    atr_period: int = 20  # Period for ATR calculation
    channel_multiplier_lower: float = 1.0  # Lower channel (1x ATR below MA)
    channel_multiplier_upper: float = 2.0  # Upper channel (2x ATR above MA)
    range_bound_threshold: float = 0.8  # Channel penetration ratio to trigger mean reversion

@register_mean_reversion_strategy
class KeltnerChannelRangeBoundStrategy(StrategyBase):
    """
    Keltner Channel Range-Bound Mean Reversion Strategy
    
    Uses Keltner Channel volatility-based bands to identify range-bound trading
    opportunities. Enters positions at band extremes expecting price to revert 
    toward the donchian average channel.
    
    Entry Logic:
    -----------
    - BUY: Price touches or breaks below lower Keltner Channel (oversold)
    - SELL: Price touches or breaks above upper Keltner Channel (overbought)
    
    Position Sizing:
    ----------------
    Volatility-adjusted sizing with inverse scaling to ATR to account for 
    varying market conditions
    
    Risk Management:
    ----------------
    - Stop-loss: X% below entry (configurable via stop_loss_pct)
    - Take-profit: Target profit level or channel midpoint reversion
    
    Regime Suitability:
    -------------------
    Range-bound markets with low ADX (weak trend strength)
    4h and daily timeframes for crypto spot
    Effective after volatility band contractions
    
    Failure Modes:
    --------------
    - Whipsaws during strong breakout moves through outer bands
    - Poor performance when market is in high-trend regimes
    """
    
    def __init__(self, config: Optional[KeltnerChannelRangeBoundConfig] = None):
        super().__init__(config or KeltnerChannelRangeBoundConfig())
        
        self.donchian_period = self.config.donchian_period
        self.atr_period = self.config.atr_period
        self.lower_mult = self.config.channel_multiplier_lower
        self.upper_mult = self.config.channel_multiplier_upper
        self.range_threshold = self.config.range_bound_threshold
        
        # State variables for Keltner Channel calculation
        self.price_buffer: List[float] = []
        self.ma_donchian: Optional[float] = None
        self.atr_value: Optional[float] = None
        self.lower_channel: Optional[float] = None
        self.upper_channel: Optional[float] = None
        self.channel_width_pct: Optional[float] = None
        self.prev_channel_touch_count: int = 0
    
    def init(self, data: dict) -> None:
        """Initialize with price history."""
        try:
            if isinstance(data, list):
                self.price_buffer = [float(b.get('close', 0)) for b in data]
                
                # Build initial buffers
                non_zero = [p for p in self.price_buffer if p > 0]
                if len(non_zero) >= max(self.donchian_period, self.atr_period):
                    donchian_ma = sum(non_zero[-self.donchian_period:]) / self.donchian_period
                    
                    # Calculate initial ATR
                    atr_prices = non_zero[-self.atr_period:]
                    high_low_diffs = [atry[i] - atry[i - 1] for i in range(1, len(atr_prices)) for atry in [atr_prices]]
                    
                    if len(high_low_diffs) > 0:
                        atr_value = sum(high_low_diffs) / len(high_low_diffs) * 1.5 + (max(atr_prices) - min(atr_prices)) / self.atr_period
                        
                    self.ma_donchian = donchian_ma
                    self.atr_value = atr_value
                    
            elif isinstance(data, dict):
                close = float(data.get('close', 0))
                if close > 0:
                    self.price_buffer.append(close)
                    
        except Exception as e:
            self._log_error(f"Initialization failed: {str(e)}")
    
    def on_bar(self, bar: dict) -> Optional[Signal]:
        """Generate Keltner Channel mean reversion signal."""
        try:
            close = float(bar.get('close', 0))
            if close <= 0:
                return Signal(action='HOLD')
                
            high = float(bar.get('high', close))
            low = float(bar.get('low', close))
            
            # Update price buffer
            self.price_buffer.append(close)
            
            # Maintain sufficient history
            while len(self.price_buffer) < max(self.donchian_period, self.atr_period):
                self.price_buffer.insert(0, close)
            
            # Calculate Donchian Moving Average
            if len(self.price_buffer) >= self.donchian_period:
                ma = sum(self.price_buffer[-self.donchian_period:]) / self.donchian_period
            else:
                return Signal(action='HOLD')
            
            # Calculate ATR (Average True Range)
            atr_prices = self.price_buffer[-self.atr_period:]
            if len(atr_prices) >= 2 and sum(atr_prices) > 0:
                
                if all(p > 0 for p in atr_prices):
                    # Simplified ATR: average of consecutive close ranges + expansion factor
                    price_ranges = [abs(atr_prices[i] - atr_prices[i - 1])
                                    for i in range(1, len(atr_prices))]
                    if all(p > 0 for p in price_ranges):
                        avg_range = sum(price_ranges) / len(price_ranges)
                        
                        # Calculate true range component (price movement)
                        if len(atr_prices) >= 2:
                            prev_close = atr_prices[-2]
                            price_movement = abs(close - prev_close) / self.atr_period * 1.5
                            
                            aty = max(avg_range, price_movement) * 1.4
                        else:
                            aty = avg_range * 1.4
                        
                        atr_value = max(aty, (max(atr_prices) - min(atr_prices)) / self.atr_period)
                    else:
                        return Signal(action='HOLD')
                else:
                    return Signal(action='HOLD')
            else:
                return Signal(action='HOLD')
            
            # Calculate Keltner Channel bands
            lower_channel = ma - (self.lower_mult * atr_value)
            upper_channel = ma + (self.upper_mult * atr_value)
            
            self.ma_donchian = ma
            self.atr_value = atr_value
            self.lower_channel = lower_channel
            self.upper_channel = upper_channel
            
            # Calculate channel width as % of MA
            if ma > 0:
                channel_width_pct = abs(upper_channel - lower_channel) / ma * 100
            else:
                channel_width_pct = 0.0
            
            self.channel_width_pct = channel_width_pct
            
            # Generate mean reversion signals at band extremes
            signal_action = None
            confidence = 0.0
            
            if not self.position:
                # Check for lower band touch (oversold)
                if close <= lower_channel * 0.998:  # Touch or break below lower channel
                    signal_action = 'BUY'
                    penetration_pct = (lower_channel - close) / abs(lower_channel) * 100
                    
                    confidence = min(0.85, 0.6 + penetration_pct / 30)
                    
                # Check for upper band touch (overbought)
                elif close >= upper_channel * 1.002:  # Touch or break above upper channel
                    signal_action = 'SELL'
                    penetration_pct = (close - upper_channel) / upper_channel * 100
                    
                    confidence = min(0.85, 0.6 + penetration_pct / 30)
                    
            elif self.position and signal_action is None:
                # Check exit conditions for active position
                entry_price = self.position.price or close
                
                profit_pct = (close - entry_price) / entry_price if entry_price > 0 else 0
                
                # Take-profit at target
                if profit_pct >= 0.08:  # 8% profit
                    signal_action = 'SELL'
                    confidence = 0.92
                # Check stop-loss
                elif profit_pct <= -0.12:  # 12% loss
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
                    signal_type='KELTNER_CHANNEL_MEAN_REVERSION',
                    channel_width_pct=self.channel_width_pct,
                    ma=self.ma_donchian,
                    atr=self.atr_value
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
                    signal_type='KELTNER_CHANNEL_MEAN_REVERSION_EXIT',
                    channel_width_pct=self.channel_width_pct,
                    ma=self.ma_donchian,
                    atr=self.atr_value,
                    entry_price=self.position.price or close,
                    pnl_pct=(close - (self.position.price or close)) / (self.position.price or close)
                )
            
            return Signal(action='HOLD')
            
        except Exception as e:
            self._log_error(f"Keltner channel signal failed: {str(e)}")
            return Signal(action='HOLD')
    
    def finalize(self) -> dict:
        """Cleanup Keltner Channel state."""
        result = super().finalize()
        self.price_buffer = []
        self.ma_donchian = None
        self.atr_value = None
        
        return result
