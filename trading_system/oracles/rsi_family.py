"""
RSI Family - Relative Strength Index and Related Oscillators
===========================================================

This module implements the complete RSI family of indicators:
1. Standard RSI - Momentum oscillator measuring rate of gain/loss
2. Stochastic RSI - Normalized RSI with faster reaction times
3. Williams %R - Momentum indicator similar to RSI but inverted

All implementations include proper NaN handling and backtesting optimization.
"""

from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Optional, List
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

# Base bar class from trading_system.base
try:
    from trading_system.strategies.base import OHLCVBar
except ImportError:
    @dataclass
    class OHLCVBar:
        timestamp: int
        open: Optional[float] = None
        high: Optional[float] = None
        low: Optional[float] = None
        close: Optional[float] = None
        volume: Optional[float] = None


@dataclass
class RSISignal:
    """Standardized RSI signal output."""
    timestamp: int
    signal_type: str  # 'BUY', 'SELL', 'NO_SIGNAL'
    strength: float  # -1 to 1 scale
    rsi_value: Optional[float] = None
    rsi_pct_change: Optional[float] = None
    entry_price: Optional[float] = None


class RSIOracle:
    """
    Relative Strength Index Oracle
    
    Measures momentum by comparing average gains vs losses over lookback period.
    
    Signal Generation:
    - RSI > 70 + threshold → Overbought → SELL signal
    - RSI < 30 - threshold → Oversold → BUY signal
    - RSI crossing above/below threshold levels
    
    Config Parameters:
    - lookback_periods: Number of periods for RSI calculation (default: 14)
    - overbought_threshold: RSI level considered overbought (default: 70.0)
    - oversold_threshold: RSI level considered oversold (default: 30.0)
    - signal_threshold_pct: Minimum deviation from threshold to trigger (default: 2.0)
    """
    
    def __init__(self, config: dict):
        self.lookback_periods = config.get('lookback_periods', 14)
        self.overbought_threshold = config.get('overbought_threshold', 70.0)
        self.oversold_threshold = config.get('oversold_threshold', 30.0)
        self.signal_threshold_pct = config.get('signal_threshold_pct', 2.0)
        
        # State variables
        self.gains: List[float] = []
        self.losses: List[float] = []
        self.rsi_values: List[float] = []
        self.position_entry_price: Optional[float] = None
        self.unrealized_pnl: float = 0.0
        
    def setup(self, ohlcv_data: List[OHLCVBar]) -> None:
        """Initialize RSI oracle with historical data."""
        self.gains = []
        self.losses = []
        self.rsi_values = []
        
        for bar in ohlcv_data:
            if bar.close is None:
                continue
                
            gain = 0.0
            loss = 0.0
            
            previous_close = getattr(self, 'previous_close', bar.close)
            change = bar.close - previous_close
            
            if change > 0:
                gain = change
            else:
                loss = abs(change)
            
            self.gains.append(gain)
            self.losses.append(loss)
            self.previous_close = bar.close
            
        # Calculate initial RSI values
        for i in range(len(self.gains)):
            if i < self.lookback_periods - 1:
                self.rsi_values.append(None)
                continue
                
            avg_gain = sum(self.gains[-self.lookback_periods:]) / self.lookback_periods
            avg_loss = sum(self.losses[-self.lookback_periods:]) / self.lookback_periods
            
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
            
            self.rsi_values.append(rsi)
        
    def on_bar(self, bar: OHLCVBar) -> Optional[RSISignal]:
        """Generate RSI signal for incoming bar."""
        if bar.close is None:
            return None
            
        gain = 0.0
        loss = 0.0
        previous_close = getattr(self, 'previous_close', bar.close)
        
        change = bar.close - previous_close
        
        if change > 0:
            gain = change
        else:
            loss = abs(change)
        
        self.gains.append(gain)
        self.losses.append(loss)
        self.previous_close = bar.close
        
        # Calculate RSI
        if len(self.rsi_values) < self.lookback_periods - 1:
            self.rsi_values.append(None)
            
        elif self.rsi_values[-1] is None:
            avg_gain = sum(self.gains[-self.lookback_periods:]) / self.lookback_periods
            avg_loss = sum(self.losses[-self.lookback_periods:]) / self.lookback_periods
            
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
            
            self.rsi_values.append(rsi)
            
        else:
            rsi = self.rsi_values[-1]
        
        # Generate signal
        signal = None
        
        if len(self.rsi_values) >= self.lookback_periods:
            current_rsi = self.rsi_values[-1]
            prev_rsi = self.rsi_values[-2]
            
            # Overbought exit
            if self.position_entry_price is not None:
                if current_rsi > self.overbought_threshold and self.unrealized_pnl > 0:
                    signal = RSISignal(
                        timestamp=bar.timestamp,
                        signal_type='SELL',
                        strength=min(1.0, (current_rsi - self.overbought_threshold) / self.signal_threshold_pct),
                        rsi_value=current_rsi
                    )
                    self.close_position()
                
            # Oversold entry
            elif current_rsi < self.oversold_threshold:
                signal = RSISignal(
                    timestamp=bar.timestamp,
                    signal_type='BUY',
                    strength=min(1.0, (self.oversold_threshold - current_rsi) / self.signal_threshold_pct),
                    rsi_value=current_rsi,
                    entry_price=bar.close
                )
                self.open_position(bar.close)
        
        return signal
    
    def open_position(self, price: float) -> None:
        self.position_entry_price = price
        self.unrealized_pnl = 0.0
        
    def close_position(self) -> None:
        self.position_entry_price = None
        self.unrealized_pnl = 0.0
    
    @property
    def rsi_value(self) -> Optional[float]:
        if len(self.rsi_values) < self.lookback_periods:
            return None
        return self.rsi_values[-1]


class StochasticRSIOracle:
    """
    Stochastic RSI Oracle
    
    Faster, normalized version of RSI that reacts quicker to price changes.
    
    Signal Generation:
    - %K > 80 → Overbought → SELL signal  
    - %K < 20 → Oversold → BUY signal
    - %D (3-period SMA) crossover signals
    
    Config Parameters:
    - rsi_lookback: Periods for underlying RSI (default: 14)
    - stochastic_lookback: Periods for Stoch calculation (default: 3)
    - overbought_threshold: Overbought level (default: 80.0)
    - oversold_threshold: Oversold level (default: 20.0)
    """
    
    def __init__(self, config: dict):
        self.rsi_lookback = config.get('rsi_lookback', 14)
        self.stochastic_lookback = config.get('stochastic_lookback', 3)
        self.overbought_threshold = config.get('overbought_threshold', 80.0)
        self.oversold_threshold = config.get('oversold_threshold', 20.0)
        
        # State variables
        self.stoch_values: List[float] = []
        self.stoch_d_values: List[float] = []
        self.gains: List[float] = []
        self.losses: List[float] = []
        self.position_entry_price: Optional[float] = None
        self.unrealized_pnl: float = 0.0
        
    def setup(self, ohlcv_data: List[OHLCVBar]) -> None:
        """Initialize Stochastic RSI with historical data."""
        # Calculate underlying RSI for all bars
        gains = []
        losses = []
        
        previous_close = None
        for bar in ohlcv_data:
            if bar.close is None:
                continue
                
            gain = 0.0
            loss = 0.0
            
            prev_close = getattr(self, 'previous_close', bar.close)
            change = bar.close - prev_close
            
            if change > 0:
                gain = change
            else:
                loss = abs(change)
            
            gains.append(gain)
            losses.append(loss)
            self.previous_close = bar.close
        
        # Calculate RSI for each bar
        rsi_values = []
        for i in range(len(gains)):
            if i < self.rsi_lookback - 1:
                rsi_values.append(None)
                continue
                
            avg_gain = sum(gains[-self.rsi_lookback:]) / self.rsi_lookback
            avg_loss = sum(losses[-self.rsi_lookback:]) / self.rsi_lookback
            
            if avg_loss == 0:
                rsi = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
            
            rsi_values.append(rsi)
        
        # Store for later use and calculate Stochastic RSI
        min_rsi = min([r for r in rsi_values if r is not None])
        max_rsi = max([r for r in rsi_values if r is not None])
        
        if min_rsi == max_rsi:
            min_rsi = min_rsi - 0.001
        
        for i, rsi in enumerate(rsi_values):
            if rsi is None or min_rsi == max_rsi:
                stoch_value = 50.0  # Neutral default
            else:
                stoch_value = (rsi - min_rsi) / (max_rsi - min_rsi) * 100.0
            
            self.stoch_values.append(stoch_value)
        
        # Calculate Stochastic D (SMA)
        for i in range(len(self.stoch_values)):
            if i < self.stochastic_lookback - 1:
                self.stoch_d_values.append(None)
                continue
                
            sma_stoch = sum(self.stoch_values[-self.stochastic_lookback:]) / self.stochastic_lookback
            self.stoch_d_values.append(sma_stoch)
    
    def on_bar(self, bar: OHLCVBar) -> Optional[RSISignal]:
        """Generate Stochastic RSI signal for incoming bar."""
        if bar.close is None:
            return None
            
        # Calculate underlying RSI
        gains = self.gains
        losses = self.losses
        gain = 0.0
        loss = 0.0
        previous_close = getattr(self, 'previous_close', bar.close)
        
        change = bar.close - previous_close
        
        if change > 0:
            gain = change
        else:
            loss = abs(change)
        
        # Calculate underlying RSI
        self.gains.append(gain)
        self.losses.append(loss)
        self.previous_close = bar.close
        
        # Calculate RSI
        if len(gains) < self.rsi_lookback:
            return None
            
        avg_gain = sum(gains[-self.rsi_lookback:]) / self.rsi_lookback
        avg_loss = sum(losses[-self.rsi_lookback:]) / self.rsi_lookback
        
        if avg_loss == 0:
            rsi = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
        
        # Calculate Stochastic RSI
        historical_rsi = self.stoch_values.copy()
        if not any(r is None for r in historical_rsi):
            min_rsi = min(historical_rsi)
            max_rsi = max(historical_rsi)
            
            if min_rsi == max_rsi:
                min_rsi = min_rsi - 0.001
            
            stoch_value = (rsi - min_rsi) / (max_rsi - min_rsi) * 100.0
            self.stoch_values.append(stoch_value)
        else:
            self.stoch_values.append(50.0)  # Neutral
        
        # Calculate Stochastic D
        if len(self.stoch_d_values) < self.stochastic_lookback - 1:
            self.stoch_d_values.append(None)
        elif any(v is None for v in self.stoch_d_values[-self.stochastic_lookback-1:-1]):
            self.stoch_d_values.append(50.0)
        else:
            sma_stoch = sum(self.stoch_values[-self.stochastic_lookback:]) / self.stochastic_lookback
            self.stoch_d_values.append(sma_stoch)
        
        # Generate signal based on overbought/oversold thresholds
        signal = None
        
        if len(self.stoch_values) >= self.rsi_lookback:
            current_stoch = self.stoch_values[-1]
            
            # Overbought exit
            if self.position_entry_price is not None:
                if current_stoch > self.overbought_threshold and self.unrealized_pnl > 0:
                    strength = min(1.0, (current_stoch - self.overbought_threshold) / 20.0)
                    signal = RSISignal(
                        timestamp=bar.timestamp,
                        signal_type='SELL',
                        strength=strength,
                        rsi_value=current_stoch
                    )
                    self.close_position()
            
            # Oversold entry  
            elif current_stoch < self.oversold_threshold:
                strength = min(1.0, (self.oversold_threshold - current_stoch) / 20.0)
                signal = RSISignal(
                    timestamp=bar.timestamp,
                    signal_type='BUY',
                    strength=strength,
                    rsi_value=current_stoch,
                    entry_price=bar.close
                )
                self.open_position(bar.close)
        
        return signal
    
    def open_position(self, price: float) -> None:
        self.position_entry_price = price
        self.unrealized_pnl = 0.0
        
    def close_position(self) -> None:
        self.position_entry_price = None
        self.unrealized_pnl = 0.0


class WilliamsROracle:
    """
    Williams %R Oracle
    
    Momentum indicator measuring distance from highest high to lowest low.
    Inverted scale (negative values): -100 to 0.
    
    Signal Generation:
    - %R > -20 → Overbought → SELL signal
    - %R < -80 → Oversold → BUY signal
    
    Config Parameters:
    - lookback_periods: Periods for Williams %R calculation (default: 14)
    - overbought_threshold: Overbought level (default: -20.0)
    - oversold_threshold: Oversold level (default: -80.0)
    """
    
    def __init__(self, config: dict):
        self.lookback_periods = config.get('lookback_periods', 14)
        self.overbought_threshold = config.get('overbought_threshold', -20.0)
        self.oversold_threshold = config.get('oversold_threshold', -80.0)
        
        # State variables
        self.high_values: List[float] = []
        self.low_values: List[float] = []
        self.williams_r_values: List[float] = []
        self.position_entry_price: Optional[float] = None
        self.unrealized_pnl: float = 0.0
        
    def setup(self, ohlcv_data: List[OHLCVBar]) -> None:
        """Initialize Williams %R with historical data."""
        self.high_values = []
        self.low_values = []
        self.williams_r_values = []
        
        for bar in ohlcv_data:
            if bar.high is None or bar.low is None:
                continue
                
            self.high_values.append(bar.high)
            self.low_values.append(bar.low)
        
        # Calculate Williams %R for each bar
        for i in range(len(self.high_values)):
            if i < self.lookback_periods - 1:
                self.williams_r_values.append(None)
                continue
                
            highest_high = max(self.high_values[-self.lookback_periods:])
            lowest_low = min(self.low_values[-self.lookback_periods:])
            
            # Williams %R formula: (Highest High - Current Close) / (Highest High - Lowest Low) * -100
            if highest_high == lowest_low:
                wr = 0.0  # Neutral
            else:
                wr = ((highest_high - bar.close) / (highest_high - lowest_low)) * -100.0
            
            self.williams_r_values.append(wr)
    
    def on_bar(self, bar: OHLCVBar) -> Optional[RSISignal]:
        """Generate Williams %R signal for incoming bar."""
        if bar.high is None or bar.low is None:
            return None
            
        self.high_values.append(bar.high)
        self.low_values.append(bar.low)
        
        # Calculate Williams %R
        if len(self.williams_r_values) < self.lookback_periods - 1:
            self.williams_r_values.append(None)
        else:
            highest_high = max(self.high_values[-self.lookback_periods:])
            lowest_low = min(self.low_values[-self.lookback_periods:])
            
            if highest_high == lowest_low:
                wr = 0.0
            else:
                wr = ((highest_high - bar.close) / (highest_high - lowest_low)) * -100.0
            
            self.williams_r_values.append(wr)
        
        # Generate signal based on overbought/oversold thresholds
        signal = None
        
        if len(self.williams_r_values) >= self.lookback_periods:
            current_wr = self.williams_r_values[-1]
            
            # Overbought exit
            if self.position_entry_price is not None:
                if current_wr > self.overbought_threshold and self.unrealized_pnl > 0:
                    strength = min(1.0, (current_wr - self.overbought_threshold) / 20.0)
                    signal = RSISignal(
                        timestamp=bar.timestamp,
                        signal_type='SELL',
                        strength=strength,
                        rsi_value=current_wr
                    )
                    self.close_position()
            
            # Oversold entry  
            elif current_wr < self.oversold_threshold:
                strength = min(1.0, (current_wr - self.oversold_threshold) / 20.0)
                signal = RSISignal(
                    timestamp=bar.timestamp,
                    signal_type='BUY',
                    strength=strength,
                    rsi_value=current_wr,
                    entry_price=bar.close
                )
                self.open_position(bar.close)
        
        return signal
    
    def open_position(self, price: float) -> None:
        self.position_entry_price = price
        self.unrealized_pnl = 0.0
        
    def close_position(self) -> None:
        self.position_entry_price = None
        self.unrealized_pnl = 0.0


__all__ = [
    'OHLCVBar',
    'RSISignal',
    'RSIOracle',
    'StochasticRSIOracle', 
    'WilliamsROracle'
]
