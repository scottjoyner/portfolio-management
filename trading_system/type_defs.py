"""
Trading System Data Types and Models.

Defines candle structures, OHLCV data models, and trading signal types
used throughout the portfolio management system.
"""

from dataclasses import dataclass, field
from typing import Optional, Union, Dict, Any
from datetime import datetime


@dataclass
class Candle:
    """
    Represents a single candlestick (OHLC) in time series format.
    
    Attributes:
        timestamp: Time of the candle's close period
        open: Opening price
        high: Highest price during the period
        low: Lowest price during the period
        close: Closing price
    
    Example:
        >>> candle = Candle(
            timestamp=datetime.now(),
            open=100.5,
            high=101.2,
            low=99.8,
            close=101.0
        )
    """
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    
    def __repr__(self) -> str:
        return f"Candle({self.open:.2f}={self.close:.2f}, h={self.high:.2f}, l={self.low:.2f})"


@dataclass
class OHLCV:
    """
    Represents a candle with volume data.
    
    Attributes:
        timestamp: Time of the candle's close period
        open: Opening price
        high: Highest price during the period
        low: Lowest price during the period
        close: Closing price
        volume: Trading volume (optional)
    
    Example:
        >>> ohlcv = OHLCV(
            timestamp=datetime.now(),
            open=100.5,
            high=101.2,
            low=99.8,
            close=101.0,
            volume=1500000
        )
    """
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = field(default=None)


@dataclass
class TradingSignal:
    """
    Represents a trading signal with metadata.
    
    Attributes:
        type: Signal type (BUY, SELL, HOLD)
        strength: Signal strength (-1 to 1)
        reason: Reason for signal generation
        timestamp: When signal was generated
        strategy_source: Strategy that generated the signal
    
    Example:
        >>> signal = TradingSignal(
            type='BUY',
            strength=1.0,
            reason='Golden Cross detected',
            timestamp=datetime.now(),
            strategy_source='triple_ma'
        )
    """
    type: str  # 'BUY', 'SELL', or 'HOLD'
    strength: float = field(default=0.0)  # -1 to +1 range
    reason: Optional[str] = field(default=None)
    timestamp: datetime = field(default_factory=datetime.now)
    strategy_source: Optional[str] = field(default=None)
    
    @property
    def signal_code(self) -> int:
        """Return numeric signal code (-1, 0, 1)."""
        if self.type == 'BUY':
            return 1
        elif self.type == 'SELL':
            return -1
        else:
            return 0
    
    @staticmethod
    def from_code(code: int) -> 'TradingSignal':
        """
        Create TradingSignal from numeric code.
        
        Args:
            code: Signal code (-1=SELL, 0=HOLD, 1=BUY)
            
        Returns:
            TradingSignal instance
        """
        types = {'BUY': 1, 'SELL': -1, 'HOLD': 0}
        type_name = next(k for k, v in types.items() if v == code)
        return TradingSignal(
            type=type_name,
            strength=max(-1.0, min(1.0, code)),
            timestamp=datetime.now()
        )


@dataclass
class Position:
    """
    Represents a trading position with risk parameters.
    
    Attributes:
        symbol: Trading symbol (e.g., 'BTC/USDT')
        size: Position size (positive=LONG, negative=SHORT)
        entry_price: Average entry price
        unrealized_pnl: Unrealized profit/loss ($)
        roi_percentage: Return on investment (%)
    
    Example:
        >>> position = Position(
            symbol='BTC/USDT',
            size=0.5,
            entry_price=45000.0
        )
    """
    symbol: str
    size: float  # Positive for LONG, negative for SHORT
    entry_price: float
    unrealized_pnl: Optional[float] = field(default=None)
    roi_percentage: Optional[float] = field(default=None)


# Type aliases for convenience
T_Candle = Union[Candle, OHLCV, Dict[str, Any]]
T_Signal = TradingSignal
T_Position = Position


def parse_candle_data(data: T_Candle, timestamp_field: str = 'timestamp') -> Candle:
    """
    Parse candle data from various formats into Candle object.
    
    Args:
        data: Candle data in dict or Candle format
        timestamp_field: Field name for timestamp (default: 'timestamp')
        
    Returns:
        Candle instance
    
    Example:
        >>> parsed = parse_candle_data({'open': 100, 'close': 101}, 'time')
    """
    if isinstance(data, Candle):
        return data
    
    if isinstance(data, dict):
        # Handle common timestamp field names
        ts_field = timestamp_field or 'timestamp' or 'time' or 'datetime'
        timestamp = data.get(ts_field)
        
        # Convert string timestamps to datetime if needed
        if isinstance(timestamp, str):
            try:
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            except (ValueError, AttributeError):
                timestamp = datetime.now()
        
        return Candle(
            timestamp=timestamp,
            open=data['open'],
            high=data['high'],
            low=data['low'],
            close=data['close']
        )
    
    raise ValueError(f"Unsupported candle data type: {type(data)}")


def parse_signal_data(signal_data: T_Signal) -> TradingSignal:
    """
    Parse signal data from various formats into TradingSignal object.
    
    Args:
        signal_data: Signal data in dict or TradingSignal format
        
    Returns:
        TradingSignal instance
    
    Example:
        >>> parsed = parse_signal_data({'type': 'BUY', 'strength': 1.0})
    """
    if isinstance(signal_data, TradingSignal):
        return signal_data
    
    if isinstance(signal_data, dict):
        return TradingSignal(
            type=signal_data.get('type', signal_data.get('signal', 'HOLD')),
            strength=signal_data.get('strength', 0.0),
            reason=signal_data.get('reason', None),
            timestamp=signal_data.get('timestamp', datetime.now())
        )
    
    raise ValueError(f"Unsupported signal data type: {type(signal_data)}")
