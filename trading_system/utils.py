"""
Simple Moving Average (SMA) and Exponential Moving Average (EMA) Utilities.

These MA functions are used by trading strategies for technical analysis signals.

SMA Formula:
    SMA(n) = sum(prices[0:n]) / n

EMA Formula:
    EMA[n] = price[n] * k + EMA[n-1] * (1 - k)
    where k = 2 / (n + 1)
"""

import logging
from typing import Optional, Union
from functools import wraps

logger = logging.getLogger(__name__)


def sma(n: int):
    """
    Decorator factory for Simple Moving Average function.
    
    Returns a callable that takes candle list and index, returns SMA value.
    
    Args:
        n: Number of periods for the moving average
    
    Example:
        >>> from trading_system.utils import SMA
        >>> s5 = SMA(5)  # Create 5-period SMA function
        >>> price = s5(candles, -1)  # Get last candle's SMA value
    """
    def decorator(func):
        @wraps(func)
        def wrapper(candles: list[dict], index: int) -> float:
            if not candles or len(candles) < n:
                logger.debug(f"SMA({n}) called with insufficient candles (got {len(candles)})")
                return None
            
            # Get prices at specified index (negative indexing supported)
            start_idx = max(0, min(-1 * n, len(candles)))
            
            # Calculate average of closing prices
            prices = [candles[i]['close'] for i in range(start_idx, start_idx + n)]
            average = sum(prices) / n
            
            return average
        
        return wrapper
    
    return decorator


def ema(n: int):
    """
    Decorator factory for Exponential Moving Average function.
    
    Returns a callable that takes candle list and index, returns EMA value.
    
    Args:
        n: Number of periods for the moving average
    
    Example:
        >>> from trading_system.utils import EMA
        >>> e10 = EMA(10)  # Create 10-period EMA function
        >>> price = e10(candles, -1)  # Get last candle's EMA value
    """
    def decorator(func):
        @wraps(func)
        def wrapper(candles: list[dict], index: int) -> float:
            if not candles or len(candles) < n:
                logger.debug(f"EMA({n}) called with insufficient candles (got {len(candles)})")
                return None
            
            k = 2 / (n + 1)
            
            # Get prices at specified index (negative indexing supported)
            start_idx = max(0, min(-1 * n, len(candles)))
            
            # First EMA initialization: use SMA of first n periods
            first_n = candles[start_idx:start_idx + n]
            initial_ema = sum(c['close'] for c in first_n) / n

            current_ema = initial_ema
            if index >= 0 and start_idx + n <= len(candles):
                # Calculate EMAs forward to the requested index
                last_idx = min(index, len(candles) - 1)

                for i in range(start_idx, last_idx + 1):
                    if candles[i]:
                        price = candles[i]['close']
                        current_ema = price * k + current_ema * (1 - k)

            return current_ema
        
        return wrapper
    
    return decorator


class SMA:
    """
    Simple Moving Average class for creating 5-period or custom MA functions.
    
    Example:
        >>> sma5 = SMA(5)
        >>> price = sma5(candles, -1)
    """
    def __init__(self, n: int):
        self.n = n
        self._func = sma(n)(lambda candles, index: None)
        self.callable = self._func  # Make callable
    
    def __call__(self, candles: list[dict], index: int) -> float:
        """Calculate SMA value at given candle index."""
        return self._func(candles, index)


class EMA:
    """
    Exponential Moving Average class for creating custom MA functions.
    
    Example:
        >>> ema10 = EMA(10)
        >>> price = ema10(candles, -1)
    """
    def __init__(self, n: int):
        self.n = n
        self._func = ema(n)(lambda candles, index: None)
        self.callable = self._func  # Make callable
    
    def __call__(self, candles: list[dict], index: int) -> float:
        """Calculate EMA value at given candle index."""
        return self._func(candles, index)


# Convenience function for creating MA with specified name
def make_ma(name: str = "SMA", period: int = 5):
    """
    Factory function for creating moving average functions.
    
    Args:
        name: 'SMA' or 'EMA' (default: 'SMA')
        period: Number of periods (default: 5)
    
    Returns:
        Callable MA function
    
    Example:
        >>> sma10 = make_ma('SMA', 10)
        >>> ema20 = make_ma('EMA', 20)
    """
    if name.upper() == 'EMA':
        return EMA(period)
    elif name.upper() == 'SMA':
        return SMA(period)
    else:
        logger.warning(f"Unknown MA type '{name}', using default SMA")
        return SMA(period)


# Convenience function for creating custom callable with decorator syntax
def create_callable_ma(name: str = "SMA", period: int = 5):
    """
    Create a callable moving average instance.
    
    Args:
        name: 'SMA' or 'EMA' (default: 'SMA')
        period: Number of periods (default: 5)
    
    Returns:
        Callable MA function
    
    Example:
        >>> m20 = create_callable_ma('EMA', 20)
        >>> price = m20(candles, -1)
    """
    if name.upper() == 'EMA':
        return ema(period)
    elif name.upper() == 'SMA':
        return sma(period)
    else:
        logger.warning(f"Unknown MA type '{name}', using default SMA")
        return sma(period)


# Module-level functions for common periods
SMA5 = sma(5)
SMA10 = sma(10)
SMA20 = sma(20)
SMA60 = sma(60)

EMA5 = ema(5)
EMA10 = ema(10)
EMA20 = ema(20)
EMA60 = ema(60)


def partial(func, *args, **kwargs):
    """Simple partial function wrapper for MA functions."""
    from functools import partial as _partial
    return _partial(func, *args, **kwargs)
