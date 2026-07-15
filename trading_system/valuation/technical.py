"""Trading Strategy Valuation Engine - Technical & Fundamental Analysis

The valuation engine provides comprehensive strategy performance analysis:
- Technical indicators (RSI, MACD, Bollinger Bands)
- Fundamental metrics for each asset class
- Consensus estimates integration
- Portfolio-level risk-adjusted returns

Usage:
    from trading_system.valuation.technical import TechnicalIndicators
    
    tech = TechnicalIndicators()
    
    # Calculate RSI for BTC-USD
    rsi_value, rsi_signal = tech.calculate_rsi('BTC-USD', price_history)

Production Features:
- All major technical indicators
- Fundamental metrics (PE, PB, EV/EBITDA)
- Moving average crossovers
- Volatility calculations
"""

from typing import Dict, List, Optional, Tuple


class TechnicalIndicators:
    """Technical analysis indicators for trading decisions."""
    
    def __init__(self):
        """Initialize technical indicators engine."""
        self.indicator_cache: Dict[str, float] = {}

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """Calculate exponential moving average series for the given period."""
        if not prices:
            return []
        k = 2 / (period + 1)
        ema = [prices[0]]
        for price in prices[1:]:
            ema.append(price * k + ema[-1] * (1 - k))
        return ema

    def calculate_rsi(
        self, 
        symbol: str, 
        prices: List[float],
        period: int = 14
    ) -> Tuple[float, str]:
        """Calculate Relative Strength Index (RSI).
        
        Args:
            symbol: Asset symbol (e.g., "BTC-USD")
            prices: List of closing prices in order
            period: RSI lookback period (default: 14)
            
        Returns:
            Tuple of (RSI value as decimal, signal string)
        
        Signal Interpretation:
            < 30: Oversold → potential buy signal
            > 70: Overbought → potential sell signal
        
        Example:
            >>> prices = [68000, 68200, 67900, 68100]
            >>> rsi, signal = indicators.calculate_rsi("BTC-USD", prices)
        
        """
        if len(prices) < period + 1:
            raise ValueError(
                f"Need at least {period + 1} price points for RSI calculation"
            )
        
        # Simplified RSI implementation
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        
        if avg_loss == 0:
            rsi_value = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_value = 100 - (100 / (1 + rs))
        
        # Determine signal
        if rsi_value < 30:
            signal = "oversold"
        elif rsi_value > 70:
            signal = "overbought"
        else:
            signal = "neutral"
        
        return rsi_value, signal
    
    def calculate_macd(
        self, 
        symbol: str, 
        prices: List[float]
    ) -> Tuple[Dict[str, float], str]:
        """Calculate Moving Average Convergence Divergence (MACD).
        
        Args:
            symbol: Asset symbol  
            prices: List of closing prices
            
        Returns:
            Dictionary with MACD line, signal line, histogram, and signal text
        
        Example:
            >>> macd_data, macd_signal = indicators.calculate_macd("BTC-USD", prices)
        
        """
        if len(prices) < 26:  # Need 12 EMA + 26 EMA
            raise ValueError(
                "Need at least 26 price points for MACD calculation"
            )
        
        # Simplified MACD implementation
        short_ema = self._calculate_ema(prices, 12)
        long_ema = self._calculate_ema(prices, 26)
        
        macd_line = short_ema[25] - long_ema[25]
        
        # Calculate signal line (9 EMA of MACD)
        macd_values = []
        for i in range(len(prices) - 10):
            sma_short = sum(prices[i:i+12]) / 12
            sma_long = sum(prices[i:i+26]) / 26
            macd_values.append(sma_short - sma_long)
        
        signal_line = sum(macd_values[max(0, len(macd_values)-9):]) / 9
        
        histogram = macd_line - signal_line
        
        # Determine signal
        if macd_line > signal_line and histogram > 0:
            signal = "bullish_crossover"
        elif macd_line < signal_line and histogram < 0:
            signal = "bearish_crossover"
        else:
            signal = "neutral"
        
        return {
            "macd_line": round(macd_line, 4),
            "signal_line": round(signal_line, 4), 
            "histogram": round(histogram, 4)
        }, signal
    
    def calculate_bollinger_bands(
        self, 
        symbol: str, 
        prices: List[float],
        period: int = 20,
        std_dev: float = 2.0
    ) -> Dict[str, float]:
        """Calculate Bollinger Bands.
        
        Args:
            symbol: Asset symbol
            prices: List of closing prices  
            period: Moving average period (default: 20)
            std_dev: Number of standard deviations (default: 2.0)
            
        Returns:
            Dictionary with upper_band, middle_band, lower_band
            
        Example:
            >>> bands = indicators.calculate_bollinger_bands("BTC-USD", prices)
        
        """
        if len(prices) < period + 1:
            raise ValueError(
                f"Need at least {period + 1} price points for Bollinger Bands"
            )
        
        # Calculate simple moving average
        sma = sum(prices[-period:]) / period
        
        # Calculate standard deviation
        variance = sum((p - sma) ** 2 for p in prices[-period:]) / period
        std_deviation = variance ** 0.5
        
        upper_band = sma + (std_deviation * std_dev)
        lower_band = sma - (std_deviation * std_dev)
        
        return {
            "upper_band": round(upper_band, 2),
            "middle_band": round(sma, 2),
            "lower_band": round(lower_band, 2)
        }
