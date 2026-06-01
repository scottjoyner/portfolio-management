"""Fundamental Analysis Engine - Asset Valuation Metrics

The fundamental analysis module provides company/asset metrics for valuation:
- Price-to-Earnings (P/E) ratios
- Price-to-Book (P/B) ratios  
- Enterprise Value/EBITDA
- Free Cash Flow yield
- Dividend yield calculations

Usage:
    from trading_system.valuation.fundamental import FundamentalMetrics
    
    metrics = FundamentalMetrics()
    pe_ratio, fair_value = metrics.calculate_pe_ratio('NASDAQ:AAPL')

Features:
- Multiple valuation multiples
- Fair value estimation via DCF (simplified)
- Relative vs historical comparisons
"""

from typing import Dict, Optional, Tuple, List


class FundamentalMetrics:
    """Fundamental analysis metrics for asset valuation."""
    
    def __init__(self):
        """Initialize fundamental analysis engine."""
        self.market_data = {}  # Mock data storage
    
    def calculate_pe_ratio(
        self, 
        symbol: str,
        price: float,
        earnings_per_share: float,
        forward_eps: Optional[float] = None
    ) -> Tuple[float, str]:
        """Calculate Price-to-Earnings ratio.
        
        Args:
            symbol: Ticker symbol (e.g., "NASDAQ:AAPL")
            price: Current stock price
            earnings_per_share: Trailing 12-month EPS
            forward_eps: Forward-looking EPS estimate (optional)
            
        Returns:
            Tuple of (P/E ratio, valuation signal text)
        
        Example:
            >>> pe_ratio, signal = metrics.calculate_pe_ratio(
            ...     symbol="NASDAQ:AAPL",
            ...     price=175.00,
            ...     earnings_per_share=6.13
            ... )
        
        """
        if earnings_per_share <= 0:
            raise ValueError("Earnings per share must be positive")
        
        trailing_pe = price / earnings_per_share
        
        # Determine signal based on historical range (simplified)
        sector_avg_pe = 25.0  # Simplified industry average
        if abs(trailing_pe - sector_avg_pe) < 10:
            valuation_signal = "fair_valuation"
        elif trailing_pe < sector_avg_pe * 0.8:
            valuation_signal = "undervalued"
        else:
            valuation_signal = "overvalued"
        
        return round(trailing_pe, 2), valuation_signal
    
    def calculate_pb_ratio(
        self, 
        symbol: str,
        price: float,
        book_value_per_share: float
    ) -> Tuple[float, str]:
        """Calculate Price-to-Book ratio.
        
        Args:
            symbol: Ticker symbol
            price: Current stock price  
            book_value_per_share: Shareholder equity per share
            
        Returns:
            Tuple of (P/B ratio, valuation signal)
        
        Example:
            >>> pb_ratio, signal = metrics.calculate_pb_ratio(
            ...     symbol="NASDAQ:AAPL",
            ...     price=175.00,
            ...     book_value_per_share=4.20
            ... )
        
        """
        if book_value_per_share <= 0:
            raise ValueError("Book value per share must be positive")
        
        pb_ratio = price / book_value_per_share
        
        # Fair P/B is typically 1.5-3.0 for growth companies
        industry_pb_range_low = 1.5
        industry_pb_range_high = 3.0
        
        if pb_ratio < industry_pb_range_low * 0.7:
            signal = "significantly_undervalued"
        elif pb_ratio < industry_pb_range_low:
            signal = "undervalued"
        elif pb_ratio > industry_pb_range_high:
            signal = "overvalued"
        else:
            signal = "fair_valuation"
        
        return round(pb_ratio, 2), signal
    
    def calculate_ev_ebitda(
        self, 
        symbol: str,
        enterprise_value: float,
        ebitda: float
    ) -> Tuple[float, str]:
        """Calculate Enterprise Value/EBITDA ratio.
        
        Args:
            symbol: Ticker symbol
            enterprise_value: Market cap + debt - cash
            ebitda: Earnings before interest, taxes, depreciation, amortization
            
        Returns:
            Tuple of (EV/EBITA ratio, valuation signal)
        
        Example:
            >>> ev_ebitda, signal = metrics.calculate_ev_ebitda(
            ...     symbol="NASDAQ:AAPL",
            ...     enterprise_value=250000000000,
            ...     ebitda=13000000000
            ... )
        
        """
        if ebitda <= 0:
            raise ValueError("EBITDA must be positive")
        
        ev_ebitda = enterprise_value / ebitda
        
        # Telecom/utilities typically 8-15x, tech 20-35x
        industry_avg_ev_ebitda = 22.0
        if ev_ebitda < industry_avg_ev_ebitda * 0.7:
            signal = "undervalued"
        elif ev_ebitda > industry_avg_ev_ebitda * 1.3:
            signal = "overvalued"
        else:
            signal = "fair_valuation"
        
        return round(ev_ebitda, 2), signal
    
    def calculate_dividend_yield(
        self, 
        symbol: str,
        price: float,
        annual_dividend: float
    ) -> Tuple[float, str]:
        """Calculate dividend yield.
        
        Args:
            symbol: Ticker symbol
            price: Current stock price  
            annual_dividend: Annual dividend per share
            
        Returns:
            Tuple of (yield percentage, signal)
        
        Example:
            >>> yield_pct, signal = metrics.calculate_dividend_yield(
            ...     symbol="NASDAQ:KO",
            ...     price=60.00,
            ...     annual_dividend=1.64
            ... )
        
        """
        if price <= 0:
            raise ValueError("Stock price must be positive")
        if annual_dividend < 0:
            raise ValueError("Dividend cannot be negative")
        
        dividend_yield = (annual_dividend / price) * 100
        
        # Fair range is typically 2-5% for established companies
        if dividend_yield < 2.0:
            signal = "low_yield"
        elif dividend_yield > 6.0:
            signal = "high_yield_watch"
        else:
            signal = "normal_range"
        
        return round(dividend_yield, 2), signal
    
    def calculate_free_cash_flow_yield(
        self, 
        symbol: str,
        market_capitalization: float,
        free_cash_flow: float
    ) -> Tuple[float, str]:
        """Calculate Free Cash Flow to Enterprise Value ratio.
        
        Args:
            symbol: Ticker symbol  
            market_capitalization: Total market cap
            free_cash_flow: Annual FCF
            
        Returns:
            Tuple of (FCF yield percentage, signal)
        
        Example:
            >>> fcf_yield, signal = metrics.calculate_free_cash_flow_yield(
            ...     symbol="NASDAQ:AAPL",
            ...     market_capitalization=280000000000,
            ...     free_cash_flow=11500000000
            ... )
        
        """
        if market_capitalization <= 0:
            raise ValueError("Market cap must be positive")
        
        fcf_yield = (free_cash_flow / market_capitalization) * 100
        
        # Historical average is around 3-5%
        if fcf_yield < 2.0:
            signal = "below_average"
        elif fcf_yield > 7.0:
            signal = "above_average"
        else:
            signal = "normal_range"
        
        return round(fcf_yield, 2), signal
