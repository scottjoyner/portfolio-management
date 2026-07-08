#!/usr/bin/env python3
"""
yfinance Data Fetcher - Unified interface for Yahoo Finance data via yfinance library.

Provides access to:
- Stock prices (AAPL, MSFT, GOOGL, TSLA)
- ETF prices (SPY, QQQ, VTI)  
- Historical price data with configurable time ranges

Requires network access to Yahoo Finance servers (fc.yahoo.com).
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class YfinanceDataFetcher:
    """Unified interface for fetching data from Yahoo Finance via yfinance library."""
    
    def __init__(self):
        self._ticker_cache: Dict[str, object] = {}
        
    def get_stock_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get information about a stock ticker.
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
            
        Returns:
            Dictionary with ticker info or None if unavailable
        """
        try:
            import yfinance as yf

            # Use cached instance if available
            if ticker not in self._ticker_cache:
                self._ticker_cache[ticker] = yf.Ticker(ticker)

            ticker_obj = self._ticker_cache[ticker]
            info = getattr(ticker_obj, "info", {}) or {}
            
            return {
                'symbol': ticker,
                'name': info.get('info', {}).get('company_name', '') if info else '',
                'sector': info.get('info', {}).get('sector', '') if info else '',
                'market_capital': info.get('marketCap', info.get('market_capital', info.get('capitalization', 0))) if info else 0,
            }
            
        except Exception as e:
            logger.warning(f"Failed to get ticker info for {ticker}: {e}")
            return None
    
    def get_historical_prices(
        self, 
        ticker: str, 
        start_date: datetime, 
        end_date: datetime,
        granularity: str = '1day'
    ) -> List[Dict[str, Any]]:
        """Get historical price data for a stock ticker.
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
            start_date: Start date for the query
            end_date: End date for the query  
            granularity: Time granularity ('1day', '5days', etc.)
            
        Returns:
            List of dictionaries with price data, each containing:
                - timestamp: datetime object
                - open: float (open price)
                - high: float (high price)
                - low: float (low price)  
                - close: float (close price)
                - volume: float (volume traded)
        """
        try:
            import yfinance as yf

            df = yf.download(
                ticker,
                start=start_date.strftime('%Y-%m-%d'),
                end=end_date.strftime('%Y-%m-%d'),
                interval='1d',
                auto_adjust=False,
                progress=False,
                threads=False,
            )

            if df is None or df.empty:
                logger.warning(f"No historical data available for {ticker}")
                return []

            prices = []
            for ts, row in df.iterrows():
                try:
                    prices.append({
                        'timestamp': ts.to_pydatetime() if hasattr(ts, 'to_pydatetime') else ts,
                        'open': float(row.get('Open', 0)),
                        'high': float(row.get('High', 0)),
                        'low': float(row.get('Low', 0)),
                        'close': float(row.get('Close', 0)),
                        'volume': float(row.get('Volume', 0))
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse history row for {ticker}: {e}")
                    continue

            return prices
            
        except Exception as e:
            logger.warning(f"Failed to get historical data for {ticker}: {e}")
            return []
    
    def get_etf_prices(
        self, 
        etf_symbol: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get ETF price data.
        
        Args:
            etf_symbol: ETF ticker symbol (e.g., 'SPY', 'QQQ')
            start_date: Start date for the query  
            end_date: End date for the query
            
        Returns:
            List of dictionaries with ETF price data
        """
        try:
            return self.get_historical_prices(etf_symbol, start_date, end_date)
            
        except Exception as e:
            logger.warning(f"Failed to get ETF data for {etf_symbol}: {e}")
            return []


def main():
    """Main entry point for testing yfinance data fetching."""
    
    print("=" * 60)
    print("yfinance Data Fetcher Test")
    print("=" * 60)
    
    # Initialize the fetcher  
    fetcher = YfinanceDataFetcher()
    
    # Test stock info retrieval
    for ticker in ['AAPL', 'MSFT']:
        info = fetcher.get_stock_info(ticker)
        if info:
            print(f"\n{ticker} Info:")
            print(f"  Name: {info['name']}")
            print(f"  Sector: {info['sector']}")
            print(f"  Market Capitalization: ${info['market_capital']:,.2f}")
        else:
            print(f"\n{ticker}: No info available (Yahoo Finance may be unreachable)")
    
    # Test historical price data retrieval  
    start_date = datetime(2024, 12, 31)
    end_date = datetime.now()
    
    for ticker in ['AAPL', 'MSFT']:
        print(f"\nFetching {ticker} history from {start_date.date()} to {end_date.date()}...")
        
        prices = fetcher.get_historical_prices(ticker, start_date, end_date)
        
        if prices:
            print(f"  Retrieved {len(prices)} price records")
            print(f"  First record: {prices[0]['timestamp'].date()} - ${prices[0]['close']:.2f}")
            print(f"  Last record: {prices[-1]['timestamp'].date()} - ${prices[-1]['close']:.2f}")
        else:
            print("  No historical data available")
    
    # Test ETF price data retrieval  
    for etf in ['SPY', 'QQQ']:
        print(f"\nFetching {etf} history from {start_date.date()} to {end_date.date()}...")
        
        prices = fetcher.get_etf_prices(etf, start_date, end_date)
        
        if prices:
            print(f"  Retrieved {len(prices)} price records")
            print(f"  First record: {prices[0]['timestamp'].date()} - ${prices[0]['close']:.2f}")  
            print(f"  Last record: {prices[-1]['timestamp'].date()} - ${prices[-1]['close']:.2f}")
        else:
            print("  No ETF data available")
    
    print("\n" + "=" * 60)
    print("yfinance Data Fetcher Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
