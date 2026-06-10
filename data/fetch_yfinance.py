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
            from yfinance.ticker import Ticker
            
            # Use cached instance if available
            if ticker not in self._ticker_cache:
                self._ticker_cache[ticker] = Ticker(ticker)
                
            ticker_obj = self._ticker_cache[ticker]
            # yfinance Ticker doesn't have proper typing annotations
            from typing import cast
            typed_ticker = cast(Ticker, ticker_obj)
            info = typed_ticker.get_info()
            
            return {
                'symbol': ticker,
                'name': info.get('info', {}).get('company_name', '') if info else '',
                'sector': info.get('info', {}).get('sector', '') if info else '',
                'market_capital': info.get('info', {}).get('capitalization', 0) if info else 0,
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
            from yfinance.ticker import Ticker
            
            # Use cached instance if available
            if ticker not in self._ticker_cache:
                self._ticker_cache[ticker] = Ticker(ticker)
                
            ticker_obj = self._ticker_cache[ticker]
            
            # Get historical data using the history_metadata method  
            metadata = ticker_obj.history_metadata(
                start_date.strftime('%Y-%m-%d'), 
                end_date.strftime('%Y-%m-%d')
            )
            
            if not metadata:
                logger.warning(f"No historical data available for {ticker}")
                return []
                
            # Parse the metadata to extract price information
            prices = []
            for item in metadata.get('history', []):
                try:
                    ts_str = item.get('date', '')
                    if not ts_str:
                        continue
                        
                    ts = datetime.strptime(ts_str, '%Y-%m-%d')
                    
                    # Extract price data from the history record
                    prices.append({
                        'timestamp': ts,
                        'open': float(item.get('open', 0)),
                        'high': float(item.get('high', 0)),
                        'low': float(item.get('low', 0)),
                        'close': float(item.get('close', 0)),
                        'volume': float(item.get('volume', 0))
                    })
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse history record for {ticker}: {e}")
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
            from yfinance import ETFQuery
            
            # Create a query instance for the ETF
            query = ETFQuery(etf_symbol)
            
            # Get tickers (should work without authentication)  
            result = query.get_tickers()
            
            if not result or 'tickers' not in result:
                logger.warning(f"No ticker data available for ETF {etf_symbol}")
                return []
                
            # Extract price information from the ticker data
            prices = []
            for ticker_data in result['tickers']:
                try:
                    ts_str = ticker_data.get('date', '')
                    if not ts_str:
                        continue
                        
                    ts = datetime.strptime(ts_str, '%Y-%m-%d')
                    
                    # Extract price data from the ticker record  
                    prices.append({
                        'timestamp': ts,
                        'open': float(ticker_data.get('open', 0)),
                        'high': float(ticker_data.get('high', 0)),
                        'low': float(ticker_data.get('low', 0)),
                        'close': float(ticker_data.get('close', 0)),
                        'volume': float(ticker_data.get('volume', 0))
                    })
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse ETF ticker record for {etf_symbol}: {e}")
                    continue
                    
            return prices
            
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
