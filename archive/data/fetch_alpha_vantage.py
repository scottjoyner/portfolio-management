#!/usr/bin/env python3
"""
Alpha Vantage Data Fetcher - Unified interface for Alpha Vantage API.

Provides access to:
- Real-time quotes (crypto, stocks, commodities)  
- Historical price data with configurable time ranges
- Technical indicators and economic data

Requires an Alpha Vantage API key (free tier available).
API key can be provided via constructor or ALPHA_VANTAGE_API_KEY environment variable.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class AlphaVantageDataFetcher:
    """Unified interface for fetching data from Alpha Vantage API."""
    
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the fetcher with an optional API key.
        
        Args:
            api_key: Alpha Vantage API key (optional)
            
        If no API key is provided, it will be read from the 
        ALPHA_VANTAGE_API_KEY environment variable.
        """
        self.api_key = api_key or os.environ.get('ALPHA_VANTAGE_API_KEY')
        
    def get_quote(self, symbol: str, currency: str) -> Optional[float]:
        """Get real-time quote for a symbol and currency pair.
        
        Args:
            symbol: Asset ticker (e.g., 'BTC', 'AAPL', 'GOOGL')  
            currency: Currency code (e.g., 'USD', 'EUR')
            
        Returns:
            Current price as float, or None if unavailable
        """
        try:
            # alpha_vantage.alphavantage has unusual package structure  
            from alpha_vantage import AlphaVantage  # type: ignore
            
            client = AlphaVantage(self.api_key)
            quote = client.quote(symbol, currency)
            
            # Extract the quote value from the response  
            if quote and 'quote' in quote:
                return float(quote['quote'])
                
        except Exception as e:
            logger.warning(f"Failed to get quote for {symbol}-{currency}: {e}")
            
        return None
    
    def get_historical_prices(
        self, 
        symbol: str, 
        currency: str,
        start_date: datetime, 
        end_date: datetime,
        granularity: str = '1d'
    ) -> List[Dict[str, Any]]:
        """Get historical price data for a symbol and currency pair.
        
        Args:
            symbol: Asset ticker (e.g., 'BTC', 'AAPL')  
            currency: Currency code (e.g., 'USD', 'EUR')
            start_date: Start date for the query
            end_date: End date for the query
            granularity: Time granularity ('1d' for daily, '1h' for hourly)
            
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
            from alpha_vantage.alphavantage import AlphaVantage
            
            client = AlphaVantage(self.api_key)
            
            # Get historical data using the timeseries endpoint  
            response = client.timeseries(
                symbol, 
                currency=currency,
                period=granularity,
                start_date=start_date.strftime('%Y-%m-%d'),
                end_date=end_date.strftime('%Y-%m-%d')
            )
            
            if not response or 'timeseries' not in response:
                logger.warning(f"No historical data available for {symbol}-{currency}")
                return []
                
            # Parse the timeseries response to extract price information  
            prices = []
            for item in response['timeseries']:
                try:
                    ts_str = item.get('date', '')
                    if not ts_str:
                        continue
                        
                    ts = datetime.strptime(ts_str, '%Y-%m-%d')
                    
                    # Extract price data from the timeseries record  
                    prices.append({
                        'timestamp': ts,
                        'open': float(item.get('Open', 0)),
                        'high': float(item.get('High', 0)),
                        'low': float(item.get('Low', 0)),
                        'close': float(item.get('Close', 0)),
                        'volume': float(item.get('Volume', 0))
                    })
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse timeseries record for {symbol}: {e}")
                    continue
                    
            return prices
            
        except Exception as e:
            logger.warning(f"Failed to get historical data for {symbol}-{currency}: {e}")
            return []


def main():
    """Main entry point for testing alpha-vantage data fetching."""
    
    print("=" * 60)
    print("Alpha Vantage Data Fetcher Test")  
    print("=" * 60)
    
    # Initialize the fetcher (will use API key from environment if available)
    api_key = os.environ.get('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        print("\nNo ALPHA_VANTAGE_API_KEY found in environment.")
        print("Set it with: export ALPHA_VANTAGE_API_KEY=your_api_key")
        return
        
    fetcher = AlphaVantageDataFetcher(api_key)
    
    # Test real-time quote retrieval  
    for symbol, currency in [('BTC', 'USD'), ('ETH', 'USD')]:
        quote = fetcher.get_quote(symbol, currency)
        
        if quote:
            print(f"\n{symbol}-{currency} Quote:")
            print(f"  Current Price: ${quote:,.2f}")
        else:
            print(f"\n{symbol}-{currency}: No quote available")
    
    # Test historical price data retrieval  
    start_date = datetime(2024, 12, 31)
    end_date = datetime.now()
    
    for symbol in ['BTC', 'ETH']:
        print(f"\nFetching {symbol} history from {start_date.date()} to {end_date.date()}...")
        
        prices = fetcher.get_historical_prices(symbol, 'USD', start_date, end_date)
        
        if prices:
            print(f"  Retrieved {len(prices)} price records")
            print(f"  First record: {prices[0]['timestamp'].date()} - ${prices[0]['close']:.2f}")  
            print(f"  Last record: {prices[-1]['timestamp'].date()} - ${prices[-1]['close']:.2f}")
        else:
            print("  No historical data available")
    
    # Test with a stock ticker (requires API key)
    for symbol in ['AAPL', 'MSFT']:  
        print(f"\nFetching {symbol} history from {start_date.date()} to {end_date.date()}...")
        
        prices = fetcher.get_historical_prices(symbol, 'USD', start_date, end_date)
        
        if prices:
            print(f"  Retrieved {len(prices)} price records")  
            print(f"  First record: {prices[0]['timestamp'].date()} - ${prices[0]['close']:.2f}")
            print(f"  Last record: {prices[-1]['timestamp'].date()} - ${prices[-1]['close']:.2f}")
        else:
            print("  No historical data available")
    
    print("\n" + "=" * 60)  
    print("Alpha Vantage Data Fetcher Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
