#!/usr/bin/env python3
"""
Backtesting Integration Module - Provides unified access to historical market data for backtesting.

This module integrates multiple data sources (yfinance, Alpha Vantage, Coinbase Advanced Trade API) 
to provide consistent historical price data for backtesting trading strategies and portfolio management systems.

The integration follows a priority order:
1. yfinance (free tier, no API key needed) - stocks/ETFs  
2. Alpha Vantage (requires user-provided API key) - stocks/ETFs/crypto
3. Coinbase Advanced Trade API (requires user-provided API key) - crypto assets

This enables backtesting with real historical data from multiple sources without requiring 
paid subscriptions or proprietary APIs.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class BacktestDataProvider:
    """Unified provider for backtesting historical market data."""
    
    def __init__(self):
        self._adapter = None
    
    @property  
    def adapter(self):
        """Lazy initialization of unified market data adapter."""
        if self._adapter is None:
            from data.fetch_unified import UnifiedMarketDataAdapter
            self._adapter = UnifiedMarketDataAdapter()
        return self._adapter
    
    def get_historical_prices(
        self, 
        symbol: str,
        start_date: datetime,  
        end_date: datetime
    ) -> List[Dict[str, Any]]:
        """Get historical price data for backtesting.
        
        Args:
            symbol: Trading pair symbol (e.g., 'AAPL', 'BTC-USD')
            start_date: Start date in YYYY-MM-DD format  
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of bar objects with open, high, low, close, volume
        """
        return self.adapter.fetch_historical_data(symbol, str(start_date), str(end_date))
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for backtesting.
        
        Args:
            symbol: Trading pair symbol (e.g., 'AAPL', 'BTC-USD')
            
        Returns:
            Current price or None if unavailable  
        """
        return self.adapter.get_current_price(symbol)


def main():
    """Main entry point for testing backtesting data provider."""
    
    print("=" * 60)
    print("Backtesting Data Provider Test")
    print("=" * 60)
    
    # Initialize the provider  
    provider = BacktestDataProvider()
    
    # Test historical data retrieval for stocks (yfinance first, Alpha Vantage fallback)
    for symbol in ['AAPL', 'MSFT']:
        start_date = datetime.strptime("2024-12-31", '%Y-%m-%d')
        end_date = datetime.now()
        
        print(f"\nFetching {symbol} history from {start_date.date()} to {end_date.date()}...")
        
        bars = provider.get_historical_prices(symbol, start_date, end_date)
        
        if bars:
            print(f"  ✅ Retrieved {len(bars)} price records")  
            print(f"     First record: {bars[0]['timestamp'].date()} - ${bars[0]['close']:.2f}")
            print(f"     Last record: {bars[-1]['timestamp'].date()} - ${bars[-1]['close']:.2f}")
        else:
            print("  ❌ No historical data available")
    
    # Test crypto price retrieval (Coinbase Advanced Trade API)  
    for symbol in ['BTC-USD', 'ETH-USD']:
        start_date = datetime.strptime("2024-12-31", '%Y-%m-%d')
        end_date = datetime.now()
        
        print(f"\nFetching {symbol} history from {start_date.date()} to {end_date.date()}...")
        
        bars = provider.get_historical_prices(symbol, start_date, end_date)
        
        if bars:
            print(f"  ✅ Retrieved {len(bars)} price records")  
            print(f"     First record: {bars[0]['timestamp'].date()} - ${bars[0]['close']:.2f}")
            print(f"     Last record: {bars[-1]['timestamp'].date()} - ${bars[-1]['close']:.2f}")
        else:
            print("  ❌ No crypto data available")
    
    # Test current price retrieval  
   for symbol in ['AAPL', 'BTC-USD']:
        price = provider.get_current_price(symbol)
        
        if price is not None:
            print(f"\n{symbol} Current Price: ${price:.2f}")
        else:
            print(f"\n{symbol}: No current price available")
    
    print("\n" + "=" * 60)  
    print("Backtesting Data Provider Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
