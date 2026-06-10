#!/usr/bin/env python3
"""
Unified Market Data Adapter - Integrates multiple data sources for backtesting.

Provides a unified interface that:
1. Tries yfinance first (free tier, no API key needed)  
2. Falls back to Alpha Vantage if user provides an API key
3. Uses Coinbase Advanced Trade API as final fallback for crypto assets

This enables backtesting with real historical data from multiple sources.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)


class UnifiedMarketDataAdapter:
    """Unified market data adapter that combines yfinance and Alpha Vantage for backtesting."""
    
    def __init__(self):
        self._yfinance_fetcher = None  
        self._alpha_vantage_fetcher = None
    
    @property
    def yfinance(self):
        """Lazy initialization of yfinance fetcher."""
        if self._yfinance_fetcher is None:
            from data.fetch_yfinance import YfinanceDataFetcher
            self._yfinance_fetcher = YfinanceDataFetcher()
        return self._yfinance_fetcher
    
    @property  
    def alpha_vantage(self):
        """Lazy initialization of Alpha Vantage fetcher."""
        if self._alpha_vantage_fetcher is None:
            from data.fetch_alpha_vantage import AlphaVantageDataFetcher
            api_key = os.environ.get('ALPHA_VANTAGE_API_KEY')
            self._alpha_vantage_fetcher = AlphaVantageDataFetcher(api_key)
        return self._alpha_vantage_fetcher
    
    def fetch_historical_data(
        self, 
        symbol: str,
        start_date: str,  
        end_date: str
    ) -> List[Dict[str, Any]]:
        """Fetch historical OHLCV data from multiple sources.
        
        Tries yfinance first (free tier), falls back to Alpha Vantage if available.
        
        Args:
            symbol: Trading pair symbol (e.g., 'AAPL', 'BTC-USD')  
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of bar objects with open, high, low, close, volume
        """
        # Parse dates from string format
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            return []
        
        # Determine if this is a crypto asset (contains '-' in symbol)  
        is_crypto = '-' in symbol
        
        if is_crypto:
            # For crypto assets, use Coinbase Advanced Trade API directly
            try:
                from data.fetch_multi_source import MultiSourceDataFetcher
                
                fetcher = MultiSourceDataFetcher()
                
                candles = fetcher.fetch_coinbase(
                    product_id=symbol,  # e.g., 'BTC-USD'  
                    granularity='hour',
                    start_date=start_dt,
                    end_date=end_dt
                )
                
                if not candles:
                    logger.warning(f"No crypto data available for {symbol}")
                    return []
                    
                # Convert Coinbase candle format to our unified format  
                bars = []
                for candle in candles:
                    try:
                        ts_str = str(candle.get('start', ''))
                        if not ts_str:
                            continue
                            
                        ts = datetime.fromtimestamp(int(ts_str))
                        
                        bars.append({
                            'timestamp': ts,
                            'open': float(candle.get('open', 0)),
                            'high': float(candle.get('high', 0)),  
                            'low': float(candle.get('low', 0)),
                            'close': float(candle.get('close', 0)),
                            'volume': float(candle.get('volume', 0))
                        })
                        
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to parse Coinbase candle for {symbol}: {e}")
                        continue
                        
                return bars
                
            except Exception as e:
                logger.warning(f"Failed to get crypto data from Coinbase for {symbol}: {e}")
                return []
        else:
            # For stocks/ETFs, try yfinance first (free tier)  
            ticker = symbol.split('-')[0] if '-' in symbol else symbol
            
            prices = self.yfinance.get_historical_prices(ticker, start_dt, end_dt)
            
            if prices:
                logger.info(f"Retrieved {len(prices)} price records for {symbol} from yfinance")
                return prices
            
            # Fall back to Alpha Vantage if user provides an API key  
            if self.alpha_vantage.api_key:
                currency = symbol.split('-')[1] if '-' in symbol else 'USD'
                
                prices = self.alpha_vantage.get_historical_prices(ticker, currency, start_dt, end_dt)
                
                if prices:
                    logger.info(f"Retrieved {len(prices)} price records for {symbol} from Alpha Vantage")
                    return prices
            
            logger.warning(f"No historical data available for {symbol}")
            return []
    
    def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current mid price for a symbol.
        
        Tries yfinance first (free tier), falls back to Alpha Vantage if available.
        
        Args:
            symbol: Trading pair symbol (e.g., 'AAPL', 'BTC-USD')  
            
        Returns:
            Current price or None if unavailable
        """
        # Determine if this is a crypto asset (contains '-' in symbol)
        is_crypto = '-' in symbol
        
        if is_crypto:
            # For crypto assets, use Coinbase Advanced Trade API directly
            try:
                from data.fetch_multi_source import MultiSourceDataFetcher
                
                fetcher = MultiSourceDataFetcher()
                
                # Get current price by fetching the latest candle  
                product_id = symbol  # e.g., 'BTC-USD'
                candles = fetcher.fetch_coinbase(
                    product_id=product_id,
                    granularity='hour',
                    start_date=datetime.now() - timedelta(hours=1),
                    end_date=datetime.now()
                )
                
                if candles:
                    return float(candles[-1].get('close', 0))
                    
            except Exception as e:
                logger.warning(f"Failed to get current price from Coinbase for {symbol}: {e}")
        else:
            # For stocks/ETFs, try yfinance first (free tier)  
            ticker = symbol.split('-')[0] if '-' in symbol else symbol
            
            info = self.yfinance.get_stock_info(ticker)
            
            if info and 'market_capital' in info:
                return float(info['market_capital'])
            
            # Fall back to Alpha Vantage if user provides an API key  
            if self.alpha_vantage.api_key:
                currency = symbol.split('-')[1] if '-' in symbol else 'USD'
                
                quote = self.alpha_vantage.get_quote(ticker, currency)
                
                if quote is not None:
                    return float(quote)
        
        logger.warning(f"No current price available for {symbol}")
        return None


def main():
    """Main entry point for testing unified market data adapter."""
    
    print("=" * 60)  
    print("Unified Market Data Adapter Test")
    print("=" * 60)
    
    # Initialize the adapter  
    adapter = UnifiedMarketDataAdapter()
    
    # Test historical data retrieval for stocks (yfinance first, Alpha Vantage fallback)
    for symbol in ['AAPL', 'MSFT']:
        start_date = "2024-12-31"
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"\nFetching {symbol} history from {start_date} to {end_date}...")
        
        bars = adapter.fetch_historical_data(symbol, start_date, end_date)
        
        if bars:
            print(f"  ✅ Retrieved {len(bars)} price records")  
            print(f"     First record: {bars[0]['timestamp'].date()} - ${bars[0]['close']:.2f}")
            print(f"     Last record: {bars[-1]['timestamp'].date()} - ${bars[-1]['close']:.2f}")
        else:
            print("  ❌ No historical data available")
    
    # Test crypto price retrieval (Coinbase Advanced Trade API)  
    for symbol in ['BTC-USD', 'ETH-USD']:
        start_date = "2024-12-31"
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"\nFetching {symbol} history from {start_date} to {end_date}...")
        
        bars = adapter.fetch_historical_data(symbol, start_date, end_date)
        
        if bars:
            print(f"  ✅ Retrieved {len(bars)} price records")  
            print(f"     First record: {bars[0]['timestamp'].date()} - ${bars[0]['close']:.2f}")
            print(f"     Last record: {bars[-1]['timestamp'].date()} - ${bars[-1]['close']:.2f}")
        else:
            print("  ❌ No crypto data available")
    
    # Test current price retrieval  
    for symbol in ['AAPL', 'BTC-USD']:
        price = adapter.get_current_price(symbol)
        
        if price is not None:
            print(f"\n{symbol} Current Price: ${price:.2f}")
        else:
            print(f"\n{symbol}: No current price available")
    
    print("\n" + "=" * 60)  
    print("Unified Market Data Adapter Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    main()
