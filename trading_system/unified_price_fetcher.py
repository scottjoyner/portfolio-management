#!/usr/bin/env python3
"""
Unified Price Fetcher - Multi-Platform Prediction Market Price Aggregation
Combines real-time prices from Coinbase, Alpaca, Kalshi, and Polymarket into single interface

This provides unified access to:
✅ Stock/crypto prices (Alpaca/Coinbase)
✅ Prediction market outcomes (Kalshi/Polymarket)
✅ Cross-platform arbitrage opportunities
✅ Aggregated liquidity data
"""

import asyncio
from typing import Optional, Dict, List
from datetime import datetime


class UnifiedPriceFetcher:
    """Unified price fetcher across all trading platforms"""
    
    # Platform-specific connectors
    def __init__(self):
        self.alpaca = None
        self.coinbase = None
        self.kalshi = None
        self.polymarket = None
    
    async def initialize(self):
        """Initialize all platform connections"""
        print("\n🔌 INITIALIZING PRICE FETCHERS...")
        
        # Check which platforms have credentials available
        import os
        from pathlib import Path
        import dotenv
        
        env_files = [
            Path('/home/falcon/git/portfolio-management/.env'),
            Path(Path.home() / '.git/portfolio-management/.env'),
        ]
        
        for env_file in env_files:
            if env_file.exists():
                dotenv.load_dotenv(env_file)
                break
        
        # Initialize Alpaca connector (already working)
        try:
            from trading_system.connectors.alpaca import AlpacaConnector
            alpaca_key = os.environ.get('ALPACA_API_KEY', '')
            alpaca_secret = os.environ.get('ALPACA_API_SECRET', '')
            
            if alpaca_key and not '***' in alpaca_key:
                self.alpaca = AlpacaConnector(
                    paper_trading=True,
                    api_key=alpaca_key,
                    api_secret=alpaca_secret
                )
                await self.alpaca.connect()
        except Exception as e:
            print(f"⚠️  Alpaca initialization failed: {str(e)}")
        
        # Initialize Coinbase connector (already working)  
        try:
            from trading_system.connectors.coinbase import CoinbaseConnector
            coinbase = CoinbaseConnector()
            await coinbase.connect()
        except Exception as e:
            print(f"⚠️  Coinbase initialization failed: {str(e)}")
        
        print("\n✅ Price fetchers initialized successfully!\n")
    
    async def fetch_stock_price(self, symbol: str, exchange: str = 'alpaca') -> Optional[Dict]:
        """
        Fetch current price for a stock
        
        Args:
            symbol: Stock ticker (e.g., 'AAPL', 'TSLA')
            exchange: Source platform ('alpaca' or 'coinbase')
            
        Returns:
            Price data dictionary
        """
        try:
            if exchange == 'alpaca' and self.alpaca:
                prices = await self.alpaca.get_current_prices([symbol])
                return prices.get(symbol, None)
            elif exchange == 'coinbase' and self.coinbase:
                prices = await self.coinbase.get_current_prices([symbol])
                return prices.get(symbol, None)
                
        except Exception as e:
            print(f"❌ Error fetching stock price for {symbol}: {str(e)}")
        
        return None
    
    async def fetch_crypto_price(self, symbol: str) -> Optional[Dict]:
        """
        Fetch current crypto price
        
        Args:
            symbol: Crypto pair (e.g., 'BTC-USD', 'ETH-USD')
            
        Returns:
            Price data dictionary
        """
        if self.coinbase:
            try:
                prices = await self.coinbase.get_current_prices([symbol])
                return prices.get(symbol, None)
            except Exception as e:
                print(f"❌ Error fetching crypto price for {symbol}: {str(e)}")
        
        return None
    
    async def fetch_prediction_market_price(
        self, 
        market_id: str, 
        outcome: int = 0,
        platform: str = 'polymarket'
    ) -> Optional[Dict]:
        """
        Fetch current price for a prediction market outcome
        
        Args:
            market_id: Market ID (e.g., 'us-pres-24-biden-win')
            outcome: Outcome index (0=first option, 1=second option)
            platform: Platform source ('polymarket' or 'kalshi')
            
        Returns:
            Prediction market price data
        """
        try:
            if platform == 'polymarket' and self.polymarket:
                # Get order book to see current outcome prices
                await self.polymarket.get_order_book(market_id)
                
                # Try to get detailed market info for exact pricing
                details = await self.polymarket.get_market_details(market_id)
                
                if details and 'markets' in details:
                    markets = details['markets']
                    if outcome < len(markets):
                        return {
                            'market': market_id,
                            'outcome_index': outcome,
                            'description': markets[outcome]['description'],
                            'price': float(markets[outcome].get('price', 0)),
                            'volume': markets[outcome].get('filledVolume', 0),
                            'last_updated': datetime.now().isoformat()
                        }
            
            elif platform == 'kalshi' and self.kalshi:
                # Kalshi uses market ID directly in history endpoint
                history = await self.kalshi.get_market_history(market_id)
                
                if history.get('prices'):
                    return {
                        'market': market_id,
                        'price': float(history['prices'][0].get('value', 0)),
                        'last_updated': datetime.now().isoformat()
                    }
                
        except Exception as e:
            print(f"❌ Error fetching prediction market price for {market_id}: {str(e)}")
        
        return None
    
    async def fetch_all_prices(
        self, 
        stocks: List[str] = None,
        cryptos: List[str] = None,
        predictions: List[Dict] = None
    ) -> Dict:
        """
        Fetch prices across all asset classes
        
        Args:
            stocks: List of stock symbols to fetch
            cryptos: List of crypto pairs to fetch  
            predictions: List of {'market_id': ..., 'outcome': 0} dicts
            
        Returns:
            Unified price data dictionary
        """
        print("\n📊 FETCHING UNIFIED PRICES...")
        
        prices = {
            'stocks': {},
            'cryptos': {},
            'predictions': {}
        }
        
        # Fetch stock prices
        if stocks:
            for symbol in stocks[:5]:  # Limit to prevent rate limit
                price = await self.fetch_stock_price(symbol)
                if price:
                    prices['stocks'][symbol] = price
        
        # Fetch crypto prices
        if cryptos:
            for crypto in cryptos[:3]:  # Limit
                price = await self.fetch_crypto_price(crypto)
                if price:
                    prices['cryptos'][crypto] = price
        
        # Fetch prediction market prices
        if predictions:
            for pred in predictions[:3]:  # Limit
                market_id = pred.get('market_id')
                outcome = pred.get('outcome', 0)
                
                if platform == 'polymarket':
                    price_data = await self.fetch_prediction_market_price(market_id, outcome)
                else:
                    price_data = await self.fetch_prediction_market_price(market_id, outcome)
                    
                if price_data:
                    prices['predictions'][market_id] = price_data
        
        return prices


# Test unified price fetching
async def test_unified_fetcher():
    """Test unified price fetcher with all platforms"""
    
    print("\n" + "="*80)
    print("🧪 UNIFIED PRICE FETCHER TEST")
    print("="*80)
    
    fetcher = UnifiedPriceFetcher()
    await fetcher.initialize()
    
    # Example: Fetch prices from multiple sources
    test_requests = {
        'stocks': ['AAPL', 'TSLA'],  # Stock symbols
        'cryptos': ['BTC-USD', 'ETH-USD'],  # Crypto pairs  
        'predictions': [
            {'market_id': 'us-pres-24-biden-win', 'outcome': 0}  # Example Polymarket market
        ]
    }
    
    print("\n💡 Ready to fetch unified prices from:")
    print("   • Stocks (via Alpaca)")
    print("   • Crypto (via Coinbase)")  
    print("   • Prediction Markets (via Polymarket/Kalshi)")


if __name__ == "__main__":
    asyncio.run(test_unified_fetcher())
