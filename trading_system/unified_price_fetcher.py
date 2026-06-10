from __future__ import annotations
from typing import Optional, Dict, List, Any
from datetime import datetime
import asyncio
import os
import dotenv
from pathlib import Path

from trading_system.core.providers.base import BaseProvider
from trading_system.core.providers.alpaca import AlpacaProvider
from trading_system.core.providers.coinbase import CoinbaseProvider
from trading_system.core.providers.kraken import KrakenProvider
from trading_system.core.providers.binance import BinanceProvider
from trading_system.core.providers.prediction_markets import KalshiProvider, PolymarketProvider

class UnifiedPriceFetcher:
    """Unified price fetcher across all trading platforms using the Provider Pattern."""
    
    def __init__(self):
        self.providers: Dict[str, BaseProvider] = {}
        
    async def initialize(self):
        print("\n🔌 INITIALIZING PRICE FETCHERS...")
        
        env_files = [
            Path('/home/falcon/git/portfolio-management/.env'),
            Path(Path.home() / '.git/portfolio-management/.env'),
        ]
        
        for env_file in env_files:
            if env_file.exists():
                dotenv.load_dotenv(env_file)
                break
        
        # Initialize Providers
        providers_to_init = [
            (AlpacaProvider, {'api_key': os.environ.get('ALPACA_API_KEY'), 'api_secret': os.environ.get('ALPACA_API_SECRET'), 'paper_trading': True}),
            (CoinbaseProvider, {'api_key': os.environ.get('COINBASE_API_KEY'), 'api_secret': os.environ.get('COINBASE_API_SECRET')}),
            (KrakenProvider, {'api_key': os.environ.get('KRAKEN_API_KEY'), 'api_secret': os.environ.get('KRAKEN_API_SECRET')}),
            (BinanceProvider, {'api_key': os.environ.get('BINANCE_API_KEY'), 'api_secret': os.environ.get('BINANCE_API_SECRET')}),
            (KalshiProvider, {}),
            (PolymarketProvider, {})
        ]
        
        for provider_cls, kwargs in providers_to_init:
            try:
                provider = provider_cls(**kwargs)
                await provider.connect()
                self.providers[provider.get_name().lower()] = provider
            except Exception as e:
                print(f"⚠️  {provider_cls.__name__} initialization failed: {str(e)}")
        
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
        provider = self.providers.get(exchange.lower())
        if provider:
            try:
                prices = await provider.get_current_prices([symbol])
                return prices.get(symbol, None)
            except Exception as e:
                print(f"❌ Error fetching stock price from {exchange}: {str(e)}")
        else:
            print(f"⚠️  Provider '{exchange}' not found.")
        
        return None

    async def fetch_crypto_price(self, symbol: str) -> Optional[Dict]:
        """
        Fetch current crypto price (Defaulting to Coinbase)
        """
        provider = self.providers.get('coinbase')
        if provider:
            try:
                prices = await provider.get_current_prices([symbol])
                return prices.get(symbol, None)
            except Exception as e:
                print(f"❌ Error fetching crypto price from coinbase: {str(e)}")
        
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
            market_id: Market ID
            outcome: Outcome index
            platform: Platform source ('polymarket' or 'kalshi')
            
        Returns:
            Prediction market price data
        """
        provider = self.providers.get(platform.lower())
        if provider:
            try:
                # We'll need to add specific methods to the mocks for this to be useful
                # But for now, we'll just call get_current_prices as a fallback
                prices = await provider.get_current_prices([market_id])
                return {
                    'market': market_id,
                    'price': prices.get(market_id, 0.0),
                    'last_updated': datetime.now().isoformat()
                }
            except Exception as e:
                print(f"❌ Error fetching prediction market price from {platform}: {str(e)}")
        
        return None

    async def fetch_all_prices(
        self, 
        stocks: List[str] = None,
        cryptos: List[str] = None,
        predictions: List[Dict] = None
    ) -> Dict:
        """
        Fetch prices across all asset classes
        """
        print("\n📊 FETCHING UNIFIED PRICES...")
        
        prices = {
            'stocks': {},
            'cryptos': {},
            'predictions': {}
        }
        
        if stocks:
            for symbol in stocks[:5]:
                price = await self.fetch_stock_price(symbol)
                if price:
                    prices['stocks'][symbol] = price
        
        if cryptos:
            for crypto in cryptos[:3]:
                price = await self.fetch_crypto_price(crypto)
                if price:
                    prices['cryptos'][crypto] = price
        
        if predictions:
            for pred in predictions[:3]:
                market_id = pred.get('market_id')
                platform = pred.get('platform', 'polymarket')
                
                if market_id:
                    price_data = await self.fetch_prediction_market_price(market_id, pred.get('outcome', 0), platform)
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
    
    test_requests = {
        'stocks': ['AAPL', 'TSLA'],
        'cryptos': ['BTC-USD', 'ETH-USD'],
        'predictions': [
            {'market_id': 'us-pres-24-biden-win', 'outcome': 0, 'platform': 'polymarket'}
        ]
    }
    
    prices = await fetcher.fetch_all_prices(
        stocks=test_requests['stocks'],
        cryptos=test_requests['cryptos'],
        predictions=test_requests['predictions']
    )
    
    print("\n📊 FETCHED PRICES:")
    import json
    print(json.dumps(prices, indent=2))
    print("\n" + "="*80)

if __name__ == "__main__":
    asyncio.run(test_unified_fetcher())
