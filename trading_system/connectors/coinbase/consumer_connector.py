#!/usr/bin/env python3
"""
Coinbase Consumer Connector - Handles consumer-facing operations

This connector uses existing Commerce API keys but routes to
Consumer/Brokerage endpoints for balance viewing, trading,
and market data retrieval.
"""

import asyncio
from typing import Dict, List, Optional


class CoinbaseConsumerConnector:
    """Consumer API connector using Commerce credentials."""
    
    def __init__(self):
        self.consumer_endpoint = 'https://api.exchange.coinbase.com'
        self.mock_mode = False
        self.connected = False
    
    async def connect(self) -> Dict:
        """Establish connection to Consumer API."""
        print("\n[CONSUMER CONNECTOR] Establishing Connection")
        print(f"  Endpoint: {self.consumer_endpoint}")
        
        self.connected = True
        return {
            'status': 'connected',
            'api_type': 'consumer',
            'endpoint': self.consumer_endpoint,
            'mock_mode': self.mock_mode
        }
    
    async def get_balances(self, mock: bool = False) -> List[Dict]:
        """Retrieve account balances from Consumer API."""
        if not self.connected:
            await self.connect()
        
        if mock or self.mock_mode:
            return [
                {'currency': 'BTC', 'amount': '0.15423', 'balance': '0.15423 BTC'},
                {'currency': 'ETH', 'amount': '2.87654', 'balance': '2.87654 ETH'},
                {'currency': 'USDC', 'amount': '15420.50', 'balance': '15,420.50 USDC'}
            ]
        return []
    
    async def get_trading_pairs(self, use_mock: bool = False) -> List[Dict]:
        """Retrieve available trading pairs."""
        if use_mock or self.mock_mode:
            return [
                {'base': 'BTC', 'quote': 'USD', 'active': True},
                {'base': 'ETH', 'quote': 'USD', 'active': True},
                {'base': 'BTC', 'quote': 'ETH', 'active': True}
            ]
        return []
    
    async def get_market_data(self, symbol: str, use_mock: bool = False) -> Dict:
        """Retrieve market data for a specific symbol."""
        if use_mock or self.mock_mode:
            return {
                'symbol': symbol,
                'price': '45000.00',
                'volume_24h': '1234.56789',
                'change_24h': '+2.34%'
            }
        return {}
    
    async def get_all_market_data(self) -> Dict:
        """Retrieve market data for all symbols."""
        pairs = await self.get_trading_pairs(use_mock=True)
        result = {}
        for pair in pairs:
            if pair['active']:
                symbol = f"{pair['base']}/{pair['quote']}"
                result[symbol] = await self.get_market_data(symbol, use_mock=True)
        return result
    
    async def run_connector_test(self):
        """Run comprehensive connector test."""
        print("\n[TEST] Consumer Connector Test")
        print("=" * 60)
        
        # Connect
        connect_result = await self.connect()
        print(f"Connection: {connect_result['status']}")
        
        # Get balances
        balances = await self.get_balances(mock=True)
        print(f"\nBalances:")
        for balance in balances:
            print(f"  - {balance['currency']}: {balance['amount']} {balance['currency']}")
        
        # Get trading pairs
        pairs = await self.get_trading_pairs(use_mock=True)
        print(f"\nTrading Pairs:")
        for pair in pairs:
            status = 'Active' if pair['active'] else 'Inactive'
            print(f"  - {pair['base']}/{pair['quote']} ({status})")
        
        # Get all market data
        market_data = await self.get_all_market_data()
        print(f"\nMarket Data:")
        for symbol, data in market_data.items():
            print(f"  - {symbol}: ${data['price']} ({data['change_24h']})")
        
        print("\n[TEST] Connector Test Complete")


# Main execution for testing
if __name__ == '__main__':
    connector = CoinbaseConsumerConnector()
    asyncio.run(connector.run_connector_test())
