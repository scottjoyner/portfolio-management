#!/usr/bin/env python3
"""Integration verification test for portfolio management trading systems"""
import asyncio
import os
from pathlib import Path
import dotenv

# Load environment
dotenv.load_dotenv('.env')

print('='*60)
print('=== INTEGRATION VERIFICATION ===')
print('='*60 + '\n')

print('Configured Credentials:')
key = os.environ.get('COINBASE_API_KEY', 'NOT SET')
print(f'  Coinbase API Key: {key}')
key2 = os.environ.get('ALPACA_API_KEY', 'Not Set')
print(f'  Alpaca API Key:   {key2}\n')

async def main():
    # Test Coinbase
    try:
        from trading_system.connectors.coinbase import CoinbaseConnector
        c = CoinbaseConnector()
        await c.connect()
        prices = await c.get_current_prices(['BTC-USD', 'ETH-USD'])
        print(f'Coinbase: ✓ WORKING')
        for s, p in prices.items():
            if p > 0:
                print(f'    {s}: {p:.2f}')
    except Exception as e:
        print(f'Coinbase: ✗ ERROR - {e}')
    
    # Test Alpaca
    try:
        from trading_system.connectors.alpaca import AlpacaConnector
        a = AlpacaConnector()
        await a.connect()
        prices = await a.get_current_prices(['AAPL', 'MSFT'])
        print(f'\nAlpaca: ✓ WORKING')
        for s, p in prices.items():
            if p > 0:
                print(f'    {s}: {p:.2f}')
    except Exception as e:
        print(f'\nAlpaca: ✗ ERROR - {e}')

asyncio.run(main())
print('\n=== VERIFICATION COMPLETE ===\n')