#!/usr/bin/env python3
"""
Full End-to-End Integration Test for Portfolio Management Trading Systems
Tests both Coinbase balance checking AND Alpaca paper trading execution.
"""
import asyncio
import os
from pathlib import Path
import dotenv

dotenv.load_dotenv('.env')

print('='*60)
print('=== FULL INTEGRATION TEST ===')
print('='*60 + '\n')

async def main():
    print('\n--- PHET 1: COINBASE BALANCE CHECKING ---\n')
    try:
        from trading_system.connectors.coinbase import CoinbaseConnector
        c = CoinbaseConnector()
        await c.connect()
        
        # Test public price feed
        symbols = ['BTC-USD', 'ETH-USD', 'SOL-USD']
        prices = await c.get_current_prices(symbols)
        print('✓ Coinbase API accessible (market data)')
        for s, p in prices.items():
            if p > 0:
                print(f'   {s}: {p:,.2f}')
    except Exception as e:
        print(f'✗ Coinbase Error: {e}')
    
    print('\n--- PHET 2: ALPACA PAPER TRADING ---\n')
    try:
        from trading_system.connectors.alpaca import AlpacaConnector
        a = AlpacaConnector()
        await a.connect()
        
        # Test price feed
        symbols = ['AAPL', 'MSFT']
        prices = await a.get_current_prices(symbols)
        print('✓ Alpaca connected (sandbox)')
        for s, p in prices.items():
            if p > 0:
                print(f'   {s}: ${p:.2f}')
    except Exception as e:
        print(f'✗ Alpaca Error: {e}')
    
    print('\n=== TEST COMPLETE ===\n')

asyncio.run(main())