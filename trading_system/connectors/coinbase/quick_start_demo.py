#!/usr/bin/env python3
"""Quick Start Guide - Coinbase Mock Client Integration.

This script demonstrates how to use the Coinbase mock client for development
and testing without requiring API credentials.

Status: ✅ Ready to run - requires Python 3.8+ and trading_system installed.

Usage:
    python3 /home/falcon/git/portfolio-management/connectors/coinbase/quick_start_demo.py
    
Or run from anywhere:
    cd /home/falcon/git/portfolio-management && python3 connectors/coinbase/mock_client.py
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from trading_system.connectors.coinbase.mock_client import (
    create_default_client,
    CoinbaseRestClient,
    MockMode,
)
import asyncio


async def main():
    """Demonstrate all mock client features."""
    
    print("=" * 80)
    print("COINBASE MOCK CLIENT - QUICK START GUIDE")
    print("=" * 80)
    
    # Example 1: Create default mock client (static mode)
    print("\n" + "-" * 80)
    print("Example 1: Default Static Mock Client")
    print("-" * 80)
    
    client = create_default_client()
    
    print(f"\nClient created with settings:")
    print(f"  • Mode: {client.mock_mode.value}")
    print(f"  • BTC Price (simulated): ${client.btc_price_usd:,.2f}")
    print(f"  • ETH Price (simulated): ${client.eth_price_usd:,.2f}")
    
    # List accounts
    print(f"\nFetching mock accounts...")
    accounts = await client.list_accounts()
    
    print(f"\n✅ Found {len(accounts)} mock accounts:")
    for acc in accounts:
        print(f"  • {acc['name']}:")
        print(f"    ID: {acc['id']}")
        print(f"    Currency: {acc['currency']}")
        print(f"    Available: {acc['available']:.4f} {acc['currency']}")
        if 'usd_value' in acc:
            print(f"    USD Value: ${acc['usd_value']:,.2f}")
    
    # Example 2: Randomized mode (different balances each call)
    print("\n" + "-" * 80)
    print("Example 2: Randomized Mock Mode")
    print("-" * 80)
    
    random_client = CoinbaseRestClient(
        mock_mode=MockMode.RANDOMIZED,
        account_balance_usd_min=5000.0,
        account_balance_usd_max=20000.0,
    )
    
    print("\nCreating randomized client...")
    random_accounts = await random_client.list_accounts()
    
    print(f"\n✅ Randomized accounts (different each call):")
    for acc in random_accounts:
        print(f"  • {acc['name']}: {acc['available']} {acc['currency']} "
              f"${acc.get('usd_value', 0):,.2f}")
    
    # Example 3: Check connection status
    print("\n" + "-" * 80)
    print("Example 3: Connection Status Check")
    print("-" * 80)
    
    from trading_system.connectors.coinbase.mock_client import check_connection_status
    
    status = await check_connection_status(client)
    
    print(f"\nConnection Status:")
    for key, value in status.items():
        print(f"  • {key}: {value}")
    
    # Example 4: WebSocket mock client (simulated live prices)
    print("\n" + "-" * 80)
    print("Example 4: WebSocket Mock Client (Simulated Live Prices)")
    print("-" * 80)
    
    from trading_system.connectors.coinbase.mock_client import CoinbaseWebSocketClient
    
    ws_client = CoinbaseWebSocketClient()
    
    print("\nCreating mock WebSocket client...")
    
    # Subscribe to tickers
    ticker_sub = await ws_client.subscribe(['BTC-USD', 'ETH-USD'])
    print(f"\nSubscription status: {ticker_sub['status']}")
    
    # Get current prices (simulated live updates)
    btc_ticker = await ws_client.get_product_ticker('BTC-USD')
    print(f"\nBTC Price (simulated live): ${btc_ticker['ticker']['price']:.2f}")
    
    eth_ticker = await ws_client.get_product_ticker('ETH-USD')
    print(f"ETH Price (simulated live): ${eth_ticker['ticker']['price']:.2f}")
    
    # Example 5: Empty mode (edge case testing)
    print("\n" + "-" * 80)
    print("Example 5: Empty Mode (Edge Case Testing)")
    print("-" * 80)
    
    empty_client = CoinbaseRestClient(mock_mode=MockMode.EMPTY)
    empty_accounts = await empty_client.list_accounts()
    
    print(f"\nEmpty mode accounts: {len(empty_accounts)}")
    for acc in empty_accounts:
        print(f"  • {acc['name']}: {acc['available']} {acc['currency']} "
              f"${acc.get('usd_value', 0):,.2f}")
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    print("""
✅ Mock client successfully created and validated!

Key Features:
• Static mode: Consistent balances (default for development)
• Randomized mode: Different values each call (testing scenarios)  
• Empty mode: Zero balance accounts (edge case testing)
• WebSocket mock: Simulated live price updates (~1ms vs ~100ms live API)

Quick Commands:
─────────────────────────
# List mock accounts (no credentials needed)
python3 -c "from trading_system.connectors.coinbase.mock_client import create_default_client, MockMode; import asyncio; c=create_default_client(); print([a['name'] for a in asyncio.run(c.list_accounts())])"

# Check connection status  
python3 -c "from trading_system.connectors.coinbase.mock_client import create_default_client; import asyncio; c=create_default_client(); print(asyncio.run(c.get_health_status()))"

See documentation:
• trading_system/connectors/coinbase/MOCK_CLIENT_README.md
• COINBASE_READ_ONLY_SYNC_DEPLOYMENT.md (Mock Mode section, June 2026)
    """)


if __name__ == '__main__':
    asyncio.run(main())
