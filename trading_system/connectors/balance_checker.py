#!/usr/bin/env python3
"""Coinbase Balance Checker - Quick Command Line Tool.

Check account balances using your configured read-only API credentials.

Usage:
    python3 /home/falcon/git/portfolio-management/trading_system/connectors/balance_checker.py
    
Or run from anywhere with full path:
    cd /home/falcon/git/portfolio-management && python3 trading_system/connectors/balance_checker.py

**Configuration**: Reads from .env file for API credentials.
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

import asyncio
from pathlib import Path
from dotenv import load_dotenv


# Load credentials from .env
env_path = Path('/home/falcon/git/portfolio-management/.env')
load_dotenv(env_path)

# Read credentials directly from .env (skip comments and empty lines)
api_key_file = env_path.read_text()
for line in api_key_file.split('\n'):
    if 'COINBASE_API_KEY=' in line and '9c346b' in line:  # Your production key
        COINBASE_API_KEY = line.split('=')[1].strip()
        break

api_secret_file = env_path.read_text()
for line in api_secret_file.split('\n'):
    if 'COINBASE_API_SECRET=' in line and 'pgEUPC' in line:  # Your production secret
        COINBASE_API_SECRET = line.split('=')[1].strip().strip('"')
        break


async def check_balance():
    """Check Coinbase account balances with read-only API."""
    
    from trading_system.connectors.coinbase.real_client import CoinbaseRestClient
    
    print("=" * 80)
    print("COINBASE ACCOUNT BALANCE CHECKER")
    print("=" * 80)
    
    # Use the real client with your credentials
    print(f"\nUsing API Key (read-only): {COINBASE_API_KEY[:6]}...{COINBASE_API_KEY[-4:]}")
    print("Mode: Live API (production read-only)")
    
    try:
        client = CoinbaseRestClient(
            api_key=COINBASE_API_KEY,
            api_secret=COINBASE_API_SECRET,
        )
        
        print("\n✅ Connected successfully!\n")
        
        # List all accounts
        print("Fetching account balances from Coinbase...")
        accounts = await client.list_accounts()
        
        print(f"\n{'=' * 80}")
        print(f"ACCOUNT BALANCES - {len(accounts)} Accounts Found")
        print(f"{'=' * 80}\n")
        
        total_usd_value = 0
        
        for acc in accounts:
            currency = acc['currency']
            available = acc['available']
            usd_value = acc.get('usd_value', 0)
            
            # Format for display
            if currency == 'BTC':
                print(f"💰 {acc['name']}:")
                print(f"   • Balance: {available:.8f} BTC")
                print(f"   • USD Value: ${usd_value:,.2f}")
                
            elif currency == 'ETH':
                print(f"🔷 {acc['name']}:")
                print(f"   • Balance: {available:.4f} ETH")
                print(f"   • USD Value: ${usd_value:,.2f}")
                
            elif currency == 'USD':
                print(f"💵 {acc['name']}:")
                print(f"   • Balance: ${available:,.2f}")
                print(f"   • Available: {available} USD")
            
            else:
                print(f"📊 {acc['name']}:")
                print(f"   • Balance: {available} {currency}")
                if usd_value:
                    print(f"   • USD Value: ${usd_value:,.2f}")
            
            total_usd_value += usd_value
            
            print()  # Empty line between accounts
        
        print(f"{'=' * 80}")
        print(f"TOTAL PORTFOLIO VALUE: ${total_usd_value:,.2f}")
        print(f"{'=' * 80}\n")
        
        # Connection info
        print("📡 Connection Info:")
        print(f"   • API Mode: {client.mode}")
        print(f"   • Status: Connected (read-only)\n")
        
    except Exception as e:
        print(f"\n❌ Error checking balance:")
        print(f"   {type(e).__name__}: {str(e)[:200]}\n")


if __name__ == '__main__':
    asyncio.run(check_balance())
