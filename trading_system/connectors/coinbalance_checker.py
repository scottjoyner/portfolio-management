#!/usr/bin/env python3
"""Coinbase Balance Checker - Simple HTTP Request to Coinbase API.

Checks your Coinbase account balances using your configured read-only credentials.

Usage:
    python3 /home/falcon/git/portfolio-management/trading_system/connectors/coinbalance_checker.py
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')


def load_credentials_from_env():
    """Load API credentials from .env file."""
    
    env_path = '/home/falcon/git/portfolio-management/.env'
    
    api_key = None
    api_secret = None
    
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            # Extract API key (read-only) - check for COINBASE_API_KEY=
            if line.startswith('COINBASE_API_KEY='):
                value = line.split('=', 1)[1]
                api_key = value.strip().strip('"').strip("'")
            
            # Extract API secret - check for COINBASE_API_SECRET=
            elif line.startswith('COINBASE_API_SECRET='):
                value = line.split('=', 1)[1]
                api_secret = value.strip().strip('"').strip("'")
    
    return api_key, api_secret


async def check_balance(api_key: str):
    """Check Coinbase account balances using read-only API."""
    
    import urllib.request
    import json
    
    # Coinbase v3 API endpoint for accounts
    base_url = "https://api.coinbase.com/v1/accounts"

    print("=" * 80)
    print("COINBASE ACCOUNT BALANCE CHECKER")
    print("=" * 80)
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'User-Agent': 'Portfolio-Management/1.0 (production)',
    }
    
    print(f"\n🔑 Using read-only API credentials...")
    print(f"   • API Key preview: {api_key[:6]}...{api_key[-4:]}")
    
    try:
        # Make HTTP request to Coinbase API v3
        req = urllib.request.Request(
            base_url,
            headers=headers
        )
        
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status != 200:
                print(f"\n⚠️  API returned status {response.status}")
                error_text = response.read().decode('utf-8')[:500]
                print(f"   • Error: {error_text}\n")
                return
            
            data = json.loads(response.read().decode('utf-8'))
            print(f"\n✅ Successfully connected to Coinbase API")
            print(f"   • Retrieved {len(data)} account(s)\n")
        
        # Parse and display accounts
        for i, account in enumerate(data, 1):
            print(f"\n{'─' * 60}")
            print(f"Account #{i}: {account.get('name', 'Unnamed')}")
            print(f"{'─' * 60}")
            if 'currency' in account:
                print(f"   Currency:      {account['currency'].upper()}")
            if 'balance' in account:
                balance = float(account['balance'])
                currency = account.get('currency', 'USD')
                print(f"   Balance:       {balance:.8f} {currency}")
            
        # Summary
        total_accounts = len(data)
        print(f"\n{'=' * 80}")
        print(f"SUMMARY")
        print('=' * 80)
        print(f"Total Accounts: {total_accounts}\n")

    except urllib.error.HTTPError as e:
        print(f"\n❌ HTTP Error {e.code}:")
        error_text = e.read().decode('utf-8')[:500]
        print(f"   • Status: {e.code}")
        print(f"   • Error: {error_text}\n")

    except Exception as e:
        print(f"\n❌ Error checking balance:")
        print(f"   {type(e).__name__}: {str(e)[:200]}\n")


if __name__ == '__main__':
    import asyncio
    
    api_key, api_secret = load_credentials_from_env()
    
    if not api_key or not api_secret:
        print("\n❌ COINBASE_API_KEY and COINBASE_API_SECRET not found in .env")
        print("Please add credentials to run this checker.\n")
    else:
        asyncio.run(check_balance(api_key))
