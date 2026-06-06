#!/usr/bin/env python3
"""
Coinbase Balance Check & Prediction Markets Integration Test
This script verifies Coinbase balance (USDC, BTC, ETH) for prediction markets trading.
Also demonstrates Alpaca paper trading integration.
"""

import asyncio
import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')
from pathlib import Path
import os
import dotenv
import requests


async def main():
    print("\n" + "="*80)
    print("💵 COINBASE BALANCE CHECK & PREDICTION MARKETS TEST")
    print("="*80)
    
    # Load .env
    env_files = [
        Path('/home/falcon/git/portfolio-management/.env'),
        Path(Path.home() / '.git/portfolio-management/.env'),
    ]
    
    for env_file in env_files:
        if env_file.exists():
            print(f"\n📖 Loading .env from {env_file}")
            dotenv.load_dotenv(env_file)
            break
    
    # ============================================
    # COINBASE BALANCE CHECK
    # ============================================
    print("\n" + "-"*60)
    print("💰 FETCHING COINBASE BALANCES (USDC, BTC, ETH)")
    print("-"*60)
    
    coinbase_api_key = os.environ.get('COINBASE_API_KEY', '')
    
    if '***' in coinbase_api_key or not coinbase_api_key:
        print("\n⚠️  COINBASE_API_KEY not configured - using mock mode")
        
        # Mock balance for demonstration (real balances would be fetched)
        print("\n   Sample Balance (when keys are added):")
        print("      USD: $***")
        print("      USDC: $***")
        print("      BTC: *** BTC")
        print("      ETH: *** ETH")
        
    else:
        # Real Coinbase connection
        from trading_system.connectors.coinbase import CoinbaseConnector
        
        coinbase = CoinbaseConnector()
        
        try:
            await coinbase.connect()
            
            currencies = ['usdc', 'bitcoin', 'ethereum']
            all_prices = await coinbase.get_current_prices(['USDC', 'BTC-USD', 'ETH-USD'])
            
            print("\n   Fetching real balances from Coinbase API...")
            
            response = requests.get(
                "https://api.exchange.coinbase.com/accounts",
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                accounts = response.json().get('data', [])
                
                print("\n   Coinbase Account Balances:")
                print("-"*50)
                
                total_usd = 0.0
                
                for account in accounts:
                    currency_id = account.get('currency', '').upper()
                    balance_str = account.get('balance', '0')
                    
                    # Fiat balances
                    if 'balance_fiat' in account and account['balance_fiat']:
                        for fiat_balance in account['balance_fiat']:
                            curr = fiat_balance.get('currency')
                            amount = float(fiat_balance.get('amount', 0))
                            currency_id = curr.upper()
                            
                            if currency_id == 'USD':
                                print(f"\n   💵 USD (Fiat): ${amount:,.2f}")
                                total_usd += amount
                            elif currency_id == 'USDC':
                                print(f"\n   🟢 USDC (Stablecoin): ${amount:,.4f}")
                                total_usd += amount
                    
                    # Crypto balances
                    if balance_str and balance_str.lower() != '0':
                        try:
                            balance_float = float(balance_str)
                            price_key = f"{currency_id}-USD"
                            price_info = all_prices.get(price_key, {})
                            last_price = float(price_info.get('last', 0)) if price_info else 0.0000001
                            
                            formatted_balance = f"{balance_float:.4f} {currency_id}"
                            usd_value = round(balance_float * last_price, 2)
                            
                            print(f"\n   💰 {formatted_balance}")
                            print(f"      USD Value: ${usd_value:,.2f}")
                            total_usd += usd_value
                        
                        except ValueError:
                            pass
                
                print(f"\n   📊 Total Portfolio Value: ${total_usd:,.2f}")
                
            else:
                print(f"\n   ⚠️  Could not fetch Coinbase balances - Status: {response.status_code}")
                
        except Exception as e:
            print(f"\n   ⚠️  Error fetching Coinbase balances: {str(e)}")
    
    # ============================================
    # ALPACA PAPER TRADING INTEGRATION
    # ============================================
    print("\n" + "-"*60)
    print("🎯 ALPACA PAPER TRADING INTEGRATION")
    print("-"*60)
    
    alpaca_key = os.environ.get('ALPACA_API_KEY', '')
    alpaca_secret = os.environ.get('ALPACA_API_SECRET', '')
    
    # Check if keys are valid format
    is_valid_alpaca = (alpaca_key and alpaca_key.startswith('pk_') and 
                       '***' not in alpaca_key and 
                       alpaca_secret)
    
    if is_valid_alpaca:
        print("\n✅ Alpaca credentials detected - attempting connection...")
        
        from trading_system.connectors.alpaca import AlpacaConnector
        
        alpaca = AlpacaConnector(
            api_key=alpaca_key,
            api_secret=alpaca_secret,
            paper_trading=True
        )
        
        try:
            await alpaca.connect()
            
            # Get account balance
            print("\n   Checking account balance...")
            account = await alpaca.get_account()
            
            if account and 'cash' in account:
                cash = float(account['cash'])
                total = float(account.get('portfolio_value', 0))
                
                print(f"\n   ✅ Account Connected!")
                print(f"      Cash Available: ${cash:,.2f}")
                print(f"      Portfolio Value: ${total:,.2f}")
                
                # Execute test trade if we have funds
                if cash > 100:
                    print("\n   📊 Executing test paper trade...")
                    
                    prices = await alpaca.get_current_prices(['AAPL'])
                    
                    if 'AAPL' in prices and prices['AAPL'] > 0:
                        aapl_price = prices['AAPL']
                        qty = 5
                        
                        order = await alpaca.submit_market_order(
                            symbol='AAPL',
                            side='buy',
                            qty=qty,
                            client_order_id='integration-test-001'
                        )
                        
                        print(f"\n   ✅ Test Trade Executed!")
                        print(f"      Order ID: {order.get('id', 'N/A')}")
                        if 'filled_qty' in order:
                            print(f"      Status: FILLED")
                    else:
                        print("\n   ⚠️  Could not fetch AAPL price for test trade")
            
            else:
                print("\n   ⚠️  Could not fetch account data")
                
        except Exception as e:
            print(f"\n   ⚠️  Alpaca connection error: {str(e)}")
    
    else:
        # Show how to add Alpaca credentials
        print("\n⚠️  Alpaca API keys need to be added with proper format:")
        print("      Format: ALPACA_API_KEY=pk_test_xxxxxxxxxxx (paper trading)")
        print("              ALPACA_API_SECRET=xxxxxxxxxxxxx")
        print()
        print("   Where to get credentials:")
        print("      https://alpaca.markets.com > Account Settings > API Keys")
        print("      - Paper Trading Key starts with 'pk_test_' (free, sandbox)")
        print("      - Live Trading Key starts with 'pk_live_' (approved account)")
    
    # ============================================
    # SUMMARY & NEXT STEPS
    # ============================================
    print("\n" + "="*80)
    print("✅ INTEGRATION TEST COMPLETE")
    print("="*80)
    
    print("\n📋 SUMMARY:")
    print("-"*50)
    
    coinbase_api_key = os.environ.get('COINBASE_API_KEY', '')
    if '***' not in coinbase_api_key and coinbase_api_key:
        print("   ✅ Coinbase: Configured")
    else:
        print("   ⚠️  Coinbase: Keys need to be added (.env)")
    
    alpaca_key = os.environ.get('ALPACA_API_KEY', '')
    is_valid_alpaca = (alpaca_key and alpaca_key.startswith('pk_') and 
                       '***' not in alpaca_key)
    if is_valid_alpaca:
        print("   ✅ Alpaca: Configured (paper trading)")
    else:
        print("   ⚠️  Alpaca: Keys need to be added (.env.prediction_markets)")
    
    print("\n📋 HOW TO ADD API KEYS:")
    print("-"*50)
    print()
    print("1. COINBASE (for USDC/BTC/ETH balances):")
    print("   - Visit: https://www.coinbase.com/account/api")
    print("   - Create API key with 'Read-only' permissions")
    print("   - Add to .env: COINBASE_API_KEY=xxxxxxxxxx")
    print()
    print("2. ALPACA (for traditional stock trading):")
    print("   - Visit: https://alpaca.markets.com/account/api")
    print("   - Create paper trading key (pk_test_*) for testing")
    print("   - Add to .env.prediction_markets:")
    print("      ALPACA_API_KEY=pk_test_xxxxxxxxxxx")
    print("      ALPACA_SECRET_KEY=xxxxxxxxxxxxx")
    print()
    
    print("="*80)


if __name__ == "__main__":
    asyncio.run(main())
