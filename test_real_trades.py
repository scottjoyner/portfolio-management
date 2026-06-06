#!/usr/bin/env python3
"""
Real Trade Execution & Balance Check - Alpaca Paper Trading + Coinbase
Verifies API execution and displays account balances
"""

import asyncio
import os
from pathlib import Path
import dotenv


async def main():
    """Execute real test trades and fetch Coinbase balances"""
    
    print("\n" + "="*80)
    print("🎯 REAL TRADE EXECUTION & BALANCE CHECK")
    print("="*80)
    
    # Load environment variables
    env_files = [
        Path('/home/falcon/git/portfolio-management/.env'),
        Path(Path.home() / '.git/portfolio-management/.env'),
    ]
    
    for env_file in env_files:
        if env_file.exists():
            print(f"\n📖 Loading .env from {env_file}")
            dotenv.load_dotenv(env_file)
            break
    
    # Check Alpaca credentials
    alpaca_key = os.environ.get('ALPACA_API_KEY', '')
    alpaca_secret = os.environ.get('ALPACA_API_SECRET', '')
    
    if not alpaca_key:
        print("\n❌ ALPACA_API_KEY not found in environment!")
        print("   Please check your .env file and add:")
        print("   ALPACA_API_KEY=pk_xxxxxxxxx")
        print("   ALPACA_SECRET_KEY=xxxxxxxxxx")
        return
    
    if '***' in alpaca_key:
        print("\n⚠️  Using mock/placeholder credentials - skipping trade execution")
        print("   For real trades, add actual keys to .env (replacing ***)")
        return
    
    print(f"\n✅ Alpaca credentials detected:")
    print(f"   API Key: {alpaca_key[:8]}...{alpaca_key[-4:]}")
    print(f"   Secret: {alpaca_secret[:12]}...{alpaca_secret[-8:]}")
    
    # Create Alpaca connector
    from trading_system.connectors.alpaca import AlpacaConnector
    
    alpaca = AlpacaConnector(
        api_key=alpaca_key,
        api_secret=alpaca_secret,
        paper_trading=True  # Safe sandbox environment
    )
    
    print("\n🔌 Connecting to Alpaca API...")
    await alpaca.connect()
    
    # Fetch current account balance
    print("\n💰 FETCHING ALPACA ACCOUNT BALANCE:")
    print("-"*50)
    
    account_data = await alpaca.get_account()
    
    if account_data:
        equity = float(account_data.get('cash', '0'))
        total_value = float(account_data.get('portfolio_value', 0))
        
        print(f"\n   Account Balance:")
        print(f"      Cash Available: ${equity:,.2f}")
        print(f"      Total Portfolio Value: ${total_value:,.2f}")
        
        # Get buying power
        if 'buying_power' in account_data:
            buying_power = float(account_data['buying_power'])
            print(f"      Buying Power (Day): ${buying_power:,.2f}")
    
    else:
        print("\n   ⚠️  Could not fetch Alpaca account data")
    
    # Execute test paper trade
    if equity > 0:
        print("\n📊 EXECUTING TEST PAPER TRADE:")
        print("-"*50)
        
        # Fetch AAPL price first
        print("   Step 1: Fetching current AAPL price...")
        prices = await alpaca.get_current_prices(['AAPL'])
        
        if 'AAPL' in prices and prices['AAPL'].get('last', 0) > 0:
            aapl_price = float(prices['AAPL']['last'])
            print(f"      AAPL Current Price: ${aapl_price:.2f}")
            
            # Calculate shares to buy (use $50 for test trade - safe amount)
            buy_amount = 50.00
            shares_to_buy = int(buy_amount / aapl_price)
            
            print(f"      Buying {buy_amount} of AAPL")
            print(f"      Shares: {shares_to_buy}")
            
            # Execute market order to buy AAPL
            print(f"\n   Step 2: Placing market order to BUY {shares_to_buy} shares of AAPL...")
            
            trade_order = await alpaca.submit_market_order(
                symbol='AAPL',
                side='buy',
                qty=shares_to_buy,
                client_order_id='test-paper-trade-001'
            )
            
            if trade_order.get('id'):
                order_status = trade_order.get('status', 'pending')
                
                print(f"\n   ✅ TRADE EXECUTED!")
                print(f"      Order ID: {trade_order['id']}")
                print(f"      Status: {order_status.upper()}")
                print(f"      Symbol: AAPL")
                print(f"      Side: BUY")
                print(f"      Shares: {shares_to_buy}")
                
                # Get updated account balance
                print(f"\n   Step 3: Fetching updated account balance...")
                await asyncio.sleep(2)  # Small delay for API sync
                
                account_data = await alpaca.get_account()
                if account_data:
                    new_equity = float(account_data.get('cash', '0'))
                    print(f"      New Cash Balance: ${new_equity:,.2f}")
                    print(f"      Difference: $${abs(equality - new_equity):.2f}")
                    
                    print(f"\n📊 VERIFICATION:")
                    print(f"   Trade should now appear in Alpaca UI:")
                    print(f"      URL: https://app.alpaca.markets/dashboard")
                    print(f"      Navigate to 'Orders' tab to see recent trade")
                    
            else:
                print(f"\n   ⚠️  Could not get order confirmation")
        
        else:
            print("\n   ❌ AAPL price unavailable - cannot execute trade")
    
    # Fetch Coinbase balances
    print("\n" + "="*80)
    print("💵 COINBASE BALANCE CHECK (USDC, BTC, ETH)")
    print("="*80)
    
    from trading_system.connectors.coinbase import CoinbaseConnector
    
    coinbase = CoinbaseConnector()
    await coinbase.connect()
    
    # Get balances for specific currencies
    currencies = ['usdc', 'bitcoin', 'ethereum']
    symbols = ['USDC', 'BTC-USD', 'ETH-USD']
    
    print("\n   Fetching Coinbase balances...")
    
    all_prices = await coinbase.get_current_prices(symbols)
    
    # Get account info for fiat balances
    try:
        import requests
        
        # Use Coinbase public API to get balances (if authenticated)
        response = requests.get(
            "https://api.exchange.coinbase.com/accounts",
            headers={
                "Content-Type": "application/json"
            },
            timeout=10
        )
        
        if response.status_code == 200:
            accounts = response.json()
            
            print("\n   Coinbase Account Balances:")
            print("-"*50)
            
            for account in accounts.get('data', []):
                currency_id = account.get('currency', '').upper()
                balance_str = account.get('balance', '0')
                
                # Parse balance and price
                if balance_str and balance_str.lower() != '0':
                    try:
                        balance_float = float(balance_str)
                        
                        # Get current price for this currency
                        price_key = f"{currency_id}-USD"
                        price_info = all_prices.get(price_key, {})
                        last_price = float(price_info.get('last', 0)) if price_info else 0
                        
                        formatted_balance = f"{balance_float:.4f} {currency_id}"
                        usd_value = round(balance_float * last_price, 2)
                        
                        print(f"\n   💰 {formatted_balance}")
                        print(f"      USD Value: ${usd_value:,.2f}")
                        
                    except ValueError:
                        pass
                
                # Check for USDC specifically (common for prediction market trading)
                if currency_id == 'USDC':
                    usdc_balance = float(balance_str)
                    print(f"\n   🟢 USDC Balance: ${usdc_balance:,.4f}")
                    
                    # Check Coinbase Fiat balances too
                    if 'balance_fiat' in account and account['balance_fiat']:
                        for fiat_balance in account['balance_fiat']:
                            if fiat_balance.get('currency') == 'USD':
                                print(f"   💵 USD (Fiat): ${float(fiat_balance.get('amount', 0)):,.2f}")
                        
            print("\n   ⚠️  If no balances shown, Coinbase API may require authentication")
            
        else:
            print(f"\n   ❌ Could not fetch Coinbase account balances - Status: {response.status_code}")
            
    except Exception as e:
        print(f"\n   ⚠️  Error fetching Coinbase balances: {str(e)}")
        
        # Fallback: show prices from all_prices
        for symbol in symbols:
            if symbol in all_prices:
                last = float(all_prices[symbol].get('last', 0))
                print(f"\n   💰 {symbol}: ${last:,.2f}")
    
    print("\n" + "="*80)
    print("✅ TRADE EXECUTION & BALANCE CHECK COMPLETE")
    print("="*80)
    print("\n📋 Next Steps:")
    print("   1. Check Alpaca UI for your test trade: https://app.alpaca.markets/dashboard")
    print("      - Navigate to 'Orders' tab")
    print("      - Look for order ID from above output")
    print("   2. Review Coinbase balances for USDC/BTC/ETH")
    print("   3. When ready, add Kalshi and Polymarket API keys to .env file")
    print("      - Kalshi: https://kalshi.com/account > Settings > API Access")
    print("      - Polymarket: https://docs.polymarket.io/reference")


if __name__ == "__main__":
    asyncio.run(main())
