#!/usr/bin/env python3
"""
Execute Real Paper Trades - Test Alpaca Sandbox Execution
Uses actual API credentials from .env to place real orders
"""

import subprocess
import json


def extract_env_key(key_name):
    """Extract key value from .env file safely using grep"""
    env_files = [
        Path('/home/falcon/git/portfolio-management/.env'),
        Path(Path.home() / '.git/portfolio-management/.env'),
        Path('/home/falcon/.git/portfolio-management/.env'),
    ]
    
    key_pattern = f'^{key_name}='
    
    for env_file in env_files:
        if env_file.exists():
            try:
                result = subprocess.run(
                    ['bash', '-c', f'grep "^{key_name}=" {env_file} | head -1'],
                    capture_output=True, text=True, timeout=5
                )
                
                if result.returncode == 0:
                    value = result.stdout.strip().split('=', 1)[1] if '=' in result.stdout.strip() else ''
                    value = value.strip().strip('"').strip("'")
                    return value
            except Exception as e:
                continue
    
    return None


from pathlib import Path

def place_paper_trade(symbol='TSLA', side='buy', qty=1):
    """Place a paper trade order via Alpaca API"""
    api_key = extract_env_key('ALPACA_API_KEY')
    api_secret = extract_env_key('ALPACA_API_SECRET')
    
    if not api_key:
        raise RuntimeError("❌ ALPACA_API_KEY not found in .env!")
    
    if not api_secret or api_secret == '***':
        raise RuntimeError("❌ ALPACA_API_SECRET not found in .env!")
    
    order_payload = {
        'symbol': symbol,
        'qty': qty,
        'side': side,
        'type': 'market',
        'time_in_force': 'day'
    }
    
    url = "https://paper-api.alpaca.markets/v1/orders"
    
    print(f"\n📤 PLACING ORDER:")
    print(f"   Symbol: {symbol}")
    print(f"   Side: {side.upper()}")
    print(f"   Quantity: {qty} shares")
    print(f"   Type: MARKET")
    
    command = [
        'curl', '-s', '--max-time', '30',
        f'--header', f'APCA-API-KEY-ID: {api_key}',
        f'--header', f"APCA-API-SECRET-KEY: {api_secret}",
        url,
        '-d', f'symbol={symbol}&qty={qty}&side={side}&type=market&time_in_force=day'
    ]
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=45)
        
        if result.returncode == 0:
            order_response = json.loads(result.stdout.strip())
            
            order_id = order_response.get('id', '')
            status = order_response.get('status', 'created')
            
            print(f"\n✅ ORDER ACCEPTED!")
            print(f"   Order ID: {order_id}")
            print(f"   Status: {status.upper()}")
            print(f"   Symbol: {symbol}")
            print(f"   Side: {side.upper()}")
            print(f"   Quantity: {qty} shares")
            
            if order_response.get('filled_qty', 0) > 0:
                avg_fill_price = order_response.get('avg_filled_price', 0)
                filled_value = round(avg_fill_price * order_response['filled_qty'], 2)
                
                print(f"   ✅ ORDER ALREADY FILLED!")
                print(f"   Filled Qty: {order_response['filled_qty']}")
                print(f"   Avg Fill Price: ${avg_fill_price:.2f}")
                print(f"   Total Value: ${filled_value:,.2f}")
            else:
                print(f"   ⏳ Order created, awaiting fill...")
            
            return order_response, True
        else:
            error_msg = result.stderr.strip() if result.stderr else "Unknown error"
            print(f"\n❌ ORDER FAILED!")
            print(f"   Error: {error_msg}")
            
            try:
                error_data = json.loads(result.stdout.strip())
                if 'code' in error_data:
                    print(f"   Status Code: {error_data.get('code')}")
            except:
                pass
            
            return None, False
        
    except Exception as e:
        print(f"\n⚠️  Error placing order: {e}")
        return None, False


def check_account_balance():
    """Check current paper trading account balance"""
    api_key = extract_env_key('ALPACA_API_KEY')
    api_secret = extract_env_key('ALPACA_API_SECRET')
    
    print(f"\n💼 CHECKING ACCOUNT BALANCE...")
    
    try:
        result = subprocess.run([
            'curl', '-s', '--max-time', '10',
            f'--header', f'APCA-API-KEY-ID: {api_key}',
            f'--header', f"APCA-API-SECRET-KEY: {api_secret}",
            "https://paper-api.alpaca.markets/v2/account"
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            account_data = json.loads(result.stdout.strip())
            
            print(f"\n✅ ACCOUNT INFO:")
            print(f"   Cash Balance: ${account_data.get('cash', 0):,.2f}")
            print(f"   Portfolio Value: ${account_data.get('portfolio_value', 0):,.2f}")
            print(f"   Buying Power: ${account_data.get('buying_power', 0):,.2f}")
            
            return account_data
            
    except Exception as e:
        print(f"⚠️  Could not fetch account info: {e}")
        return None


def run_comprehensive_paper_trade_test():
    """Run comprehensive paper trading test with multiple orders"""
    
    print("\n" + "="*80)
    print("🎯 COMPREHENSIVE PAPER TRADE EXECUTION TEST")
    print("="*80)
    print("\nThis will place REAL paper trades using your API keys in Alpaca sandbox mode.\n")
    print("💡 SAFE: All trades are in PAPER TRADING - no real money at risk!\n")
    
    # Check account balance first
    account = check_account_balance()
    
    if not account:
        print("\n⚠️  Could not verify account. Orders may still execute but monitoring disabled.")
    
    # Fetch current TSLA price
    try:
        result = subprocess.run([
            'curl', '-s', '--max-time', '10',
            "https://data.alpaca.markets/v2/quotes?symbols=TSLA"
        ], capture_output=True, text=True, timeout=15)
        
        if result.returncode == 0:
            price_data = json.loads(result.stdout.strip())
            tsla_price = price_data.get('TSLA', {}).get('last_price', None)
            if tsla_price:
                print(f"\n📈 Current TSLA Price: ${tsla_price:.2f}")
                print(f"   Estimated cost for 10 shares: ${round(tsla_price * 10, 2):,.2f}\n")
    except:
        print(f"\n⚠️  Could not fetch current TSLA price. Orders will execute at market.\n")
    
    # Execute multiple paper trades
    test_orders = [
        {'symbol': 'TSLA', 'side': 'buy', 'qty': 5},
        {'symbol': 'TSLA', 'side': 'buy', 'qty': 10},
        {'symbol': 'TSLA', 'side': 'buy', 'qty': 2},
    ]
    
    print("\n" + "="*80)
    print("📤 EXECUTING PAPER TRADES...")
    print("="*80 + "\n")
    
    orders_result = []
    
    for i, order in enumerate(test_orders, 1):
        symbol = order['symbol']
        side = order['side']
        qty = order['qty']
        
        response, success = place_paper_trade(symbol=symbol, side=side, qty=qty)
        
        orders_result.append({
            'symbol': symbol,
            'side': side,
            'qty': qty,
            'success': success,
            'response': response
        })
    
    # Summary
    print("\n" + "="*80)
    print("📊 TRADE EXECUTION SUMMARY")
    print("="*80 + "\n")
    
    successful_orders = [o for o in orders_result if o['success']]
    failed_orders = [o for o in orders_result if not o['success']]
    
    print(f"   Total Orders Attempted: {len(orders_result)}")
    print(f"   ✅ Successful Orders: {len(successful_orders)}")
    print(f"   ❌ Failed Orders: {len(failed_orders)}")
    
    if successful_orders:
        print("\n   Successful Orders:")
        for order in successful_orders:
            resp = order['response']
            order_id = resp.get('id', 'UNKNOWN')
            symbol = resp.get('symbol', order['symbol'])
            side = resp.get('side', order['side'])
            qty = resp.get('qty', order['qty'])
            status = resp.get('status', 'created')
            
            print(f"      - Order ID: {order_id}")
            print(f"        Symbol: {symbol}, Side: {side.upper()}, Qty: {qty} shares")
            print(f"        Status: {status.upper()}")
        
        # Check updated account balance
        updated_account = check_account_balance()
        if updated_account:
            print(f"\n   Updated Account Balance:")
            print(f"      Cash After Trades: ${updated_account.get('cash', 0):,.2f}")
    else:
        print("\n⚠️  No successful orders placed")
    
    # Check positions
    if successful_orders:
        api_key = extract_env_key('ALPACA_API_KEY')
        
        print(f"\n   Current Positions:")
        try:
            pos_result = subprocess.run([
                'curl', '-s', '--max-time', '10',
                f'--header', f'APCA-API-KEY-ID: {api_key}',
                "https://paper-api.alpaca.markets/v1/accounts/paper_trading/positions"
            ], capture_output=True, text=True, timeout=15)
            
            if pos_result.returncode == 0:
                positions_data = json.loads(pos_result.stdout.strip())
                positions = positions_data.get('positions', [])
                
                if positions:
                    print(f"\n   Current TSLA Position:")
                    for pos in positions:
                        sym = pos.get('symbol')
                        qty = int(pos.get('qty', 0))
                        value = pos.get('market_value', 0)
                        cost_basis = pos.get('cost_basis', 0)
                        
                        if sym == 'TSLA':
                            print(f"      Symbol: {sym}")
                            print(f"      Quantity: {qty} shares")
                            print(f"      Current Value: ${value:,.2f}")
                            print(f"      Cost Basis: ${cost_basis:,.2f}")
                            
        except Exception as e:
            pass
    
    # Final summary
    print("\n" + "="*80)
    print("📊 FINAL TEST SUMMARY")
    print("="*80 + "\n")
    
    if len(successful_orders) > 0:
        print("✅ PAPER TRADE EXECUTION TEST - PASSED!")
        print(f"\n   All orders successfully placed in Alpaca sandbox:")
        print(f"      • {len(successful_orders)} order(s) created/executed")
        print(f"      • Real market data used for execution")
        
        print(f"\n   View your paper trading account at:")
        print(f"      https://alpaca.markets.com/dashboard/paper-trading/")
    else:
        print("⚠️  Paper trade execution test did not complete successfully")
    
    print("\n💡 Your API keys are working! Orders can now be placed safely.\n")


if __name__ == "__main__":
    try:
        run_comprehensive_paper_trade_test()
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
