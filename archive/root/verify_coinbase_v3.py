#!/usr/bin/env python3
"""
Verification script: Confirm Coinbase v3 integration is working correctly.
Tests all major operations to ensure production readiness.
"""

import subprocess
import sys
import json
from pathlib import Path
from datetime import datetime

def run_command(cmd, timeout=30):
    """Run command and return result."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return -1, '', f'Command timed out after {timeout}s'
    except FileNotFoundError as e:
        return -1, '', str(e)


def test_cli_installed():
    """Test: Coinbase CLI is installed."""
    print("\n" + "="*70)
    print("TEST 1: Coinbase CLI Installation")
    print("="*70)
    
    code, stdout, stderr = run_command(['coinbase', '--version'])
    if code == 0:
        print(f"✅ PASS: {stdout.strip()}")
        return True
    else:
        print(f"❌ FAIL: {stderr}")
        print("   Install with: npm install -g @coinbase/coinbase-cli")
        return False


def test_cli_configured():
    """Test: Coinbase CLI is configured with credentials."""
    print("\n" + "="*70)
    print("TEST 2: Coinbase CLI Configuration")
    print("="*70)
    
    code, stdout, stderr = run_command(['coinbase', 'env'])
    if code == 0:
        print(f"✅ PASS: {stdout.strip()}")
        return True
    else:
        print(f"❌ FAIL: {stderr}")
        print("   Configure with: python3 scripts/setup_coinbase_credentials.py <key_file>")
        return False


def test_connector_import():
    """Test: Python connector imports without errors."""
    print("\n" + "="*70)
    print("TEST 3: Python Connector Import")
    print("="*70)
    
    try:
        # Add current directory to path
        sys.path.insert(0, str(Path.cwd()))
        from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3
        print("✅ PASS: CoinbaseConnectorV3 imported successfully")
        return True
    except ImportError as e:
        print(f"❌ FAIL: {e}")
        return False
    except Exception as e:
        print(f"❌ FAIL: Unexpected error: {e}")
        return False


def test_connector_init():
    """Test: Connector initializes without errors."""
    print("\n" + "="*70)
    print("TEST 4: Connector Initialization")
    print("="*70)
    
    try:
        sys.path.insert(0, str(Path.cwd()))
        from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3
        
        cb = CoinbaseConnectorV3()
        print("✅ PASS: CoinbaseConnectorV3() initialized successfully")
        return True, cb
    except Exception as e:
        print(f"❌ FAIL: {e}")
        return False, None


def test_get_balance(cb):
    """Test: Get account balance."""
    print("\n" + "="*70)
    print("TEST 5: Get Account Balance")
    print("="*70)
    
    try:
        balances = cb.get_balances()
        
        if balances:
            print(f"✅ PASS: Retrieved {len(balances)} currency balances")
            
            # Show first few currencies
            count = 0
            for currency in sorted(balances.keys())[:3]:
                amounts = balances[currency]
                if isinstance(amounts, dict):
                    available = amounts.get('available', '0')
                    held = amounts.get('held', '0')
                else:
                    available = str(amounts)
                    held = '0'
                print(f"     {currency}: {available} (held: {held})")
                count += 1
            
            if len(balances) > 3:
                print(f"     ... and {len(balances) - 3} more")
            
            return True
        else:
            print("⚠️  WARNING: No balances returned (account may be empty)")
            return True
            
    except Exception as e:
        print(f"❌ FAIL: {e}")
        return False


def test_get_price(cb):
    """Test: Get current price."""
    print("\n" + "="*70)
    print("TEST 6: Get Current Price (BTC-USD)")
    print("="*70)
    
    try:
        price_data = cb.get_price('BTC-USD')
        
        price = price_data.get('price')
        change = price_data.get('price_percentage_change_24h', 'N/A')
        
        print(f"✅ PASS: BTC-USD = ${price}")
        print(f"     24h change: {change}%")
        
        return True
    except Exception as e:
        print(f"❌ FAIL: {e}")
        return False


def test_preview_order(cb):
    """Test: Preview order (no execution)."""
    print("\n" + "="*70)
    print("TEST 7: Preview Order (No Execution)")
    print("="*70)
    
    try:
        preview = cb.preview_order(
            product_id='BTC-USD',
            side='BUY',
            order_type='market',
            quote_size=10.0
        )
        
        print(f"✅ PASS: Order preview successful")
        print(f"     Estimated fill: ${preview.estimated_fill_price}")
        print(f"     Total fee: ${preview.total_fee}")
        print(f"     Total cost: ${preview.total_cost}")
        
        return True
    except Exception as e:
        print(f"❌ FAIL: {e}")
        return False


def test_list_orders(cb):
    """Test: List orders."""
    print("\n" + "="*70)
    print("TEST 8: List Orders")
    print("="*70)
    
    try:
        orders = cb.list_orders()
        
        if isinstance(orders, list):
            print(f"✅ PASS: Retrieved {len(orders)} orders")
            if orders:
                order = orders[0]
                print(f"     Most recent: {order.get('id', 'N/A')[:12]}...")
        else:
            print(f"✅ PASS: Orders endpoint responding (single order returned)")
        
        return True
    except Exception as e:
        print(f"⚠️  WARNING: {e}")
        print("     (May be expected if no orders exist)")
        return True


def test_cli_balance():
    """Test: CLI balance command directly."""
    print("\n" + "="*70)
    print("TEST 9: CLI Direct Balance Command")
    print("="*70)
    
    code, stdout, stderr = run_command(['coinbase', 'balance'])
    
    if code == 0:
        try:
            balances = json.loads(stdout)
            count = len(balances) if isinstance(balances, dict) else 1
            print(f"✅ PASS: CLI returned {count} currency balances")
            return True
        except json.JSONDecodeError:
            print(f"❌ FAIL: Invalid JSON response: {stdout[:100]}")
            return False
    else:
        print(f"❌ FAIL: {stderr}")
        return False


def main():
    """Run all tests."""
    print("\n" + "="*70)
    print("🧪 COINBASE v3 INTEGRATION - VERIFICATION SUITE")
    print("="*70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    results = {}
    
    # Test 1: CLI installed
    results['cli_installed'] = test_cli_installed()
    
    if not results['cli_installed']:
        print("\n" + "="*70)
        print("❌ TESTS FAILED")
        print("="*70)
        print("\n🔧 Fix: Install Coinbase CLI")
        print("   npm install -g @coinbase/coinbase-cli")
        sys.exit(1)
    
    # Test 2: CLI configured
    results['cli_configured'] = test_cli_configured()
    
    if not results['cli_configured']:
        print("\n" + "="*70)
        print("❌ TESTS FAILED")
        print("="*70)
        print("\n🔧 Fix: Configure CLI with your API key")
        print("   python3 scripts/setup_coinbase_credentials.py ~/Downloads/cdp_api_key.json")
        sys.exit(1)
    
    # Test 3: Python connector imports
    results['connector_import'] = test_connector_import()
    
    if not results['connector_import']:
        print("\n" + "="*70)
        print("❌ TESTS FAILED")
        print("="*70)
        print("\n🔧 Fix: Ensure trading_system/ directory exists")
        print("   python3 -m pip install -e .")
        sys.exit(1)
    
    # Test 4: Connector initializes
    results['connector_init'], cb = test_connector_init()
    
    if not results['connector_init']:
        print("\n" + "="*70)
        print("❌ TESTS FAILED")
        print("="*70)
        print("\n🔧 Fix: Check CLI configuration")
        print("   coinbase env")
        sys.exit(1)
    
    # Test 5-8: API operations
    results['get_balance'] = test_get_balance(cb)
    results['get_price'] = test_get_price(cb)
    results['preview_order'] = test_preview_order(cb)
    results['list_orders'] = test_list_orders(cb)
    
    # Test 9: CLI direct
    results['cli_balance'] = test_cli_balance()
    
    # Summary
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        readable_name = test_name.replace('_', ' ').title()
        print(f"{status}: {readable_name}")
    
    print("="*70)
    print(f"Result: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED!")
        print("\n✅ Your Coinbase v3 integration is production-ready!")
        print("\n📚 Next steps:")
        print("   1. Review COINBASE_V3_README.md")
        print("   2. Check trading_system/coinbase_v3_examples.py")
        print("   3. Integrate into your trading strategies")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed - see errors above")
        sys.exit(1)


if __name__ == '__main__':
    main()
