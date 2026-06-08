#!/usr/bin/env python3
"""
Example: Using the production Coinbase v3 connector for real trading.
This demonstrates all major operations: balances, prices, previews, and orders.
"""

import sys
import logging
from pathlib import Path

# Add trading_system to path
sys.path.insert(0, str(Path(__file__).parent))

from trading_system.connectors.coinbase_v3 import CoinbaseConnectorV3

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s'
)
logger = logging.getLogger(__name__)


def example_check_balance():
    """Example: Check account balance."""
    print("\n" + "="*70)
    print("📊 EXAMPLE: Check Account Balance")
    print("="*70)
    
    cb = CoinbaseConnectorV3()
    balances = cb.get_balances()
    
    print("\n💰 Your Account Balances:")
    total_usd = 0
    for currency, amounts in balances.items():
        if isinstance(amounts, dict):
            available = amounts.get('available', '0')
            held = amounts.get('held', '0')
            print(f"  {currency:6s}: {available:>15s} (held: {held:>10s})")
        else:
            print(f"  {currency:6s}: {amounts}")
    
    return balances


def example_get_price(product_id: str = 'BTC-USD'):
    """Example: Get current price."""
    print("\n" + "="*70)
    print(f"💹 EXAMPLE: Get Price for {product_id}")
    print("="*70)
    
    cb = CoinbaseConnectorV3()
    price_data = cb.get_price(product_id)
    
    price = price_data.get('price', 'N/A')
    change_24h = price_data.get('price_percentage_change_24h', 'N/A')
    volume_24h = price_data.get('volume_24h', 'N/A')
    
    print(f"\n  Product:    {product_id}")
    print(f"  Price:      ${price}")
    print(f"  24h Change: {change_24h}%")
    print(f"  24h Volume: {volume_24h}")
    
    return price_data


def example_preview_order(
    product_id: str = 'BTC-USD',
    side: str = 'BUY',
    amount: float = 100.0
):
    """Example: Preview an order WITHOUT executing."""
    print("\n" + "="*70)
    print(f"👁️  EXAMPLE: Preview Order (No Execution)")
    print("="*70)
    
    cb = CoinbaseConnectorV3()
    
    print(f"\n  Product:   {product_id}")
    print(f"  Side:      {side}")
    print(f"  Amount:    ${amount}")
    print(f"\n  Running preview...")
    
    preview = cb.preview_order(
        product_id=product_id,
        side=side,
        order_type='market',
        quote_size=amount if side == 'BUY' else None,
        base_size=None
    )
    
    print(f"\n  📋 Preview Results:")
    print(f"    Estimated Fill Price: ${preview.estimated_fill_price}")
    print(f"    Total Cost:          ${preview.total_cost}")
    print(f"    Total Fees:          ${preview.total_fee}")
    
    return preview


def example_list_orders():
    """Example: List recent orders."""
    print("\n" + "="*70)
    print("📋 EXAMPLE: List Recent Orders")
    print("="*70)
    
    cb = CoinbaseConnectorV3()
    orders = cb.list_orders()
    
    print(f"\n  Found {len(orders)} orders:")
    for order in orders[:5]:  # Show first 5
        status = order.get('status', 'UNKNOWN')
        product_id = order.get('product_id', 'N/A')
        side = order.get('side', 'N/A')
        filled_size = order.get('filled_size', '0')
        filled_value = order.get('filled_value', '0')
        
        print(f"\n    Order ID:    {order.get('id', 'N/A')[:12]}...")
        print(f"    Product:     {product_id}")
        print(f"    Side:        {side}")
        print(f"    Status:      {status}")
        print(f"    Filled:      {filled_size} @ ${filled_value}")
    
    return orders


def example_complete_trading_flow(
    product_id: str = 'BTC-USD',
    buy_amount: float = 10.0
):
    """Example: Complete trading workflow (preview → execute)."""
    print("\n" + "="*70)
    print("🚀 EXAMPLE: Complete Trading Workflow")
    print("="*70)
    
    cb = CoinbaseConnectorV3()
    
    # Step 1: Check balance before
    print(f"\n[1/4] Checking balance...")
    balances = cb.get_balances()
    usd_balance = float(balances.get('USD', {}).get('available', 0))
    print(f"  ✅ Available USD: ${usd_balance}")
    
    if usd_balance < buy_amount:
        print(f"  ❌ Insufficient funds (need ${buy_amount}, have ${usd_balance})")
        return None
    
    # Step 2: Get current price
    print(f"\n[2/4] Getting current price for {product_id}...")
    price_data = cb.get_price(product_id)
    current_price = float(price_data.get('price', 0))
    print(f"  ✅ Current price: ${current_price}")
    
    # Step 3: Preview the order
    print(f"\n[3/4] Previewing ${buy_amount} market buy order...")
    preview = cb.preview_order(
        product_id=product_id,
        side='BUY',
        order_type='market',
        quote_size=buy_amount
    )
    print(f"  ✅ Estimated fill price: ${preview.estimated_fill_price}")
    print(f"  ✅ Total fees: ${preview.total_fee}")
    print(f"  ✅ Total cost: ${preview.total_cost}")
    
    # Step 4: Execute (commented out for safety - uncomment when ready)
    print(f"\n[4/4] READY TO EXECUTE (currently in DEMO mode)")
    print(f"  ⚠️  To actually place the order, uncomment the code below:")
    print(f"  \n  # order = cb.create_order(")
    print(f"  #     product_id='{product_id}',")
    print(f"  #     side='BUY',")
    print(f"  #     order_type='market',")
    print(f"  #     quote_size={buy_amount}")
    print(f"  # )")
    print(f"  # print(f'Order placed: {{order.order_id}}')")
    
    return preview


def main():
    """Run all examples."""
    print("\n" + "="*70)
    print("🎯 COINBASE v3 CONNECTOR - PRODUCTION EXAMPLES")
    print("="*70)
    print("\nThese examples demonstrate real trading operations using the")
    print("production Coinbase v3 connector with proper JWT authentication.")
    print("\nAll operations use the official Coinbase CLI under the hood.")
    
    try:
        # Example 1: Check balance
        example_check_balance()
        
        # Example 2: Get price
        example_get_price('BTC-USD')
        
        # Example 3: Preview order
        example_preview_order('BTC-USD', 'BUY', 100.0)
        
        # Example 4: List orders
        example_list_orders()
        
        # Example 5: Complete workflow
        example_complete_trading_flow('BTC-USD', 10.0)
        
        print("\n" + "="*70)
        print("✅ All examples completed successfully!")
        print("="*70)
        print("\nNext steps:")
        print("  1. Import CoinbaseConnectorV3 in your trading system")
        print("  2. Use it to fetch prices, preview orders, and execute trades")
        print("  3. Build your strategy on top of this solid foundation")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
