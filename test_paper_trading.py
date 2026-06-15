#!/usr/bin/env python3
"""Test script for paper trading system."""

from paper_trading_system import fetch_coinbase_price, generate_constrained_prices, MultiStrategyPaperTrading

# Test Coinbase API connection
print("Testing Coinbase API...")
price = fetch_coinbase_price('BTC-USD')
if price:
    print(f"✓ BTC-USD price from Coinbase: ${price:,.2f}")
else:
    print("⚠ Failed to fetch BTC price (may be rate limited)")

# Test signal generation
print("\nTesting signal generation...")
prices = generate_constrained_prices(50, 45000)
trader = MultiStrategyPaperTrading()

for p in prices[-10:]:
    dominant, strength, signals = trader.get_signal_strength(prices, p['close'])
    print(f"  {p['date']}: ${p['close']:,.2f} -> {dominant} (strength: {strength:+.3f})")

print(f"\nGenerated {len(prices)} price bars")
print(f"Price range: ${min(p['close'] for p in prices):,.2f} - ${max(p['close'] for p in prices):,.2f}")
