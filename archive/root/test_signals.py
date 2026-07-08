from paper_trading_system import MultiStrategyPaperTrading, generate_constrained_prices

print("Testing signal generation...")
prices = generate_constrained_prices(50, 45000)
trader = MultiStrategyPaperTrading()

for p in prices[-10:]:
    dominant, strength, signals = trader.get_signal_strength(prices, p['close'])
    print(f"  {p['date']}: ${p['close']:,.2f} -> {dominant} (strength: {strength:+.3f})")

print(f"\nGenerated {len(prices)} price bars")
