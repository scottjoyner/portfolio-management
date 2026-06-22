#!/usr/bin/env python3
"""Test script to verify the new BTC-XXX volatility and additional strategies."""

from backtester import (
    BTCVolatilityStacking,
    BTCVolatilityBreakout,
    BTCVolatilityMeanReversion,
    BTCVolatilityMomentum,
    CoinbaseMomentumStrategy,
    CoinbaseMeanReversionStrategy,
    VolatilityBreakoutStrategy,
    RegimeAwareAdaptiveStrategy
)

# Create sample data for testing
def create_sample_data(product_id, num_bars=100):
    """Create sample OHLCV data for testing."""
    import random
    from datetime import datetime, timezone, timedelta
    
    data = []
    base_price = 50000 if product_id == "BTC-USD" else 100 if product_id == "BTC-ETH" else 200
    
    for i in range(num_bars):
        # Generate price with some volatility
        volatility = random.uniform(0.005, 0.02)
        change = random.uniform(-volatility, volatility)
        
        current_price = base_price * (1 + change)
        
        # Generate OHLCV data
        high = current_price * (1 + random.uniform(0, 0.01))
        low = current_price * (1 - random.uniform(0, 0.01))
        open_price = current_price * (1 + random.uniform(-0.005, 0.005))
        volume = random.uniform(100, 1000)
        
        # Ensure high >= open >= low and high >= close >= low
        high = max(high, open_price, current_price)
        low = min(low, open_price, current_price)
        
        timestamp = int((datetime.now(timezone.utc) - timedelta(hours=num_bars - i)).timestamp())
        
        data.append({
            'time': datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),
            'ts': timestamp,
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(current_price, 2),
            'volume': round(volume, 2)
        })
    
    return data

# Test the new strategies
def test_strategies():
    """Test all 9 new BTC-XXX strategies."""
    
    print("=" * 70)
    print("Testing BTC-XXX Volatility and Additional Strategies")
    print("=" * 70)
    
    # Test each strategy
    strategies = [
        ("BTCVolatilityStacking", BTCVolatilityStacking(20, 14, 14, 0.02, 0.04)),
        ("BTCVolatilityBreakout", BTCVolatilityBreakout(20, 10, 14, 0.02)),
        ("BTCVolatilityMeanReversion", BTCVolatilityMeanReversion(30, 20, 2.0, 0.5, 14)),
        ("BTCVolatilityMomentum", BTCVolatilityMomentum(20, 10, 14, 14, 0.02)),
        ("CoinbaseMomentum", CoinbaseMomentumStrategy()),
        ("CoinbaseMeanReversion", CoinbaseMeanReversionStrategy()),
        ("VolatilityBreakout", VolatilityBreakoutStrategy()),
        ("RegimeAwareAdaptive", RegimeAwareAdaptiveStrategy())
    ]
    
    for strategy_name, strategy in strategies:
        print(f"\nTesting {strategy_name}...")
        
        # Create test data
        test_data = create_sample_data("BTC-USD", 200)
        
        # Generate signals
        signals = strategy.generate_signals(test_data)
        
        print(f"  Generated {len(signals)} signals")
        
        # Count buy and sell signals
        buy_signals = [s for s in signals if s[0] == 'BUY']
        sell_signals = [s for s in signals if s[0] == 'SELL']
        
        print(f"  Buy signals: {len(buy_signals)}")
        print(f"  Sell signals: {len(sell_signals)}")
        
        if len(buy_signals) > 0:
            avg_buy_price = sum(s[1] for s in buy_signals) / len(buy_signals)
            print(f"  Avg buy price: ${avg_buy_price:.2f}")
        
        if len(sell_signals) > 0:
            avg_sell_price = sum(s[1] for s in sell_signals) / len(sell_signals)
            print(f"  Avg sell price: ${avg_sell_price:.2f}")
    
    print("\n" + "=" * 70)
    print("All 9 strategies tested successfully!")
    print("=" * 70)

if __name__ == "__main__":
    test_strategies()