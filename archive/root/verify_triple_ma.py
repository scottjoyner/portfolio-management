#!/usr/bin/env python3
"""
Quick verification test for TripleMovingAverageSystemStrategy.

Tests basic functionality to ensure implementation works correctly.
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')


from datetime import datetime
from trading_system.strategies.trend.triple_ma_strategy import (
    TripleMovingAverageSystemStrategy,
    create_triple_ma_strategy
)


def test_basic_functionality():
    """Test basic signal generation with sample data."""
    
    # Create strategy instance
    print("Creating TripleMovingAverageSystemStrategy...")
    strategy = TripleMovingAverageSystemStrategy(
        short_ma_period=5,
        medium_ma_period=20,
        long_ma_period=60
    )
    print(f"  Strategy periods: Short={strategy.short_ma_period}, "
          f"Medium={strategy.medium_ma_period}, Long={strategy.long_ma_period}")
    
    # Test with empty data (should return HOLD)
    print("\nTesting with minimal candles...")
    strategy = TripleMovingAverageSystemStrategy(
        short_ma_period=3,  # Reduce for testing
        medium_ma_period=5,
        long_ma_period=7
    )
    
    minimal_candles = [
        {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5},
        {'open': 100.6, 'high': 101.5, 'low': 100.0, 'close': 101.0}
    ]
    
    signal = strategy.generate_signal(minimal_candles)
    print(f"  Signal with minimal data: {signal} (expected: 0/HOLD)")
    assert signal == 0, "Minimal candles should return HOLD"
    
    # Test factory function
    print("\nTesting factory function...")
    factory_strategy = create_triple_ma_strategy(
        short_period=5,
        medium_period=20,
        long_period=60
    )
    print(f"  Factory strategy: Short={factory_strategy.short_ma_period}, "
          f"Medium={factory_strategy.medium_ma_period}, Long={factory_strategy.long_ma_period}")
    
    # Test with sufficient candles for signal generation
    print("\nTesting with full candle sequence...")
    from trading_system.utils import SMA
    import functools
    
    test_candles = []
    timestamp = datetime.now() - timedelta(days=60)
    price = 100.0
    
    # Generate 70 candles of upward trend
    for i in range(70):
        test_candles.append({
            'open': price + (i * 0.2),
            'high': price + (i * 0.2) + 1.5,
            'low': price + (i * 0.2) - 0.5,
            'close': price + (i * 0.2) + 1.0
        })
    
    signal = factory_strategy.generate_signal(test_candles)
    print(f"  Signal in uptrend: {signal}")
    
    # Generate 70 candles of downward trend
    down_candles = []
    timestamp = datetime.now() - timedelta(days=60)
    price = 100.0
    
    for i in range(70):
        down_candles.append({
            'open': price - (i * 0.2),
            'high': price - (i * 0.2) + 1.5,
            'low': price - (i * 0.2) - 0.5,
            'close': price - (i * 0.2) - 1.0
        })
    
    signal = factory_strategy.generate_signal(down_candles)
    print(f"  Signal in downtrend: {signal}")
    
    print("\n✅ All basic functionality tests passed!")


if __name__ == '__main__':
    test_basic_functionality()
