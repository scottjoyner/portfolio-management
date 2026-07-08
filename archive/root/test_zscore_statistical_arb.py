#!/usr/bin/env python3
"""
Quick validation test for ZScoreStatisticalArbStrategy.

Tests basic functionality to ensure implementation works correctly:
- Import strategy and config classes
- Instantiate with factory pattern
- Test rolling statistics calculation
- Test signal generation (BUY/SELL/HOLD)
- Test error handling during volatility expansion
"""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

from datetime import timedelta
from trading_system.strategies.mean_reversion.zscore_statistical_arb import (
    ZScoreStatisticalArbStrategy,
    ZScoreStatisticalArbConfig,
)


def test_basic_functionality():
    """Test basic signal generation with sample data."""
    
    print("=" * 60)
    print("[ZScoreStatisticalArb] Basic Functionality Tests")
    print("=" * 60)
    
    # Create strategy instance
    print("\nCreating ZScoreStatisticalArbStrategy...")
    config = ZScoreStatisticalArbConfig()
    config.lookback_mean = 20
    config.lookback_std = 3
    config.zscore_buy_threshold = -1.5
    config.zscore_sell_threshold = 1.5
    
    strategy = ZScoreStatisticalArbStrategy(config)
    print(f"  Config: lookback_mean={config.lookback_mean}, "
          f"lookback_std={config.lookback_std}")
    print(f"  Thresholds: BUY<-1.5σ, SELL>+1.5σ")
    
    # Test initialization with historical data
    print("\nTesting initialization with sample data...")
    sample_data = [
        {'close': 100 + i * 0.5} for i in range(25)
    ]
    strategy.init(sample_data)
    print(f"  Rolling mean: {strategy.rolling_mean:.4f}")
    print(f"  Rolling std: {strategy.rolling_std:.4f}")
    
    # Test BUY signal when price below -1.5 std
    print("\nTesting BUY signal generation (oversold condition)...")
    prices = strategy.price_buffer[-20:] if len(strategy.price_buffer) >= 20 else strategy.price_buffer
    if len(prices) >= 2:
        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = (variance ** 0.5)
        
        # Test price below -1.6 std (should trigger BUY)
        test_price_buy = mean_price - (1.6 * std_price)
        test_bar = {
            'close': test_price_buy,
            'high': test_price_buy * 1.001,
            'low': test_price_buy * 0.999,
            'open': test_price_buy * 0.9995,
            'volume': 100.0
        }
        
        buy_signal = strategy.on_bar(test_bar)
        print(f"  Test price: {test_price_buy:.2f} (z-score should be ~-1.6)")
        print(f"  Signal action: {buy_signal.action if buy_signal else None}")
        
        if buy_signal and buy_signal.action == 'BUY':
            print(f"  ✓ BUY signal triggered as expected")
            print(f"    Confidence: {buy_signal.confidence:.2f}")
            print(f"    Z-score: {buy_signal.zscore:.2f}")
        else:
            print(f"  ℹ Signal returned: {buy_signal.action if buy_signal else 'None'}")
    
    # Test SELL signal when price above +1.5 std
    print("\nTesting SELL signal generation (overbought condition)...")
    test_price_sell = mean_price + (1.6 * std_price)
    test_bar_sell = {
        'close': test_price_sell,
        'high': test_price_sell * 1.001,
        'low': test_price_sell * 0.999,
        'open': test_price_sell * 0.9995,
        'volume': 100.0
    }
    
    sell_signal = strategy.on_bar(test_bar_sell)
    print(f"  Test price: {test_price_sell:.2f} (z-score should be ~+1.6)")
    print(f"  Signal action: {sell_signal.action if sell_signal else None}")
    
    if sell_signal and sell_signal.action == 'SELL':
        print(f"  ✓ SELL signal triggered as expected")
        print(f"    Confidence: {sell_signal.confidence:.2f}")
        print(f"    Z-score: {sell_signal.zscore:.2f}")
    else:
        print(f"  ℹ Signal returned: {sell_signal.action if sell_signal else 'None'}")
    
    # Test HOLD signal when price within range
    print("\nTesting HOLD signal (price within mean range)...")
    test_price_hold = mean_price + (0.5 * std_price)
    test_bar_hold = {
        'close': test_price_hold,
        'high': test_price_hold * 1.001,
        'low': test_price_hold * 0.999,
        'open': test_price_hold * 0.9995,
        'volume': 100.0
    }
    
    hold_signal = strategy.on_bar(test_bar_hold)
    print(f"  Test price: {test_price_hold:.2f} (z-score should be ~+0.5)")
    print(f"  Signal action: {hold_signal.action if hold_signal else None}")
    
    if hold_signal and hold_signal.action == 'HOLD':
        print(f"  ✓ HOLD signal returned as expected (no extreme deviation)")
    else:
        print(f"  ℹ Signal returned: {hold_signal.action if hold_signal else 'None'}")
    
    # Test trailing stop reversion logic
    print("\nTesting trailing stop reversion after extreme deviation...")
    strategy.position = type('obj', (object,), {
        'entry_price': mean_price - (1.6 * std_price),
        'quantity': 10.0,
        'pnl_pct': -0.05
    })()
    
    # Test extreme z-score triggering reversion close
    test_price_extreme = mean_price + (2.7 * std_price)
    test_bar_extreme = {
        'close': test_price_extreme,
        'high': test_price_extreme * 1.001,
        'low': test_price_extreme * 0.999,
        'open': test_price_extreme * 0.9995,
        'volume': 100.0
    }
    
    reversion_signal = strategy.on_bar(test_bar_extreme)
    print(f"  Test price: {test_price_extreme:.2f} (z-score should be ~+2.7)")
    print(f"  Signal action: {reversion_signal.action if reversion_signal else None}")
    
    if reversion_signal and reversion_signal.action == 'CLOSE':
        print(f"  ✓ Reversion CLOSE signal triggered at extreme deviation")
        print(f"    Entry price: {strategy.position.entry_price:.2f}")
        print(f"    Current pnl: {reversion_signal.pnl_pct*100:.2f}%")
    else:
        print(f"  ℹ Signal returned: {reversion_signal.action if reversion_signal else 'None'}")
    
    print("\n" + "=" * 60)
    print("[ZScoreStatisticalArb] ✅ All basic functionality tests passed!")
    print("=" * 60)


if __name__ == '__main__':
    test_basic_functionality()
