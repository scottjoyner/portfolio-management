"""Verify TripleMovingAverageSystemStrategy imports work correctly."""

import sys
sys.path.insert(0, '/home/falcon/git/portfolio-management')

# Test 1: Import main strategy
from trading_system.strategies.trend.triple_ma_strategy import (
    TripleMovingAverageSystemStrategy,
    create_triple_ma_strategy,
    create_triple_ma_strategy_ema
)
print("✅ Main imports successful")

# Test 2: Create strategy instance
strategy = create_triple_ma_strategy()
assert strategy.short_ma_period == 5
assert strategy.medium_ma_period == 20
assert strategy.long_ma_period == 60
print("✅ Strategy creation successful")

# Test 3: Import supporting modules
from trading_system.utils import SMA, EMA
print("✅ Utils imports successful")

from trading_system.types import Candle, TradingSignal
print("✅ Types imports successful")

print("\n" + "="*50)
print("All verification checks passed!")
print("="*50)
