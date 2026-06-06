#!/usr/bin/env python3
"""Quick validation that ZScoreStatisticalArbStrategy imports and basic functionality works."""

from trading_system.strategies.mean_reversion.zscore_statistical_arb import (
    ZScoreStatisticalArbStrategy,
    ZScoreStatisticalArbConfig,
)

print("[ZScoreStatisticalArb] Strategy class imported successfully")
print("[ZScoreStatisticalArb] Config class imported successfully")

# Test configuration
config = ZScoreStatisticalArbConfig()
config.lookback_mean = 20
config.lookback_std = 3
config.zscore_buy_threshold = -1.5
config.zscore_sell_threshold = 1.5

print(f"[ZScoreStatisticalArb] Config created: lookback_mean={config.lookback_mean}, zscore_thresholds=(-{abs(config.zscore_buy_threshold)}, {abs(config.zscore_sell_threshold)})")

# Test strategy instantiation
strategy = ZScoreStatisticalArbStrategy(config)
print(f"[ZScoreStatisticalArb] Strategy instantiated: {strategy.get_name()}")

# Verify key attributes exist
assert hasattr(strategy, 'lookback_mean'), "Missing lookback_mean attribute"
assert hasattr(strategy, 'lookback_std'), "Missing lookback_std attribute"
assert hasattr(strategy, 'zscore_buy_threshold'), "Missing zscore_buy_threshold attribute"
assert hasattr(strategy, 'zscore_sell_threshold'), "Missing zscore_sell_threshold attribute"

print("[ZScoreStatisticalArb] All attributes present")

# Test init with sample data
sample_data = [{'close': 100 + i * 0.5} for i in range(25)]
strategy.init(sample_data)
print(f"[ZScoreStatisticalArb] Init completed successfully, rolling_mean={strategy.rolling_mean:.4f}, rolling_std={strategy.rolling_std:.4f}")

print("\n" + "="*60)
print("[ZScoreStatisticalArb] All validation checks passed!")
print("="*60)
