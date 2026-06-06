"""
Comprehensive Backtesting Infrastructure Guide
===============================================

This guide covers all aspects of production-ready backtesting infrastructure with regime 
classification, performance metrics aggregation, and out-of-sample validation support.

SYSTEM ARCHITECTURE:
--------------------
The backtesting system is designed with three core components:

1. STRATEGY TESTING LAYER:
   - Unit tests for each strategy with deterministic inputs
   - Backtesting engine supporting batch data processing  
   - Performance metrics aggregation (win rate, profit factor, drawdown)
   - Regime classification tools (trending/ranging/volatile)

2. DEPLOYMENT INFRASTRUCTURE:
   - Docker containerization for fleet deployment
   - Configuration management via environment variables
   - Health check endpoints and metrics exposure
   - Error handling with automatic circuit breakers

3. MONITORING & ALERTING:
   - Performance degradation alerts
   - Regime change detection notifications  
   - Risk parameter monitoring
   - Drawdown threshold warnings

USAGE EXAMPLES:
---------------

Basic Usage Pattern:
--------------------
from trading_system.backtesting.engine import BacktestEngine, BacktestConfig

# Initialize engine with configuration parameters
config = BacktestConfig(
    risk_free_rate=0.05,          # Annual risk-free rate for Sharpe calc
    slippage_bps=10,              # 10 basis points slippage per trade
    commission_pct=0.001,         # 0.1% transaction commission
)

engine = BacktestEngine(config)

# Add strategies to test with historical data
ohlcv_data = get_ohlcv("BTC-USD", periods=365*24)  # Load 1 year of hourly data

# Initialize individual strategies before adding to engine
from trading_system.strategies.trend.macd_signal_crossover import MACDSignalCrossoverStrategy
macd_strategy = MACDSignalCrossoverStrategy()
macd_strategy.init(ohlcv_data)

engine.add_strategy('macdsignalcrossover', macd_strategy, ohlcv_data)

# Run backtest  
results = engine.run_backtest()

# Get aggregated performance metrics
for strategy_name, metrics in engine.metrics.items():
    print(f"{strategy_name}:")
    print(f"  Win Rate: {metrics['win_rate']:.1f}%")
    print(f"  Profit Factor: {metrics['profit_factor']:.2f}")
    print(f"  Sharpe Ratio: {metrics['sharpe_ratio']:.2f}")

Advanced Usage Pattern:
------------------------
from trading_system.backtesting.engine import RegimeClassifier, BacktestEngine

# Classify market regime for backtesting analysis
ohlcv_data = get_ohlcv("BTC-USD", periods=365*24)
regime = RegimeClassifier().classify_regime(ohlcv_data)

print(f"Current Market Regime: {regime}")
# Output could be 'TRENDED', 'RANGING', or 'VOLATILE'

Then conditionally run backtests based on regime:
if regime == 'TRENDED':
    # Focus on trend-following strategies
    trend_strategies = ['macdsignalcrossover', 'triplema', 'donchianchannel']
else:
    # Focus on mean-reversion and arbitrage strategies  
    arb_strategies = ['zscorearb', 'williamsrmeanrevert', 'spotfuturesbasis']

engine.add_strategy_list(trend_strategies, ohlcv_data)
results = engine.run_backtest()

OUT-OF-SAMPLE VALIDATION:
--------------------------
from trading_system.backtesting.engine import BacktestEngine

# Split data into training (70%) and test (30%) sets
split_idx = int(len(ohlcv_data) * 0.7)
train_data = ohlcv_data[:split_idx]
test_data = ohlcv_data[split_idx:]

# Train model on historical period (e.g., last 2 years)
train_ohlcv = get_ohlcv("BTC-USD", periods=365*18, start_date="2020-01-01")

# Test on out-of-sample period (e.g., most recent year)  
test_ohlcv = get_ohlcv("BTC-USD", periods=365*24, start_date="2024-01-01")

# Run backtest on training data
train_engine = BacktestEngine(config)
train_engine.add_strategy('macdsignalcrossover', train_ohlcv)
train_results = train_engine.run_backtest()

# Run backtest on out-of-sample test data  
test_engine = BacktestEngine(config)
test_engine.add_strategy('macdsignalcrossover', test_ohlcv)
test_results = test_engine.run_backtest()

# Compare performance metrics between training and testing sets
train_metrics = train_engine.metrics['macdsignalcrossover']
test_metrics = test_engine.metrics['macdsignalcrossover']

print(f"Training Set Win Rate: {train_metrics['win_rate']:.1f}%")
print(f"Test Set Win Rate: {test_metrics['win_rate']:.1f}%")
print(f"Strategy Robustness: {'HIGH' if abs(train_metrics['win_rate'] - test_metrics['win_rate']) < 10 else 'MODERATE'}")

BACKTESTING METRICS EXPLAINED:
-------------------------------

The backtesting engine calculates the following key metrics for each strategy:

1. WIN RATE (total_signals):
   - Percentage of trades that were profitable  
   - Calculated as: (winning_trades / total_trades) * 100
   - Target by strategy type:
     • Trend Following: 40-55% with high risk/reward (>2.0 average)
     • Mean Reversion: 50-60% with moderate risk/reward (1.3-2.0)  
     • Arbitrage: 55-70% (pure arb has higher win rates but limited profit per trade)

2. PROFIT FACTOR (profit_factor):
   - Gross profit divided by gross loss ratio  
   - Indicates quality of winning trades vs magnitude of losing trades
   - Target by strategy type:
     • Trend Following: 1.2-1.8 depending on market regime
     • Mean Reversion: 1.3-1.9 depending on entry/exit precision
     • Arbitrage: 1.4-2.2 (pure arb should have strong profit factors)

3. SHARPE RATIO (sharpe_ratio):
   - Annualized return divided by volatility of returns  
   - Risk-adjusted performance metric where higher is better
   - Calculated as: ((avg_daily_return - risk_free_rate/365) / std_daily_return) * sqrt(365)
   - Target threshold: >0.5 for strong strategy, >1.0 for exceptional

4. MAX DRAWDOWN (max_drawdown_pct):
   - Maximum equity drawdown from peak since strategy inception
   - Indicates worst-case scenario for position risk  
   - Lower is better - target <20% for well-diversified portfolios

5. TOTAL RETURN (total_return_pct):
   - Cumulative return across all trades in backtest period
   - Not annualized - shows raw performance over test window

REGIME CLASSIFICATION TOOLS:
----------------------------

The RegimeClassifier class helps identify current market conditions:

1. TRENDED_REGIME: Strong directional bias (>15% price range)
   - Best for: Trend-following strategies (MACD, Triple MA, Donchian)
   - Poor performance expected from: Mean-reversion, arbitrage

2. RANGING_REGIME: Low volatility oscillation (<8% price range)  
   - Best for: Mean-reversion strategies (Z-Score, Williams %R)
   - Poor performance expected from: Trend-following breakout systems

3. VOLATILE_REGIME: Extreme ATR expansion (2x normal levels)
   - All strategies should reduce position sizes during volatility regimes
   - Risk management becomes more important than signal generation

USAGE RECOMMENDATIONS FOR REGIME CLASSES:
------------------------------------------

For TRENDED_REGIME:
  • Deploy trend-following strategies at full position sizing  
  • Reduce mean-reversion exposure or set smaller position limits  
  • Focus on breakout-based signals (Donchian, MACD crossovers)

For RANGING_REGIME:
  • Deploy mean-reversion strategies at full position sizing
  • Reduce trend-following exposure or increase stop-loss tightness
  • Focus on oscillation-based entries (RSI, Williams %R extremes)

For VOLATILE_REGIME:
  • Reduce position sizes across all strategies by 50%
  • Increase stop-loss distances to avoid noise triggers  
  - Add volatility filters before executing trades

ERROR HANDLING & CIRCUIT BREAKERS:
-----------------------------------

The backtesting engine includes robust error handling:

1. NaN Guards:
   - Rejects invalid/zero prices before processing
   - Handles missing volume fields gracefully
   - Prevents crashes from malformed OHLCV data

2. Null Field Checks:
   - Validates required fields before signal generation
   - Logs warnings for optional field absence  
   - Continues operation with safe defaults

3. Circuit Breakers:
   - max_consecutive_losses=5: Triggers after 5 consecutive losing trades
   - cooldown_period_minutes=60: Cooldown duration before re-enablement
   - recovery_threshold_pct=1.5: Minimum performance improvement to resume

DEPLOYMENT TO PRODUCTION FLEET:
---------------------------------

After successful backtesting, strategies can be deployed using:

from trading_system.deployment.deploy_strategies import deploy_all_strategies

# Deploy all production-ready strategies  
results = deploy_all_strategies(profile='default', service_prefix='trading')

print(results)

For fleet management and monitoring:
  - Health check endpoints at /api/v1/health/check/{strategy}
  - Prometheus metrics exposure at /metrics/prometheus
  - Performance tracking via API calls to /api/v1/strategies/{name}/performance

SCALING TO 200+ STRATEGIES:
---------------------------

Current Status (June 2026):
- Phase 1 COMPLETE: 12 production-ready strategies with full documentation and tests
- Backtesting infrastructure operational for batch testing across all strategies

Scaling Path:
- Phase 2 (~4 weeks): Add 30 more strategies following established patterns
- Phase 3 (~8 more weeks): Add 70+ from established trading literature  
- Phase 4 (~4 more weeks): Complete comprehensive backtesting across all 200+ strategies

Backtesting Performance Targets:
- All strategies should demonstrate >1.2 Sharpe ratio on out-of-sample test data
- Win rates must be consistent (>40% for trend-following, >50% for mean-reversion)
- Max drawdown should remain <30% under normal market conditions (no extreme events)

CONTACT & SUPPORT:
------------------
Author: Portfolio Management System Team  
Date: June 2026  
Documentation: https://docs.hermes.dev/trading-strategies/backtesting
Contact: portfolio@hermes.dev

END OF BACKTESTING INFRASTRUCTURE GUIDE
"""
__all__ = ['BACKTESTING_GUIDE']
