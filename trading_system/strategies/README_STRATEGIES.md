# Trading Strategy Catalog - Comprehensive Implementation Guide
## Portfolio Management System v1.0 (June 2026)

---

## TABLE OF CONTENTS
1. [Strategy Overview](#strategy-overview)
2. [Implementation Categories](#implementation-categories)  
3. [Production-Ready Strategies](#production-ready-strategies)
4. [Testing & Validation](#testing--validation)
5. [Deployment Guide](#deployment-guide)
6. [Scaling to 200+ Strategies](#scaling-to-200-strategies)

---

## STRATEGY OVERVIEW

This directory contains **12 production-ready trading strategies** across three implementation categories:

### STRATEGIES BY TYPE:

**TREND FOLLOWING (7 strategies)**
- Capture explosive price moves during trending markets
- Best suited for bull/bear cycles with clear directional bias
- Win rate target: 40-55% | Risk/reward target: >2.0

1. **MACD Signal Crossover Strategy** (`strategies/trend/macd_signal_crossover.py`)
   - Classic momentum strategy using MACD histogram crossovers for entry/exit
   - Fast MA (default 12-period) crosses Slow MA (default 26-period) with signal line smoothing (9-period)
   - Production-ready with comprehensive error handling

2. **Triple Moving Average System Strategy** (`strategies/trend/triple_ma_strategy.py`)
   - Trend confirmation system using three MAs: short (5), medium (20), long (60) periods
   - Golden Cross = BUY, Death Cross = SELL
   - Multi-timeframe trend analysis for robust signals

3. **Bollinger Band Squeeze Strategy** (`strategies/trend/bollinger_band_squeeze.py`)
   - Volatility-based trend-following strategy that buys during low-volatility (squeeze) and sells on breakout
   - Detects compression/expansion of Bollinger bands (20-period SMA, 2.0 standard deviations)
   - Captures explosive moves after volatility contraction

4. **VWAP Momentum Strategy** (`strategies/trend/vwap_momentum.py`)
   - Volume-weighted average price as dynamic support/resistance
   - Buys on pullbacks to VWAP during uptrends, sells on weakness below VWAP
   - Institutional order flow proxy for entry timing

5. **Volume Breakout Strategy** (`strategies/trend/volume_breakout.py`)
   - Captures explosive price moves following high-volume breakouts above key resistance
   - Requires volume confirmation (2x average) and breakout threshold separation (0.5% above recent high/MA)
   - Trails profits with trailing stop to capture momentum extension

6. **Ichimoku Cloud Trend Strategy** (`strategies/trend/ichimoku_cloud.py`)
   - Comprehensive trend-following system using Ichimoku Cloud indicator
   - Multi-timeframe trend analysis through cloud visualization
   - Entry based on Tenkan/Kijun crossovers filtered by cloud position and thickness

7. **Keltner Channel Trend Strategy** (`strategies/trend/keltner_channel.py`)
   - Dynamic support/resistance using ATR-adjusted bands (20-period EMA center, 20-period ATR width)
   - Channel boundaries widen during high-volatility periods, contract during calm
   - Buys on lower band touch during uptrend, sells on upper band breach

---

**MEAN REVERSION (3 strategies)**
- Exploit price deviations from statistical fair value
- Best suited for ranging markets with mean-reverting distribution
- Win rate target: 50-60% | Profit factor target: 1.3-1.9

8. **Z-Score Statistical Arbitrage Strategy** (`strategies/mean_reversion/zscore_statistical_arb.py`)
   - Statistical mean-reversion using z-score for price deviation analysis from historical mean
   - BUY below -1.5 std, SELL above +1.5 std from rolling mean
   - Classic statistical arbitrage with configurable thresholds

9. **Bollinger Band Mean Reversion Strategy** (`strategies/mean_reversion/bollinger_mean_revert.py`)
   - Mean reversion using Bollinger Band breaches and z-score analysis
   - BUY when price below lower band OR z_score < -1.8 std, SELL when above upper band
   - Exploits overreactions in ranging markets

10. **RSI Mean Reversion Strategy** (`strategies/mean_reversion/rsi_mean_revert.py`)
    - RSI oscillator-based mean reversion strategy  
    - BUY when RSI < 30 (statistically undervalued), SELL when RSI > 70 (overvalued)
    - Classic retail-level logic with institutional-grade implementation

---

**ARBITRAGE (2 strategies)**
- Exploit price discrepancies between related assets or markets
- Higher win rates typical for pure arbitrage (<60% max due to execution constraints)
- Win rate target: 55-70% | Profit factor target: 1.4-2.2

11. **Spot-Futures Basis Arbitrage Strategy** (`strategies/arbitrage/spot_futures_basis_arb.py`)
    - Exploits price discrepancy between spot and futures markets
    - Captures basis convergence plus arbitrage profit from price differential  
    - BUY undervalued leg, SELL overvalued one simultaneously

12. **Cross-Exchange Basis Arbitrage Strategy** (`strategies/arbitrage/cross_exchange_basis_arb.py`)
    - Exploits price discrepancies between different crypto exchanges
    - Spatial arbitrage on multi-exchange platforms (Binance/Uniswap/etc.)
    - Requires low-latency connections and efficient routing

---

## IMPLEMENTATION CATEGORIES

### FACTORY PATTERN LIFECYCLE

All strategies implement consistent factory pattern lifecycle:

```python
# 1. INITIALIZATION
config = StrategyConfig(
    parameter_a=default_value,
    parameter_b=default_value,
)
strategy = StrategyClass(config)
strategy.init(historical_data)  # Compute indicators, establish baseline

# 2. ON-BAR SIGNAL GENERATION  
signal = strategy.on_bar(latest_bar)  # Returns dict or None if no signal triggered

# 3. POSITION STATE MANAGEMENT (optional - depends on strategy type)
if signal and signal.get("action") in ("BUY", "SELL"):
    position = strategy.handle_signal(signal)  # Track open positions

# 4. PERFORMANCE METRICS
metrics = strategy.get_performance_metrics()
```

### DOCSTRING STANDARDS

Each strategy includes comprehensive documentation covering:
- **Purpose**: What the strategy does and why it works
- **Regime Suitality**: Market conditions where strategy excels  
- **Failure Modes**: Scenarios where strategy may lose money extensively
- **Expected Performance**: Target win rate, profit factor, drawdown
- **Configuration Parameters**: All configurable parameters with defaults
- **Logic Explanation**: Detailed description of indicator calculations and signal generation
- **Usage Example**: Complete working example for easy implementation

### ERROR HANDLING GUARDRAILS

All strategies include production-grade error handling:
- NaN price guards (reject invalid/zero prices)
- Null field checks (handle missing optional fields gracefully)
- Minimum data validation before strategy execution
- Structured logging integration via `enable_logging` parameter

---

## TESTING & VALIDATION

### UNIT TESTS

Each strategy has comprehensive unit tests covering:
1. **Initialization Tests**: Valid historical data, minimum bars required
2. **Signal Generation Tests**: Buy signals on threshold breach
3. **Idle Period Tests**: No signals when conditions not met  
4. **Position State Tests**: Correct PnL tracking and stop-loss updates
5. **Edge Case Tests**: NaN prices, zero volume, missing fields

### RUNNING TESTS

Run all strategy tests:
```bash
python -m trading_system.tests.all_strategy_unit_tests --all-strategies
```

Or individual strategy:
```bash
python -m trading_system.strategies.trend.macd_signal_crossover.unit_tests
```

### TESTING RESULTS FORMAT

Tests output formatted summary:
```
Strategy: MACD Signal Crossover | Status: PASSED (12/12 tests)
Strategy: Triple MA System     | Status: PASSED (15/15 tests)
...

ALL STRATEGY UNIT TESTS COMPLETED SUCCESSFULLY!

All 12 production strategies passed comprehensive unit testing
Total: 12 production-ready trading strategies tested and validated
```

---

## DEPLOYMENT GUIDE

### DOCKERIZATION

Each strategy can be deployed via Docker container:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY trading_system/strategies/ ./strategies/
COPY trading_system/tests/ ./tests/

RUN pip install pandas numpy scipy backtrader --quiet

CMD ["python", "-m", "trading_system.strategies.trend.macd_signal_crossover"]
```

### HEALTH CHECKS

Strategies expose performance metrics at:
- `/api/v1/strategies/{strategy_name}/performance`
- `/api/v1/health/check/{strategy_name}`
- `/metrics/prometheus` (Prometheus-compatible metrics)

### CIRCUIT BREAKERS

All strategies support automatic circuit breakers with configurable parameters:
- `max_consecutive_losses=5`: Trigger after 5 consecutive losing trades
- `cooldown_period_minutes=60`: Circuit breaker cooldown duration
- `recovery_threshold_pct=1.5`: Minimum performance improvement to re-enable

### LOGGING INTEGRATION

Strategies integrate with structured logging system:
```python
strategy.enable_logging = True  # Enable logs to shared log file
```

Logs written to: `/tmp/{service}.log` (WSL fleet convention)

---

## SCALING TO 200+ STRATEGIES

### CURRENT STATUS (June 2026)

**12 production-ready strategies** implemented with comprehensive documentation and testing

### SCALING PATH PHASES:

**Phase 1 - COMPLETE**: Foundational strategies with full docs/tests (12 strategies)  
✅ Trend Following: MACD, Triple MA, Bollinger Squeeze, VWAP, Volume Breakout, Ichimoku Cloud, Keltner Channel (7)  
✅ Mean Reversion: Z-Score, Bollinger Band RSI (3)  
✅ Arbitrage: Spot-Futures Basis, Cross-Exchange (2)

**Phase 2**: Add 50+ more diverse strategies following factory pattern
- **Trend Following**: VWAP patterns (VWAP pullback, VWAP breakout), Keltner variations, Donchian channel, Hull MA crossover
- **Mean Reversion**: Stochastic RSI mean revert, Williams %R breakout-revert, CCI oscillation arb, Bollinger Band reversion with volume filter
- **Arbitrage**: Cross-exchange pairs arb (BTC/ETH pair), volatility basis arb, funding rate arbitrage, triangular arbitrage

**Phase 3**: Add 70+ strategies from established trading literature
- **Market Making**: Order book imbalance, liquidity provision, bid-ask spread arb
- **Volatility Strategies**: VIX skew arb, realized vol hedge, ATM skews, volatility surface arb
- **Statistical Arb**: Cointegration pairs trading, correlation breakdown, momentum factor rotation
- **Machine Learning**: Neural network trend classifier, LSTM price predictor, gradient boosted tree ensembles

**Phase 4**: Comprehensive backtesting all strategies
- Run batch backtests across 10+ year historical data  
- Regime classification for each strategy (trending/ranging/volatile)
- Risk-adjusted performance metrics aggregation
- Robustness testing against parameter changes

### QUALITY ASSURANCE CHECKLIST:

All new strategies must pass:
- [ ] Factory pattern lifecycle implemented (init → on_bar)
- [ ] Comprehensive docstring with purpose, regime fit, failure modes
- [ ] Unit tests covering initialization, signal generation, edge cases  
- [ ] Production-ready error handling (NaN guards, null checks)
- [ ] Compatible with existing structured logging system

### BACKTESTING INFRASTRUCTURE:

Full-scale backtesting framework includes:
1. **Strategy Testing Layer**: Unit tests for each strategy with deterministic inputs
2. **Batch Backtesting Engine**: Multi-strategy batch processing support  
3. **Performance Aggregation**: Metrics across strategies (win rate, profit factor, drawdown)
4. **Regime Classification Tools**: Trending/ranging/volatile market detection

---

## USAGE EXAMPLES

### BASIC USAGE:

```python
from trading_system.strategies.trend.macd_signal_crossover import MACDSignalCrossoverStrategy

# Initialize with default configuration
strategy = MACDSignalCrossoverStrategy()

# Setup with historical data (must include volume field)
ohlcv_data = get_ohlcv("BTC-USD", periods=100, interval="1h")
strategy.init(ohlcv_data)

# Generate signals on new bars  
signal = strategy.on_bar(latest_bar)
if signal and signal["action"] == "BUY":
    execute_trade(signal)

# Monitor performance
metrics = strategy.get_performance_metrics()
print(f"Win Rate: {metrics['win_rate']:.1f}%")
```

### CONFIGURED USAGE:

```python
from trading_system.strategies.trend.macd_signal_crossover import MACDSignalCrossoverConfig, MACDSignalCrossoverStrategy

config = MACDSignalCrossoverConfig(
    fast_period=12,      # MACD fast MA period  
    slow_period=26,      # MACD slow MA period
    signal_period=9,     # Signal line period
    position_size_usd=5000  # Position size in USD
)

strategy = MACDSignalCrossoverStrategy(config)
ohlcv_data = get_ohlcv("ETH-USD", periods=200)
strategy.init(ohlcv_data)
```

---

## CONTACT & SUPPORT

**Author**: Portfolio Management System Team  
**Date**: June 2026  
**License**: Proprietary (contact for distribution terms)  

For production deployment, custom configurations, or integration questions:
- Contact: portfolio@hermes.dev
- Documentation: https://docs.hermes.dev/trading-strategies
- Support Slack: #trading-strategies channel

---

## LEGEND

### STRATEGY CATEGORIES:
- **Trend Following**: Captures directional price moves with trailing stop exits
- **Mean Reversion**: Exploits deviations from statistical fair value  
- **Arbitrage**: Pure spatial/time arbitrage (higher win rates, different risk profile)

### PERFORMANCE TARGETS:
- **Win Rate Target by Type**:
  - Trend Following: 40-55% with high risk/reward (>2.0 average)
  - Mean Reversion: 50-60% with moderate risk/reward (1.3-2.0)  
  - Arbitrage: 55-70% (pure arb has higher win rates but limited profit per trade)

- **Maximum Historical Drawdown**: Varies by strategy type and market regime
- **Profit Factor**: Target >1.4 for all strategies under normal conditions

---

**END OF CATALOG DOCUMENTATION**
