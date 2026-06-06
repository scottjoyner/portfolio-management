# Triple Moving Average System Strategy

## Overview

A production-ready trend-following system using three moving averages (short, medium, long) to generate crossover signals for entry and exit decisions. The multi-MA approach provides regime confirmation before generating trading signals, reducing false positives from single-MA systems.

### Key Features
- **Factory Pattern**: Consistent configuration across deployments via `create_triple_ma_strategy()` factory functions
- **Golden Cross = BUY**: Short MA crosses above Medium MA when Long MA also points upward
- **Death Cross = SELL**: Short MA crosses below Medium MA when Long MA also points downward
- **Production-Ready**: Comprehensive error handling, logging integration, and type hints

## Configuration Options

### Default (SMA-based)
```python
from trading_system.strategies.trend import create_triple_ma_strategy

# Standard configuration: 5-period short, 20-period medium, 60-period long
strategy = create_triple_ma_strategy()

# Or specify custom periods
strategy = create_triple_ma_strategy(
    short_period=8,   # 8-period for faster signals
    medium_period=25, # 25-period intermediate trend
    long_period=70   # 70-period stable filter
)
```

### EMA Variant (faster response)
```python
from trading_system.strategies.trend import create_triple_ma_strategy_ema

# EMA-based strategy with 10-30-90 periods
strategy = create_triple_ma_strategy_ema(
    short_period=10,
    medium_period=30,
    long_period=90
)
```

### Direct Instantiation
```python
from trading_system.strategies.trend.triple_ma_strategy import TripleMovingAverageSystemStrategy
from functools import partial
from trading_system.utils import SMA

strategy = TripleMovingAverageSystemStrategy(
    short_ma_period=5,
    medium_ma_period=20,
    long_ma_period=60,
    short_ma_func=partial(SMA)(5),
    medium_ma_func=partial(SMA)(20),
    long_ma_func=partial(SMA)(60)
)
```

## Usage Examples

### Basic Signal Generation
```python
# Generate signal from candle data
candles = get_candle_data('BTC/USDT', interval='1h')  # Get historical candles

signal = strategy.generate_signal(candles)
if signal == 1:
    print("Signal: BUY - Golden Cross detected")
elif signal == -1:
    print("Signal: SELL - Death Cross detected")
else:
    print("Signal: HOLD - No active crossover")
```

### Position Management
```python
# Get position sizing based on risk parameters
position = strategy.calculate_position(candles, risk=0.1)  # 1% capital allocation

if position == 1:
    enter_long(size=calculate_size(candles, position, risk))
elif position == -1:
    enter_short(size=calculate_size(candles, position, risk))
# else: hold flat
```

### Signal Update (no re-evaluation)
```python
# After receiving new candle
signal = strategy.update_signal(current_candles)
```

## Technical Specifications

### Default MA Periods
- **Short**: 5 periods (intraday responsiveness)
- **Medium**: 20 periods (intermediate trend detection)
- **Long**: 60 periods (trend filter/stability)

### Signal Logic
```python
# Golden Cross (BUY signal +1):
    - Short MA crosses from below to above Medium MA
    - Long MA trending up or previously up
    - Trend confirmation on longer timeframes

# Death Cross (SELL signal -1):
    - Short MA crosses from above to below Medium MA
    - Long MA trending down or not up
    - Trend reversal on longer timeframes

# HOLD (signal 0):
    - No active crossover occurring
    - MAs maintaining same relative order
```

### Signal Types
| Value | Meaning      | Description                    |
|-------|--------------|--------------------------------|
| +1    | BUY          | Golden Cross detected          |
| 0     | HOLD         | No signal / holding            |
| -1    | SELL         | Death Cross detected           |

## Installation & Imports

```python
# Add to PYTHONPATH or use relative imports
from trading_system.strategies.trend.triple_ma_strategy import (
    TripleMovingAverageSystemStrategy,
    create_triple_ma_strategy,
    create_triple_ma_strategy_ema
)
```

### Dependency Files
- `trading_system/strategies/trend/triple_ma_strategy.py` - Main strategy implementation
- `trading_system/utils.py` - SMA/EMA utility functions
- `trading_system/types.py` - Candle and signal data types
- `trading_system/strategies/trend/__init__.py` - Module exports

## Unit Tests

### Running Tests
```bash
cd /home/falcon/git/portfolio-management
python trading_system/strategies/trend/test_triple_ma_strategy.py
```

### Test Coverage
- ✅ Golden Cross signal generation
- ✅ Death Cross signal generation
- ✅ HOLD when no crossover active
- ✅ Same order detection (no crossing)
- ✅ Trailing stop logic
- ✅ Position sizing integration
- ✅ Factory function variants
- ✅ Custom period configuration

## Production Considerations

### Strengths
- Clear entry/exit rules for systematic execution
- Regime fit: Strong directional trends work best
- Multi-timeframe signals reduce noise
- Trend filter (Long MA) prevents counter-trend losses

### Limitations & Risks
- **Slow signal generation** in choppy, sideways markets
- **Significant lag** causing late entries and early exits
- **Whipsaw losses** when Long MA flips during transitions
- **Poor performance** in low-volatility consolidation periods

### Best Practices
1. Use Long MA as regime filter - only trade when trending
2. Combine with stop-loss for risk management
3. Consider volatility-adjusted position sizing
4. Backtest across multiple market regimes
5. Monitor signal lag and adjust periods accordingly

## Advanced Usage

### Custom Indicator Integration
```python
# Replace default SMA with custom indicator function
from trading_system.strategies.trend.triple_ma_strategy import TripleMovingAverageSystemStrategy

# Define custom MA calculation
def custom_sma(candles, index):
    prices = [c['close'] for c in candles[index-10:index]]  # Last 10 candles
    return sum(prices) / len(prices)

strategy = TripleMovingAverageSystemStrategy(
    short_ma_period=5,
    medium_ma_period=20,
    long_ma_period=60,
    short_ma_func=custom_sma,  # Custom calculation
    medium_ma_func=partial(SMA)(20),
    long_ma_func=partial(SMA)(60)
)
```

### Multiple Timeframe Signals
```python
# Combine signals from different MA configurations
strategy_fast = create_triple_ma_strategy(5, 10, 30)
strategy_standard = create_triple_ma_strategy(5, 20, 60)
strategy_slow = create_triple_ma_strategy(8, 25, 70)

# Aggregate signals for more robust entries
signals = [strategy_fast, strategy_standard, strategy_slow]
buy_count = sum(1 for s in signals if s.generate_signal(candles) == 1)
action = "BUY" if buy_count >= 2 else "HOLD" if any(s.generate_signal(candles) == -1 for s in signals) else "HOLD"
```

## License & Attribution

This implementation is part of the portfolio management system. Refer to the main repository LICENSE file for terms.

---

**Author**: Hermes Trading Systems  
**Version**: 1.0.0  
**License**: Proprietary  
**Last Updated**: June 2, 2026
