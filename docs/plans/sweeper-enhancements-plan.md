# Paper Sweeper Enhancement Plan

## Current State
14 strategies running across USD/USDC/BTC pairs on Coinbase, generating 200 signals per sweep using 5m OHLCV candles + best bid/ask quotes. Strategies are independent (no shared context, no position awareness, no regime gating).

## Proposed Enhancements

### 1. Candlestick Pattern Detection
- **Patterns**: doji, hammer, shooting star, engulfing (bullish/bearish), morning star, evening star, three white soldiers, three black crows
- **Input**: 2-3 consecutive 5m candles (open, high, low, close)
- **Output**: pattern name + direction + confidence (0.2–0.5)
- **Effect**: Layered on top of existing momentum/reversal strategies — confirms or overrides signals

### 2. Market Regime Detection
- **Regimes**: trending_up, trending_down, mean_reverting, quiet, volatile
- **Input**: 20-candle window of returns + volume + ADX-like computation
- **Output**: regime label + strength (0–1)
- **Effect**: Strategy gating — kill mean-reversion during strong trends, kill momentum during quiet periods, adjust confidence scores by regime

### 3. Order Book Imbalance
- **Input**: Coinbase CLI `get_product_book` (10-level depth)
- **Output**: bid_vs_ask_volume_ratio, pressure direction
- **Effect**: Standalone signal / momentum confirmation
- **New CLI call required**: `get_product_book` action added to `bridge_cli.mjs`

### 4. Stop-Loss Generation
- **Input**: Open paper positions from state + current market quotes
- **Output**: Exit signals for positions exceeding max drawdown or trailing stop
- **Effect**: Position-aware sweep — closes losing positions, locks in trailing gains
- **Must be wired to paper engine**: Published alongside market signals

### 5. ATR Position Sizing
- **Input**: 14-candle ATR (Average True Range) per product
- **Output**: Recommended position size fraction of base capital
- **Effect**: High-vol pairs get smaller sizes; stable pairs get larger sizes
- **Applied at execution time**: `quantity` field in signal reflects ATR-adjusted size

### 6. Pivot Points / Support & Resistance
- **Input**: 20-candle window swing highs/lows
- **Output**: Support and resistance levels per product
- **Effect**: Standalone signals (bounce off support, break through resistance) + confirmation for channel/mean-reversion strategies

### 7. Running P&L Tracker
- **Input**: Sweep history log
- **Output**: Win rate, avg return, strategy-level P&L
- **Effect**: Dashboard data, strategy pruning, confidence adjustment
- **Storage**: In-memory sweep history with optional persistence to operator store

## Implementation Order
1. Candlestick patterns (pure function, no new dependencies)
2. Market regime detection (pure function, no new dependencies)
3. ATR position sizing (pure function, no new dependencies)
4. Pivot points / S&R (pure function, no new dependencies)
5. Stop-loss generation (needs paper position state)
6. Order book imbalance (needs CLI bridge addition)
7. Running P&L tracker (needs persistent storage)

## Key Constraints
- All strategies must fire in any market condition (no data droughts)
- BTC-pair strategies must discover 24 existing BTC pairs
- No placeholder/fake data — all inputs from live CLI
- Total sweep time should stay under 120s for 200 markets
