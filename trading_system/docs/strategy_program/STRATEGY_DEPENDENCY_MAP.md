# STRATEGY_DEPENDENCY_MAP

## Runtime dependency layers

1. Market data feeds (candles, trades, orderbook)
2. Feature/indicator pipeline (ATR, RSI, realized vol, microstructure features)
3. Strategy contract + registry mapping
4. Risk/approvals + capital buckets
5. Execution + treasury sweep hooks
6. Capital orchestration allocator (`portfolio/allocator/capital_orchestrator.py`)

## Strategy-to-subsystem mapping

| Strategy | Concrete Impl | Data | Risk Gates | Execution Path | Notes |
|---|---|---|---|---|---|
| Multi-timeframe breakout | TrendFollowingBreakoutStrategy | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Donchian channel breakout | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| ATR channel breakout | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Moving-average crossover | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| EMA ribbon trend-following | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Supertrend continuation | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| ADX trend-strength breakout | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Trend pullback continuation | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Opening-range breakout | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Volatility expansion breakout | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Turtle-style breakout | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Keltner channel breakout | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Regression-slope momentum | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Time-series momentum | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Cross-sectional momentum rotation | CrossSectionalRelativeStrengthStrategy | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Z-score mean reversion | MeanReversionZScoreStrategy | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Bollinger band reversion | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| RSI exhaustion reversal | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Short-term reversal after large candle | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Gap-fill mean reversion | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| VWAP reversion intraday | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Reversion to anchored VWAP | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Range-bound fade | GridRebalanceCaptureStrategy | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Channel-touch reversal | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Order-block rejection reversion | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Market overextension snapback | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Liquidity vacuum snapback | LiquidityVacuumSnapbackStrategy | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Exhaustion-volume reversal | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Session close reversion | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |
| Micro pullback reversion | research adapter | candles, trades, orderbook | risk.engine + approvals | execution/router | fill realism depends on latency, partial fills, and queue assumptions |

(First 30 shown; full canonical set is in STRATEGY_CATALOG.)
