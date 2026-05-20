# STRATEGY_CATALOG

> Canonical 100-strategy program catalog with explicit mapping to concrete implementations when available.

| # | Strategy | Impl Mapping | Family | Risk Tier | Status | Backtest | Replay | Paper | Live |
|---:|---|---|---|---|---|---|---|---|---|
| 1 | Multi-timeframe breakout | TrendFollowingBreakoutStrategy | trend_momentum | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 2 | Donchian channel breakout | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 3 | ATR channel breakout | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 4 | Moving-average crossover | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 5 | EMA ribbon trend-following | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 6 | Supertrend continuation | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 7 | ADX trend-strength breakout | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 8 | Trend pullback continuation | - | trend_momentum | TIER_2_MODERATE_RISK | partial | True | True | True | False |
| 9 | Opening-range breakout | - | trend_momentum | TIER_2_MODERATE_RISK | partial | True | True | True | False |
| 10 | Volatility expansion breakout | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 11 | Turtle-style breakout | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 12 | Keltner channel breakout | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 13 | Regression-slope momentum | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 14 | Time-series momentum | - | trend_momentum | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 15 | Cross-sectional momentum rotation | CrossSectionalRelativeStrengthStrategy | trend_momentum | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 16 | Z-score mean reversion | MeanReversionZScoreStrategy | mean_reversion | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 17 | Bollinger band reversion | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 18 | RSI exhaustion reversal | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 19 | Short-term reversal after large candle | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 20 | Gap-fill mean reversion | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 21 | VWAP reversion intraday | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 22 | Reversion to anchored VWAP | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 23 | Range-bound fade | GridRebalanceCaptureStrategy | mean_reversion | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 24 | Channel-touch reversal | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 25 | Order-block rejection reversion | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 26 | Market overextension snapback | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 27 | Liquidity vacuum snapback | LiquidityVacuumSnapbackStrategy | mean_reversion | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 28 | Exhaustion-volume reversal | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 29 | Session close reversion | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 30 | Micro pullback reversion | - | mean_reversion | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 31 | Pairs trading with fixed hedge ratio | PairsTradingStrategy | relative_value | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 32 | Pairs trading with Kalman hedge ratio | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 33 | Cointegration basket arbitrage | - | relative_value | TIER_2_MODERATE_RISK | partial | True | True | True | False |
| 34 | Residual spread z-score arb | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 35 | Cross-exchange spot arbitrage abstraction | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 36 | CEX-DEX arbitrage abstraction | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 37 | Triangular arbitrage | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 38 | Stablecoin imbalance arbitrage | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 39 | Basis arbitrage | BasisCarryDerivativesStrategy | relative_value | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 40 | Carry trade allocator | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 41 | Funding-rate differential strategy | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 42 | Dispersion trading across correlated assets | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 43 | Correlation breakdown arb | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 44 | Lead-lag relative value strategy | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 45 | ETF/index proxy relative value abstraction | - | relative_value | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 46 | Stair-step market maker | StairStepMarketMakerStrategy | market_making | TIER_3_HIGH_RISK | implemented | True | True | True | True |
| 47 | Adaptive spread market maker | AdaptiveSpreadMMStrategy | market_making | TIER_3_HIGH_RISK | implemented | True | True | True | True |
| 48 | Inventory-skewed market maker | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 49 | Avellaneda-Stoikov-inspired market maker | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 50 | Queue-reactive market maker | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 51 | Volatility-scaled market maker | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 52 | Microprice-driven quote engine | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 53 | Toxic-flow-aware market maker | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 54 | Spread floor/ceiling market maker | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 55 | Post-only maker with fade logic | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 56 | Maker ladder with quote clustering controls | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 57 | Dynamic quote refresh policy strategy | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 58 | Inventory liquidation overlay | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 59 | Maker pause under stress overlay | - | market_making | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 60 | Maker-taker switcher | - | market_making | TIER_3_HIGH_RISK | partial | True | True | True | False |
| 61 | Order book imbalance strategy | OrderBookImbalanceStrategy | microstructure | TIER_4_EXPERT_HIGH_RISK | implemented | True | True | True | True |
| 62 | Trade-flow imbalance strategy | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 63 | Queue pressure predictive strategy | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 64 | Sweep detection momentum | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 65 | Refill-speed resiliency strategy | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 66 | Quote fade/cancel pressure predictor | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 67 | Microburst momentum strategy | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 68 | Bid-ask bounce capture | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 69 | Short-horizon microprice drift strategy | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 70 | Spread collapse event strategy | - | microstructure | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 71 | TWAP execution algo | VwapTwapExecutionStrategy | execution | TIER_1_LOW_RISK | implemented | True | True | True | True |
| 72 | VWAP execution algo | VwapTwapExecutionStrategy | execution | TIER_1_LOW_RISK | implemented | True | True | True | True |
| 73 | Adaptive participation algo | - | execution | TIER_1_LOW_RISK | partial | True | True | True | False |
| 74 | Arrival-price shortfall minimizer | - | execution | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 75 | Iceberg replenishment strategy | - | execution | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 76 | Laddered entry/exit execution | - | execution | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 77 | Dynamic pegged execution abstraction | - | execution | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 78 | Liquidity-seeking execution strategy | - | execution | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 79 | Impact-aware rebalancer | - | execution | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 80 | Passive-first then aggressive execution overlay | - | execution | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 81 | Volatility compression then expansion | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 82 | Realized-vol breakout | VolatilityBreakoutStrategy | volatility_regime | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 83 | Volatility mean reversion | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 84 | Regime-switching allocator | RegimeSwitchingEnsembleAllocator | volatility_regime | TIER_2_MODERATE_RISK | implemented | True | True | True | True |
| 85 | Hidden-Markov market regime classifier | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 86 | Trend/chop classifier with strategy routing | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 87 | Panic-mode defensive allocator | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 88 | Tail-risk hedge trigger overlay | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 89 | Convexity allocator | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 90 | Volatility targeting portfolio overlay | - | volatility_regime | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 91 | Risk parity allocator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 92 | Minimum variance allocator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 93 | Equal risk contribution allocator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 94 | Momentum-weighted sleeve allocator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 95 | Drawdown-aware capital decay allocator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 96 | Strategy leaderboard allocator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 97 | Meta-strategy selector | - | portfolio_treasury | TIER_1_LOW_RISK | partial | True | True | True | False |
| 98 | Profit-sweep treasury allocator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
| 99 | Dynamic reserve deployment allocator | LongHorizonDcaStrategy | portfolio_treasury | TIER_1_LOW_RISK | implemented | True | True | True | True |
| 100 | Crash-response opportunistic accumulator | - | portfolio_treasury | TIER_5_RESEARCH_ONLY | research_only | False | False | True | False |
