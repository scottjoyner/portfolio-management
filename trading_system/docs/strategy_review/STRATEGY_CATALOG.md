# Strategy Catalog (Implementation Reality)

Status scale used: `implemented`, `partial`, `placeholder/spec-only`.

| Strategy | Location | Type | Status | Modes | Products | Notes |
|---|---|---|---|---|---|---|
| TrendFollowingBreakoutStrategy | `strategies/trend/breakout.py` | Trend | partial | paper/replay/backtest/live | BTC-USD default | Threshold scaffold with input requirements; no full order intent wiring. |
| MeanReversionZScoreStrategy | `strategies/mean_reversion/zscore.py` | Mean reversion | partial | paper/replay/backtest/live | BTC-USD default | Uses generic threshold trigger, improved metadata contract. |
| GridRebalanceCaptureStrategy | `strategies/mean_reversion/grid_capture.py` | Mean reversion/grid | partial | paper/replay/backtest | BTC-USD default | Inventory/grid intent but no true grid state machine. |
| CrossSectionalRelativeStrengthStrategy | `strategies/ensemble/rotation.py` | Relative strength | partial | paper/replay/backtest/live | multi-asset conceptual | Ranking is placeholder score input only. |
| RegimeSwitchingEnsembleAllocator | `strategies/ensemble/regime_allocator.py` | Ensemble/meta allocator | partial | paper/replay/backtest | portfolio-level conceptual | Regime + strategy quality required as inputs. |
| StairStepMarketMakerStrategy | `strategies/market_making/stair_step_mm.py` | Market making | partial | paper/replay/backtest/live | spot | No true quote ladder generation yet in strategy layer. |
| AdaptiveSpreadMMStrategy | `strategies/market_making/adaptive_spread_mm.py` | Market making | partial | paper/replay/backtest/live | spot | Spread adaptation represented as feature gating only. |
| OrderBookImbalanceStrategy | `strategies/microstructure/orderbook_imbalance.py` | Microstructure | partial | paper/replay/backtest/live | spot | Requires imbalance inputs; signal logic still simplified. |
| VwapTwapExecutionStrategy | `strategies/execution_algos/vwap_twap.py` | Execution algo | partial | paper/replay/backtest/live | spot | Represents execution signal gating, not child-order scheduler. |
| PairsTradingStrategy | `strategies/stat_arb/pairs.py` | Stat arb | partial | paper/replay/backtest | pairs conceptual | Requires spread/hedge_ratio features; no hedge execution sequencing. |
| VolatilityBreakoutStrategy | `strategies/volatility/vol_breakout.py` | Volatility breakout | partial | paper/replay/backtest/live | spot | ATR/vol required; still score-based core. |
| LongHorizonDcaStrategy | `strategies/accumulation/dca.py` | Accumulation | partial | paper/replay/backtest/live | spot treasury sleeve | DCA cadence represented via cooldown only. |
| LiquidityVacuumSnapbackStrategy | `strategies/special/liquidity_snapback.py` | Special/microstructural reversal | partial | paper/replay/backtest | spot | Needs liquidity gap feature, still threshold decision. |
| BasisCarryDerivativesStrategy | `strategies/special/basis_carry.py` | Basis/carry | partial | paper/replay/backtest | derivatives conceptual | Requires basis/funding inputs, no carry curve model. |
| GenericSpecStrategy (+36 specs) | `strategies/catalog/advanced.py` | Spec catalog | placeholder/spec-only | paper/replay/backtest | varied | Metadata-rich catalog rows used for audit/registry coverage, not true implementations. |
| CLMM + LP overlays | `onchain/strategies/amm_lp/*.py` | Onchain LP + overlays | implemented/partial mix | paper/replay/live-approval | EVM pools | Utility modules implemented (width rotation, tail retreat, adverse selection defense, etc.). |
| HybridHedgeLinker | `onchain/strategies/hedging/hybrid_hedge.py` | Hybrid hedge | implemented | paper/replay/live-approval | CEX hedge legs | Produces hedge plans with atomicity class and confidence. |
| ProfitCaptureEngine | `onchain/strategies/treasury/profit_sweep.py` | Treasury sweep overlay | implemented | paper/replay/live-approval | treasury buckets | Real bucket split logic for realized pnl. |
| OpportunityRanker | `onchain/strategies/execution/opportunity_ranker.py` | Routing/ranking | implemented | paper/replay/live-approval | onchain opportunities | Net-edge/trust scoring with rejection reasons. |
