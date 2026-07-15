# Portfolio Management System — AGENTS.md

## Repository Overview

Multi-module algorithmic trading platform spanning Python (primary) and Node.js/TypeScript (prediction markets). Integrates with Coinbase CDP v3 (primary broker), Kalshi, Polymarket, and includes a full trading pipeline: market data → signal generation → confidence scoring → portfolio optimization → approval workflow → execution → bracket management → state persistence → UI dashboard.

## Directory Structure

| Path | Purpose |
|------|---------|
| `coinbase/src/` | **Execution engine**: `cb_client.py` (RESTClient wrapper), `execution.py` (order placement + bracket management + trailing stop + poll loop), `data.py` (candle fetching + cache), `config.py` (pydantic Settings), `bandit.py` (UCB1 / Thompson), `tcost.py` (transaction cost), `bridge_execution.py` (Node subprocess bridge), `run_trader.py` (main trading loop), `run_trader_v2.py` (hardened unified trader), `run_trader_v4.py` (EventTraderV4 with Rust path + batch scan), `pair_discovery.py` (discovers 400+ Exchange pairs by volume), `rest_feed.py` (urllib3 batch candle fetcher) |
| `trading_system/` | Core domain: `core/portfolio_manager.py`, `core/state_manager.py`, `core/models/`, `core/signal_aggregator.py` (unified cross-product SignalAggregator with 25-strategy scan), `ui/dashboard_server.py` (15 REST endpoints incl. `/strategies/rebalance`, `/strategies/rebalance/presets`, `/strategies/stairstep`), `ui/dashboard.html`, `signal_confidence.py` (ConfidenceEngine with 8 modifiers) |
| `graph-alpha-bot/` | Graph-based AI trading bot: ~30 strategies using Neo4j knowledge graph, news ingestion pipeline, MCP server |
| `backtester.py` | 14+ Coinbase-specific strategies + `MarketDataFetcher` + `Backtester` + benchmark runner |
| `strategy_engine.py` | 74+ strategies (72 Rust + 2 PM + 3 external-data) + `run_strategies()` + `backtest_strategy()` with pass/fail verdict. Imports Rust native path for all 74 Rust strategies when `_HAS_RUST` is True. External-data strategies: `FundingRateContrarian` (Binance funding), `ExchangeFlowSignal` (CoinGecko flows), `BTCDXYCorrelation` (Yahoo Finance cross-asset). |
| `portfolio_optimizer.py` | Continuous daemon: TLH detection, fee-tier optimization, rebalancing (80/15/5), strategy signal integration + **unified signal accumulator integration** (news, PM, arb, divergence) + **SignalAggregator integration** (universe-wide cross-product ranking), uses ConfidenceMatrix + ConfidenceEngine |
| `portfolio_manager.py` | Backtesting framework: Position/Portfolio classes, run strategies on historical CSV data |
| `approval_server.py` | Lightweight HTTP server for human-in-the-loop trade approval via email approve/deny links |
| `state_store.py` | Thread-safe SQLite persistence: trades, snapshots, bt_cache, position_ages, meta tables |
| `confidence_matrix.py` | Multi-strategy aggregation: 4 independence groups (trend/momentum/volatility/volume), weighted by backtest perf, group-agreement boosting |
| `data/` | Market data CSV files + fetchers (yfinance, Alpha Vantage, Coinbase, unified fetcher); **`feed_cache.py`** = NAS-backed durable cache (parquet append + de-dup) for candles/on-chain/PM/news, env `NAS_FEED_ROOT` (falls back to `data/feed_cache` when NAS not writable); adds `compact_all()` retention + `get_metrics()` cache-hit counters; **`data/approvals_inbox/`** = permission-tolerant cross-user store for manual dashboard orders |
| `multi_strategy_paper_trading.py` | Holistic paper trading orchestrator with VolumeOptimizer + FeeTierManager + ConfidenceEngine |
| `paper_trading_system.py` | Base paper trading system — live price feed + synthetic backtesting |
| `backtest/` | Historical data provider for backtesting |
| `tests/` | Unit tests for signal confidence, regime detection, adaptive stop loss |
| `docs/` | 49 Markdown docs: architecture, execution model, runbooks, API specs, production readiness checklists |
| `apps/` + `packages/` | Node.js/TypeScript: prediction-market arbitrage scaffold (web UI, API, execution, confidence, market matching) |
| `deploy/` | Docker Compose + K8s configs for production/staging |
| `scripts/` | Utility scripts for setup, credential management, data collection |

## Key Modules — Detailed

### Prediction Market Integration (Kalshi + Polymarket)

A unified pipeline consolidates the previously fragmented implementations (4 Kalshi + 3 Polymarket) into one coherent path:

**`event_markets/unified_client.py`** — Single client wrapping both platforms:
- `UnifiedPredictionMarketClient` with `search_all()`, `search_kalshi()`, `search_polymarket()`, `get_crypto_markets()`
- Common `PredictionMarket` dataclass (platform, question, outcome_prices, volume, spread, liquidity_score, probability_extremity)
- Kalshi auth via email+password (SHA256 signed), Polymarket public (no auth needed for CLOB data)
- Filters by min_volume, max_spread, crypto/macro keywords

**`event_markets/signal_adapter.py`** — `PredictionMarketAdapter` converts prediction market data into `AccumulatedSignal` objects for the UnifiedSignalAccumulator. Maps market questions to crypto symbols (BTC-USD, ETH-USD, etc.), generates BUY on high YES probability, SELL on low YES probability.

**`strategy_engine.py: KalshiSignal + PolymarketSignal`** — Strategy classes that implement `on_bar()` (matching the strategy interface) but fetch live prediction market data via the unified client. Registered in `ALL_STRATEGIES` as `"kalshi"` and `"polymarket"`, mapped to `CLASS_STRATEGIES` for growth/speculative asset classes.

**`confidence_matrix.py`** — Added `"prediction_market"` independence group (strategies: `kalshi`, `polymarket`), default weights (0.5 each), and `CLASS_BOOST` entries (safe=0.9, growth=1.0, speculative=1.2).

**Flow:**
```
Kalshi API / Polymarket CLOB
  → UnifiedPredictionMarketClient (unified_client.py)
    → PredictionMarketAdapter (signal_adapter.py)  ──→ UnifiedSignalAccumulator → Dashboard
    → KalshiSignal / PolymarketSignal (strategy_engine.py)
      → ConfidenceMatrix (prediction_market group)
        → PortfolioOptimizer (_detect_event_markets → actionable STRATEGY_SIGNAL)
```

Prediction market signals now:
1. Appear in the dashboard alongside Coinbase strategy signals (via accumulator)
2. Participate in confidence scoring with their own independence group (via ConfidenceMatrix)
3. Generate actionable Coinbase trades when the prediction market question maps to a crypto asset (via portfolio_optimizer)
4. Remain as notifications for non-crypto events (inflation, politics, sports)

### Knowledge Gap Analysis (event_markets/knowledge_gap.py)

Detects information asymmetries between prediction market probabilities and available web/news evidence. For each prediction market, searches Wikipedia (via API, no auth) and RSS news feeds (CoinDesk, Cointelegraph, NYT, CryptoSlate) for relevant content, computes an aggregate evidence score, and compares it against the market's implied probability.

**Architecture:**
- `SearchResult` — dataclass for a single search hit (source, title, snippet, url, relevance score)
- `KnowledgeGapAssessment` — dataclass with market_question, market_probability, evidence_score, gap, direction (overvalued/undervalued/fair), confidence, sources_used
- `SentimentAnalyzer` — keyword-based sentiment (reuses positive/negative word lists from the existing news pipeline)
- `WebResearcher` — Wikipedia API search (free, no auth, rate-limit friendly)
- `NewsResearcher` — RSS feed fetcher + keyword filter with 5-min cache
- `KnowledgeGapAnalyzer` — orchestrator: extracts topics from the question → searches web+news → aggregates sentiment → normalizes to evidence probability (0-1) → compares to market → returns assessment with `is_significant` property

**Integration:**
- `portfolio_optimizer.py` initializes `KnowledgeGapAnalyzer` when `_pm_client` is available
- `_detect_event_markets()` runs knowledge gap analysis on top 5 prediction markets
- If gap is significant (|gap| > 10%):
  - Evidence **contradicts** market direction → confidence boosted 1.4× (contrarian edge)
  - Evidence **confirms** market direction → confidence boosted 1.2× (conviction)
- Knowledge gap metadata (gap %, direction, evidence score, source list, sentiment) added to opportunity `meta`
- Reason string enriched with `[kg: undervalued gap=25%]` for visibility

**CLI:**
```bash
# Analyze a specific question
python3 -m event_markets.knowledge_gap --question "Will BTC reach $100k?" --probability 0.65

# Batch analyze from live prediction markets
python3 -m event_markets.knowledge_gap --kalshi-email ... --kalshi-password ... --batch 10

# Disable web or news search
python3 -m event_markets.knowledge_gap --question "Will ETH merge?" --no-news
```

**Flow:**
```
PredictionMarket (mid_price=0.75)
  → KnowledgeGapAnalyzer.analyze()
    → _extract_topics("Will Ethereum 2.0 improve scalability?")
    → WebResearcher.search("ethereum 2.0 scalability")       # Wikipedia
    → NewsResearcher.search("ethereum 2.0 scalability")      # RSS feeds
    → SentimentAnalyzer on all results
    → evidence_probability = normalize(avg_sentiment)         # e.g., 0.52
    → gap = 0.52 - 0.75 = -0.23 → "overvalued" by 23%
    → KnowledgeGapAssessment(confidence=46%, is_significant=true)
  → PortfolioOptimizer: confidence 1.4× boost, meta tagged
```

**Data sources used:**
- Wikipedia API (`en.wikipedia.org/w/api.php`) — factual research, free, no key
- RSS feeds — CoinDesk, Cointelegraph, NYT Business, CryptoSlate (30-min cache)
- Keyword-based sentiment (existing pattern, no ML/NLP libraries needed)

### Signal Generation Layer (3 parallel engines)

**`strategy_engine.py`** — 74+ strategies across 4 rounds/families:
- Trend: EMA_Crossover, MACD, TRIX, ADX, ParabolicSAR, HullMA, Aroon
- Momentum: RSI_MeanReversion, ChandeMomentum, WilliamsR, ZScoreReversion, ForceIndex
- Volatility: BollingerBreakout, VWAP_Reversion, KeltnerChannels, DonchianChannels
- Volume: VolumeMomentum, OBV_Divergence, ChaikinMoneyFlow, VolumePriceTrend
- Round 3: PriceEfficiencyRatio, SimplifiedCCI, RangeExpansionIndex, EMADeviation, SignalToNoiseRatio
- `run_strategies(currency, asset_class, closes, volumes, current_price, highs, lows)` filters by asset class (safe=5, growth=25, speculative=22), returns `List[Signal]` sorted by confidence.
- `backtest_strategy(closes, ...)` walks OHLCV forward, returns `BacktestVerdict` (win_rate, sharpe, profit_factor, max_drawdown, passed bool). Passes if win_rate>=0.4 AND sharpe>0.2 AND profit_factor>1.05 AND total_return>-20%.

**`backtester.py`** — 14+ Coinbase-only strategies (MA_Crossover, RSI, BollingerBands, DonchianChannel, TrendFollowing, BTCVolatilityStacking, BTCVolatilityBreakout, BTCVolatilityMeanReversion, BTCVolatilityMomentum, CoinbaseMomentumStrategy, CoinbaseMeanReversionStrategy, VolatilityBreakoutStrategy, RegimeAwareAdaptiveStrategy + 6 novel: VolumeProfileStrategy, MultiTimeframeConfluenceStrategy, OrderFlowPressureStrategy, VolatilityContractionExpansionStrategy, StatisticalArbitrageZScorePairStrategy, LiquidationHeatmapStrategy). Uses `MarketDataFetcher` (wraps `coinbase products candles` CLI command). Run benchmark with `run_benchmark("BTC-USD")`.

**`graph-alpha-bot/`** — Graph-based strategies (MA crossover on graph, news centrality momentum, supply chain shock diffusion, insider cluster drift).

**External-data strategies (3)** — Pure Python strategies fetching independent data sources:
- `FundingRateContrarian` (`strategy_engine.py`) — Binance Futures public API (`premiumIndex`);
  extreme funding > 0.1bps → fade (crowded long SELL, crowded short BUY). Completely
  independent of price action — uses derivatives market data.
- `ExchangeFlowSignal` (`strategy_engine.py`) — CoinGecko `market_chart` volume anomaly
  detection; volume spike on up-move = distribution (SELL), on down-move = accumulation
  (BUY). Uses on-chain volume proxy, independent of technical indicators.
- `BTCDXYCorrelation` (`strategy_engine.py`) — Yahoo Finance BTC-USD × DXY rolling 90-day
  correlation; > 2σ deviation from 1yr mean → reversion trade. Macro cross-asset signal.

### SignalAggregator — Universe-Wide Cross-Product Ranking

**`trading_system/core/signal_aggregator.py`** — Runs all 25 Rust strategies on every product simultaneously, backtest-validates each signal, computes long-term trend, and produces `UnifiedSignal` per product with a unified score (-1 to +1) and cross-product priority.

**`UnifiedSignal` fields:**
- `unified_score` — net direction magnitude (-1 strong SELL to +1 strong BUY)
- `consensus_score` — weighted consensus from all 25 strategies
- `backtest_quality` — avgs `win_rate × sharpe × profit_factor` of passing strategies
- `trend_score` — long-term trend from 50/200 SMA, ADX, volume ratio
- `conviction` — fraction of strategies agreeing on dominant direction
- `priority` — composite for cross-product ranking (= abs(score) × conviction × bt_quality)
- `top_strategies` — top 5 strategies by (confidence × backtest quality)

**Integration into portfolio_optimizer.py:**
- `_detect_aggregator_signals()` runs as the **10th detection dimension** (after accumulator, before event markets)
- Gets top-50 pairs by 24h USD volume via `pair_discovery`
- Fetches 100 hourly candles via `rest_feed.fetch_candles_batch()` (parallel)
- Runs `SignalAggregator.scan_universe()` → ranked results
- Filters to top-5 BUY/SELL with `priority >= 0.05`
- Trend-aligned trades (BUY + bullish trend, SELL + bearish trend) get **1.2× confidence multiplier**
- Creates `OpportunityType.STRATEGY_SIGNAL` opportunities with full metadata
- 300s cooldown

**Flow:**
```
top_coinbase_pairs(n=50) → fetch_candles_batch(50 pairs)
  → SignalAggregator.scan_universe()
    → Rust evaluate_all (0.04ms × 50 = 2ms)
    → batch_backtest_rust (rayon, ~2ms)
    → _compute_trend (50/200 SMA + ADX + volume)
    → _compute_unified (consensus × bt_quality × trend × conviction)
  → ranked UnifiedSignal list
  → _detect_aggregator_signals() → top-5 → Opportunity[]
```

## Confidence Scoring Pipeline

```
Raw Signal → ConfidenceMatrix (rust_core/src/confidence.rs)
               Groups 74+ strategies into 12 independence families:
               trend (13), momentum (11), volatility (10), volume (11),
               pattern (8), momentum_adv (9), prediction_market (2), sentiment (1),
               derivatives (1), onchain (1), order_flow (1), macro_risk (2)
               + Regime-aware group weighting (trending/ranging/volatile)
               + Correlation penalty (sqrt scaling within each group)
               +15% per additional agreeing group
               1.15x for 5+ strategies, 1.1x for 3+
               Extended: aggregate_ext(signals, asset, currency, bt_weights, regime)

           → ConfidenceEngine (trading_system/signal_confidence.py)
               8 modifiers applied in order:
               1. Liquidity tiering (1-5 based on 24h vol ranking)
               2. Spread adjustment (bps)
               3. Consecutive signal confirmation
               4. Win-rate tracking
               5. Sentiment integration
               6. Global consensus
               7. Regime confidence gating (caps by regime)
               8. Cross-correlation penalty

           → Priority scoring (portfolio_optimizer.py)
           → VolumeOptimizer boost (up to +40% for fee-tier volume)
```

### Portfolio Optimization (portfolio_optimizer.py)

Continuous daemon with 10+ detection dimensions:
1. **TLH** — sell positions with unrealized loss > 5% for 20% tax savings
2. **Fee-Tier Volume** — generate volume to reach lower Coinbase fee tiers (7 tiers, 0.6%/1.2% → 0.05%/0.15%)
3. **Rebalancing** — maintain target allocation (safe=80%, growth=15%, speculative=5%)
4. **Strategy Signals** — run all 25 strategies, apply backtest validation + ConfidenceMatrix + ConfidenceEngine
5. **Unified Signal Accumulator** — integrates news, PM, arb, divergence signals from the accumulator, mapped to `OpportunityType.ACCUMULATOR_SIGNAL`
6. **Rebalance Bot** — `OpportunityType.REBALANCE_BOT`, driven by `coinbase/src/rebalance_engine.py` (`RebalanceEngine`/`StairStepEngine`/`RebalanceBot`) with allocation presets `core_balanced` / `volatile_tilt` / `safe` and slim-profit partial sells
7. **Stair-Step Profit Taker** — `OpportunityType.STAIRSTEP`, incremental take-profit ladders per symbol
8. **Order-Flow / Funding / Onchain signals** — `_detect_order_flow_signals`, `_detect_funding_and_onchain_signals`
9. **Event Markets** — Kalshi/Polymarket arbitration + non-actionable notifications
10. **Universe & Stock scans** — top-coinbase-pair volume scan + stock opportunities

Opportunities ranked by `priority * confidence * boost`, max 10 positions, 20% risk per position.

Rebalancer env config (all optional): `REBALANCE_PRESET`, `REBALANCE_DRIFT`, `REBALANCE_PROFIT_TAKE`, `REBALANCE_MIN_NOTIONAL`, `STAIRSTEP_ENABLED`, `STAIRSTEP_SYMBOLS`. The Rust rebalancer lives in `rust_core/src/rebalance.rs` (PyO3 `PyRebalancer` / `PyStairStepProfitTaker`); the Python wrapper `RebalanceEngine.from_preset()` reads presets from `ALLOCATION_PRESETS`.

Detection order per tick: TLH → coinbase universe → stock → fee tier → rebalance → strategy signals → volume cycles → accumulator signals → rebalance_bot → stairstep → event markets.

### Approval Workflow (approval_server.py)

1. Optimizer writes `pending_approvals.json` with UUID token
2. `TradeNotifier` sends email with Approve/Deny links → `http://<server>/approve/<token>`
3. ApprovalHandler routes: `/approve/<token>` (sets approved), `/deny/<token>` (sets denied), `/status` (HTML table), `/api/status` (JSON)
4. Next optimizer tick executes approved trades

### Execution Engine (coinbase/src/execution.py)

- `preview_order()` → check fees before placing
- `place_order_bracket()` → market order entry + stop loss + take profit + trailing stop
- `manage_brackets()` → poll loop: check mid vs stop/target, move stop to breakeven after R multiple, trail stop, log analytics, update bandit scores
- Uses `cb_client.py` (Coinbase CDP v3 RESTClient wrapper with pagination)
- Bracket state uses pydantic `Bracket` model; `_get/_set` helpers handle both pydantic and dict for migration safety

### Coinbase v2 Modules (coinbase/src/)

- `product_rotation.py` — `ProductRotator` + `MomentumRotationStrategy` for multi-timeframe ranking and top-N rotation
- `adaptive_mode.py` — `AdaptiveModeSelector` for SCALP / SWING / TREND / HOLD switching
- `dual_mm.py` — `DualMarketMaker` + `MarketMakingStrategy` for inventory-skewed market making
- `ranking.py` — `StrategyRanking` + `StrategyRankingFilter` for rolling performance ranking and persistence
- `news_risk.py` — `NewsRiskAdjuster` and `NewsAwareRiskStrategy` using knowledge-graph sentiment and MCP guards
- `market_condition.py` — `MarketConditionProfile` + `MarketConditionStrategySelector` for archetype-based strategy gating
- `graph/` — CoinGecko-to-Neo4j sync helpers, `GraphSignalStrategy`, and `graph_weight_overlays()` for graph-aware weighting
- `graph/sync_coingecko_universe.py` — refreshes the CoinGecko top-5000 universe into Neo4j from cached JSON or live CoinGecko pages
- `portfolio_optimizer.py` — graph universe now biases buy/rebalance candidate ranking via Neo4j graph scores
- `run_trader_v2.py` — unified Coinbase trader with startup validation, graceful shutdown, health endpoint, and tick-stage error boundaries
- `trading_system/ui/dashboard_server.py` — `/market/universe` and `/execution/status` now include graph summaries/overlays

### Coinbase v2 Hardening (coinbase/src/)

- `run_trader_v2.py` now validates config at startup, seeds price history, and shuts down cleanly on `SIGINT` / `SIGTERM`
- `run_trader_v2.py` wraps every major tick stage in error boundaries so one broken source does not stop the loop
- `run_trader_v2.py` can expose `--health-port <port>` and serves `/health` with tick, regime, uptime, and ranking status
- `StrategyRanking` persists to `data/ranking_state.json` and reloads on startup
- `run_trader_v2.py` creates a `CBClient` and subscribes configured products so the feed actually polls; `PollingFeed(cb_client=None, ...)` still remains inert for harnesses
- `run_trader_v2.py` optionally registers `GraphSignalStrategy` and applies Neo4j graph overlays when the graph package is available
- `portfolio_optimizer.py` uses cached Neo4j graph scores to bias strategy-signal sizing and rebalance candidate selection
- Dashboard universe/execution views expose graph scores and overlays so the bias is visible in the UI
- `Ticker` uses `price` (not `mid_price`) and `TickerCache` uses `get_ticker()`; these are the canonical field/method names in the Coinbase feed stack

### State Persistence

- **SQLite** (`state_store.py`): trades, snapshots (portfolio state), bt_cache (30-day TTL), position_ages, meta
- **Neo4j** (`neo4j_store.py`): optional graph-based system of record
- **JSON files**: pending_approvals.json, .unified_signal_cache.json, optimizer_state.db

### UI Dashboard (trading_system/ui/)

- `dashboard_server.py`: HTTP server with 15 REST endpoints (/health, /accounts, /positions, /strategies, /approvals, /performance, /evaluations/price/{instrument}, /research/hypotheses, /market/regime, /signals/opportunities, /signals/feed, /strategies/performance, /strategies/rebalance, /strategies/rebalance/presets, /strategies/stairstep)
- `dashboard.html`: Dark mode, collapsible cards, signal filtering (BUY/SELL/all), CSS shimmer loading skeletons, toast notifications, keyboard shortcuts (r=refresh, d=dark mode, ?=help), auto-refresh countdown bar, responsive two-column grid

## End-to-End Data Flow

```
Coinbase API/CLI ──→ MarketDataFetcher (backtester.py) ──┐
            ┌───── pair_discovery (400+ pairs) ─────┐     │
            │           ▼                            │     ▼
            │  rest_feed.fetch_candles_batch()       │
            │           ▼                            │
            │  SignalAggregator.scan_universe()      │
            │  (50 Rust strategies, backtest, trend) │
            │           ▼                            │
            │  UnifiedSignal ranked list             │
            └───── ─ ─ ─ ─ ─ ─ ─ ─ ─ ── ─ ─ ─ ─ ─ ┘
                                                      │
                                                      ▼
YFinance/Alpha Vantage ──→ data/unified_fetcher.py ──→ strategy_engine.py (74 strategies)
                                                          │
                                                          ▼
                                                  ConfidenceMatrix (group & weight)
                                                          │
                                                          ▼
                                                  ConfidenceEngine (8 modifiers)
                                                          │
                                                          ▼
                                                  portfolio_optimizer.py (10 detection dimensions)
                                                    TLH / universe / stock / fee-tier / rebalance /
                                                    strategy signals / volume cycles / accumulator /
                                                    **aggregator** / event markets
                                                          │
                                                          ▼
                                                  approval_server.py (email approve/deny)
                                                          │
                                                          ▼
                                                  coinbase/src/execution.py (place bracket order)
                                                          │
                                                          ▼
                                                  manage_brackets.py (poll stop/target/trail)
                                                          │
                                                          ▼
                                                  state_store.py (SQLite trades+snapshots)
                                                          │
                                                          ▼
                                                  dashboard_server.py (REST API) → dashboard.html
```

## Key Conventions & Gotchas

- **Python 3 only** — `python3` not `python`
- **pydantic not installed** — all `trading_system/core/models/domain.py` imports use try/except; keep guards
- **Coinbase CLI** — use `coinbase products candles` and `coinbase products ticker`, NOT `price` or `candles` subcommands (CLI version may differ from SDK)
- **BTC-ETH, BTC-SOL not valid Coinbase products** — these are ratio pairs returned as 404, handled gracefully in price provider
- **Strategy naming** — `strategy_engine.py` uses `on_bar()` method returning `Optional[Signal]`; `backtester.py` strategies use `generate_signals(data)` returning `List[Tuple[str, float]]`; two different interfaces
- **Stateful strategies** — warmed up by iterating all historical bars in `run_strategies` before the final signal call
- **Inspecting history** — use `git log --oneline -20` and `git diff` before making changes; never commit unless explicitly asked
- **Test runner** — `run_all_tests.sh` runs `python3 test_paper_trading_system.py` and `python3 test_unified_signal_accumulator.py`. The full coverage suite uses **pytest** (installed, v9.1.1) via `coverage run -m pytest tests/coverage/<dir>`; `tests/coverage/` holds per-module suites. Python 3 venv at `.venv/bin/python`.
- **Mode defaults** — always `--dry-run` unless `--live` explicitly passed; `MODE=mock` in .env, `PAPER_TRADING=true`
- **Coinbase trader health** — use `python3 coinbase/src/run_trader_v2.py --mode paper --health-port 9090` to run the hardened loop with a local health endpoint
- **Live mode validation** — `run_trader_v2.py` refuses non-paper startup if the Coinbase CLI is missing
- **Ranking persistence** — `data/ranking_state.json` is created automatically; do not hand-edit it unless you know the schema

## Symbol Universe

All trading pairs defined in `graph-alpha-bot/app/strategies/coinbase_universe.py` (single source of truth). **34 real Coinbase spot pairs.**

| Tier | Pairs | Count |
|------|-------|-------|
| Core (safe) | BTC-USD, ETH-USD, SOL-USD | 3 |
| Growth | XRP, ADA, DOGE, AVAX, DOT, LINK, UNI, POL, ATOM, LTC, BCH, NEAR, APT, SUI, ARB, OP, FIL, INJ, SEI, TIA | 20 |
| Speculative | ALGO, XLM, STX, HBAR, ICP, GRT, SHIB, PEPE, BONK, TRUMP, FLOKI | 11 |

**Removed:** BTC-XXX ratio pairs (never real products), MATIC→POL (migrated), delisted FTM/TRX.

## How to Run

```bash
# Portfolio optimizer (daemon)
python3 portfolio_optimizer.py --dry-run

# Approval server
python3 approval_server.py

# Dashboard UI
python3 trading_system/ui/dashboard_server.py
# → open http://localhost:8080

# Benchmark all strategies on BTC-USD
python3 backtester.py

# Paper trading
python3 paper_trading_system.py

# Unified signal generator
python3 graph-alpha-bot/app/strategies/unified_signal_generator.py

# Tests
bash run_all_tests.sh
python3 -m unittest test_paper_trading_system
python3 -m unittest test_unified_signal_accumulator
```

## Restart / Redeploy

```bash
# Check current state
python3 run_production.py status

# Restart everything under systemd (preferred after code/UI changes)
sudo systemctl restart portfolio-trader.service

# Stop all managed processes
sudo systemctl stop portfolio-trader.service

# Start the full stack again
sudo systemctl start portfolio-trader.service
```

Recommended order for a code/UI redeploy:
1. Run the relevant compile checks or tests.
2. Update the dashboard/UI if new API fields were added.
3. Restart the supervisor with `sudo systemctl restart portfolio-trader.service`.
4. Confirm `run_production.py status` shows all children healthy.

Notes:
1. `run_production.py start` is systemd-aware and stays in the foreground when invoked by the unit.
2. `deploy/portfolio-trader.service` is the canonical unit definition for future installs.
3. If the installed unit drifts, copy the repo unit into `/etc/systemd/system/portfolio-trader.service` and run `sudo systemctl daemon-reload`.

## Config Files

| File | Purpose |
|------|---------|
| `coinbase/src/config.py` | Pydantic Settings: products, risk per trade (1%), bracket params, Kelly caps, bandit algo, tcost |
| `config.yaml` | Data source priority (yfinance → alphavantage → default), rate limits |
| `.env` | Live trading safety controls (KILL_SWITCH, MAX_NOTIONAL, MIN_EDGE, REQUIRE_APPROVAL) |
| `trading_system/configs/*.yaml` | 24 pre-defined operational modes (paper, live-approval, shadow, canary, aggressive, market-making, accumulation) |

## Strategy Count

- **strategy_engine.py / rust_core**: 74+ strategies across 12 independence groups
- **backtester.py**: 14+ strategies (9 core + 6 novel)
- **graph-alpha-bot**: ~30 graph-based strategies
- **multi_strategy_paper_trading.py**: 6 strategies (momentum, mean-reversion, RSI, breakout, volatility, scalping)
- **External-data custom**: 3 strategies (funding_contrarian, exchange_flow, btc_dxy_corr)
- **Total**: ~120+ strategies across all engines (some overlap)

## All-Rust Signal Pipeline (74 strategies)

All 50 technical strategies run in native Rust via `evaluate_all_py()` / `evaluate_all_opens_py()`. The old NumPy/GPU batch path (5 strategies) and Python `run_strategies()` are fallbacks only.

### Rust Strategy Modules

| File | Contents |
|------|----------|
| `rust_core/src/indicators.rs` | 20+ indicators: SMA, EMA, EMA slice, WMA, WMA slice, RSI, Bollinger, Z-score, MACD, TRIX, ATR, highest, lowest, index_of_highest/lowest, OBV series, VPT series, SAR series, Wilder smooth, `ema_last_two()` |
| `rust_core/src/strategies.rs` | All 25 strategy functions + `evaluate()` (single) + `evaluate_all()` (batch). Crossover detection computed from data slices (no state tracking). |
| `rust_core/src/backtest.rs` | Walk-forward backtester for all 25 strategies. Symmetric entry/exit on opposite signals. |
| `rust_core/src/confidence.rs` | Confidence Matrix — multi-strategy signal aggregation with independence groups, backtest weighting, asset-class boosts. Mirrors `confidence_matrix.py`. |
| `rust_core/src/streaming.rs` | Streaming (incremental) indicators — RingBuffer, StreamingIndicators (EMA/SMA/RSI/MACD), StreamingEngine. O(1) per-tick. Mirrors `trading_system/core/streaming.py`. |
| `rust_core/src/regime.rs` | Regime detection — ADX, trend strength, volatility, Hurst exponent, skewness, kurtosis, price position, serial correlation. 8 regime classifier. Mirrors `coinbase/src/regime.py`. |
| `rust_core/src/lib.rs` | PyO3 bindings: `evaluate_all_py()`, `run_strategy_py()`, `backtest_strategy_py()`, `backtest_multi_py()`, `confidence_aggregate_py()`, `detect_regime_py()`, `PyStreamingEngine`, `PyStreamingIndicators`, indicator wrappers |

### All 74 Strategies

| # | Name | Type | Data Req'd | Logic |
|---|------|------|-----------|-------|
| 1 | `ema_cross` | Trend | closes | EMA(9) × EMA(21) crossover |
| 2 | `rsi_revert` | Momentum | closes | RSI(14) < 30 or > 70 |
| 3 | `boll_break` | Volatility | closes | Price × Bollinger(20,2) |
| 4 | `zscore_revert` | Momentum | closes | Z-score(30) < -2 or > 2 |
| 5 | `vol_mom` | Volume | closes, volumes | Vol > 1.5× avg + price Δ > 5% |
| 6 | `macd` | Trend | closes | MACD(12,26,9) histogram × 0 |
| 7 | `vwap_revert` | Volatility | closes, volumes | Price 3% from VWAP |
| 8 | `obv_div` | Volume | closes, volumes | Price/OBV divergence over 14 bars |
| 9 | `cmo` | Momentum | closes | CMO(14) < -50 or > 50 |
| 10 | `trix` | Trend | closes | Triple EMA(15) × 0 |
| 11 | `adx` | Trend | closes, highs, lows | ADX(14) > 25 + DI crossover |
| 12 | `keltner` | Volatility | closes, highs, lows | EMA(20) ± ATR(14)×2 |
| 13 | `chaikin_mf` | Volume | closes, volumes, highs, lows | CMF(21) > 0.1 or < -0.1 |
| 14 | `williams_r` | Momentum | closes, highs, lows | %R(14) < -80 or > -20 |
| 15 | `psar` | Trend | closes, highs, lows | Parabolic SAR flip (af=0.02) |
| 16 | `hma` | Trend | closes | HMA(9) × HMA(21) crossover |
| 17 | `force_idx` | Volume | closes, volumes | Force Index(13) × 0 |
| 18 | `vpt` | Volume | closes, volumes | VPT × EMA(21) crossover |
| 19 | `donchian` | Volatility | closes, highs, lows | 20-bar channel breakout |
| 20 | `aroon` | Trend | closes, highs, lows | Aroon(25) oscillator × 50 |
| 21 | `price_eff` | Volume | closes, volumes | Efficiency WMA(7) × 0.8 |
| 22 | `scci` | Momentum | closes | sCCI(28) < -30 or > 30 |
| 23 | `range_exp_idx` | Volatility | closes, highs, lows | REI(21) × 0 |
| 24 | `ema_dev` | Trend | closes | EMA(14) deviation × 0 |
| 25 | `snr_idx` | Momentum | closes | Signal-to-Noise ratio > 1.0 |
| 26 | `candle_pat` | Pattern | closes, opens, highs, lows | Engulfing, doji, hammer + volume confirm |
| 27 | `sup_res` | Pattern | closes, opens, highs, lows | Swing-based S/R bounce signal |
| 28 | `liq_vac` | Pattern | closes, opens, highs, lows, volumes | ATR breakout after tight range |
| 29 | `cvd_flow` | Momentum | closes, opens, highs, lows, volumes | CVD divergence vs price |
| 30 | `vcp` | Volatility | closes, opens, highs, lows | Volatility compression pattern |
| 31 | `impulse_exh` | Pattern | closes, opens, highs, lows, volumes | After 3+ strong bars + fading volume |
| 32 | `mom_accel` | Momentum | closes, volumes | ROC(5) acceleration > threshold |
| 33 | `rsi_fail` | Momentum | closes | RSI failure swing (M/W top/bottom) |
| 34 | `avwap` | Pattern | closes, opens, highs, lows, volumes | VWAP bounces through volume profile |
| 35 | `donch_pull` | Pattern | closes, opens, highs, lows | Pullback to Donchian mean after breakout |
| 36 | `vol_prof` | Volume | closes, opens, highs, lows, volumes | Volume profile 70% value area at S/R |
| 37 | `bb_squeeze` | Volatility | closes, opens, highs, lows | Bollinger band squeeze + ATR pop |
| 38 | `multi_rsi` | Momentum | closes | RSI(7) × RSI(21) crossover |
| 39 | `linreg_slope` | Momentum | closes, volumes | Linreg slope > 0.1 with volume |
| 40 | `hurst` | Momentum | closes, opens, volumes | Hurst < 0.5 mean-reversion, > 0.5 trend |
| 41 | `elder_ray` | Trend | closes, highs, lows | Bull/Bear Power vs EMA(13) |
| 42 | `klinger` | Volume | closes, volumes, highs, lows | VF MA(13) × MA(55) crossover |
| 43 | `pivot_points` | Pattern | closes, highs, lows | Swing high/low breakout |
| 44 | `ichimoku` | Trend | closes, highs, lows | Conversion(9) × Base(26) cross |
| 45 | `choppiness` | Volatility | closes, highs, lows | CI > 60 range / < 30 trend |
| 46 | `true_cci` | Momentum | closes, highs, lows | CCI(20) < -100 or > 100 |
| 47 | `dpo` | Trend | closes | DPO(20) × 0 cross |
| 48 | `kst` | Momentum | closes | 4-period summed ROC MA cross |
| 49 | `mass_idx` | Volatility | closes, highs, lows | MI > 27 reversal after extension |
| 50 | `ulcer` | Momentum | closes | UI drawdown recovery / risk rise |
| ... | (51-72 updated previously) | | | |
| 73 | `kalman_mr` | Momentum | closes | Kalman filter adaptive mean reversion; 1D state-space model, trade > 2σ deviation |
| 74 | `hp_trend` | Trend | closes | HP filter trend-cycle decomposition; trade cycle extremes & zero-crossings |

### Performance

| Operation | Latency | Speedup vs Old |
|-----------|---------|----------------|
| 1 product × 50 strategies | **0.04ms** | 25× (was 1ms for 5 strat NumPy) |
| 5 products × 50 strategies | **0.2ms** | 80× (was 16ms) |
| 34 products × 50 strategies | **~1.4ms** | projected |
| All 50 backtests (rayon) | **~2ms** | similar |
| **Full tick (34 prods)** | **~8ms** | — |
| **WebSocket → eval** | **~34ms** | 26ms network + 8ms compute |

### Architecture

```
WebSocket (~1s ticker)
  → StreamingIndicators.update()          O(1) per tick
    → _evaluate(product_id)               on price change > 0.05%
      → rust_core.evaluate_all_py()       0.04ms per product
      → batch_backtest_rust()             ~2ms rayon-parallel
      → opportunity ranking               confidence × win-rate × sharpe
                                          (regime-weighted + correlation-penalized)
```

### Key Files

- `strategy_engine.py` — `batch_signals_rust()` (prefers Rust), `batch_signals_fast()` (NumPy fallback), `_RUST_STRATEGIES` (all 50), `batch_backtest_rust()`
- `coinbase/src/sentiment/crypto_news_sentiment.py` — `CryptoNewsSentiment` fetches 6 crypto RSS feeds (CoinDesk, CoinTelegraph, CryptoSlate, The Block, Decrypt, Bitcoin Magazine), maps articles to product IDs via `KNOWN_CRYPTO_SYMBOLS`, scores sentiment with expanded keyword lists (+90 positive, +60 negative), generates BUY/SELL signals with confidence. Integrates as background thread in `run_trader_v4.py`, feeds opportunities via `_paper_execute()`, appears in `health_status["news_sentiment"]`. 5-min TTL in-memory cache.
- `coinbase/src/run_trader_v4.py` — `EventTraderV4` using `rust_core.evaluate_all_py()` as primary path. Structured logging with per-strategy counters, best-opp summary, periodic stats dump every 50 ticks.
  - **Paper trader config** (all tunable via CLI):
    - `paper_min_confidence=0.55` — minimum signal confidence (was 0.40)
    - `paper_min_edge_bps=15.0` — minimum net edge in bps after fees (was 5.0)
    - `paper_min_trade_usd=100` — minimum trade notional (was $25)
    - `paper_min_win_rate=0.60` — minimum strategy backtest win rate (was 0.55)
    - `paper_min_sharpe=0.8` — minimum Sharpe for strategy entry (was 0.5)
    - `paper_max_new_positions=12` — max concurrent open positions (was 30)
    - `paper_product_cooldown_s=1800` — per-product cooldown in seconds (was 900)
    - `paper_maker_pct=0.50` — fraction of orders simulated as maker (limit) vs taker
  - **Dynamic fee model**: Uses `FEE_TIERS` matching Coinbase Advanced fee schedule. `_fee_tier()` returns (tier, taker_bps, maker_bps) based on `paper_trailing_volume_30d`. Effective rate blended by `paper_maker_pct`. Persisted across restarts.
  - **Unknown-regime skip**: Products with `regime="unknown"` or `atr_14<=0` are skipped (insufficient data).
  - **Pulse-aware confidence gate**: Repeat signals on same product+strategy >3 pulses within 1800s get confidence halved.
  - **Health endpoint**: Exposes `fee_tier`, `trailing_volume_30d`, `taker_bps`, `maker_bps`, `effective_fee_bps`, `maker_pct`.
- `rust_core/src/` — Complete Rust implementation: indicators, strategies, backtest, confidence matrix, streaming, PyO3 bindings
- `trading_system/core/streaming.py` — StreamingIndicators (O(1) incremental)
- `trading_system/core/timing.py` — LatencyProfiler (per-stage p50/p95/p99)
- `trading_system/core/signal_aggregator.py` — UnifiedSignal dataclass, SignalAggregator with scan_universe(), _compute_trend() (50/200 SMA, ADX, volume), _compute_unified() (consensus × backtest_quality × trend × conviction)
- `trading_system/core/performance_model.py` — LatencyProfile, expected_round_trip_ms(), expected_fill_delay_ms(), expected_exit_delay_ms(), latency_tuned_priority()
- `coinbase/src/pair_discovery.py` — get_all_coinbase_pairs() (volume-filtered 400+ pairs), top_coinbase_pairs() (top N by volume)
- `coinbase/src/multi_hop.py` — route planner for direct and bridged conversion paths
- `coinbase/src/rest_feed.py` — fetch_candles_rest() (urllib3 keep-alive), fetch_candles_batch() (parallel, max_workers)
- `strategy_engine.py`: `FundingRateContrarian`, `ExchangeFlowSignal`, `BTCDXYCorrelation` — 3 Python
  external-data strategies (funding rates, CoinGecko flows, Yahoo Finance macro)

## Coverage Campaign & Gate

Per-module **line + branch coverage gate (default 90%)** enforced by `scripts/coverage_gate.py` across every Python source module in the manifest (`scripts/gen_manifest.py` → `scripts/coverage/python_manifest.txt`). Node (runnable `.mjs` sources) and Rust (`rust_core`, via `cargo-llvm-cov`) are gated by the same tool with `--lang node` / `--lang rust`. A module passes only when BOTH its line% and branch% meet the threshold.

Tooling (under `scripts/coverage/`):
- `coverage_gate.py` — loads the coverage JSON + manifest, prints per-module PASS/FAIL.
- `gen_manifest.py` — emits the manifest of every importable source `.py` (excludes tests, venvs, broken files).
- `python_coverage.json` — combined `coverage.py` JSON. Regenerate per-dir (NOT a single `coverage run --source=.` over the whole tree) to avoid the cross-dir under-report artifact and the 259-file collection error.
- `node_manifest.txt`/`node_cov.txt`, `rust_manifest.txt`/`rust_cov.json` — Node/Rust gate data.

Workflow:
```bash
# regenerate (exclude the 3 known hang-files: coinbase/test_config_manager.py,
# coinbase/test_smart_feed.py, optimizer/test_portfolio_optimizer_full.py)
for d in tests/coverage/*/; do
  .venv/bin/python -m coverage run --append --source=. -m pytest "$d"
done
.venv/bin/python -m coverage json -o scripts/coverage/python_coverage.json
.venv/bin/python scripts/coverage_gate.py --lang python \
  --manifest scripts/coverage/python_manifest.txt --data scripts/coverage/python_coverage.json
```
Measure a single module accurately with the dotted-module `--source` form (the file-path form is unsupported in coverage 7.15.1):
`.venv/bin/python -m coverage run --source=portfolio_optimizer -m pytest tests/coverage/optimizer/ -q && .venv/bin/python -m coverage report`

Status (this campaign): the rebalancer / portfolio-management execution cluster is at 100% (`rebalance_engine.py`, `brokers/*`, `risk/engine`, `risk/auto_approval/rules_engine`, `execution/hybrid/*`, `maker_engine/engine`). `portfolio_optimizer.py` is ~76% line / ~82% branch and `coinbase/src/run_trader_v4.py` ~75% line — both are large legacy files still below the 90% gate.

## Critical Safety Rules

1. Never commit .env files, private keys, or secrets
2. `KILL_SWITCH=true` in .env disables all live trading regardless of --live flag
3. Always test with `--dry-run` first
4. `REQUIRE_MANUAL_APPROVAL=true` adds human-in-the-loop gate for live trades
5. `MAX_NOTIONAL_PER_TRADE_USD=10` caps single trade size by default
6. `manage_brackets` loops indefinitely; exit via `KeyboardInterrupt` or `poll_secs=0`
7. `data.py` has in-memory cache — call `invalidate_cache()` after new candles arrive
