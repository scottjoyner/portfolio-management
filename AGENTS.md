# Portfolio Management System — AGENTS.md

## Repository Overview

Multi-module algorithmic trading platform spanning Python (primary) and Node.js/TypeScript (prediction markets). Integrates with Coinbase CDP v3 (primary broker), Kalshi, Polymarket, and includes a full trading pipeline: market data → signal generation → confidence scoring → portfolio optimization → approval workflow → execution → bracket management → state persistence → UI dashboard.

## Directory Structure

| Path | Purpose |
|------|---------|
| `coinbase/src/` | **Execution engine**: `cb_client.py` (RESTClient wrapper), `execution.py` (order placement + bracket management + trailing stop + poll loop), `data.py` (candle fetching + cache), `config.py` (pydantic Settings), `bandit.py` (UCB1 / Thompson), `tcost.py` (transaction cost), `bridge_execution.py` (Node subprocess bridge), `run_trader.py` (main trading loop — rebalance, rr-trades, manage-brackets), `run_trader_v2.py` (hardened unified trader with startup validation, graceful shutdown, health endpoint, ranking persistence) |
| `trading_system/` | Core domain: `core/portfolio_manager.py`, `core/state_manager.py`, `core/models/`, `ui/dashboard_server.py` (12 REST endpoints), `ui/dashboard.html`, `signal_confidence.py` (ConfidenceEngine with 8 modifiers) |
| `graph-alpha-bot/` | Graph-based AI trading bot: ~30 strategies using Neo4j knowledge graph, news ingestion pipeline, MCP server |
| `backtester.py` | 14+ Coinbase-specific strategies + `MarketDataFetcher` + `Backtester` + benchmark runner |
| `strategy_engine.py` | 25 general strategies + `run_strategies()` + `backtest_strategy()` with pass/fail verdict |
| `portfolio_optimizer.py` | Continuous daemon: TLH detection, fee-tier optimization, rebalancing (80/15/5), strategy signal integration + **unified signal accumulator integration** (news, PM, arb, divergence), uses ConfidenceMatrix + ConfidenceEngine |
| `portfolio_manager.py` | Backtesting framework: Position/Portfolio classes, run strategies on historical CSV data |
| `approval_server.py` | Lightweight HTTP server for human-in-the-loop trade approval via email approve/deny links |
| `state_store.py` | Thread-safe SQLite persistence: trades, snapshots, bt_cache, position_ages, meta tables |
| `confidence_matrix.py` | Multi-strategy aggregation: 4 independence groups (trend/momentum/volatility/volume), weighted by backtest perf, group-agreement boosting |
| `data/` | Market data CSV files + fetchers (yfinance, Alpha Vantage, Coinbase, unified fetcher) |
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

**`strategy_engine.py: KalshiSignal + PolymarketSignal`** — Strategy classes that implement `on_bar()` (matching the 25-strategy interface) but fetch live prediction market data via the unified client. Registered in `ALL_STRATEGIES` as `"kalshi"` and `"polymarket"`, mapped to `CLASS_STRATEGIES` for growth/speculative asset classes.

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

**`strategy_engine.py`** — 25 strategies across 4 rounds/families:
- Trend: EMA_Crossover, MACD, TRIX, ADX, ParabolicSAR, HullMA, Aroon
- Momentum: RSI_MeanReversion, ChandeMomentum, WilliamsR, ZScoreReversion, ForceIndex
- Volatility: BollingerBreakout, VWAP_Reversion, KeltnerChannels, DonchianChannels
- Volume: VolumeMomentum, OBV_Divergence, ChaikinMoneyFlow, VolumePriceTrend
- Round 3: PriceEfficiencyRatio, SimplifiedCCI, RangeExpansionIndex, EMADeviation, SignalToNoiseRatio
- `run_strategies(currency, asset_class, closes, volumes, current_price, highs, lows)` filters by asset class (safe=5, growth=25, speculative=22), returns `List[Signal]` sorted by confidence.
- `backtest_strategy(closes, ...)` walks OHLCV forward, returns `BacktestVerdict` (win_rate, sharpe, profit_factor, max_drawdown, passed bool). Passes if win_rate>=0.4 AND sharpe>0.2 AND profit_factor>1.05 AND total_return>-20%.

**`backtester.py`** — 14+ Coinbase-only strategies (MA_Crossover, RSI, BollingerBands, DonchianChannel, TrendFollowing, BTCVolatilityStacking, BTCVolatilityBreakout, BTCVolatilityMeanReversion, BTCVolatilityMomentum, CoinbaseMomentumStrategy, CoinbaseMeanReversionStrategy, VolatilityBreakoutStrategy, RegimeAwareAdaptiveStrategy + 6 novel: VolumeProfileStrategy, MultiTimeframeConfluenceStrategy, OrderFlowPressureStrategy, VolatilityContractionExpansionStrategy, StatisticalArbitrageZScorePairStrategy, LiquidationHeatmapStrategy). Uses `MarketDataFetcher` (wraps `coinbase products candles` CLI command). Run benchmark with `run_benchmark("BTC-USD")`.

**`graph-alpha-bot/`** — Graph-based strategies (MA crossover on graph, news centrality momentum, supply chain shock diffusion, insider cluster drift).

### Confidence Scoring Pipeline

```
Raw Signal → ConfidenceMatrix (confidence_matrix.py)
               Groups strategies into 4 independence families:
               trend, momentum, volatility, volume
               Weights by backtest cache performance
               +15% per additional agreeing group
               1.1x for 3+ strategies

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

Continuous daemon with 5 detection dimensions:
1. **TLH** — sell positions with unrealized loss > 5% for 20% tax savings
2. **Fee-Tier Volume** — generate volume to reach lower Coinbase fee tiers (7 tiers, 0.6%/1.2% → 0.05%/0.15%)
3. **Rebalancing** — maintain target allocation (safe=80%, growth=15%, speculative=5%)
4. **Strategy Signals** — run all 25 strategies, apply backtest validation + ConfidenceMatrix + ConfidenceEngine
5. **Unified Signal Accumulator** — integrates news, PM, arb, divergence signals from the accumulator, mapped to `OpportunityType.ACCUMULATOR_SIGNAL`

Opportunities ranked by `priority * confidence * boost`, max 10 positions, 20% risk per position.

Detection order per tick: TLH → coinbase universe → stock → fee tier → rebalance → strategy signals → volume cycles → **accumulator signals** → event markets.

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

- `dashboard_server.py`: HTTP server with 12 REST endpoints (/health, /accounts, /positions, /strategies, /approvals, /performance, /evaluations/price/{instrument}, /research/hypotheses, /market/regime, /signals/opportunities, /signals/feed, /strategies/performance)
- `dashboard.html`: Dark mode, collapsible cards, signal filtering (BUY/SELL/all), CSS shimmer loading skeletons, toast notifications, keyboard shortcuts (r=refresh, d=dark mode, ?=help), auto-refresh countdown bar, responsive two-column grid

## End-to-End Data Flow

```
Coinbase API/CLI ──→ MarketDataFetcher (backtester.py) ──┐
                                                          ▼
YFinance/Alpha Vantage ──→ data/unified_fetcher.py ──→ strategy_engine.py (25 strategies)
                                                          │
                                                          ▼
                                                  ConfidenceMatrix (group & weight)
                                                          │
                                                          ▼
                                                  ConfidenceEngine (8 modifiers)
                                                          │
                                                          ▼
                                                  portfolio_optimizer.py (TLH/fee/rebalance/signals)
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
- **Test runner** — `run_all_tests.sh` runs `python3 test_paper_trading_system.py` and `python3 test_unified_signal_accumulator.py`; no pytest installed, tests use `unittest`
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

## Config Files

| File | Purpose |
|------|---------|
| `coinbase/src/config.py` | Pydantic Settings: products, risk per trade (1%), bracket params, Kelly caps, bandit algo, tcost |
| `config.yaml` | Data source priority (yfinance → alphavantage → default), rate limits |
| `.env` | Live trading safety controls (KILL_SWITCH, MAX_NOTIONAL, MIN_EDGE, REQUIRE_APPROVAL) |
| `trading_system/configs/*.yaml` | 24 pre-defined operational modes (paper, live-approval, shadow, canary, aggressive, market-making, accumulation) |

## Strategy Count

- **strategy_engine.py**: 25 strategies
- **backtester.py**: 14+ strategies (9 core + 6 novel)
- **graph-alpha-bot**: ~30 graph-based strategies
- **multi_strategy_paper_trading.py**: 6 strategies (momentum, mean-reversion, RSI, breakout, volatility, scalping)
- **Total**: ~75+ strategies across all engines (some overlap)

## Critical Safety Rules

1. Never commit .env files, private keys, or secrets
2. `KILL_SWITCH=true` in .env disables all live trading regardless of --live flag
3. Always test with `--dry-run` first
4. `REQUIRE_MANUAL_APPROVAL=true` adds human-in-the-loop gate for live trades
5. `MAX_NOTIONAL_PER_TRADE_USD=10` caps single trade size by default
6. `manage_brackets` loops indefinitely; exit via `KeyboardInterrupt` or `poll_secs=0`
7. `data.py` has in-memory cache — call `invalidate_cache()` after new candles arrive
