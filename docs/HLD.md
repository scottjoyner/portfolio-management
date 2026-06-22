# High-Level Design — Portfolio Management System

## 1. Purpose

A multi-strategy crypto trading and portfolio management platform supporting live paper trading, backtesting, fee-tier optimization, and tax-loss harvesting via Coinbase (primary) with connectors to Alpaca, Binance, Kraken, Polymarket, and Kalshi.

---

## 2. System Components

### 2.1 Core Services

```
┌───────────────────────────────────────────────────────────────┐
│                    Portfolio Optimizer                         │
│                  (portfolio_optimizer.py)                      │
│   Continuous daemon: fetch prices → run detectors → execute    │
│   Strategies: TLH, fee-tier volume, rebalancing, signal-driven │
└────────────────────────────┬──────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
  ┌─────────────┐    ┌──────────────┐   ┌────────────────┐
  │ Strategy     │    │ Multi-Strat  │   │ Paper Broker   │
  │ Engine       │    │ Paper Trade  │   │ (paper_broker) │
  │ (strategy_   │    │ (multi_      │   │ Signal→order   │
  │ engine.py)   │    │ strategy_    │   │ → simulated    │
  │              │    │ trading.py)  │   │ execution      │
  └─────────────┘    └──────────────┘   └────────────────┘
         ▲                   ▲
         │                   │
  ┌──────────────────────────┴──────────────────────────┐
  │              Exchange Connectors                     │
  │ CoinbaseV3 · Alpaca · Binance · Kraken · Polymarket  │
  │ Kalshi · Coinboard (OAuth)                           │
  └─────────────────────────────────────────────────────┘
```

### 2.2 Persistence Layer

| Store | Purpose | Technology |
|-------|---------|------------|
| StateStore | Trades, snapshots, backtest cache, position ages | SQLite WAL mode |
| Neo4jStore | Graph analytics: trades ↔ market data joins | Neo4j (bolt://) |
| pending_approvals.json | Approval workflow state | JSON file |

### 2.3 Node.js Services (apps/)

| Service | Role | Port |
|---------|------|------|
| apps/api | REST API — auth, operator flows, opportunities, metrics | configurable |
| apps/web | React UI — dashboard | configurable |
| apps/cli | CLI tool for execution and diagnostics | N/A |
| apps/worker | Background worker processes | N/A |

### 2.4 Backtesting Engine

```
trading_system/backtesters/     ← Strategy runner (Python)
coinbase/src/backtest/new_strategies.py  ← Strategy implementations
coinbase/src/backtest/run_backtest.py    ← Backtest harness + mock data gen
historical_backtest.py                ← Historical CSV-based backtests
backtest_v2.py                        ← Standalone backtester v2
```

### 2.5 Analytics & Storage

- `neo4j_store.py` — Neo4j graph store for cross-system analytics (optimizer trades ↔ market data)
- `state_store.py` — SQLite state store with WAL journaling, thread-safe
- `portfolio_analyzer.py` — One-shot portfolio analysis and metrics
- `confidence_matrix.py` — Confidence scoring across strategies

---

## 3. Data Flow

```
Coinbase API (public + auth)
    ↓ price feeds + balances + fee tier info
Portfolio Optimizer Daemon
    ├── Fetches live data (interval default: 300s)
    ├── Runs detectors in priority order:
    │   1. Tax-Loss Harvest (>5% unrealized loss)
    │   2. Fee Tier Volume (reach next tier)
    │   3. Rebalancing (drift >5% from target)
    │   4. Strategy Signals (EMA, RSI, Bollinger, Z-score, Volume)
    ├── Previews order via Coinbase CLI dry-run
    ├── If --require-approval: saves to pending_approvals.json + sends email
    └── Executes trade → persists to SQLite + Neo4j

Multi-Strategy Paper Trading
    ↓ runs independently or alongside optimizer
    Fetches ALL USD pairs → scores by risk-adjusted opp → ranks cross-market
    Fee-tier boost for trades that advance Coinbase fee tier
    Sends approval emails when configured
```

---

## 4. Key Design Decisions

### 4.1 Dual-Store Architecture (SQLite + Neo4j)

**Decision:** Persist trade state to both SQLite (local, fast, durable) and Neo4j (analytics graph).

**Rationale:**
- SQLite for the optimizer's operational needs: survives restarts, low latency, thread-safe via WAL mode.
- Neo4j for analytics: joins optimizer trades with market data from `graph-alpha-bot`, enabling queries like "find all sell-offs that preceded price recoveries."
- Neo4j is optional; system runs without it (graceful degradation).

### 4.2 Coinbase as Primary Exchange

**Decision:** Coinbase CLI is the primary exchange interaction layer, not a Python SDK.

**Rationale:**
- Coinbase CLI handles auth natively (OAuth tokens in `~/.cb_sdk_env/`).
- Provides preview mode for safe dry-run order validation before execution.
- Python wrapper calls it as subprocess; no direct API key management needed.

### 4.3 Strategy Engine Architecture

Five strategies feed into the optimizer's opportunity detectors:

| Strategy | Type | Signal Pattern |
|----------|------|----------------|
| EMA Crossover | Trend following | BUY when short EMA crosses above long EMA |
| RSI Reversal | Mean reversion | BUY when RSI < 30, SELL when > 70 |
| Bollinger Breakout | Volatility expansion | BUY when price breaks upper band |
| Z-Score Statistical Arb | Cointegration | BUY/SELL based on spread deviation |
| Volume Surge | Momentum | BUY when volume exceeds N-sigma threshold |

Each returns `Signal(action, price, confidence, reason)` objects.

### 4.4 Fee Tier Optimization

Coinbase has a rolling 30-day fee tier system (7 tiers from 0.6%/1.2% down to 0.05%/0.15%). The optimizer explicitly scores trades for their contribution toward reaching the next tier, boosting otherwise marginal opportunities. This is tracked via `FeeTierManager` with rolling volume counters and pruning of old trade records.

### 4.5 Paper Trading vs Live Execution Separation

- `paper_broker.py` executes signals against simulated data (no exchange interaction)
- `multi_strategy_paper_trading.py` orchestrates paper trades with cross-market scoring
- `portfolio_optimizer.py --live` executes real Coinbase orders — **always requires explicit flag**
- All live orders go through preview first (CLI dry-run)

### 4.6 Approval Workflow

When `--require-approval` is enabled:
1. Trade is not executed; state saved to `pending_approvals.json`
2. Email sent with approve/deny links via SMTP
3. `approval_server.py` listens for approval actions (optional HTTP handler)
4. Approver clicks → trade executes or is denied

---

## 5. Deployment Model

### 5.1 Docker Compose

```yaml
# docker-compose.yml — base services
# docker-compose.trading.yml — trading-specific overrides

Services:
  api        — apps/api (Node/TypeScript)
  web        — apps/web (React UI)
  worker     — apps/worker (background jobs)
  neo4j      — graph database on 100.64.43.123:7687
```

### 5.2 Local Development

All Python code runs in `.venv` with requirements.txt dependencies. Node modules under `node_modules/`. No Docker required for local dev.

---

## 6. Security & Risk Controls

- **Circuit breaker:** 5 consecutive failures → 10 min cooldown (configurable per component)
- **Position sizing:** Max 10% of capital per trade in paper trading
- **Live trades require `--live` flag** — never executed by default
- **Preview before execute:** All Coinbase orders dry-run first
- **Cooldowns by strategy type:** TLH=24h, fee-tier=1h, rebalance=12h, strategy-signal=5min, cycle=10min
- **Null-safe price handling:** `bar.close or 0.0` pattern throughout

---

## 7. Open Questions / Decisions Pending

### 7.1 API Ownership (Node vs Python)

Current architecture has Node (`apps/api`) and Python (`trading_system/`) as separate codebases with unclear service boundaries. Decision needed:
- Option A: FastAPI in Python as primary product API
- Option B: Keep Node API, call Python over RPC
- Option C: Two-service architecture with explicit ownership

### 7.2 Knowledge Vault Storage

`~/knowledge/ssd_mount/` → `/media/scott/SSD_4TB/knowledge` (broken — symlink exists but mount point is empty). SMB/CIFS mount not configured on this host. Docs should live in the git repo under `docs/`.

---

## 8. Document History

| Date | Author | Change |
|------|--------|--------|
| 2026-06-18 | xwing | Initial HLD based on current codebase audit |
