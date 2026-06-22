# Low-Level Design — Portfolio Management System

## 1. Strategy Engine (`strategy_engine.py`)

### Class: Signal

```python
@dataclass
class Signal:
    action: str        # "BUY" | "SELL" | "HOLD"
    price: float       # execution price
    confidence: float  # 0.0 - 1.0
    reason: str        # human-readable explanation
    strategy: str      # source strategy name (e.g., "ema_crossover")
```

### Helper Functions

All helpers accept `List[float]` and return a single float. All include null-safe fallbacks (return last value or 0.0 if list is empty).

| Function | Purpose | Formula |
|----------|---------|---------|
| `_sma(values, period)` | Simple Moving Average | mean of last N values |
| `_ema(values, period)` | Exponential MA | k = 2/(period+1), exponential decay |
| `_rsi(values, period=14)` | RSI (14-period) | RS = avg_gain/avg_loss; RSI = 100 - 100/(1+RS) |

### Strategy Implementations

Each strategy is a class inheriting from the base pattern. The engine runs all five and aggregates their signals:

| # | Class Name | Type | Signal Logic |
|---|-----------|------|--------------|
| 1 | `EMACrossoverStrategy` | Trend following | BUY when short EMA crosses above long; SELL on cross below |
| 2 | `RSIReversalStrategy` | Mean reversion | BUY when RSI < 30 (oversold); SELL when RSI > 70 (overbought) |
| 3 | `BollingerBreakoutStrategy` | Volatility breakout | BUY when price breaks above upper band; SELL on lower band break |
| 4 | `ZScoreArbStrategy` | Statistical arb | BUY/SELL based on rolling z-score of spread deviation |
| 5 | `VolumeSurgeStrategy` | Momentum | BUY when volume > N sigma above recent mean |

### Configuration

Default parameters per strategy are defined inline; override via constructor. Key defaults:
- EMA short=12, long=26 (standard MACD settings)
- RSI period=14, thresholds 30/70
- Bollinger window=20, mult=2.0
- Z-score window=20, threshold=2.0
- Volume surge: lookback=20, sigma_multiplier=2.5

---

## 2. Portfolio Optimizer (`portfolio_optimizer.py`)

### Main Daemon Loop

```
interval (default: 300s) → fetch data → run detectors → preview → approve/execute → persist
```

### Opportunity Detectors (priority order)

| Detector | Condition | Cooldown | Action |
|----------|-----------|----------|--------|
| Tax-Loss Harvest | unrealized loss > 5% per position | 24h | Sell losing positions, buy back in correlated asset |
| Fee Tier Volume | current tier < next tier + volume needed < threshold | 1h | Generate trading volume to advance fee tier |
| Rebalancing | allocation drift > 5% from target (80/15/5) | 12h | Sell overweight, buy underweight |
| Strategy Signals | any strategy confidence > threshold | 5min per signal, 10min per cycle | Execute trade if approved |

### CLI Flags

```bash
python3 portfolio_optimizer.py [options]

--interval <secs>     # daemon loop interval (default: 300)
--live                # execute real Coinbase orders (REQUIRED for live trading)
--dry-run             # preview only, no execution
--require-approval    # send email with approve/deny links before executing
--once                # single tick mode (debugging)
--summary             # print all trades in state store

# Neo4j analytics (optional)
--neo4j-uri <url>     # bolt://host:port
--neo4j-password <pwd>
--neo4j-db <name>     # default: trading

# Email approval workflow
--smtp-user <email>
--smtp-password <pass>
--approval-base-url <http://...>  # URL for approve/deny links
```

### State Persistence (`state_store.py`)

SQLite tables with WAL journaling (thread-safe):

| Table | Key Columns | Purpose |
|-------|-------------|---------|
| `trades` | id, timestamp, type, side, currency, size_usd, fee, reason, order_id, dry_run | Trade history |
| `snapshots` | id, timestamp, total_value, holding_count, usdc_balance, fee_volume_30d, fee_tier_* | Portfolio snapshots per cycle |
| `bt_cache` | key, verdict_json, created_at | Backtest result cache (TTL-based) |
| `position_ages` | currency (PK), age | Age tracking for TLH cooldowns |
| `meta` | key (PK), value | System metadata |

---

## 3. Multi-Strategy Paper Trading (`multi_strategy_paper_trading.py`)

### Fee Tier Table (hardcoded)

```python
COINBASE_FEE_TIERS = [
    FeeTier(0,       0.0060, 0.0120),   # Base: 0.6% maker / 1.2% taker
    FeeTier(1_000,   0.0035, 0.0075),
    FeeTier(10_000,  0.0025, 0.0040),
    FeeTier(50_000,  0.0015, 0.0025),
    FeeTier(100_000, 0.0010, 0.0020),
    FeeTier(1M,      0.0008, 0.0018),
    FeeTier(20M,     0.0005, 0.0015),
]
```

### Classes

| Class | Purpose |
|-------|---------|
| `FeeTier` | Dataclass: min_volume, maker_rate, taker_rate |
| `FeeTierManager` | Tracks rolling 30d volume via `_trades_30d` list; prunes old entries on each trade. Methods: `get_current_tier()`, `get_next_tier()`, `volume_to_next_tier()` |

### Orchestrator Flow

1. Fetch all Coinbase USD pairs concurrently (ThreadPoolExecutor)
2. Score each pair by risk-adjusted opportunity value
3. Rank across markets
4. Boost trades that advance fee tier
5. Execute paper trade or send approval email

---

## 4. Paper Broker (`paper_broker.py`)

### Data Classes

```python
@dataclass(slots=True)
class Candle:          # OHLCV single bar
    timestamp: str     # ISO-8601 or epoch string
    open_p, high_p, low_p, close_p: float
    volume: float
    @property mid -> (high+low)/2

@dataclass(slots=True)
class OrderBook:       # Bid/ask snapshot
    timestamp: str
    bids: list[(price_str, qty)]     # sorted ascending
    asks: list[(price_str, qty)]     # sorted descending

@dataclass(slots=True)
class MarketData:      # Time-step container
    timestamp: str
    candle: Candle | None
    orderbook: OrderBook | None
    @property mid_price -> weighted average or candle midpoint
    @property best_price -> close_p (candle) or mid_price (orderbook)
    @property volume

@dataclass(slots=True)
class Position:
    asset_id: str
    quantity: int = 0
    avg_cost: float = 0.0
    open_time: str | None
```

### Execution Logic

Paper broker accepts `Signal` objects and executes against simulated data (no exchange interaction). Maintains position tracking with PnL calculation from entry_price vs current price.

---

## 5. Exchange Connectors (`trading_system/connectors/`)

| Connector | File | Auth Method | Notes |
|-----------|------|-------------|-------|
| CoinbaseV3 | `coinbase_v3.py` | OAuth (CLI) | Primary exchange; uses system-installed coinbase CLI |
| Alpaca | `alpaca.py` / `alpaca_real.py` | API keys | Paper trading via sandbox mode |
| Binance | `binance.py` | API key/secret | Fallback connector |
| Kraken | `kraken.py` | API key | Fallback connector |
| Polymarket | `polymarket.py` | Auth token | Prediction market integration |
| Kalshi | `kalshi.py` | Auth token | Prediction market integration |
| Coinboard | `coinboard/rest/client.py` | OAuth | REST client with circuit breaker built in |
| Unified | `unified.py` | Factory pattern | Single entry point for all exchanges |

### Unified Execution Pattern (`trading_system/unified_execution/`)

```python
# adapters/mock.py    — paper trading (no exchange)
# adapters/coinbase.py — Coinbase V3 adapter
# interfaces.py       — BaseExchange interface with execute_order()
```

---

## 6. Persistence Layer

### StateStore (`state_store.py`)

- SQLite with WAL mode for concurrent reads/writes
- Thread-safe via `threading.Lock()` on schema init and write operations
- Path configurable (default: `optimizer_state.db` in CWD)
- Auto-creates tables on first use (`_init_schema()`)

### Neo4jStore (`neo4j_store.py`)

- Uses `neo4j.GraphDatabase.driver()` with connection pool size 10
- Creates database if not exists (`CREATE DATABASE IF NOT EXISTS`)
- Schema auto-initialized on connect
- URI, user, password configurable via constructor (env vars also supported)
- Production instance: `bolt://100.64.43.123:7687`, user=`neo4j`, db=`trading`

### Neo4j Graph Schema

Node types: Ticker, News, PriceData, Sentiment, Trade, Signal

Key relationships:
- `(Ticker)-[:MENTIONED_IN]->(News)` — ticker mentioned in news article
- `(Ticker)<-[PRICED_AT]-(PriceData)` — price data points for a ticker  
- `(Ticker)-[:SIGNAL_TYPE]->(Signal)` — signal generated for ticker
- `(Sentiment)-[:DERIVED_FROM]->(News)` — sentiment derived from news

Indexes: `TICKER.symbol`, `NEWS.timestamp`, `SIGNAL.timestamp`

---

## 7. Approval Workflow

### Files

| File | Role |
|------|------|
| `pending_approvals.json` | Shared approval state (file-based) |
| `approval_server.py` | HTTP endpoint for approve/deny actions |
| Email via SMTP | Sends approve/deny links to operator email |

### Flow

```
optimizer detects opportunity → --require-approval set
  → saves trade to pending_approvals.json
  → sends email with links: https://base_url/approve?trade_id=X&secret=HASH
  → waits for human action
  
approval_server.py receives callback → verifies hash → updates pending_approvals.json
  → if approved: optimizer picks up and executes
  → if denied: trade is discarded
```

---

## 8. Backtesting System

### Strategy Runner (`trading_system/backtesters/`)

- Accepts `BaseStrategy` implementations (from `coinbase/src/backtest/new_strategies.py`)
- Runs backtest engine with mock or historical data
- Returns standardized metrics: win rate, Sharpe ratio, max drawdown, profit factor

### Data Generation (`run_backtest.py`, `generate_synthetic_btc_data.py`, etc.)

- `simulate_mock_data(1500)` — 4 years of daily OHLCV bars, seedable for reproducibility
- Regime-aware generation with sine-wave driven bull/bear/mean-reverting phases (~180-day cycle via `np.sin(i/60)`)
- Historical CSV loading from `historical_data/` directory

### Metrics Thresholds (paper trading qualification)

| Metric | Minimum | Rationale |
|--------|---------|-----------|
| Win Rate | 60% | Statistical edge |
| Sharpe Ratio | 1.5 | Risk-adjusted returns acceptable |
| Profit Factor | 1.2 | Gross profit exceeds gross loss |
| Min Trades | 30 | Statistical significance in test period |

---

## 9. Document History

| Date | Author | Change |
|------|--------|--------|
| 2026-06-18 | xwing | Initial LLD based on codebase audit |
