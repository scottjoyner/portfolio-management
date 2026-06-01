# Trading System Backtesting Infrastructure - Complete Documentation

## Overview

The backtesting infrastructure provides comprehensive strategy evaluation capabilities including:
- Historical market data replay
- Paper execution simulation  
- Performance metrics calculation (Sharpe, drawdown, win rate)
- Trade lifecycle simulation
- Database-backed results storage
- REST API for trigger/retrieve/validate operations

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Backtest Core Layer                       │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐    ┌──────────────────┐               │
│  │ BacktesterEngine │    │ StrategySimulator│               │
│  │ Market Replay     │    │ Paper Execution  │               │
│  └──────────────────┘    └──────────────────┘               │
│                    ↓                 ↓                       │
│  ┌──────────────────┐    ┌──────────────────┐               │
│  │ Performance       │    │ Database         │               │
│  │ Metrics Calculator│    │ Results Store   │               │
│  └──────────────────┘    └──────────────────┘               │
├─────────────────────────────────────────────────────────────┤
│              REST API Layer                                  │
│           /backtests /triggers /retrieve                     │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. BacktesterEngine

Primary backtesting engine that handles:
- Historical OHLCV data loading and replay
- Market price simulation with slippage models
- Order execution simulation
- Realized/unrealized P&L tracking
- Equity curve generation
- Trade lifecycle logging

Usage example:
```python
from trading_system.backtest.engine import BacktesterEngine, Config, BacktestReport

config = Config(
    start_date="2025-01-01",
    end_date="2025-05-31",
    initial_capital=100000.0,
)

engine = BacktesterEngine(config=config)
results = engine.run_backtest(strategy_id="btc-momentum-strategy")
report = BacktestReport(results)
print(report.to_dict())
```

### 2. StrategySimulator

Paper trading simulation for validation:
- Signal generation from strategy templates
- Fill execution with realistic slippage
- Fee modeling (maker/taker fees)
- Position tracking and exposure calculation
- Win rate and profit factor computation

### 3. Database Models

Backtest results persisted to PostgreSQL with tables:
- `backtest_results` - Complete backtest metrics
- `equity_curve_points` - Time-series equity snapshots
- `backtest_trades` - Individual trade records
- `strategy_certifications` - Validation certification records

### 4. REST API Routes

Available endpoints:

#### POST /api/v1/backtests
Trigger new backtest execution
```json
POST /api/v1/backtests
{
    "strategy_id": "btc-momentum",
    "config_version": "v1.2.0",
    "start_date": "2025-01-01",
    "end_date": "2025-05-31"
}
```

Response:
```json
{
    "status": "success",
    "strategy_id": "btc-momentum",
    "backtest_id": "a7f3b9d2",
    "results": {
        "trade_count": 23,
        "total_return_pct": 8.5,
        "sharpe_ratio": 1.45,
        ...
    }
}
```

#### GET /api/v1/backtests/{id}
Retrieve backtest results by ID or strategy:
```json
GET /api/v1/backtests/btc-momentum
```

Returns complete metrics, equity curve, and trade log.

#### DELETE /api/v1/backtests/{id}
Invalidate existing backtest for re-run.

#### POST /api/v1/backtests/import
Import external backtest data (CSV/JSON).

## Performance Metrics

### Calculated Metrics

| Metric | Description | Calculation |
|--------|-------------|-------------|
| Sharpe Ratio | Risk-adjusted return | Annualized return / volatility |
| Max Drawdown | Worst peak-to-trough decline | Peak - Trough (percent) |
| Win Rate | Percentage of profitable trades | Wins / Total Trades |
| Profit Factor | Gross profits to gross losses | Gains / Losses |
| Slippage Cost | Execution price vs fair value | Sum(price_diff * qty) |
| Fee Impact | Trading fee total | Maker/Taker fees sum |

### Equity Curve Generation

Equity curve tracks:
- Available capital (cash balance)
- Realized P&L (completed trades)
- Unrealized P&L (open positions)
- Total equity (sum of all above)
- Time-stamped snapshots for visualization

## Database Integration

All backtest results are persisted to PostgreSQL tables in the `trading_system` database schema.

### Schema Tables

1. **backtest_results** - Main performance metrics table
   - Strategy ID, config hash, time period
   - Capital tracking (initial/current)
   - Risk metrics (Sharpe, drawdown, Sortino)
   - Trading stats (win rate, profit factor)
   - Certification status

2. **equity_curve_points** - Time-series data
   - Timestamps for equity curve visualization
   - Running capital and P&L totals
   - Used for chart plotting and analysis

3. **backtest_trades** - Detailed trade log
   - Individual order fills with prices
   - Fee and slippage tracking
   - Order type and status

4. **strategy_certifications** - Validation records
   - Certification scores and dates
   - Performance thresholds
   - Rejection reasons for failed certifications

## End-to-End Testing

### Test Script Location
`tests/backtest/test_backtesting_e2e.py`

### Run Tests
```bash
python -m pytest tests/backtest/ -v --no-isolate
```

### Test Coverage
- [ ] Backtest trigger execution
- [ ] Results retrieval by ID and strategy
- [ ] Equity curve generation accuracy
- [ ] Trade log completeness
- [ ] Performance metrics calculation
- [ ] Database persistence and querying
- [ ] API endpoint integration

## Sample Usage Complete Workflow

```python
from trading_system.backtest.engine import BacktesterEngine, Config
from datetime import datetime

# 1. Configure backtest parameters
config = Config(
    start_date="2025-01-01",
    end_date="2025-05-31",
    initial_capital=100000.0,
    slippage_model="volume_weighted"
)

# 2. Initialize engine
engine = BacktesterEngine(config=config)

# 3. Run backtest (in production: load historical data first)
results = engine.run_backtest(strategy_id="btc-momentum")

# 4. Generate report
report_dict = {
    "strategy": results["strategy_id"],
    "period": {
        "start": results["start_date"],
        "end": results["end_date"]
    },
    "metrics": {
        "trade_count": results["trade_count"],
        "return_pct": results["capital"]["total_return_pct"],
        "sharpe_ratio": results["risk_metrics"]["sharpe_ratio"],
    },
    "equity_curve": results["equity_curve"]
}

# 5. Store in database (automatic in production)
backtest_id = store_backtest_result(results)

# 6. Trigger API endpoint (production)
response = await post_request(
    "/api/v1/backtests",
    json={
        "strategy_id": "btc-momentum",
        "config_version": "v1.0"
    }
)

print(f"Backtest ID: {backtest_id}")
print(f"Return: {report_dict['metrics']['return_pct']}%")
```

## API Response Schemas

### Trigger Response Schema
```typescript
interface BacktestTriggerResponse {
    status: "success";
    strategy_id: string;
    backtest_id: string;  // UUID or timestamp-based
    results: BacktestResultSummary | null;
}

interface BacktestResultSummary {
    trade_count: number;
    total_return_pct: number;
    sharpe_ratio: number;
    max_drawdown_pct: number;
    win_rate_pct: number;
    profit_factor: number;
    fees_paid_usd: number;
}
```

### Retrieve Response Schema
```typescript
interface BacktestResultsResponse {
    status: "success";
    strategy_id: string;
    backtest_id: string;
    period: Period;
    capital: CapitalMetrics;
    risk_metrics: RiskMetrics;
    trading_stats: TradingStats;
    cost_analysis: CostAnalysis;
    equity_curve: EquityCurvePoint[];
}

interface Period {
    start: string;  // YYYY-MM-DD
    end: string;
}

interface CapitalMetrics {
    initial_usd: number;
    realized_pnl_usd: number;
    unrealized_pnl_usd: number;
    total_return_pct: number;
}

interface RiskMetrics {
    sharpe_ratio: number;
    max_drawdown_pct: number;
    sortino_ratio: number;
}

interface TradingStats {
    trade_count: number;
    winning_trades: number;
    losing_trades: number;
    win_rate_pct: number;
    profit_factor: number;
    avg_trade_pnl_usd: number;
    gross_traded_usd: number;
}

interface CostAnalysis {
    fees_paid_usd: number;
    slippage_costs_usd: number;
    total_cost_usd: number;
}

interface EquityCurvePoint {
    timestamp: string;
    available_capital: number;
    realized_pnl: number;
    unrealized_pnl: number;
    total_equity: number;
}
```

## Production Deployment Checklist

- [ ] Backtest engine integrates with live market data feeds
- [ ] Historical data loading from Coinbase/Kraken APIs
- [ ] Real-time execution simulation layer
- [ ] Database connection to production PostgreSQL
- [ ] API route registration in main.py
- [ ] Authentication and rate limiting enabled
- [ ] Error handling and logging configured
- [ ] Performance metrics calculation verified
- [ ] Equity curve visualization tested
- [ ] Trade log completeness validated
- [ ] Database migrations applied

## Next Phases (P2/P3)

### P2: Account Foundation Integration
- Plaid API integration for real account balances
- Position tracking from live positions table
- Portfolio-level aggregation across multiple accounts

### P3: Event Broker Adapter
- Kafka/SNS message bus integration
- Webhook delivery system for backtest results
- Event sourcing for audit trail

## Known Limitations (Current Implementation)

1. Simulated trade data for demonstration
2. Fixed base prices (BTC ~$69K, ETH ~$3.8K)
3. Simplified slippage models
4. No live market data integration yet
5. Basic fee structures (not dynamic)

## References

- `trading_system/backtest/engine.py` - Core backtesting engine
- `trading_system/backtest/simulator.py` - Paper execution simulator
- `trading_system/backtest/models.py` - Database ORM models
- `trading_system/api/routes_backtest.py` - REST API endpoints
- `tests/backtest/` - End-to-end test suite
