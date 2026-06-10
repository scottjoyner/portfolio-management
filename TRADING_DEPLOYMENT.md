# Trading System Deployment Guide

## Overview

This deployment provides:
- **Coinbase V3 connector** with JWT/ES256 authentication
- **Backtester service** for historical strategy testing  
- **API server** for real-time trading operations
- **UI Dashboard** at http://localhost:8501
- **Strategy worker** for continuous signal generation

## Quick Start

### 1. Set up Coinbase credentials

```bash
# Ensure your Coinbase CLI key exists
ls -la ~/.coinbase/cdp_api_key.json

# If not present, run setup script
python scripts/setup_coinbase_credentials.py
```

### 2. Create environment files

```bash
# Copy base env
cp .env.example .env

# Create Coinbase-specific env
cat > .env.coinbase << EOF
COINBASE_ENV=live
TRADING_MODE=paper
LIVE_TRADING_ENABLED=true
EOF
```

### 3. Start the deployment

```bash
cd /home/scott/git/portfolio-management

# Build and start all services
docker-compose -f docker-compose.trading.yml up -d --build

# Check status
docker-compose -f docker-compose.trading.yml ps
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| UI Dashboard | 8501 | Operator dashboard with real-time status |
| Trading API | 8000 | REST API for trading operations |
| Backtester | - | Background backtesting service |
| Strategy Worker | - | Continuous strategy execution |
| PostgreSQL | 5432 | Transaction database |
| Redis | 6379 | Cache and event queue |

## Access Points

- **Dashboard**: http://localhost:8501
- **API Health**: http://localhost:8000/health
- **Runtime Status**: http://localhost:8000/runtime/status
- **Coinbase Balances**: http://localhost:8000/coinbase/balances
- **Strategy Catalog**: http://localhost:8000/strategies/catalog

## Backtesting

### Run backtest manually

```bash
docker-compose -f docker-compose.trading.yml exec backtester python -m trading_system.backtester --interval=1h
```

### View results

```bash
ls -la /home/scott/git/portfolio-management/backtest_results/
cat backtest_results/*.json
```

## Strategy Triggers

The system monitors these trigger types:

1. **Price-based**: Entry/exit signals from price movements
2. **Indicator-based**: Technical indicators (RSI, MACD, etc.)
3. **ML-based**: Machine learning predictions
4. **Arbitrage**: Cross-exchange price discrepancies
5. **Time-based**: Scheduled rebalancing

### Enable strategies

Edit `.env` and set:
```bash
ENABLED_STRATEGIES=mean_reversion,momentum_clustering,ml_grid
```

## Logs

```bash
# View all logs
docker-compose -f docker-compose.trading.yml logs -f

# View specific service
docker-compose -f docker-compose.trading.yml logs -f ui-dashboard
docker-compose -f docker-compose.trading.yml logs -f trading-api
docker-compose -f docker-compose.trading.yml logs -f backtester
```

## Stop deployment

```bash
docker-compose -f docker-compose.trading.yml down
# Or keep volumes: docker-compose -f docker-compose.trading.yml down -v
```

## Troubleshooting

### Coinbase auth fails

```bash
# Check key file exists
ls -la ~/.coinbase/cdp_api_key.json

# Verify CLI works
docker exec -it portfolio-management-coinbase-cli-1 coinbase balance -e live
```

### API not responding

```bash
# Restart API service
docker-compose -f docker-compose.trading.yml restart trading-api

# Check logs
docker-compose -f docker-compose.trading.yml logs trading-api
```

### Backtest results empty

```bash
# Ensure historical data exists
python scripts/download_historical_data.py

# Re-run backtester
docker-compose -f docker-compose.trading.yml exec backtester python -m trading_system.backtester --force-reload
```
