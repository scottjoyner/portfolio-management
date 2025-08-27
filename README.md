# Coinbase Quant Bot (Advanced Trade API)

A minimal, event-driven trading scaffold that connects to Coinbase **Advanced Trade API** and implements a robust,
risk-managed momentum strategy with volatility targeting and portfolio rebalancing.

> **Use at your own risk. Not financial advice.** Start in `DRY_RUN=true` and test against the Coinbase **Advanced Trade Sandbox**.

## Features
- Official SDK: [`coinbase-advanced-py`](https://github.com/coinbase/coinbase-advanced-py)
- Pulls candles via REST, prices via Best Bid/Ask, and places/preview orders
- Cross-asset trend following (SMA-50 > SMA-200) with **volatility targeting**
- Idempotent orders with `client_order_id`
- Drawdown kill-switch, per-trade and per-day risk caps
- Dry-run mode with **order preview** (no execution)
- Simple backtest (vectorized, daily candles)
- Logging + configurable universe

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Copy and edit environment
cp .env.example .env

# Dry run (no real orders)
python -m src.run_trader --rebalance
```

## Environment
Set API creds as **CDP Advanced Trade** keys (SDK reads them from env):

```bash
export COINBASE_API_KEY="organizations/{org_id}/apiKeys/{key_id}"
export COINBASE_API_SECRET=$'-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----\n'
```

Optional (or place in `.env` and use `python-dotenv`):
- `DRY_RUN=true` (preview only)
- `PRODUCTS=BTC-USD,ETH-USD,SOL-USD`
- `CASH=USD`
- `BAR_GRANULARITY=ONE_HOUR` (ONE_MINUTE, FIVE_MINUTE, ... , ONE_DAY)
- `LOOKBACK_DAYS=240`
- `REBALANCE_FREQ=1d` (used by scheduler/cron)
- `TARGET_VOL=0.10` (annualized)
- `RISK_PER_TRADE=0.01`
- `MAX_DD=0.15` (kill-switch)
- `MIN_NOTIONAL=50`

## References
- Advanced Trade REST endpoints & portfolios: see docs
- Public/private candles + granularity enums
- WebSocket endpoints for market/user streams

