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



## New: Risk/Reward Setups + Brackets (synthetic OCO)
This adds two low-risk/high-R strategies and manages protective stops & targets:

- **Donchian Breakout**: close > 20D high → stop = entry − k·ATR, target = entry + m·ATR.
- **Trend RSI Pullback**: Uptrend + RSI<35 pullback → stop = entry − k·ATR, target = 20D high (fallback 2·ATR).

**Commands**
```bash
# Scan universe, place bracketed entries that pass RR filter (min 2.0 by default)
python -m src.run_trader --rr-trades

# Continuously manage synthetic OCO exits (move to breakeven after +1R; optional ATR trailing)
python -m src.run_trader --manage-brackets
```

**Config**
See `.env.example` for:
`MIN_RR, STOP_ATR_MULT, TARGET_ATR_MULT, TRAIL_ATR_MULT, BREAK_EVEN_AFTER_R, MANAGER_POLL_SECS`.


## New: Shorts, Kelly sizing, Bandit allocator
- **Short entries** (Donchian breakdown, Trend RSI rip). *Note:* true net shorts require margin/derivatives; on spot-only accounts we only sell existing holdings. Disable via `ENABLE_SHORTS=false` (default).
- **Kelly-capped sizing** using rolling trade outcomes per setup (`trades.csv`). Env: `ENABLE_KELLY`, `KELLY_CAP`, `KELLY_FLOOR`.
- **Bandit allocator** to prioritize setups with higher expectancy: `BANDIT_MODE=ucb1|thompson|none`, `UCB_C` for exploration.
- **Trade logging** to `state/trades.csv` with realized R-multiples and PnL.

**Flow**
1. `--rr-trades` proposes bracketed entries (long/short) that pass `MIN_RR`.

2. Bandit ranks setups; Kelly scales per-trade risk.

3. `--manage-brackets` executes stops/targets, moves to breakeven after +1R, optional ATR trailing.




## Transaction Costs & Slippage
Configurable bps knobs applied when sizing/gating R:R:
- `TAKER_FEE_BPS` (default **8** bps)
- `SLIPPAGE_BPS` (extra safety margin)
- `IMPACT_COEFF` (bps × sqrt($notional/10k))

We estimate **effective fill** from mid using half-spread + fees + impact.

## Per-Product / Per-Setup Kelly Caps
Use JSON env vars to restrict aggressiveness:
```bash
export KELLY_CAPS_PRODUCT_JSON='{"BTC-USD":0.6,"ETH-USD":0.5,"SOL-USD":0.4}'
export KELLY_CAPS_SETUP_JSON='{"donchian_breakout":0.5,"trend_rsi_pullback":0.4,"donchian_breakdown":0.5,"trend_rsi_rip":0.4}'
```

## Live Dashboard
Install `streamlit`, then run:
```bash
PYTHONPATH=. streamlit run src/dashboard_app.py
```
Shows recent trades, equity, setup stats, bandit scores, open brackets, and Kelly caps.


## Paper Trading (Bracket Logic)
Run a historical simulation of the bracket strategy (long/short) with risk-per-trade sizing and simple transaction costs:
```bash
python -m src.run_paper --products BTC-USD,ETH-USD --granularity ONE_HOUR --lookback-days 240 --initial-cash 20000 --risk-per-trade 0.01 --min-rr 2.0
```
Results are written to `state/paper_equity.csv` and trades to `state/trades.csv`.

## CoinMarketCap → Neo4j
Fetch top-5000 listings + detailed info, cache JSON, then ingest to Neo4j:
```bash
# 1) Fetch (requires CMC_API_KEY)
python -m src.fetch_cmc

# 2) Ingest
python -c "from src.neo4j_cmc import ingest_from_files; ingest_from_files('data/cmc/json')"
```
Schema uses nodes: `:Asset(cmc_id)`, `:Network(name)`, `:Category(name)`, `:Tag(name)` with relationships:
- `(:Asset)-[:ON_NETWORK]->(:Network)`
- `(:Asset)-[:HAS_CATEGORY]->(:Category)`
- `(:Asset)-[:HAS_TAG]->(:Tag)`
