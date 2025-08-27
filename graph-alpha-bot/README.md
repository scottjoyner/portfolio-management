# Graph Alpha Bot

End-to-end pipeline to ingest financial data, store it in Neo4j, generate trading signals using graph-native strategies, and route orders via broker adapters.

> **Legal / Data**  
> - `yfinance` is suitable for prototyping and research. For production or commercial use, obtain properly licensed market data (e.g., Polygon, Finnhub, Nasdaq Data Link).  
> - Merrill Lynch: no public trading API. Treat as read-only (e.g., via Plaid) and stage manual tickets.  
> - Fidelity: supported via SnapTrade connection; this repo includes a SnapTrade-based adapter.

## Quick start

### 0) Prereqs
- Docker & Docker Compose
- Python 3.10+ and `virtualenv`

### 1) Bring up Neo4j
```bash
docker compose up -d neo4j
# first-time, seed constraints/schema
bash scripts/init_db.sh
```

### 2) Set up the app environment
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env with your values
```

### 3) Ingest sample data
```bash
# Daily bars for a handful of symbols via yfinance
python app/data/ingest_prices.py --symbols AAPL,MSFT,AMZN,GOOGL,META --period 5y --interval 1d
# SEC filings for Apple (CIK 0000320193) as an example
python app/data/ingest_edgar.py --cik 320193
# Build correlation edges (90d lookback) for the symbols above
python app/data/build_correlation.py --symbols AAPL,MSFT,AMZN,GOOGL,META --window 90
```

### 4) Run strategies and create signals
```bash
python app/strategies/run_strategies.py --symbols AAPL,MSFT,AMZN,GOOGL,META
```

### 5) Produce a candidate trade list & (optionally) route via Fidelity/SnapTrade
```bash
python app/exec/rebalance.py --broker fidelity --dry-run
# remove --dry-run to place live orders (ensure your .env is correctly configured)
```

### 6) Schedule the daily job
```bash
# One-shot daily
bash scripts/run_daily.sh
# Or add to cron
```

## Repo layout
```
graph-alpha-bot/
  app/
    data/                # ETL
    graph/               # schema & graph queries
    strategies/          # alpha models
    exec/                # broker adapters & order engine
    backtest/            # simple backtesting scaffold
    utils/               # helpers
  config/                # example universe & mappings
  scripts/               # init and job runner
  docker-compose.yml
  requirements.txt
  .env.example
```


---

## S&P 500 universe + EPS YoY screener

1) Refresh and load current S&P 500 constituents (from Wikipedia) and map to SEC CIKs:
```bash
python app/data/refresh_sp500.py
# writes config/symbols_sp500.csv and config/sp500_cik.csv and MERGEs :Ticker nodes
```

2) Compute EPS (diluted) YoY from the SEC XBRL API (company-concept endpoint):
```bash
# set SEC_USER_AGENT in .env first (contact email + app name)
python app/data/ingest_eps_sec.py            # process all
# or:
python app/data/ingest_eps_sec.py --limit 50 # first 50 for a quick run
```

3) Screen for the top EPS growers:
```bash
python app/screen/screener_eps_growth.py --top 25 --csv top_eps_yoy.csv
```

**Notes**
- EPS series is drawn from `us-gaap:EarningsPerShareDiluted` (fallback to Basic) in *10-K* facts.
- YoY computed on last two fiscal years where the prior year EPS > 0 to avoid distorted ratios.
- Respect SEC rate limits and include a clear `SEC_USER_AGENT` (email + app name).
