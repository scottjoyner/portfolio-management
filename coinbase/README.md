# Coinbase Quant Bot — Full Suite

Event‑driven crypto trading scaffold for **Coinbase Advanced Trade API** with:
- Vol‑targeted **portfolio rebalance**
- **Risk/Reward** gated setups (long & short) with **synthetic brackets** (stop/target, breakeven, ATR‑trail)
- **Kelly‑capped** sizing + **bandit allocator** (UCB1/Thompson)
- **Transaction cost** model (fees, spread, impact)
- **Paper trading simulator** (replays candles using same bracket logic)
- **Dashboard** (Streamlit)
- **Alt metadata ingestion** (CoinGecko + Token Lists + Chainlist + CoinPaprika fallback) into **Neo4j**

> **Use at your own risk. Not financial advice.** Start with `DRY_RUN=true`. For shorts, spot accounts cannot go net short without margin/derivatives.

## Quickstart
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then edit keys and toggles
```

### Dry‑run trading examples
```bash
# Vol-target rebalance
python -m src.run_trader --rebalance

# Scan & place RR‑gated bracket trades (long/short, Kelly-scaled, bandit-ranked)
python -m src.run_trader --rr-trades

# Manage exits (synthetic OCO, +1R breakeven, optional ATR trail)
python -m src.run_trader --manage-brackets
```

### Paper Trading
```bash
python -m src.run_paper --products BTC-USD,ETH-USD --granularity ONE_HOUR --lookback-days 240   --initial-cash 20000 --risk-per-trade 0.01 --min-rr 2.0
# Outputs: state/paper_equity.csv ; trades appended to state/trades.csv
```

### Dashboard
```bash
PYTHONPATH=. streamlit run src/dashboard_app.py
```

### Alternative Metadata → Neo4j
```bash
# Fetch caches (watch rate limits)
python -c "from src.alt.fetch_assets_alt import fetch_top_by_marketcap, fetch_assets_meta; top=fetch_top_by_marketcap(5000); ids=[r['id'] for r in top]; fetch_assets_meta(ids[:1000])"
python -c "from src.alt.fetch_assets_alt import fetch_tokenlists_and_chains; fetch_tokenlists_and_chains()"
python -c "from src.alt.fetch_assets_alt import fetch_paprika_basics; fetch_paprika_basics()"

# Ingest to Neo4j
python -c "from src.alt.neo4j_alt_ingest import ingest_assets_markets; ingest_assets_markets('data/alt/json/coingecko_markets_top_5000.json')"
python -c "from src.alt.neo4j_alt_ingest import ingest_assets_meta; ingest_assets_meta('data/alt/json/coingecko_assets_meta.json')"
python -c "from src.alt.neo4j_alt_ingest import ingest_tokenlists; ingest_tokenlists('data/alt/json/tokenlists.json')"
```

## Env (.env.example)
Trading: `DRY_RUN, PRODUCTS, CASH, BAR_GRANULARITY, LOOKBACK_DAYS, TARGET_VOL, RISK_PER_TRADE, MAX_DD, MIN_NOTIONAL`  
Brackets: `MIN_RR, STOP_ATR_MULT, TARGET_ATR_MULT, TRAIL_ATR_MULT, BREAK_EVEN_AFTER_R, MANAGER_POLL_SECS, MAX_OPEN_BRACKETS, ENABLE_SHORTS`  
Kelly & Bandit: `ENABLE_KELLY, KELLY_CAP, KELLY_FLOOR, DEFAULT_RR, BANDIT_MODE, UCB_C, KELLY_CAPS_PRODUCT_JSON, KELLY_CAPS_SETUP_JSON`  
Costs: `TAKER_FEE_BPS, SLIPPAGE_BPS, IMPACT_COEFF`  
Alt metadata: `COINGECKO_API_KEY, TOKENLIST_URLS, CHAINLIST_URL`  
Neo4j: `NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, NEO4J_DATABASE`


# A) Run your original strategy pack bar-by-bar + monthly portfolio overlay
python -m src.run_backtest \
  --products BTC-USD,ETH-USD,SOL-USD,LINK-USD,MATIC-USD \
  --granularity ONE_HOUR \
  --lookback-days 365 \
  --adapters suite,pm \
  --initial-cash 20000 --risk-per-trade 0.01 --fee-bps 8 --slip-bps 1.5 --fill-when next

# B) Pure portfolio (monthly rebalance only)
python -m src.run_backtest --adapters pm

# C) Blend: triple-MA + Donchian + monthly portfolio
python -m src.run_backtest --adapters ma,donch,pm