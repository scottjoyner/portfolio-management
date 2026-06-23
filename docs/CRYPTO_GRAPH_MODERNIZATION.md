# Crypto graph modernization

This brings the old `scottjoyner/crypto-graph` idea into the current `portfolio-management` system without copying the legacy prototype directly.

## Legacy intent

The old repo was oriented around:

- NFT/project membership research
- wallet and on-chain asset discovery
- transaction data
- Discord/conversation search as future graph context
- Neo4j as the graph layer
- external market/on-chain metadata sources

The legacy README also contains an API-key-looking value. Do not reuse legacy credentials from that repository. Rotate anything that could still be active.

## Modernized implementation

New modules:

```text
coinbase/src/graph/
  schema.py             # Neo4j constraints and indexes
  models.py             # typed graph records and graph signals
  neo4j_graph.py        # Neo4j store and graph feature scoring
  coingecko_ingest.py   # adapters for market/meta JSON payloads
  portfolio_overlay.py  # graph-score weight overlays
  register.py           # helper to register graph strategy with scanners

coinbase/src/strategies/
  graph_signal.py       # Coinbase BaseStrategy-compatible graph signal
```

## Graph model

Core nodes:

```text
(:Asset {cg_id, symbol_key, product_id, market_cap_rank, available_on_coinbase})
(:Token {key, address, symbol, decimals})
(:Network {key})
(:Category {name})
(:Wallet {address})
(:Project {slug})
(:Transaction {hash})
```

Core relationships:

```text
(:Asset)-[:HAS_CATEGORY]->(:Category)
(:Token)-[:IMPLEMENTS]->(:Asset)
(:Token)-[:ON_NETWORK]->(:Network)
(:Wallet)-[:HOLDS]->(:Token)
(:Wallet)-[:MEMBER_OF]->(:Project)
(:Wallet)-[:SENT|RECEIVED]->(:Transaction)
```

## Usage

Apply schema:

```python
from coinbase.src.graph.neo4j_graph import CryptoGraphStore

store = CryptoGraphStore()
store.apply_schema()
store.close()
```

Ingest saved CoinGecko-style payloads:

```python
from coinbase.src.graph.coingecko_ingest import ingest_markets_file, ingest_meta_file
from coinbase.src.graph.neo4j_graph import CryptoGraphStore

store = CryptoGraphStore()
store.apply_schema()
ingest_markets_file("data/coingecko_markets.json", store, coinbase_symbols={"BTC", "ETH", "SOL"})
ingest_meta_file("data/coingecko_meta.json", store, coinbase_symbols={"BTC", "ETH", "SOL"})
store.close()
```

Register the graph strategy with a scanner:

```python
from coinbase.src.graph.register import register_graph_strategy

register_graph_strategy(scanner, min_graph_score=0.45)
```

Use portfolio overlays:

```python
from coinbase.src.graph.portfolio_overlay import fetch_graph_weight_overlays, apply_graph_overlay

overlays = fetch_graph_weight_overlays(["BTC-USD", "ETH-USD", "SOL-USD"])
weights = apply_graph_overlay({"BTC-USD": 0.5, "ETH-USD": 0.3, "SOL-USD": 0.2}, overlays)
```

## Safety boundary

The graph layer creates research signals and allocation overlays only. Sizing, ranking, mode controls, and any order routing remain inside the Coinbase orchestrator and risk stack.
