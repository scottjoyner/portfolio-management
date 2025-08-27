#!/usr/bin/env bash
set -euo pipefail
source .venv/bin/activate || true

# Example daily routine (adjust universe and timings as needed)
python app/data/ingest_prices.py --symbols AAPL,MSFT,AMZN,GOOGL,META --period 1y --interval 1d
python app/data/ingest_edgar.py --cik 320193
python app/data/build_correlation.py --symbols AAPL,MSFT,AMZN,GOOGL,META --window 90
python app/strategies/run_strategies.py --symbols AAPL,MSFT,AMZN,GOOGL,META
python app/exec/rebalance.py --broker fidelity --dry-run
