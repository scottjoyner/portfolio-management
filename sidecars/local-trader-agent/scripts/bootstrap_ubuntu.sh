#!/usr/bin/env bash
set -euo pipefail

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip wheel setuptools
python -m pip install -e .

cat <<'MSG'
Installed local-trader-agent.

Run a deterministic backtest:
  local-trader-agent backtest --config config/example.yaml

Run agent mode after llama.cpp server is listening on http://127.0.0.1:8080/v1:
  local-trader-agent agent --config config/example.yaml "Backtest GOOGL using RSI cross above 30, +2% take profit, -1% stop loss, no overlapping positions, then produce a report."
MSG
