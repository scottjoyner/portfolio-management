#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root regardless of where the script is called from
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}"

# Activate venv if present
if [[ -f "${REPO_ROOT}/.venv/bin/activate" ]]; then
  source "${REPO_ROOT}/.venv/bin/activate"
fi

# Work from repo root so dotenv finds .env
cd "${REPO_ROOT}"

# Universe (comma-separated; quoted so it passes as one arg)
SYMS="AAL,AMD,BAC,BLSH,CB,COIN,CPT,CRCL,CRM,CSGP,DUK,HYMB,INTC,MAA,MO,NXST,O,OPTT,PLTR,PM,RDDT,RR,UBER,UNH,VICI,AMZN,ASML,CVX,DUOL,EFX,GOOGL,INTU,MA,MCO,MSCI,SPGI,VRSK"

# Example daily routine (adjust as needed)
python -m app.data.ingest_prices --symbols "$SYMS" --period 1y --interval 1d
# python -m app.data.ingest_edgar --cik 320193
cut -d',' -f1,2 config/sp500_cik.csv | tail -n +2 | while IFS=',' read -r SYMBOL CIK COMPANY; do
  echo "CIK=$CIK ($SYMBOL)"
  PYTHONPATH=. python -m app.data.ingest_edgar --cik "$CIK" || true
  sleep 0.3
done
python -m app.data.build_correlation --symbols "$SYMS" --window 90
python -m app.strategies.run_strategies --symbols "$SYMS"
# python -m app.exec.rebalance --broker fidelity --dry-run
