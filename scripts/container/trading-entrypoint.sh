#!/usr/bin/env bash
set -euo pipefail

if [[ -f /run/secrets/coinbase_cdp_api_key ]]; then
  mkdir -p "$HOME/.coinbase"
  cp /run/secrets/coinbase_cdp_api_key "$HOME/.coinbase/cdp_api_key.json"
  chmod 600 "$HOME/.coinbase/cdp_api_key.json"
  if command -v coinbase >/dev/null 2>&1; then
    coinbase env live --allow-plaintext-secrets --key-file "$HOME/.coinbase/cdp_api_key.json" >/dev/null || true
  fi
fi

export TRADING_MODE="${TRADING_MODE:-paper}"
export LIVE_TRADING_ENABLED="${LIVE_TRADING_ENABLED:-false}"
export PYTHONPATH="${PYTHONPATH:-/app}"

exec "$@"
