#!/usr/bin/env bash
# hermes_digest_cron.sh — Phase 13 daily agent-vs-bot digest.
# Run by the Hermes scheduler (no_agent); prints the digest to stdout, which is
# delivered to the operator's Hermes chat. Paper-only, read-only.
set -euo pipefail
cd "$(dirname "$0")/.."
KILL_SWITCH="${KILL_SWITCH:-false}" .venv/bin/python scripts/hermes_digest.py
