#!/usr/bin/env bash
# Cron wrapper: run the Hermes agent paper-trader loop and report ONLY when it
# records a paper signal (avoids HOLD noise in chat). Paper-only; never live.
#
# The agent is the AGGRESSIVE book: AGENT_LEVERAGE=3.0 (bot runs --max-leverage 2.0),
# MAX_NOTIONAL_PER_TRADE_USD=250 margin cap (exposure = margin * leverage). Both the
# agent and the bot-v4 paper book start at a flat $10,000 (level playing field).
set -u
cd /home/scott/git/portfolio-management || exit 1
export AGENT_LEVERAGE="${AGENT_LEVERAGE:-3.0}"
export MAX_NOTIONAL_PER_TRADE_USD="${MAX_NOTIONAL_PER_TRADE_USD:-250}"
OUT=$(.venv/bin/python scripts/hermes_agent_loop.py --quiet 2>&1)
# Print only if a paper fill was recorded this iteration
if echo "$OUT" | grep -qE "signal_recorded|short_opened"; then
  echo "[hermes-agent-loop] $(date -u +%H:%M:%SZ) activity:"
  echo "$OUT" | grep -E "signal_recorded|short_opened" | sed 's/^/  /'
  # surface the live head-to-head vs the bot-v4 paper book (both start $10k)
  .venv/bin/python scripts/hermes_race_digest.py 2>/dev/null | sed 's/^/  /'
fi
exit 0
