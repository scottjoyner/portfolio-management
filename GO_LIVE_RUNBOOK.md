# Go-Live Runbook — run_trader_v4 (bot side of the paper competition)

## Pre-flight (once)
1. Scope the Coinbase API key: trade-only, IP allowlist, withdrawals OFF.
2. Ensure `data/tuner_state_v4.json` exists with INTENTIONAL values (restored from HEAD; review it).
3. Commit the safety changes so the live run is reproducible:

   cd /home/scott/git/portfolio-management
   git add coinbase/src/run_trader_v4.py coinbase/src/live_performance.py \
           coinbase/src/portfolio_risk.py coinbase/src/rest_feed.py \
           tests/coverage/coinbase/test_run_trader_v4.py \
           tests/coverage/coinbase/test_portfolio_risk.py \
           tests/test_circuit_breaker_proof.py \
           tests/test_strategy_concentration_guard.py \
           scripts/trader_kill_watchdog.py \
           GO_LIVE_CHECKLIST.md GO_LIVE_RUNBOOK.md
   git commit -m "trading: paper->live safety hardening (accounting, leverage cap, circuit-breaker proof, kill-switch watchdog, auto-disable on $-loss, ledger archival, concentration+depth guards)"

   (Do NOT `git push` without explicit review — shared-state action.)

## Launch live (first real capital — TRIVIAL size)
Kill the paper bot, then relaunch in live mode with the hardened posture:

  # stop paper bot
  kill <paper_pid>            # or: touch data/trading_kill_switch (watchdog kills in <=1 min)

  # launch live, leverage+shorts OFF for first capital
  cd /home/scott/git/portfolio-management
  .venv/bin/python3 coinbase/src/run_trader_v4.py --mode live \
      --log-file logs/run_trader_v4_live.log

  (Run it under a process manager / nohup so it survives the shell. The
   trader-kill-watchdog cronjob will keep the out-of-band kill working.)

## Emergency stop (out-of-band, works even if bot is hung)
  touch /home/scott/git/portfolio-management/data/trading_kill_switch
  # watchdog (cronjob de11250703e5, every 1 min) force-kills the trader process

## Monitoring
  - Bot STATS log: equity / pnl / wr / dd / trades per tick.
  - Archival ledger: data/state_backups/ (recoverable if main file vanishes).
  - portfolio-watchdog cronjob (every 15 min) + hermes-agent-paper-loop (separate book).

## Scale-up criteria (only AFTER trivial capital matches paper)
  - Live fills/slippage match paper expectations.
  - No unexpected drawdown beyond circuit-breaker thresholds.
  - Edge broadens (top-3 concentration drops below ~50% of pnl) via auto-disable pruning.
  THEN consider enabling leverage/shorts gradually — NOT before.
