# Go-Live Checklist — portfolio-management (agent vs bot paper competition)

Last updated: 2026-07-18. Status of the BOT (run_trader_v4) safety work.

## TIER 1 — Code safety (DONE + VERIFIED this session)
All present in running source, 246 tests passing.

- [x] Honest accounting — `_paper_equity` = cash + unrealized (no phantom notional)
- [x] Hard failsafe on corrupt/missing state (`_load_paper_state` validates + aborts)
- [x] P&L counters reconciled (paper_realized_pnl authoritative; ledger under-count flagged)
- [x] Gross-leverage HARD-capped at 1.5x equity (`PortfolioRiskManager.check_pre_trade` takes MIN across cluster/asset/leverage — was bypassable before fix)
- [x] Circuit breakers PROVEN to halt (test_circuit_breaker_proof.py forces each breach)
- [x] Out-of-band kill switch (cronjob `trader-kill-watchdog` every 1 min; `touch data/trading_kill_switch` force-kills even if bot is hung)
- [x] No withdrawal code path exists anywhere in coinbase/src
- [x] Auto-disable on absolute $-loss (`auto_disable(max_loss_pnl=-500)` kills bleeders like FIS-USD immediately, not after 10 trades)
- [x] Ledger archival backups (`data/state_backups/`, throttled 60s, pruned to 10) — ledger cannot vanish
- [x] Concentration guard (`max_strategy_pnl_share=0.30`) — no single strategy can dominate the book
- [x] Sample-depth guard (`min_trades_for_full_sizing=20`) — low-sample "winners" can't overdrive size

## TIER 2 — Deployment hygiene (TODO before live)
- [ ] Commit the safety changes + new files (see GO_LIVE_RUNBOOK.md). Working tree is
      currently DIRTY: core fixes (run_trader_v4.py, live_performance.py, portfolio_risk.py)
      are uncommitted; tuner_state_v4.json was accidentally deleted (restored from HEAD).
- [ ] Decide the LIVE tunable config. The bot currently runs on TUNABLE_KNOBS DEFAULTS
      (tuner_state_v4.json was missing). Committed tuned overrides exist (min_conf 0.50,
      min_win_rate 0.45, max_new_positions 50, maker_pct 0.80, etc.) — choose intentional values.
- [ ] Confirm NO second v4 trader instance (only one PID expected). The hermes_agent_loop
      cron is a SEPARATE aggressive book, not a duplicate v4 bot — that's fine.
- [ ] Live run must match the hardened posture: `--mode live` (NOT paper), but start with
      leverage DISABLED and shorts DISABLED (current run has them off — keep it that way
      for first real capital).

## TIER 3 — Manual gates (YOUR ACTION — cannot be done from agent/code)
- [ ] **API KEY SCOPING (critical):** create a TRADE-ONLY Coinbase key, IP allowlist,
      WITHDRAWALS DISABLED. The code cannot move funds, but the key's own perms are the
      only real risk. Do this at Coinbase before any live capital.
- [ ] **Live execution proof:** run TRIVIAL real capital (money you can lose) with the
      watchdog + trade-only key. Verify live fills/slippage/partial-fills match paper.
      The +37% is 100% IN-SAMPLE PAPER — no out-of-sample validation exists yet.
- [ ] **Out-of-sample edge:** edge is concentrated (top-3 strategies = 110% of pnl;
      median strategy unprofitable). Let auto-disable prune bleeders over more trades
      before scaling.

## GO/NO-GO
- Code safety: GO (Tier 1 complete).
- Deployment: NO-GO until Tier 2 committed + config decided.
- Real money: NO-GO until Tier 3 (key scoping + trivial-capital live proof) done.
