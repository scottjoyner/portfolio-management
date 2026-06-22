# Paper Trading & Strategy Deployment Runbook

## 1. Paper Trading System Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    Multi-Strategy Paper Trading                │
│                  (multi_strategy_paper_trading.py)             │
│                                                                │
│  Fetches ALL Coinbase USD pairs concurrently                 │
│  Scores by risk-adjusted opportunity value                   │
│  Ranks cross-market                                          │
│  Boosts trades advancing fee tier                            │
└───────────────────────────────▲──────────────────────────────┘
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
           paper_broker.py         approval_server.py
        (signal→order, no         (email + HTTP callback)
         exchange interaction)
```

## 2. Running Paper Trading

### 2.1 Interactive Mode

```bash
cd ~/git/portfolio-management

# Run a single cycle of all strategies
python3 multi_strategy_paper_trading.py --once

# Continuous mode with approval workflow
python3 multi_strategy_paper_trading.py \
    --interval 300 \
    --require-approval \
    --smtp-user you@gmail.com \
    --smtp-password "app_password" \
    --approval-base-url http://YOUR_IP:8080

# Dry-run (no real Coinbase orders, paper only)
python3 multi_strategy_paper_trading.py --dry-run
```

### 2.2 Paper Broker Usage

For backtesting-specific signal execution against simulated data:

```bash
python3 run_paper_trading.py [subcommand]

Subcommands:
    connect     — Test connections to price feeds and broker
    trade       — Execute a single paper order (interactive)
    positions   — Show current paper positions
    pnl         — PnL summary (realized + unrealized)
    status      — System health check
```

### 2.3 Legacy Paper Trading Scripts

These scripts have historical value but are superseded by the multi-strategy system:

| Script | Status | Notes |
|--------|--------|-------|
| `paper_trading_system.py` (16KB) | Superseded | Original integration platform; still works standalone |
| `run_paper_trading.sh` (11KB) | Legacy | Bash CLI wrapper with connect/trade/positions subcommands |
| `strategies_paper_trading.py` | Superseded | Pre-multi-strategy implementation |

## 3. Strategy Signal Generation

### 3.1 Confidence Engine (`trading_system/signal_confidence.py`)

Used by both the optimizer and multi-strategy paper trading:

- Computes per-signal confidence scores based on historical backtest performance
- Aggregates across multiple strategies for composite signals
- Uses rolling windows of recent trade outcomes to weight current signals

### 3.2 Signal Quality Requirements

Before a strategy signal is accepted for execution (paper or live):

| Metric | Threshold | Rationale |
|--------|-----------|-----------|
| Win Rate | ≥40% (optimizer) / ≥60% (new paper trading) | Statistical edge over random |
| Sharpe Ratio | >0.2 (optimizer) / >1.5 (paper trading qualification) | Risk-adjusted returns |
| Profit Factor | >1.05 (optimizer) / >1.2 (paper trading) | Gross profit exceeds gross loss |
| Min Trades | ≥3 per backtest period | Statistical significance |

### 3.3 Signal Validation Pipeline

```
Raw signal from strategy → Confidence Engine → Backtest validation → 
  → If passes thresholds → queue for execution
  → If fails → log and skip (logged in optimizer_state.db bt_cache)
```

## 4. Paper Trading vs Live Execution Checklist

Before switching a strategy from paper to live:

- [ ] Strategy has run in paper trading for ≥30 days
- [ ] Win rate ≥60% on paper trading data
- [ ] Sharpe ratio ≥1.5 on paper trading data  
- [ ] Max drawdown ≤20% of paper capital
- [ ] At least 30 trades executed (statistical significance)
- [ ] No circuit breaker activations in the last 7 days
- [ ] Live-only testing starts with `--dry-run` flag first
- [ ] Position sizes reduced by 50% for the first live week

## 5. Fee Tier Optimization via Paper Trading

Paper trading can be used to safely test fee tier advancement strategies:

```bash
# Simulate trades that would advance your Coinbase fee tier
python3 multi_strategy_paper_trading.py --paper-only \
    --focus-fee-tier \
    --interval 60   # more frequent for volume generation
```

This generates paper trades on the most liquid pairs to test whether real execution of these same trades would be profitable after accounting for Coinbase fees.

## 6. Common Paper Trading Issues

### 6.1 "No price available" errors

**Cause:** Asset not listed on Coinbase's public API, or rate-limited.
**Fix:** Check `coinbase products list` to verify the asset is tradable. The system uses yfinance/alphavantage as fallbacks per config.yaml source_priority.

### 6.2 "Insufficient USDC balance" in paper mode

This should not happen — paper trading simulates execution against a virtual portfolio starting at $100k (configurable). If you see this, the script is reading from a stale state file or using live exchange data instead of pure paper mode. Check for `--live` flag leakage.

### 6.3 Circuit breaker open during paper trading

**Cause:** 5 consecutive failures in price fetching or execution simulation.
**Fix:** Check network connectivity to Coinbase API and yfinance. Restart the script — circuit breaker auto-recovers after 10 minutes.

## 7. Backtesting Operations Guide

### 7.1 Running a Backtest

```bash
# Quick backtest with synthetic data (reproducible)
python3 coinbase/src/backtest/run_backtest.py --symbols BTC-USD ETH-USD SOL-USD \
    --period 500 --seed 42

# Historical CSV-based backtest
python3 historical_backtest.py

# Backtest v2
python3 backtest_v2.py
```

### 7.2 Interpreting Results

| Output | What to look for | Red flag |
|--------|------------------|----------|
| Win rate ≥60% | Strategy has edge | <40% — strategy is losing money on average |
| Sharpe >1.5 | Returns justify risk | <0.5 — strategy barely beats buy-and-hold |
| Profit factor >1.2 | Gross wins exceed gross losses | <1.0 — strategy loses more than it gains |
| Max drawdown <30% | Acceptable drawdown period | >50% — too much volatility for live trading |

### 7.3 Strategy Qualification Criteria (Paper Trading)

A strategy qualifies for paper trading deployment if ALL of these are met:
- Win Rate ≥ 60%
- Sharpe Ratio > 1.5
- Profit Factor > 1.2
- Minimum 30 trades in backtest period

### 7.4 Synthetic Data Generation

For testing without external API dependencies:

```python
from coinbase.src.backtest.run_backtest import simulate_mock_data

# ~4 years of daily bars, seedable for reproducibility
bars = simulate_mock_data(1500)  # or with seed=something_specific
```

Regime-aware generator (sine-wave driven ~180-day cycles):
- `generate_regime_btc(days)` uses `np.sin(i/60)` pattern — change divisor to adjust cycle length
- Bullish phases: daily return mean +2.5%, vol multiplier 2.8x
- Bearish phases: -1.5% mean, 2.2x vol  
- Mean-reverting phases: +0.5% mean, 1.8x vol

### 7.5 Common Backtest Pitfalls

| Bug | Pattern | Fix |
|-----|---------|-----|
| Close window index error | Building `close_window` from bars list before it's populated | Build from previously appended bars only; use max(0, i-29) for indexing |
| Float len() error | Calling `len()` on bar.close (a float) | Use fixed integer or `len(symbol_data)` instead |
| Strategy init param mismatch | Wrong parameter names in constructor call | Check each strategy's `__init__` signature before instantiating |
| Zero trades produced | Signal conditions too strict for test data regime | Print debug info per bar; relax thresholds for testing, tighten later |
| Metrics type errors | `metrics.avg_win` as special numeric type causing iteration failures | Convert to float: `float(metrics.avg_win)` before arithmetic |

## 8. Quick Reference

```bash
# === Paper trading (main) ===
python3 multi_strategy_paper_trading.py --once         # Single cycle
python3 multi_strategy_paper_trading.py --dry-run      # Safe test mode

# === Run all backtests ===
python3 run_backtest.py                                 # With mock data

# === Analyze portfolio state ===
python3 portfolio_analyzer.py                           # Full report
python3 portfolio_optimizer.py --summary                # Trade summary only

# === Check system health ===
coinbase auth status                                    # Exchange connectivity
curl http://localhost:8080/api/status                   # Approval queue
```
