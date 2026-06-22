# Portfolio Optimizer — Production Runbook

## 1. System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Production Stack                         │
├─────────────────────────────────────────────────────────────┤
│  portfolio_optimizer.py    — Main daemon (always running)    │
│  approval_server.py        — HTTP approval handler (opt)     │
│  optimizer_dashboard.py    — Streamlit dashboard (on demand) │
│  portfolio_analyzer.py     — One-shot analysis (on demand)   │
├─────────────────────────────────────────────────────────────┤
│  state_store.py            — SQLite (fast local cache)       │
│  neo4j_store.py            — Neo4j trading DB (analytics)    │
│  pending_approvals.json    — Shared approval state file      │
├─────────────────────────────────────────────────────────────┤
│  Coinbase CLI (system-installed) — All exchange interaction  │
└─────────────────────────────────────────────────────────────┘
```

### Core Loop

Every `--interval` seconds (default 300s / 5min), the optimizer:

1. Fetches live balances, prices, and fee data from Coinbase
2. Runs 4 opportunity detectors in priority order:
   - **Tax-Loss Harvest** — sell positions with unrealized loss > 5%
   - **Fee Tier Volume** — generate trading volume to reach lower fee tier
   - **Rebalancing** — correct allocation drift beyond 5% from 80/15/5 target
   - **Strategy Signals** — 5 algorithmic strategies (EMA, RSI, Bollinger, Z-score, Volume) validated by backtest
3. Previews each opportunity via Coinbase CLI (dry-run order)
4. If `--require-approval` is set and not dry-run: saves pending approval and sends email
5. Otherwise: executes the trade (or logs it if `--dry-run`)
6. Persists state to both SQLite and (optionally) Neo4j

---

## 2. Components

### 2.1 `portfolio_optimizer.py` — Main Daemon

**Purpose:** Continuously monitors the Coinbase portfolio and executes improvement trades.

**Always-running service.** This is the core of the system.

```bash
# Dry-run mode (recommended first)
python3 portfolio_optimizer.py --interval 300

# Live execution
python3 portfolio_optimizer.py --live --interval 300

# With Neo4j analytics store
python3 portfolio_optimizer.py --live \
    --neo4j-uri bolt://100.64.43.123:7687 \
    --neo4j-password knowledge_graph_2026 \
    --neo4j-db trading

# With email approval workflow
python3 portfolio_optimizer.py --live --require-approval \
    --smtp-user your@gmail.com \
    --smtp-password "abcd efgh ijkl mnop" \
    --approval-base-url http://YOUR_IP:8080

# Single tick (debugging)
python3 portfolio_optimizer.py --once

# Summary of all trades
python3 portfolio_optimizer.py --summary
```

**Key behaviors:**

- `--live`: executes real Coinbase orders. **Without this flag, no real trades are placed.**
- `--require-approval`: sends email with approve/deny links. Trade executes only after approval.
- All orders go through `preview` first (CLI dry-run) before execution.
- BUY orders use `-USDC` pairs (wallet holds USDC); SELL orders use `-USD` pairs.
- Position sizes are rounded to product `quote_increment` / `base_increment` to avoid HTTP 400 errors.
- Cooldowns prevent repeated execution of the same type: TLH=24h, fee=1h, rebalance=12h, strategy=5min, cycle=10min.
- Strategy signals are backtest-validated before becoming opportunities (≥3 trades, win rate ≥40%, Sharpe >0.2, Pf >1.05).
- At most 5 opportunities are processed per tick.

### 2.2 `approval_server.py` — HTTP Approval Handler

**Purpose:** Lightweight HTTP server (stdlib only, no dependencies) that handles approve/deny callbacks from email links.

**Always-running when `--require-approval` is used.**

```bash
# Default port 8080
python3 approval_server.py

# Custom port
python3 approval_server.py --port 9090

# Custom pending file (must match optimizer's --pending-file)
python3 approval_server.py --pending-file /path/to/pending_approvals.json
```

**Endpoints:**

| Endpoint | Description |
|----------|-------------|
| `GET /approve/<token>` | Marks a pending trade as approved |
| `GET /deny/<token>` | Marks a pending trade as denied |
| `GET /status` | HTML status dashboard of all pending/approved/denied trades |
| `GET /api/status` | JSON status (for programmatic access) |
| `GET /` | Health check page |

**State:** reads/writes `pending_approvals.json` (shared with the optimizer).

### 2.3 `notification.py` — Email Notifier

**Purpose:** Sends rich HTML trade alerts via Gmail SMTP with approve/deny links.

**Not a standalone service** — imported by `portfolio_optimizer.py`.

**Setup:**
1. Enable 2-Factor Authentication on your Google account
2. Generate an App Password at https://myaccount.google.com/apppasswords
3. Pass it via `--smtp-password` (use quotes for the spaced format)

### 2.4 `optimizer_dashboard.py` — Streamlit Dashboard

**Purpose:** Web UI for monitoring portfolio, fee tiers, strategy performance, and trade history.

**On-demand.** Not required for core operation.

```bash
PYTHONPATH=. streamlit run optimizer_dashboard.py
```

### 2.5 `portfolio_analyzer.py` — One-Shot Analysis

**Purpose:** Generate a full portfolio health report including TLH candidates, allocation analysis, and strategy fit.

**On-demand.**

```bash
python3 portfolio_analyzer.py
```

### 2.6 `state_store.py` — SQLite Persistence

**File:** `optimizer_state.db` (configurable via `--db`)

Stores: trades, portfolio snapshots, backtest cache, position ages.

Thread-safe. WAL mode. No external dependencies.

### 2.7 `neo4j_store.py` — Neo4j Analytics Store

**Database:** `trading` on the Neo4j server.

Stores the same data as SQLite but in a graph model (`:Trade`, `:Snapshot`, `:BacktestCache`, `:PositionAge` nodes) enabling cross-system analytics with graph-alpha-bot's `:Ticker`, `:News`, `:Strategy` data.

---

## 3. Production Deployment

### 3.1 Prerequisites

```bash
# Python 3.12+
python3 --version

# Coinbase CLI (must be in PATH, authenticated)
coinbase --version
coinbase auth status   # Should show authenticated

# Virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Optional: Neo4j driver for analytics
pip install neo4j
```

### 3.2 systemd Service Files

#### `/etc/systemd/system/portfolio-optimizer.service`

```ini
[Unit]
Description=Portfolio Optimizer Daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/home/YOUR_USER/git/portfolio-management
ExecStart=/home/YOUR_USER/git/portfolio-management/.venv/bin/python \
    /home/YOUR_USER/git/portfolio-management/portfolio_optimizer.py \
    --interval 300 \
    --db /home/YOUR_USER/optimizer_state.db \
    --neo4j-uri bolt://100.64.43.123:7687 \
    --neo4j-password knowledge_graph_2026 \
    --neo4j-db trading
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal
Environment=PYTHONPATH=/home/YOUR_USER/git/portfolio-management

[Install]
WantedBy=multi-user.target
```

#### `/etc/systemd/system/portfolio-approval.service`

```ini
[Unit]
Description=Portfolio Trade Approval Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=YOUR_USER
WorkingDirectory=/home/YOUR_USER/git/portfolio-management
ExecStart=/home/YOUR_USER/git/portfolio-management/.venv/bin/python \
    /home/YOUR_USER/git/portfolio-management/approval_server.py \
    --port 8080 \
    --pending-file /home/YOUR_USER/pending_approvals.json
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

#### Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable portfolio-optimizer portfolio-approval
sudo systemctl start portfolio-optimizer portfolio-approval
sudo systemctl status portfolio-optimizer portfolio-approval
```

### 3.3 tmux Alternative (if systemd unavailable)

```bash
# Start optimizer
tmux new-session -d -s opt 'python3 portfolio_optimizer.py --interval 300 --live'

# Start approval server  
tmux new-session -d -s approve 'python3 approval_server.py'

# Attach to view logs
tmux attach -t opt

# Detach: Ctrl+B, D
```

### 3.4 Monitoring with journalctl

```bash
# Follow optimizer logs
sudo journalctl -u portfolio-optimizer -f

# Last 100 lines
sudo journalctl -u portfolio-optimizer -n 100

# With timestamps
sudo journalctl -u portfolio-optimizer --since "5 minutes ago"

# Approval server logs
sudo journalctl -u portfolio-approval -f
```

---

## 4. Operations Guide

### 4.1 Normal Startup Sequence

1. Ensure Coinbase CLI is authenticated: `coinbase auth status`
2. Start Neo4j if used: `sudo systemctl start neo4j` (on the Neo4j host)
3. Start approval server: `sudo systemctl start portfolio-approval`
4. Start optimizer: `sudo systemctl start portfolio-optimizer`
5. Verify logs: `sudo journalctl -u portfolio-optimizer -n 20`
6. Open dashboard: `http://YOUR_HOST:8080/status` (approval server status)

### 4.2 Normal Shutdown

```bash
sudo systemctl stop portfolio-optimizer portfolio-approval
```

The optimizer saves state on every tick. It will resume from where it left off.

### 4.3 Health Checks

| Check | Command | Expected |
|-------|---------|----------|
| Optimizer running | `systemctl is-active portfolio-optimizer` | `active` |
| Approval server | `systemctl is-active portfolio-approval` | `active` |
| Recent trade activity | `journalctl -u portfolio-optimizer --since "1 hour ago" \| grep -c "TICK"` | ≥ ticks in period |
| Coinbase CLI auth | `coinbase auth status` | Authenticated |
| DB size | `ls -lh optimizer_state.db` | Grows slowly |
| Neo4j connectivity | `python3 -c "from neo4j import GraphDatabase; d=GraphDatabase.driver('bolt://100.64.43.123:7687',auth=('neo4j','knowledge_graph_2026')); d.verify_connectivity(); d.close(); print('OK')"` | `OK` |

### 4.4 Backup

```bash
# SQLite DB (safe to copy while running — WAL mode)
cp optimizer_state.db backups/optimizer_state_$(date +%Y%m%d).db

# Pending approvals
cp pending_approvals.json backups/pending_$(date +%Y%m%d).json

# Neo4j dump (run on Neo4j host)
docker exec graphalpha-neo4j neo4j-admin dump --database=trading --to=/backups/trading_$(date +%Y%m%d).dump
```

### 4.5 Recovery

```bash
# Corrupt SQLite DB → restore from backup
cp backups/optimizer_state_20250101.db optimizer_state.db

# Empty state → optimizer rebuilds from live Coinbase data automatically

# Neo4j restore
docker exec graphalpha-neo4j neo4j-admin load --database=trading --from=/backups/trading_20250101.dump
```

---

## 5. Email Approval Workflow

### 5.1 Setup

1. Enable 2FA on your Google account
2. Generate App Password: https://myaccount.google.com/apppasswords
3. Start the system:
   ```bash
   python3 approval_server.py &
   python3 portfolio_optimizer.py --live --require-approval \
       --smtp-user your@gmail.com \
       --smtp-password "abcd efgh ijkl mnop" \
       --approval-base-url http://YOUR_IP:8080
   ```

### 5.2 Flow

```
1. Optimizer detects opportunity
2. Preview + risk check pass
3. → Email sent with trade details + approve/deny links
4. User clicks "Approve" or "Deny" in email
5. Approval server handles click, updates pending_approvals.json
6. On next tick, optimizer checks pending_approvals.json
7. If approved → executes trade, logs it
8. If denied → removes from pending, no trade
```

### 5.3 Email Template

Each alert includes:
- Trade type, direction, currency, size
- Expected fee and fee impact
- Backtest metrics: win rate, Sharpe, profit factor, max drawdown
- Expected value (EV) estimate
- Approve and Deny buttons

### 5.4 Security

- The approval server listens on `0.0.0.0:8080` by default. For production:
  - Bind to `127.0.0.1` and use a reverse proxy (nginx/caddy) with HTTPS
  - Or use a VPN/tailscale to expose only to your devices
  - Or use token-based auth on the approval endpoints
- App Password is never stored in logs; pass via environment variable or secure config file
- Approve/deny tokens are UUIDs — not guessable

---

## 6. Database Architecture

### 6.1 SQLite (Primary Hot Store)

**File:** `optimizer_state.db`

- Trade log (append-only, indexed by timestamp)
- Portfolio snapshots (append-only, indexed by timestamp)
- Backtest cache (key-value with TTL, replaced on write)
- Position ages (currency → days held)
- Meta key-value store

**Thread-safe:** threading.Lock around all writes
**WAL mode:** concurrent reads during writes

### 6.2 Neo4j (Analytics Graph)

**Database:** `trading` on `bolt://100.64.43.123:7687`

**Nodes:**
```
(:Trade {id, timestamp, type, side, currency, size_usd, fee, reason, order_id, dry_run})
(:Snapshot {id, timestamp, total_value, holding_count, usdc_balance, fee_tier, holdings_json})
(:BacktestCache {key, strategy, currency, win_rate, sharpe_ratio, profit_factor, max_drawdown_pct, passed})
(:PositionAge {currency, age})
(:Meta {key, value})
```

**Benefits over SQLite:**
- Cross-system join with graph-alpha-bot `:Ticker`, `:News`, `:Strategy` data
- Relationship queries: "which news articles preceded TLH trades?" / "what strategies performed best on correlated assets?"
- Cypher pattern matching for multi-hop portfolio analysis

### 6.3 Dual-Write Strategy

```
Hot Path:  Trade → SQLite (microseconds, local file)
Analytics: Trade → Neo4j (milliseconds, network) — non-blocking, errors logged
Startup:   Load from Neo4j (authoritative) → Sync to SQLite (warm cache)
```

---

## 7. Strategy Engine Reference

| Strategy | Type | Logic | Backtest Threshold |
|----------|------|-------|-------------------|
| EMA Crossover | Trend | 12-period EMA crosses 26-period EMA | WR≥40%, Sharpe>0.2, Pf>1.05 |
| RSI Mean Reversion | Mean Rev | RSI(14) < 30 oversold / > 70 overbought | WR≥40%, Sharpe>0.2, Pf>1.05 |
| Bollinger Breakout | Volatility | Price breaks outside 2σ Bollinger Bands | WR≥40%, Sharpe>0.2, Pf>1.05 |
| Z-Score Reversion | Statistical | Price z-score beyond ±1.5σ on 20-period window | WR≥40%, Sharpe>0.2, Pf>1.05 |
| Volume Momentum | Volume | Volume-weighted price trend over 10 periods | WR≥40%, Sharpe>0.2, Pf>1.05 |

All strategies use 100 hourly candles for backtest validation. A minimum of 3 simulated trades is required for a passing verdict.

---

## 8. Configuration Reference

### 8.1 Fee Tiers (Coinbase Advanced Trade)

| Volume (30d) | Maker | Taker |
|---|---|---|
| $0 | 0.60% | 1.20% |
| $1K | 0.35% | 0.75% |
| $10K | 0.25% | 0.40% |
| $50K | 0.15% | 0.25% |
| $100K | 0.10% | 0.20% |
| $1M | 0.08% | 0.18% |
| $20M | 0.05% | 0.15% |

### 8.2 Target Allocation

| Class | Target | Assets |
|-------|--------|--------|
| Safe | 80% | BTC, USDC, USDT, DAI, ETH |
| Growth | 15% | SOL, LINK, AVAX, DOT, ADA, ATOM, UNI, MATIC |
| Speculative | 5% | Everything else |

Rebalance trigger: >5% absolute deviation from target.

---

## 9. Troubleshooting

### 9.1 "Preview failed, skipping"

**Cause:** Coinbase CLI rejected the order parameters — typically incorrect product ID or size rounding.

**Fix:** Check `best_product()` logic. If the pair doesn't exist (e.g., MON-USDC), the optimizer can't trade that asset. The error is logged at DEBUG level; run with `--once` to reproduce:

```bash
python3 portfolio_optimizer.py --once 2>&1 | grep -i preview
```

### 9.2 "Fee too high, skipping"

**Cause:** The preview estimated fees >2% of trade value. This is a safety guard.

**Fix:** Check the current fee tier (`coinbase fees`). If you're on tier 0 (1.2% taker), small trades are uneconomical. The fee tier volume detector will try to generate volume to reach a better tier.

### 9.3 "Neo4j write failed"

**Cause:** Neo4j server unreachable or credentials changed.

**Fix:** The optimizer continues with SQLite only. Check Neo4j status:

```bash
curl -I http://100.64.43.123:7474  # HTTP browser
```

### 9.4 "Backtest verdict: SKIP (insufficient data)"

**Cause:** Fewer than 30 hourly candles available for the asset, or fewer than 3 backtest trades generated.

**Fix:** Newly listed assets won't have strategy signals until enough history exists. This is normal.

### 9.5 "Order failed" with HTTP 400

**Cause:** Coinbase CLI rejected the order. Common causes:
- Product not found (wrong pair format)
- Size below minimum or above maximum
- Insufficient USDC balance (for BUY)
- Insufficient asset balance (for SELL)

**Fix:** Check the product details with `coinbase products get {PRODUCT_ID}`. The optimizer rounds to `quote_increment`/`base_increment` automatically.

### 9.6 Approval link returns "Invalid Token"

**Cause:** Token not found in `pending_approvals.json`. The optimizer may have restarted and recreated the file, or the token expired.

**Fix:** Check the approval server status page. Denied/expired tokens are removed.

---

## 10. Performance Guidelines

| Trade Size | Fee at Tier 5 | Break-even | Recommendation |
|------------|---------------|------------|----------------|
| $50 | $0.10 | ~$0.15 profit | Minimum threshold (`--min-value 50`) |
| $500 | $1.00 | ~$1.50 profit | OK for strategy signals |
| $5,000 | $10.00 | ~$15 profit | Best for rebalancing |
| $50,000+ | $100+ | Negotiable | Best for fee tier advancement |

For TLH: any trade size is worth it if the tax savings exceed the fee by at least 2×.

---

## 11. Future Roadmap

- [x] Portfolio health analysis
- [x] Continuous optimization daemon
- [x] 5 strategy engines with backtest validation
- [x] SQLite persistence
- [x] Neo4j analytics store
- [x] Email approvals with approve/deny
- [ ] Polymarket event market integration
- [ ] Kalshi event market integration
- [ ] Cross-market arbitrage detection
- [ ] Automated fee tier progression scheduler
- [ ] Telegram/Discord notifications
- [ ] Web UI dashboard (production-grade)
- [ ] Prometheus metrics endpoint
- [ ] Docker containerization
- [ ] CI/CD pipeline for strategy backtests
- [ ] Multi-account support

---

## 12. Quick Reference

```bash
# === Run Everything (production) ===
python3 approval_server.py &                    # Background: approval handler
python3 portfolio_optimizer.py --live \         # Main daemon
    --interval 300 \
    --require-approval \
    --smtp-user you@gmail.com \
    --smtp-password "app password" \
    --neo4j-uri bolt://100.64.43.123:7687 \
    --neo4j-password knowledge_graph_2026

# === Monitor ===
sudo journalctl -u portfolio-optimizer -f      # Live logs
curl http://localhost:8080/status               # Approval dashboard
python3 portfolio_optimizer.py --summary        # Trade summary

# === One-shot analysis ===
python3 portfolio_analyzer.py                   # Full portfolio report
python3 portfolio_analyzer.py --tlh             # Tax-loss harvest candidates

# === Backup ===
cp optimizer_state.db backup_$(date +%Y%m%d).db
cp pending_approvals.json backup_$(date +%Y%m%d).json

# === Emergency stop ===
# No live trades without --live flag. Double-check before adding it.
# To halt execution immediately: kill the process (or systemctl stop)
```
