# Human TODO — Production Readiness Checklist

> Actions needed from you before the system is fully operational.

## What I still need from you

### 1. Coinbase API - Enable Trading Scope

**Status:** 🔴 Blocks execution

The Coinbase CLI/API key still needs trade scope for live order submission. Without it, execution stays paper/read-only.

```bash
# Verify current state:
coinbase orders create product_id=BTC-USDC side=BUY type=market \
  quote_size=10.00 client_order_id=$(uuidgen)
# → "Missing required scopes"
```

**Fix:** Regenerate the API key at https://exchange.coinbase.com/settings/api with `trade` scope checked.

**Needed from you:**
- Coinbase API key with trade permission
- Coinbase API secret
- Confirmation that the key is allowed to trade the account you want used

---

### 2. Gmail App Password - Email Notifications

**Status:** 🟡 Ready, needs credentials

The `TradeNotifier` and approval workflow are built and tested. It needs a Gmail App Password to send approve/deny links.

**Steps:**
1. Enable 2FA on your Google account (if not already)
2. Go to https://myaccount.google.com/apppasswords
3. Generate an App Password (16-char spaced format, e.g. `abcd efgh ijkl mnop`)
4. Provide it when running the optimizer:

```bash
python3 portfolio_optimizer.py --live --require-approval \
    --smtp-user you@gmail.com \
    --smtp-password "abcd efgh ijkl mnop" \
    --approval-base-url http://YOUR_IP:8080
```

**Needed from you:**
- Gmail address to send from
- Gmail App Password
- The public or LAN URL the approval links should use

---

### 3. Kalshi API Credentials

**Status:** 🟡 Ready, needs credentials

The Kalshi connector is built and read-only capable, but live authenticated access needs your Kalshi login email + password.

```bash
python3 portfolio_optimizer.py --kalshi-email you@example.com --kalshi-password your_pass
```

Kalshi does SHA256-based authentication. The client logs in once, gets a token, and re-authenticates on 401.

**Needed from you:**
- Kalshi email
- Kalshi password

---

### 4. Polymarket Write Access

**Status:** 🟡 Read-only works, write access needs wallet material

The Polymarket connector can read markets without credentials, but live order submission needs wallet signing inputs.

**Needed from you if you want live Polymarket trading:**
- Wallet address
- Private key or signing setup you’re comfortable using

---

### 5. Neo4j - Verify Analytics Database

**Status:** ✅ Connected and validated

The `trading` database was created on `100.64.43.123:7687`. All constraints are in place. Data is being dual-written from the optimizer.

**Needed only if the password changes:**
- Updated `NEO4J_PASSWORD`

**Verify:**
```bash
python3 -c "from neo4j_store import Neo4jStore; s=Neo4jStore(uri='bolt://100.64.43.123:7687',password='knowledge_graph_2026',database='trading'); print(s.stats()); s.close()"
```

---

### 6. Python Dependencies

**Status:** ⚠️ May need installation

Ensure these are installed in your `.venv`:

```bash
.venv/bin/pip install neo4j       # Required for Neo4jStore
.venv/bin/pip install streamlit   # Required for dashboard
# No other external deps — all other modules use stdlib (smtplib, http.server, urllib, sqlite3, json)
```

---

### 7. Polymarket - Active Markets

**Status:** 🟢 Connected, but no active crypto markets

The Polymarket CLOB API returns 1000 markets but none currently have `accepting_orders=true`. The connector is correctly filtering and will detect active markets when they appear. No action needed.

---

### 8. Production Deployment

**Status:** 🟡 systemd service files ready, needs customization

Edit the systemd files in `runbook.md` with your actual user home path, then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable portfolio-optimizer portfolio-approval
sudo systemctl start portfolio-optimizer portfolio-approval
```

---

### 9. Capital Policy Presets

**Status:** ✅ Editable from dashboard

The reserve/core/opportunity split is now configurable from the dashboard. No key is needed here.

If you want a different default posture, tell me which preset should be the starting point:
- Conservative
- Balanced
- Aggressive

---

### 10. Confidence Matrix - Strategy Weighting

**Status:** ✅ Implemented (10 strategies)

The confidence matrix now aggregates signals from 10 strategies (5 original + 5 new). Strategy independence groups prevent over-weighting correlated signals. Asset-class-specific strategy mappings are configurable.

---

## Quick-Start (Once Credentials Are Ready)

```bash
# Terminal 1: Approval server
python3 approval_server.py

# Terminal 2: Optimizer (live)
python3 portfolio_optimizer.py --live \
    --require-approval \
    --smtp-user you@gmail.com \
    --smtp-password "abcd efgh ijkl mnop" \
    --neo4j-uri bolt://100.64.43.123:7687 \
    --neo4j-password knowledge_graph_2026 \
    --polymarket \
    --kalshi-email you@example.com \
    --kalshi-password your_pass

# Terminal 3: Monitor
curl http://localhost:8080/status
journalctl -u portfolio-optimizer -f
```
