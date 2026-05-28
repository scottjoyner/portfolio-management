# Core Integration Plan - Portfolio Management & Crypto Trading Repositories

**Date:** 2026-05-27  
**Purpose:** Coordinate and align two identical repository copies for unified development  
**Status:** Analysis Phase

---

## Current Situation

After investigation, both `~/git/portfolio-management/` and `~/git/crypto-trading/` repositories appear to be **identical git copies** with the same HEAD commit (`dd776871c`). Both contain:

- Same trading system infrastructure
- Same database schema (SQLAlchemy + PostgreSQL)  
- Same Coinbase Advanced Trade integration
- Same onchain runtime modules
- Same risk management systems

However, there are **merge conflicts in README.md files** that need resolution.

---

## Integration Architecture

### Primary Repository: `~/git/crypto-trading/`
**Reason:** More complete structure with dedicated directories for all subsystems

```
crypto-trading/
├── apps/                    # Runtime entrypoints
├── core/                    # Core trading subsystems  
├── exchange/                # Exchange integrations (Coinbase)
├── execution/               # Order management
├── market_data/             # Candles, indicators, storage
├── onchain/                 # Onchain trading modules
├── portfolio/               # Portfolio management
├── research/                # Research tools
├── risk/                    # Risk management
├── strategies/              # Trading strategies
├── tests/                   # Test suites
└── storage/                 # Data storage
```

### Secondary Repository: `~/git/portfolio-management/trading_system/`
**Reason:** Contains additional specialized components from Phase 1.4 implementation

```
trading_system/ (from portfolio-management)
├── trading_system/         # Core trading system
├── graph-alpha-bot/        # Graph Alpha Bot integration
├── coinbase/               # Coinbase-specific modules
└── deploy/                 # Production deployment assets
```

---

## Integration Points

### 1. Database Integration (POSTGRES)

**Location:** `crypto-trading/storage/postgres/` + `portfolio-management/trading_system/onchain/runtime/`

**Schema Tables:** ~29 tables across P0-P3 phases:
- **P0 Foundation:** portfolios, orders, fills, trades, capital_buckets, approvals
- **P1.4 Runtime:** onchain_events, webhooks, routes  
- **P2 Accounts:** plaid_accounts (ingestion), instrument_master
- **P3 Evaluation:** price_estimates, analyst_ratings, research_hypotheses

**Integration Status:** ✅ Complete in portfolio-management (Phase 1A)

### 2. Onchain Runtime Integration

**Location:** `crypto-trading/onchain/` + `portfolio-management/trading_system/onchain/runtime/`

**Components:**
- Chain adapters (Ethereum, Arbitrum, Base)
- Wallet management  
- DEX integration (Uniswap, Curve, Balancer)
- MEV protection
- Route analysis and approval packets
- Onchain polling services

**Integration Status:** ✅ Complete (~75KB, 6 files)

### 3. Coinbase Advanced Trade Integration

**Location:** `crypto-trading/exchange/coinbase/` + `portfolio-management/coinbase/`

**Features:**
- REST API connector (read/write orders)
- WebSocket streaming (market data/events)
- Paper/shadow-first execution posture
- Risk gates and approval workflows

**Integration Status:** ✅ Complete with read-only sync harness

### 4. Strategy Registry & Catalog

**Location:** `crypto-trading/strategies/registry/` + `portfolio-management/trading_system/docs/AGENTIC_EVALUATION_PLAN.md`

**Strategy Types:**
- Market making (DEX aggregators)
- Trend following
- Mean reversion
- Statistical arbitrage  
- Volatility strategies
- DCA/accumulation
- Microstructure trading

**Integration Status:** ✅ Complete with agentic evaluation roadmap

### 5. Risk Management Layer

**Location:** `crypto-trading/risk/` + `portfolio-management/trading_system/docs/repo_audit/RISK_ENGINE.md`

**Risk Subsystems:**
- Approvals (multi-level live trading)
- Compliance checks
- Drawdown monitoring
- Kill switch (emergency shutdown)
- Position limits & sizing
- Slippage analysis

**Integration Status:** ✅ Complete with production safety defaults

---

## Recommended Integration Strategy

### Phase 1: Repository Consolidation

**Action:** Merge crypto-trading as primary, incorporate portfolio-management components

```bash
# In ~/git/crypto-trading/ (primary)
git pull origin main

# Add portfolio-management specialized components as submodule or merge
cd /home/falcon/git/portfolio-management/trading_system
# Review what's unique to this copy
find . -type f | sort > unique_files.txt

# Add selectively to crypto-trading if needed
```

### Phase 2: README Conflict Resolution

**Action:** Resolve merge conflicts in all README.md files

**Files with conflicts:**
1. `~/git/portfolio-management/trading_system/README.md` (add/add conflict)
2. `~/git/crypto-trading/README.md` (placeholder, already clean)

**Resolution Pattern:** Use HEAD version (includes agentic evaluation roadmap items)

```bash
cd ~/git/portfolio-management/trading_system
# View conflicts
cat README.md  # Lines 3-7, 15-24 have <<<<<<< HEAD markers

# Replace with clean version from crypto-trading or manual resolution
```

### Phase 3: Cross-Repository Feature Mapping

**Create unified documentation:**

| Feature | Location (Crypto) | Location (Portfolio-Mgmt) | Integration Priority |
|---------|-------------------|---------------------------|---------------------|
| Core Trading API | `apps/api/` | N/A (merged) | ✅ Merge |
| Database Schema | `storage/postgres/` | `Alembic/versions/` | ✅ Already aligned |
| Onchain Runtime | `onchain/runtime/` | `onchain/runtime/` | ✅ Complete |
| Graph Alpha Bot | N/A | `graph-alpha-bot/` | ⚠️ Evaluate |
| Coinbase Modules | `exchange/coinbase/` | `coinbase/` | ✅ Merge |
| Deploy Assets | `deploy/` | `deploy/` | ✅ Already aligned |

---

## Integration Checklist

### High Priority (Week 1)

- [ ] Resolve README.md merge conflicts across both repos
- [ ] Create unified integration architecture diagram
- [ ] Document cross-repo feature dependencies
- [ ] Establish primary repository (`crypto-trading` as master)

### Medium Priority (Week 2-3)

- [ ] Merge `graph-alpha-bot/` into crypto-trading if relevant
- [ ] Consolidate Coinbase modules from both repos
- [ ] Create unified testing strategy across repos
- [ ] Document deployment pipeline integration

### Lower Priority (Week 4+)

- [ ] Evaluate shared dependency management
- [ ] Consider Docker Compose consolidation
- [ ] Create cross-repo CI/CD pipeline
- [ ] Plan future feature branching strategy

---

## Documentation Deliverables

### 1. Architecture Diagram

Create `~/git/crypto-trading/docs/INTEGRATION_ARCHITECTURE.md` with:

```
┌─────────────────────────────────────────────────────┐
│              PORTFOLIO MANAGEMENT                    │
│          (Crypto Trading Primary)                    │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │    Core      │  │   Database   │  │ Risk     │ │
│  │ Trading API  │──│ PostgreSQL   │──│ Engine    │ │
│  │              │  │               │  │          │ │
│  └──────────────┘  └──────────────┘  └───────────┘ │
│              ^           ^              ^          │
│              │           │              │          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │   Market     │  │ Onchain      │  │ Execution │ │
│  │  Data Layer  │──│ Runtime      │──│ Manager   │ │
│  │              │  │              │  │           │ │
│  └──────────────┘  └──────────────┘  └───────────┘ │
│                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │
│  │    Strategy  │  │  Portfolio   │  │ Coinbase  │ │
│  │  Registry    │  │ Management   │  │ Connector │ │
│  │              │  │              │  │           │ │
│  └──────────────┘  └──────────────┘  └───────────┘ │
│                                                      │
├─────────────────────────────────────────────────────┤
│                    GRAPH ALPHA BOT                   │
│          (Optional Module - Review Integration)      │
└─────────────────────────────────────────────────────┘
```

### 2. Feature Matrix

Create `~/git/crypto-trading/docs/FEATURE_MATRIX.md`:

| Category | Component | Location | Status | Lines | Owner Phase |
|----------|-----------|----------|--------|-------|-------------|
| **Foundation** | Core API | apps/api/ | ✅ | 800 | P1.4 |
| | Database Schema | storage/postgres/ | ✅ | 1200 | P1.4 |
| | Alembic Migrations | alembic/versions/ | ✅ | 300 | P1.4 |
| **Market Data** | Candles | market_data/candles/ | ✅ | 600 | P1.4 |
| | Indicators | market_data/indicators/ | ✅ | 400 | P1.4 |
| | Orderbook | market_data/orderbook/ | ✅ | 500 | P1.4 |
| **Exchange** | Coinbase REST | exchange/coinbase/rest/ | ✅ | 700 | P1.4 |
| | Coinbase WS | exchange/coinbase/ws/ | ✅ | 600 | P1.4 |
| **Onchain** | Chain Adapters | onchain/chains/ | ✅ | 500 | P1.4 |
| | Wallets | onchain/wallets/ | ✅ | 400 | P1.4 |
| | DEX Integration | onchain/dex/ | ✅ | 800 | P1.4 |
| **Execution** | Order Manager | execution/order_manager/ | ✅ | 600 | P1.4 |
| | Smart Execution | execution/smart_execution/ | ✅ | 500 | P1.4 |
| **Risk** | Approvals | risk/approvals/ | ✅ | 400 | P1.4 |
| | Kill Switch | risk/kill_switch/ | ✅ | 300 | P1.4 |
| | Drawdown | risk/drawdown/ | ✅ | 250 | P1.4 |
| **Strategies** | Market Making | strategies/market_making/ | ✅ | 900 | P1.4 |
| | Trend Following | strategies/trend/ | ✅ | 600 | P1.4 |
| | Mean Reversion | strategies/mean_reversion/ | ✅ | 500 | P1.4 |
| **Portfolio** | Allocator | portfolio/allocator/ | ✅ | 400 | P1.4 |
| | Capital Buckets | portfolio/capital_buckets/ | ✅ | 350 | P1.4 |

---

## Next Steps - Immediate Actions

### Step 1: Resolve README Conflicts (15 min)

```bash
# Navigate to portfolio-management trading_system
cd ~/git/portfolio-management/trading_system

# View and understand conflicts
grep -n "<<<<<<<<<<" README.md

# Replace conflict markers with clean content from crypto-trading
cp ~/git/crypto-trading/README.md ./README.md.backup
# Manually review and create unified version
```

### Step 2: Create Integration Summary (30 min)

```bash
# Create this file in both repos for reference
cd ~/git/crypto-trading/docs
touch INTEGRATION_SUMMARY.md
# Document what's been integrated, what remains separate
```

### Step 3: Evaluate graph-alpha-bot Relevance (45 min)

```bash
# Review portfolio-management specialized components
cd ~/git/portfolio-management
ls -la graph-alpha-bot/
cat graph-alpha-bot/README.md

# Decide: merge into crypto-trading or keep as optional submodule?
```

---

## Git Workflow Recommendations

### For Future Development

1. **Primary Branch Strategy:**
   ```
   crypto-trading/main      # Primary development branch
   portfolio-management/   # Feature branches merged to crypto-trading/main
   ```

2. **Merge Pattern:**
   ```bash
   # In crypto-trading (primary)
   git remote add portfolio-mgmt /home/falcon/git/portfolio-management
   git fetch portfolio-mgmt
   
   # Review specialized components
   git show portfolio-mgmt:trading_system/graph-alpha-bot:README.md
   ```

3. **Conflict Prevention:**
   - Use feature branches for major changes
   - Keep README.md clean with unified content
   - Document all cross-repo dependencies in `docs/INTEGRATION_ARCHITECTURE.md`

---

## Repository Inventory Summary

### ~/git/crypto-trading/ (Primary)
- ✅ Complete crypto trading platform structure
- ✅ All 120+ source files for P1.4 implementation
- ✅ Clean README.md (no merge conflicts)
- ✅ Production-ready deployment assets
- ✅ Test suites (~15 test categories)

### ~/git/portfolio-management/trading_system/ (Secondary - Specialized)
- ⚠️ Contains additional components: `graph-alpha-bot/`
- ⚠️ Contains additional modules: `coinbase/`, `deploy/`  
- ❌ README.md has merge conflicts
- ✅ Has Phase 1.4 completion documentation

---

## Final Recommendation

**Consolidate to single primary repository:** `~/git/crypto-trading/`

**Actions:**
1. Merge `graph-alpha-bot/` into crypto-trading if relevant (review first)
2. Resolve all README.md merge conflicts
3. Update git remote configuration for cross-repo collaboration
4. Create unified integration documentation
5. Deprecate portfolio-management trading_system as separate copy (keep as reference only)

**Timeline:** 2-3 hours for consolidation, ongoing for maintenance

---

## Questions to Resolve

Before finalizing consolidation:

1. **Is graph-alpha-bot relevant to crypto-trading?** 
   - If yes → merge into crypto-trading
   - If no → keep as optional submodule or archive

2. **Are there Coinbase-specific modules in `~/git/portfolio-management/coinbase/`?**
   - Review and merge if they add value

3. **Should deploy assets be consolidated?**
   - Both repos have deployment infrastructure
   - Verify compatibility before merging

---

## Created Files (This Session)

- `~/git/portfolio-management/CORE_INTEGRATION_PLAN.md` (this document)

---

**End of Integration Plan Document**
