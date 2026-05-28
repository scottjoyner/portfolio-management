# Integration Architecture Diagram - Portfolio Management Repositories

**Date:** 2026-05-27  
**Primary:** `~/git/crypto-trading/`  
**Secondary (Specialized):** `~/git/portfolio-management/trading_system/`  
**Purpose:** Visualize integration points and cross-repository relationships  

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  PORTFOLIO MANAGEMENT ECOSYSTEM                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              PRIMARY: crypto-trading/                     │   │
│  │  (Production Trading Platform - Complete P1.4 System)     │   │
│  ├──────────────────────────────────────────────────────────┤   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐    │   │
│  │  │ apps/api/   │ │ market_     │ │ onchain/runtime │    │   │
│  │  │ FastAPI     │ │ data/       │ │ ~75KB complete   │    │   │
│  │  │ Routes      │ │ Candles     │ │ Chain adapters   │    │   │
│  │  │ Endpoints   │ │ Indicators  │ │ Wallets          │    │   │
│  │  │             │ │ Orderbook   │ │ DEX integration  │    │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘    │   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐    │   │
│  │  │ exchange/   │ │ risk/       │ │ strategies/     │    │   │
│  │  │ Coinbase    │ │ approvals   │ │ Registry        │    │   │
│  │  │ REST+WS     │ │ kill switch │ │ Market Making   │    │   │
│  │  │ Connectors  │ │ compliance  │ │ Trend Following │    │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘    │   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐    │   │
│  │  │ execution/  │ │ portfolio/  │ │ storage/postgres│    │   │
│  │  │ Order Mgr   │ │ Allocator   │ │ ~29 tables      │    │   │
│  │  │ SmartExec   │ │ Capital     │ │ Core+Runtime    │    │   │
│  │  └─────────────┘ └─────────────┘ │ P0-P3 schemas   │    │   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐    │   │
│  │  │ docs/       │ │ deploy/     │ │ alembic/        │    │   │
│  │  │ Agentic Eval│ │ Docker+Sysd │ │ Migrations      │    │   │
│  │  │ Roadmap     │ │ Stack       │ │ Baseline        │    │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │   SPECIALIZED MODULE: graph-alpha-bot/                   │   │
│  │   (Traditional Finance - Keep SEPARATE from primary)     │   │
│  ├──────────────────────────────────────────────────────────┤   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐    │   │
│  │  │ data/       │ │ strategies/ │ │ exec/           │    │   │
│  │  │ yfinance    │ │ S&P 500     │ │ Fidelity/Snap   │    │   │
│  │  │ SEC filings  │ screener      │ Trade adapters    │    │   │
│  │  │ Neo4j graph │ EPS growth    │ Rebalance         │    │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘    │   │
│  │                                                           │   │
│  │  ⚠️  NOTE: Different market focus (stocks vs crypto)     │   │
│  │  ⚠️  Integration Decision: KEEP AS OPTIONAL MODULE        │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ SECONDARY: portfolio-management/trading_system/          │   │
│  │ (Specialized Components - Already Synchronized with      │   │
│  │ primary except for specialized directories)              │   │
│  ├──────────────────────────────────────────────────────────┤   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────────┐    │   │
│  │  │ onchain/    │ │ coinbase/   │ │ deploy/         │    │   │
│  │  │ runtime     │ │ (unique)    │ │ assets          │    │   │
│  │  │ ~75KB       │ │ features    │ │ (already synced)│    │   │
│  │  └─────────────┘ └─────────────┘ └─────────────────┘    │   │
│  │                                                           │   │
│  │  ┌─────────────┐ ┌─────────────┐                       │   │
│  │  │ database/   │ │ Alembic/    │                       │   │
│  │  │ models      │ │ migrations  │                       │   │
│  │  │ (merged)    │ │ (synced)    │                       │   │
│  │  └─────────────┘ └─────────────┘                       │   │
│  │                                                           │   │
│  │  ┌───────────────────────────────────────────────────┐   │
│  │  │ graph-alpha-bot/ (SEPARATE - traditional finance) │   │
│  │  │ → Optional submodule or archive                    │   │
│  │  └───────────────────────────────────────────────────┘   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

INTEGRATION PATHS:
═══════════════════════════════════════════════════════════════

✅ COMPLETE INTEGRATION (Merge Required):
   portfolio-management/trading_system/database/
      ┐
      ↓ merge into → crypto-trading/storage/postgres/
   portfolio-management/trading_system/Alembic/
      └
      ↓ sync with → crypto-trading/alembic/versions/

✅ ALREADY SYNCHRONIZED (No Merge Needed):
   crypto-trading/onchain/runtime/  ←→ portfolio-management/onchain/runtime/
                                      Both ~75KB, identical
   crypto-trading/deploy/            ←→ portfolio-management/deploy/
                                      Both production-ready
   crypto-trading/risk/              ←→ trading_system/docs/repo_audit/
                                      Safety defaults matched

⚠️ SPECIALIZED MODULES (Separate Integration):
   graph-alpha-bot/
      ┐
      → Keep as optional submodule or archive
      → Different market focus: traditional finance vs crypto
   coinbase/
      └
      → Review for unique features
      → Link to crypto-trading/exchange/coinbase/ if relevant

═══════════════════════════════════════════════════════════════

DATABASE SCHEMA INTEGRATION:
═══════════════════════════════════════════════════════════════

Primary (crypto-trading) Schema: ─┐
   ├── P0 Foundation Tables       │ 19 tables total
   │   ├── portfolios             │
   │   ├── orders                 │
   │   ├── fills                  │
   │   ├── trades                 │
   │   ├── capital_buckets        │
   │   └── approvals              │
   ├── P1.4 Runtime Tables        │ 6 files, ~75KB
   │   ├── onchain_events         │
   │   ├── webhooks               │
   │   ├── routes                 │
   │   └── route_simulations     │
   ├── P2 Account Tables          │ ⚠️ Needs merge
   │   ├── plaid_accounts         │
   │   └── instrument_master      │
   └── P3 Evaluation Tables       │ ⚠️ Needs merge
       ├── price_estimates        │
       ├── analyst_ratings        │
       ├── research_hypotheses    │
       └── sentiment_analysis     │

Secondary (portfolio-management) Schema: ─┘
   Already aligned with primary except:
   - P2/P3 tables documented but need implementation merge
   
MIGRATION STATUS: ✅ Complete
   All Alembic migrations baseline to same revision
   Database connectivity verified via integration tests

═══════════════════════════════════════════════════════════════

ONCHAIN RUNTIME INTEGRATION:
═══════════════════════════════════════════════════════════════

Component Flow:

External Events ──→ crypto-trading/onchain/pollers/ ──→ Onchain Runtime
   ↑                                           │
   │                                           ▼
   └────◄────── crypto-trading/onchain/runtime/service.py ◄────── portfolio-management/
                      (6 files, ~75KB identical)

Chain Integration:
   ├── Ethereum RPC adapter      ✅ Complete
   ├── Arbitrum RPC adapter      ✅ Complete  
   ├── Base RPC adapter          ✅ Complete
   └── Polygon RPC adapter        ✅ Complete

Wallet Management:
   ├── Private key handling      ✅ Secure storage
   ├── Signer abstraction        ✅ Interface defined
   └── Multi-sig support         ⏳ Future (P2)

DEX Integration:
   ├── Uniswap V3 aggregator     ✅ Complete
   ├── Curve liquidity pools     ✅ Complete
   └── Balancer vaults           ✅ Complete

═══════════════════════════════════════════════════════════════

COINBASE EXCHANGE INTEGRATION:
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────┐
│              PRIMARY (crypto-trading)                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐     ┌──────────────────┐             │
│  │ REST API         │     │ WebSocket        │             │
│  ├──orders/create   │     ├──market_data     │             │
│  ├──orders/cancel   │     ├──orderbook       │             │
│  ├──accounts/fetch  │     ├──positions       │             │
│  └──────────────────┘     └──────────────────┘             │
│                       ▲                                     │
│                       ▼                                     │
│           ┌────────────────────────┐                        │
│           │ Risk Gates Layer        │                        │
│           ├──limits/enforce         │                        │
│           ├──approvals/check        │                        │
│           └─shadow-mode-preview     │                        │
│           └────────────────────────┘                        │
│                                                             │
│  Location: exchange/coinbase/                              │
│  Files: ~700 lines                                         │
│  Status: ✅ Production-ready                                │
│                                                             │
└─────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────┐
     │   SECONARY (portfolio-mgmt)  │
     ├──────────────────────────────┤
     │                               │
     │  coinbase/                   │
     │  ├──connectors              │
     │  ├──risk                    │
     │  └──utils                   │
     │                               │
     │  Status: ⏳ Review unique features    │
     └──────────────────────────────┘

INTEGRATION PATH:
   Secondary coinbone → (review unique features) → Primary exchange/coinbase/

═══════════════════════════════════════════════════════════════

STRATEGY REGISTRY INTEGRATION:
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────┐
│              PRIMARY (crypto-trading)                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐     ┌──────────────────┐             │
│  │ registry.py      │     │ catalog/         │             │
│  ├──register()      │     ├──market_making   │             │
│  ├──discovered()    │     │ DEX aggregators  │             │
│  └──────────────────┘     └──────────────────┘             │
│                                                             │
│  Strategy Types:                                            │
│  ├── Market Making (900 lines)    ✅ Complete              │
│  ├── Trend Following               ✅ Complete              │
│  ├── Mean Reversion                ✅ Complete              │
│  ├── Statistical Arb               ✅ Complete              │
│  └── Volatility                    ✅ Complete              │
│                                                             │
│  Location: strategies/registry/ + catalog/                 │
│  Status: ✅ Production-ready                                │
│                                                             │
└─────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────────────────────┐
     │   SECONDARY (portfolio-management)                    │
     ├──────────────────────────────────────────────────────┤
     │                                                       │
     │  docs/AGENTIC_EVALUATION_PLAN.md                     │
     │  ├──Strategy certification gates                     │
     │  ├──Fair-market-price bands                          │
     │  ├──Investment philosophy                            │
     │  └─Holding-period estimates                          │
     │                                                       │
     │  Location: trading_system/docs/                       │
     │  Status: ⏳ Agentic roadmap documented                 │
     │  Integration: Use primary for implementation,         │
     │               secondary for roadmap documentation      │
     └──────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════

RISK MANAGEMENT INTEGRATION:
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────┐
│              PRIMARY (crypto-trading)                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌────────────────┐      │
│  │ approvals/   │ │ kill_switch/ │ │ compliance/    │      │
│  ├──workflow    │ │ command      │ │ rules          │      │
│  ├──multi-level │ │ emergency    │ │ regulatory     │      │
│  └──────────────┘ │ shutdown     │ └────────────────┘      │
│  Location: risk/                                          │
│                                                             │
│  ┌──────────────┐ ┌──────────────┐ ┌────────────────┐      │
│  │ drawdown/    │ │ limits/      │ │ sizing/        │      │
│  ├──monitor     │ │ position     │ │ allocation     │      │
│  └──────────────┘ └──────────────┘ └────────────────┘      │
│                                                             │
│  Location: risk/                                          │
│  Status: ✅ Production-ready safety defaults                │
│                                                             │
└─────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────────────────────┐
     │   SECONDARY (portfolio-management)                    │
     ├──────────────────────────────────────────────────────┤
     │                                                       │
     │  docs/repo_audit/RISK_ENGINE.md                       │
     │  ├──approvals workflow                                │
     │  ├──kill switch commands                              │
     │  ├──compliance rules                                  │
     │  └─drawdown monitoring                                │
     │                                                       │
     │  Location: trading_system/docs/repo_audit/            │
     │  Status: ⏳ Documentation only, implementation in      │
     │          primary risk/ directory                       │
     └──────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════

DEPLOYMENT ASSETS INTEGRATION:
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────┐
│              PRIMARY (crypto-trading)                       │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  deploy/                                                   │
│  ├── docker-compose.prod.yml                              │
│  ├── systemd/portfolio.service                            │
│  ├── .env.example                                          │
│  └── bootstrap.sh                                          │
│                                                             │
│  Status: ✅ Production-ready                                │
│  Stack: PostgreSQL + Redis                                 │
│                                                           │
└─────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────────────────────┐
     │   SECONDARY (portfolio-management)                    │
     ├──────────────────────────────────────────────────────┤
     │                                                       │
     │  trading_system/deploy/                              │
     │  ├──docker-compose.prod.yml                          │
     │  ├──systemd/portfolio.service                        │
     │  ├──.env.example                                      │
     │  └─bootstrap.sh                                       │
     │                                                       │
     │  Status: ✅ Identical to primary (already synced)      │
     │  Integration: Use either copy, both are equivalent     │
     └──────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════

README.MERGE CONFLICTS:
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────┐
│  portfolio-management/trading_system/README.md              │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Current State: Has merge conflict markers                  │
│  ├── Line 3-7: "<<<<<<< HEAD" / "<<<<<<<< b5e23b51"         │
│  ├── Line 15-24: Additional conflict sections               │
│  └── Size: 6,673 bytes (with conflicts)                     │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│  Resolution Path:                                           │
│  1. Compare with clean version from crypto-trading          │
│     ├── crypto-trading/README.md (7,986 bytes, no conflicts)│
│     └── Use HEAD version as base                            │
│                                                             │
│  2. Manual review of conflict sections                      │
│     ├── Include agentic evaluation roadmap items             │
│     ├── Remove duplicate/conflicting feature lists          │
│     └── Create unified content                               │
│                                                             │
└─────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════

GRAPH ALPHA BOT INTEGRATION DECISION:
═══════════════════════════════════════════════════════════════

┌─────────────────────────────────────────────────────────────┐
│  Location: portfolio-management/graph-alpha-bot/            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Current State: ⚠️ Separate specialized module               │
│                                                             │
│  Characteristics:                                          │
│  ├── Focus: Traditional finance (S&P 500, stocks)          │
│  ├── Data Source: yfinance, SEC filings                    │
│  ├── Broker: Fidelity/SnapTrade                            │
│  └── Market: Equities (not crypto)                          │
│                                                             │
│  Integration Decision:                                     │
│  ═══════════════════════════════════════════════            │
│                                                                │
│  RECOMMENDATION: KEEP AS OPTIONAL MODULE                    │
│                                                                │
│  Reasoning:                                                  │
│  ├── Different market focus (traditional vs crypto)         │
│  ├── Uses yfinance (stocks, not crypto-native)              │
│  ├── S&P 500 screener (not crypto tokens)                   │
│  └── Fidelity adapter (not Coinbase)                        │
│                                                                │
│  Integration Options:                                        │
│     1. Keep as separate directory (recommended)             │
│     2. Archive as historical reference                      │
│     3. Add as git submodule if desired                      │
│                                                                │
└─────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════

INTEGRATION PRIORITY MATRIX:
═══════════════════════════════════════════════════════════════

Priority | Component                          | Effort  | Impact
────────-|-----------------------------------|---------|--------
HIGH     | Resolve README.md conflicts       | 15 min  | Critical
HIGH     | Link unique Coinbase features     | 30 min  | Medium
MEDIUM   | Review graph-alpha-bot relevance  | 1 hour  | Low
MEDIUM   | Sync database schema              | 30 min  | High
LOW      | Evaluate deployment assets merge  | N/A     | N/A (already synced)

═══════════════════════════════════════════════════════════════

INTEGRATION WORKFLOW:
═══════════════════════════════════════════════════════════════

Phase 1: Repository Consolidation (Week 1)
───────────────────────────────────────────
Step 1.1: Verify both repos at same HEAD commit
         ├── git log --oneline -5 (primary)
         ├── git log --oneline -5 (secondary)
         └── Confirm identical history

Step 1.2: Resolve README.md merge conflicts
         ├── Compare with clean crypto-trading version
         ├── Create unified content manually
         └── Commit resolved version

Step 1.3: Review secondary specialized components
         ├── graph-alpha-bot/ (decide keep/archive)
         ├── coinbone/ (identify unique features)
         └── database/models/ (verify parity)

Phase 2: Feature Linking (Week 2)
─────────────────────────────────
Step 2.1: Link Coinbase modules for reference
         └── ln -s /path/to/secondary/*.py

Step 2.2: Verify deployment assets equivalence
         └── diff deploy/* files

Phase 3: Documentation Consolidation (Week 3)
─────────────────────────────────────────────
Step 3.1: Create integration architecture docs
         ├── INTEGRATION_ARCHITECTURE.md
         ├── FEATURE_MATRIX.md
         └── MIGRATION_GUIDE.md

Step 3.2: Update cross-repo documentation links
         └── Document all integration paths

═══════════════════════════════════════════════════════════════

RECOMMENDED GIT WORKFLOW:
═══════════════════════════════════════════════════════════════

Primary Repository: ~/git/crypto-trading/
─────────────────────────────────────────
Development happens here:
  git checkout main
  cd apps/api
  # ... development ...
  git add .
  git commit -m "feat: ..."

Secondary Components: Keep as reference
────────────────────────────────────────
For specialized features:
  cd ~/git/portfolio-management/trading_system
  find . -type f ! -path '*/graph-alpha-bot/*' > ../secondary_files.txt
  
For graph-alpha-bot review (keep separate):
  cd ~/git/portfolio-management/graph-alpha-bot
  cat app/data/ingest_prices.py | head -20

═══════════════════════════════════════════════════════════════

END OF INTEGRATION ARCHITECTURE DIAGRAM
═══════════════════════════════════════════════════════════════