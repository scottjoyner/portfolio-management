# Falcon Portfolio Management - May 27, 2026 Action Plan

## Mission Statement
Complete production hardening of portfolio management system, clearing P0→P1 backlog for staging deployment to fleet.

---

## 📋 EXECUTIVE SUMMARY

**Current State**: P0 clearance complete (Alembic migrations committed, Coinbase read-only sync deployed, E2e tests written). Awaiting P0.3 artifact cleanup and systematic P1 completion.

**Goal**: Transform partially-wired signal→fill pipeline into production-ready system with:
- Clean repo (no runtime artifacts in source tree)
- Fully functional WebSocket market feed hub
- Onchain ingestion runtime
- Rate limiting + Redis pub/sub hardening
- Deployment smoke scripts + health checks

**Timeline**: 3-4 sessions (~6-8 hours total work)
- Session 1: P0 Clearance (artifact audit, CI validation, migration smoke tests) ✅ IN PROGRESS
- Session 2: P1.2 WebSocket Wiring (hub→worker connection, market feed testing)
- Session 3: P1.4 Onchain Runtime (RPC poller, token metadata, safety scoring)
- Session 4: P2 Hardening Suite (rate limiting, Redis, smoke scripts, secrets mgmt)

---

## 🎯 BREAKDOWN BY PRIORITY LEVEL

### **P0 — BLOCKERS BEFORE STAGING** (30-60 mins total)
*Must complete before any production deployment to fleet.*

#### **P0.1 ✅ Complete - Alembic Migration Baseline Committed**
- **Status**: COMPLETE
- **Deliverables**:
  - `trading_system/alembic/versions/0001_initial.py` (211 lines)
  - `trading_system/docs/MIGRATION_GUIDE.md` (5.3KB)
  - CI integration with upgrade verification
  - Table definitions for all 15 models
  
- **Verification**:
  ```bash
  alembic current  # Should show latest revision
  echo "P0.1 ✅ Complete"
  ```

#### **P0.2 ✅ Complete - DB-backed Integration Harness Deployed**
- **Status**: COMPLETE
- **Deliverables**:
  - 5 read-only sync API endpoints (`apps/api/ops_layer.py`)
  - `tests/e2e/test_coinbase_sync.py` (4.3KB)
  - Migration smoke tests (`tests/migrations/test_smoke.py`)
  
- **Verification**:
  ```bash
  pytest trading_system/tests/e2e/test_coinbase_sync.py -v
  echo "P0.2 ✅ Complete"
  ```

#### **P0.3 ⏳ IN PROGRESS — Remove Runtime Artifacts from Repo**
- **Status**: NEEDS AUDIT
- **Scope**: 
  - Audit `.gitignore` for generated files that should NOT be committed
  - Move runtime artifacts to `tests/fixtures/` or `docs/evidence/`
  - Examples of artifacts to review:
    - `trading_system/.pytest_cache/`
    - `trading_system/src/__pycache__/`
    - Any database dump files
    - Build artifacts (`.so`, `.pyc`)
  
- **Action Items**:
  ```bash
  # Step 1: Audit current .gitignore
  cat trading_system/.gitignore
  
  # Step 2: Find generated files in source tree
  find trading_system -type d -name "__pycache__" -o -name ".pytest_cache" -o -name ".venv"
  
  # Step 3: Review runtime state files that shouldn't be tracked
  grep -r "\.sql\.json\|\.sqlite\|fixtures/" trading_system --include="*.py" | head -20
  ```

- **Acceptance Criteria**:
  - No `.gitignore` entries pointing to source directory
  - All generated files either gitignored or moved to proper locations
  - CI pipeline passes on clean repo (no pre-built artifacts)

#### **P0-complete ✅ — All P0 Items Cleared**
- **Status**: Awaiting P0.3 verification
- **Milestone**: System ready for staging deployment once P0.3 cleared

---

### **P1 — CORE RUNTIME WIRING** (2-3 sessions)
*Complete partially-built functionality, prepare for production traffic.*

#### **P1.1 ✅ Complete - Signal-to-Fill E2E Workflow Written**
- **Status**: COMPLETE (test coverage only, runtime partial)
- **Deliverables**:
  - `tests/e2e/test_signal_to_fill.py` (10.2KB)
  - Test classes: `TestPaperModeE2E`, `TestPaperOrderLifecycle`, etc.
  
#### **P1.2 ⏳ IN PROGRESS — WebSocket Hub to Worker Wiring**
- **Status**: PARTIAL (client built, hub needs wiring)
- **Current State**:
  - ✅ Market feed client: `exchange/coinbase/websocket/market_feed.py` (6.3KB)
  - ✅ API route wired: `/ws/market/{product_id}`
  - ❌ Hub pub/sub not connected to worker loop
  
- **Scope**:
  ```bash
  # Wire hub to worker consumption
  # Files to modify:
  #   - apps/worker/main.py (add subscription)
  #   - exchange/coinbase/websocket/hub.py (if exists, else create)
  #   - apps/api/ws_router.py (subscribe to topics)
  ```

- **Acceptance Criteria**:
  ```bash
  # Worker subscribes to market feed:
  curl "http://localhost:8000/api/v1/worker/status" | grep "subscribed"
  
  # Verify market events flowing through hub:
  curl "ws://localhost:8000/ws/market/BTC-USD" &
  
  echo "P1.2 ✅ Complete"
  ```

#### **P1.3 ✅ Complete - Coinbase Live Connector Staging Harness**
- **Status**: COMPLETE
- **Deliverables**:
  - Read-only sync API with 5 endpoints
  - Credential gating with safe defaults
  - `COINBASE_READ_ONLY_SYNC_DEPLOYMENT.md` (10.5KB)
  
#### **P1.4 ⏳ TO DO — Onchain Ingestion Runtime**
- **Status**: NEEDS IMPLEMENTATION
- **Scope**:
  ```bash
  # Build RPC poller service for Ethereum/Base pools:
  #   - Pool event listener (Uniswap v3, Curve, Balancer)
  #   - Token metadata fetching service
  #   - Safety scoring before route approval
  #   - Ingestion monitoring dashboard
  ```

- **Acceptance Criteria**:
  ```bash
  # Health check:
  curl "http://localhost:8001/onchain/health"
  
  # Poller status:
  curl "http://localhost:8001/onchain/poller/status" | grep "active"
  
  echo "P1.4 ✅ Complete"
  ```

---

### **P2 — PRODUCTION HARDENING** (3-4 sessions)
*Make system fleet-ready with reliability features.*

#### **P2.1 ⏳ TO DO - Rate Limiting Middleware**
- **Status**: NEEDS IMPLEMENTATION
- **Tech**: `fastapi` + `slowapi` or custom middleware
- **Scope**:
  ```python
  # Implement tiered rate limits:
  #   - Anonymous: 10 req/min
  #   - Authenticated: 60 req/min
  #   - WebSocket: 5 frames/sec per connection
  ```

#### **P2.2 ⏳ TO DO - Redis-backed Pub/Sub Hub**
- **Status**: NEEDS IMPLEMENTATION
- **Tech**: `redis` + `aioredis` (async)
- **Scope**:
  ```bash
  # Replace memory pub/sub with Redis:
  #   - Cluster-ready architecture
  #   - Persistence enabled
  #   - Pub/sub channel management
  ```

#### **P2.3 ⏳ TO DO - Deployment Smoke Scripts**
- **Status**: NEEDS IMPLEMENTATION
- **Scope**:
  ```bash
  # Create deployment verification scripts:
  #   - check_health.sh (verify all endpoints)
  #   - verify_migration.sh (validate DB schema)
  #   - rollback_checklist.sh (emergency procedures)
  ```

#### **P2.4 ⏳ TO DO - Secrets/Key Management Plan**
- **Status**: NEEDS DOCUMENTATION
- **Scope**:
  - `trading_system/docs/secrets_management_plan.md` implementation
  - Environment variable templates per tier
  - Key rotation procedures

#### **P2.5 ⏳ TO DO - Operator UI/API Contract Hardening**
- **Status**: NEEDS DOCUMENTATION
- **Scope**:
  - API contract documentation (OpenAPI spec validation)
  - Error message standardization
  - Rate limit response headers

---

### **P3 — COMPLETENESS & RESEARCH** (Optional/Long-term)
*Enhance quality gates and research capabilities.*

#### **P3.1 Strategy Catalog Quality Gates**
- Scope: Validate all strategy implementations against spec

#### **P3.2 Backtesting Evidence Pack**
- Scope: Historical performance data, risk metrics, edge cases

#### **P3.3 Advanced Onchain Modules**
- MEV detection and arbitrage modules
- Cross-chain bridge integration (LayerZero/Hedera)
- DEX routing optimizer
- LP position management automation
- Gas optimization strategies

#### **P3.4 Documentation System**
- Mkdocs site deployment
- CI/CD documentation pipeline
- Auto-generated API docs

---

## 🚀 SESSION 1 EXECUTION PLAN (Today)

### **PHASE 1: P0.3 Audit & Cleanup** (~45 mins)
```bash
# Step 1.1: Review current .gitignore status
echo "=== P0.3 Phase 1: Artifact Audit ==="
cat trading_system/.gitignore

# Step 1.2: Find all generated files that shouldn't be committed
find trading_system -type d \( -name "__pycache__" -o -name ".pytest_cache" \) | xargs echo "Generated directories:"

# Step 1.3: Check for any runtime artifacts in source tree
grep -rl "\.sql\.json\|\.sqlite$" trading_system --include="*.py" || echo "No loose sqlite files found"

# Step 1.4: If issues found, move to tests/fixtures/ or docs/evidence/
```

**Action**: Audit completed → Report findings in terminal output

### **PHASE 2: P0 Clearance Verification** (~15 mins)
```bash
# Run migration smoke test to verify P0.1/P0.2 are solid
pytest trading_system/tests/migrations/test_smoke.py -v

# Verify read-only sync endpoints work
pytest trading_system/tests/e2e/test_coinbase_sync.py -v -k "test_exchange_accounts"
```

**Action**: Verification run → Update status in progress file

### **PHASE 3: P0-complete Milestone Check** (~5 mins)
```bash
# Confirm all P0 items are either complete or have clear action plan
echo "=== P0 CLEARANCE SUMMARY ==="
echo "P0.1 Alembic Baseline: ✅ COMPLETE"
echo "P0.2 Integration Harness: ✅ COMPLETE"  
echo "P0.3 Artifact Cleanup: ⏳ IN PROGRESS (audit pending)"
echo "P0-complete: ✅ READY FOR STAGING (after P0.3 cleared)"
```

---

## 📊 EXPECTED DELIVERABLES (Session 1)

After completing today's work:

- [ ] `.gitignore` audited and updated if needed
- [ ] All generated files properly gitignored or relocated
- [ ] Migration smoke tests passing
- [ ] Read-only sync endpoints validated
- [ ] P0-complete milestone reached
- [ ] Terminal output documenting what was done/remaining

**Success Metrics**:
- Clean repo (no untracked generated files)
- CI pipeline passes on fresh clone
- All P0 items in "complete" or "clear action plan" state

---

## 🎯 SESSION 2 PLAN (Next Session)

### **Focus: P1.2 WebSocket Hub Wiring**
```bash
# Tasks:
[ ] Wire apps/worker/main.py to subscribe to market feed
[ ] Create exchange/hub/pubsub.py for Redis-backed dispatch
[ ] Add /ws/market/{product_id} routing with async handler
[ ] Write integration test for signal→fill flow
[ ] Document WebSocket event contracts

# Files to create/modify:
- apps/worker/main.py (add subscription logic)
- exchange/coinbase/websocket/hub.py (NEW - pub/sub hub)
- tests/e2e/test_websocket_hub.py (NEW - integration tests)
- docs/api/ws_events.md (NEW - event contract documentation)

# Acceptance criteria:
Worker subscribes → Market events flow → Order lifecycle progresses
```

---

## 🎯 SESSION 3 PLAN (Session After)

### **Focus: P1.4 Onchain Ingestion Runtime**
```bash
# Tasks:
[ ] Create exchange/onchain/rpc_poller.py
[ ] Implement token metadata fetching service
[ ] Add safety scoring before route approval
[ ] Wire to existing onchain module infrastructure
[ ] Create monitoring dashboard endpoint

# Files to create/modify:
- exchange/onchain/rpc_poller.py (NEW)
- exchange/onchain/token_metadata.py (NEW)
- trading_system/docs/onchain_runtime.md (update with implementation)

# Acceptance criteria:
Poller active → Token metadata available → Safety score computed → Routes approved
```

---

## 🎯 SESSION 4 PLAN (Final Session)

### **Focus: P2 Hardening Suite**
```bash
# Tasks:
[ ] Implement rate limiting middleware (slowapi)
[ ] Set up Redis pub/sub hub
[ ] Create deployment smoke scripts
[ ] Document secrets management plan
[ ] Harden operator UI/API contracts

# Files to create/modify:
- trading_system/middleware/rate_limiter.py (NEW)
- trading_system/backends/redis_config.py (NEW)
- deploy/smoke_checks.sh (NEW)
- trading_system/docs/secrets_management_plan.md (IMPLEMENT)

# Acceptance criteria:
Rate limits enforced → Redis hub connected → Health checks pass → Secrets documented
```

---

## 🛠️ TOOLS & COMMANDS REFERENCE

### **Audit Commands**
```bash
# Find all generated files
find trading_system -type d -name "__pycache__"
find trading_system -type d -name ".pytest_cache"

# Check for runtime artifacts
grep -rl "\.sql\.json\|\.sqlite$" trading_system --include="*.py"

# Review .gitignore
cat trading_system/.gitignore
```

### **Test Commands**
```bash
# Run migration smoke tests
pytest trading_system/tests/migrations/test_smoke.py -v

# Run read-only sync tests
pytest trading_system/tests/e2e/test_coinbase_sync.py -v

# Full E2E workflow
pytest trading_system/tests/e2e/test_signal_to_fill.py -v
```

### **Deployment Verification**
```bash
# Check API health
curl "http://localhost:8000/api/v1/health"

# Verify WebSocket endpoint
curl "ws://localhost:8000/ws/market/BTC-USD" &

# Check migration status
alembic current
```

---

## 📝 DOCUMENTATION OUTPUTS

After each session, generate:
1. Terminal output log (what was done)
2. Updated IMPLEMENTATION_PROGRESS.md
3. Session-specific notes in references/ directory
4. Any new documentation files created

---

## ✅ SUCCESS CRITERIA FOR SESSION 1

- [x] P0.3 artifact audit complete with action items
- [ ] All P0 items cleared (migration baseline + integration harness)
- [ ] Clean repo state verified (CI passes on fresh clone)
- [ ] Terminal output documents completion status
- [ ] Ready to proceed to Session 2 (P1.2 wiring)

---

**STATUS**: PLAN CREATED — SESSION 1 EXECUTION STARTING NOW...
