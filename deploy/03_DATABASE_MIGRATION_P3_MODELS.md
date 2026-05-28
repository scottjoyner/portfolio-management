# Database Migration - P3 Models Addition (2026-05-27)
## Target Machine: destroyer | Status: Ready to execute

---

## Overview

This migration adds Phase 3 agentic evaluation system database tables:
- Evaluation package (price estimates, position quality metrics)
- Approval package (audit trails, risk assessments)
- Research package (hypotheses, market regimes)

Total new tables: **11**

---

## Quick Start Commands

### Step 1: Check Current Migration Status
```bash
cd /home/destroyer/trading_system/alembic/versions
ls -la *.py
```

**Expected output:**
- `0001_initial.py` - P0 schema foundation (~11KB)
- `0002_onchain_runtime.py` - P1.4 onchain runtime (~4KB)
- `P3_models_addition.py` - P3 agentic evaluation system (NEW)

### Step 2: Generate New Migration File
```bash
cd /home/destroyer/trading_system
alembic revision --autogenerate -m "Add P3 evaluation, approval, research tables"
```

**Output:** `versions/P3_models_addition_YYYYMMDD_HHMMSS.py` (NEW)

### Step 3: Review Generated Migration
```bash
# Check what's being created
cat alembic/versions/*P3*.py | grep "CREATE TABLE" -A 5
```

**Expected output:** Should show CREATE TABLE statements for new P3 models.

### Step 4: Apply Migration to Database
```bash
alembic upgrade head
```

**Output should include:**
```
INFO  Running revision <revision-id>: Add P3 evaluation, approval, research tables
# ... SQL DDL output ...
INFO  Revision <head-revision> up to date
```

---

## Detailed Migration Output

### Step 1: Check Migration Files
```bash
ls -lh /home/destroyer/trading_system/alembic/versions/*.py
```

**Expected:**
- `0001_initial.py` (P0 - ~12KB)
- `0002_onchain_runtime.py` (P1.4 - ~4KB)  
- `P3_models_addition_*.py` (P3 - NEW, ~8KB)

### Step 2: Generate Migration
```bash
cd /home/destroyer/trading_system
alembic revision --autogenerate \
    --sql-file p3_migration.sql \
    -m "Add P3 evaluation, approval, research tables"
```

**Output:** Creates `p3_migration.sql` with expected new tables.

### Step 3: Manual Migration Execution (Alternative)
If auto-generate doesn't create migration:

```bash
cd /home/destroyer/trading_system

# Create P3 models manually (already exists at ./trading_system/evaluation/models.py, etc.)

# Create new migration file manually
cat > alembic/versions/P3_models_addition_20260527.py << 'EOF'
\"\"\"Add P3 evaluation, approval, research tables

Revision ID: <generated>
Revises: 0002_onchain_runtime
Create Date: 2026-05-27

\"\"\"
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'P3_models_addition_20260527'
down_revision = '0002_onchain_runtime'
branch_labels = None
depends_on = None


def upgrade():
    # Evaluation Package Tables (4 tables)
    # Table 1: price_estimates - Price target predictions
    op.create_table('price_estimates',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('instrument', sa.String(20), nullable=False),
        sa.Column('model_type', sa.String(50), nullable=False),  # fundamental, technical, consensus, ml_predictive
        sa.Column('target_price', sa.Float(), nullable=True),
        sa.Column('confidence_score', sa.Float(), nullable=True),
        sa.Column('buy_level', sa.Float(), nullable=True),
        sa.Column('sell_level', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True, default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )

    # Table 2: position_quality_metrics - Position scoring
    op.create_table('position_quality_metrics',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('symbol', sa.String(10), nullable=False),
        sa.Column('risk_score', sa.Float(), nullable=True),  # 0-1
        sa.Column('alpha_score', sa.Float(), nullable=True),  # Expected excess return (bps)
        sa.Column('beta_exposure', sa.Float(), nullable=True),  # Market sensitivity (-1 to 1)
        sa.Column('correlation_to_index', sa.Float(), nullable=True),
        sa.Column('volatility_regime', sa.String(20), nullable=True),  # low/moderate/high/extreme
        sa.PrimaryKeyConstraint('id')
    )

    # Table 3: evaluation_config - Configuration for price estimation
    op.create_table('evaluation_config',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('price_source', sa.String(50), nullable=True, default='fundamental'),
        sa.Column('volatility_threshold_high', sa.Float(), nullable=True),  # e.g., 0.4
        sa.PrimaryKeyConstraint('id')
    )

    # Table 4: evaluation_history - Historical price estimates
    op.create_table('evaluation_history',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('price_estimate_id', sa.Integer(), sa.ForeignKey('price_estimates.id'), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=True, default=sa.func.now()),
        sa.PrimaryKeyConstraint('id')
    )

    # Approval Package Tables (4 tables)
    # Table 5: approval_requests - Pending and completed approvals
    op.create_table('approval_requests',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('strategy_key', sa.String(50), nullable=False),
        sa.Column('version', sa.String(20), nullable=True),
        sa.Column('capital_allocation_usd', sa.Float(), nullable=True),
        sa.Column('risk_level', sa.Float(), nullable=True),  # 0-1 risk score
        sa.Column('target_performance_pct', sa.Float(), nullable=True),
        sa.Column('status', sa.String(20), nullable=True, default='pending'),  # pending/auto_approved/rejected
        sa.Column('tier', sa.String(30), nullable=True),  # AUTO_APPROVE/CANARY_PHASE/FULL_SCALE
        sa.Column('submitted_at', sa.DateTime(), nullable=True, default=sa.func.now()),
        sa.Column('approved_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Table 6: audit_trails - Approval decision tracking
    op.create_table('audit_trails',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('approval_request_id', sa.Integer(), sa.ForeignKey('approval_requests.id'), nullable=True),
        sa.Column('timestamp', sa.DateTime(), nullable=True, default=sa.func.now()),
        sa.Column('event', sa.String(50), nullable=False),  # submitted/reviewed/approved/rejected
        sa.Column('reviewer_id', sa.String(50), nullable=True),
        sa.Column('decision', sa.String(20), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Table 7: risk_assessments - Risk scoring results
    op.create_table('risk_assessments',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('approval_request_id', sa.Integer(), sa.ForeignKey('approval_requests.id'), nullable=True),
        sa.Column('validation_results_json', sa.JSON(), nullable=True),  # Code review, security scan results
        sa.Column('risk_score', sa.Float(), nullable=True),  # Overall risk assessment (0-1)
        sa.Column('is_at_capacity', sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Table 8: capacity_tracking - Per-strategy capital limits
    op.create_table('capacity_tracking',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('strategy_key', sa.String(50), nullable=False, unique=True),
        sa.Column('monthly_limit_usd', sa.Float(), nullable=True),  # e.g., 500000
        sa.Column('total_approved_capital', sa.Float(), nullable=True),
        sa.Column('remaining_capacity', sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Research Package Tables (3 tables)
    # Table 9: trading_hypotheses - Generated trading hypotheses
    op.create_table('trading_hypotheses',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(100), nullable=False),  # e.g., "eth_btc_convergence"
        sa.Column('confidence_score', sa.Float(), nullable=True),  # 0-1 confidence
        sa.Column('market_state', sa.String(50), nullable=True),  # bull/bear/sideways
        sa.Column('strategy_type', sa.String(50), nullable=True),  # trend-following, mean-reversion, etc.
        sa.Column('instrument_pairs_json', sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Table 10: market_regime_snapshots - Market regime classification
    op.create_table('market_regime_snapshots',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.DateTime(), nullable=True, default=sa.func.now()),
        sa.Column('state', sa.String(20), nullable=True),  # bull/bear/sideways
        sa.Column('average_volatility_20d', sa.Float(), nullable=True),
        sa.Column('market_momentum_30d_pct', sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Table 11: backtest_results - Backtest results linked to hypotheses
    op.create_table('backtest_results',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('hypothesis_id', sa.Integer(), sa.ForeignKey('trading_hypotheses.id'), nullable=True),
        sa.Column('total_return_pct', sa.Float(), nullable=True),
        sa.Column('sharpe_ratio', sa.Float(), nullable=True),
        sa.Column('max_drawdown_pct', sa.Float(), nullable=True),
        sa.Column('backtest_period_from', sa.DateTime(), nullable=True),
        sa.Column('backtest_period_to', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )

    # Create indexes for performance
    op.create_index(op.f('idx_price_estimates_instrument'), 'price_estimates', ['instrument'])
    op.create_index(op.f('idx_approval_requests_strategy'), 'approval_requests', ['strategy_key'])
    op.create_index(op.f('idx_audit_trails_timestamp'), 'audit_trails', ['timestamp'])
    op.create_index(op.f('idx_market_regime_timestamp'), 'market_regime_snapshots', ['timestamp'])


def downgrade():
    # Drop tables in reverse order of creation
    op.drop_table('backtest_results')
    op.drop_table('market_regime_snapshots')
    op.drop_table('trading_hypotheses')
    op.drop_table('capacity_tracking')
    op.drop_table('risk_assessments')
    op.drop_table('audit_trails')
    op.drop_table('approval_requests')
    op.drop_table('evaluation_history')
    op.drop_table('evaluation_config')
    op.drop_table('position_quality_metrics')
    op.drop_table('price_estimates')
```

EOF
```

### Step 4: Apply Migration
```bash
alembic upgrade head
```

**Expected output:**
```
INFO  Running revision P3_models_addition_20260527
# ... SQL DDL for creating tables ...
INFO  Revision <head-revision-id> up to date
```

---

## Verification Commands

### Check Migration Status
```bash
alembic history | tail -10
```

**Expected:** Should show all migrations including P3.

### Verify Tables Created
```bash
cd /home/destroyer/trading_system
docker-compose exec trading-runtime-destructor python3 -c "
from sqlalchemy import inspect, create_engine
import os

# Get database URL from environment
db_url = os.environ.get('DATABASE_URL', 'postgresql://trading_db/trading_system')
engine = create_engine(db_url.replace('{{PASSWORD}}', ''))  # Remove placeholder password

with engine.connect() as conn:
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print('Tables in database:')
    for table in sorted(tables):
        if 'price' in table.lower() or 'approval' in table.lower() or 'hypothesis' in table.lower() or 'regime' in table.lower():
            print(f'  ✅ {table}')
"
```

**Expected output:** Should show all P3 tables (11 total):
- price_estimates
- position_quality_metrics  
- evaluation_config
- evaluation_history
- approval_requests
- audit_trails
- risk_assessments
- capacity_tracking
- trading_hypotheses
- market_regime_snapshots
- backtest_results

---

## Summary of Changes

| Component | Before P3 Migration | After P3 Migration | Status |
|-----------|---------------------|--------------------|--------|
| Total tables (P0+P1) | 8 | 8 | Unchanged |
| + P3 Evaluation tables | - | 4 | Added ✓ |
| + P3 Approval tables | - | 4 | Added ✓ |
| + P3 Research tables | - | 3 | Added ✓ |
| **Total** | **8** | **19** | **+11 tables** |

---

## Next Steps After Migration

1. ✅ Verify all 19 tables present in database
2. ✅ Test integration with price estimation engine
3. ✅ Validate approval routing uses new audit trail tables
4. ✅ Confirm hypothesis generation writes to research tables
5. ✅ Run full test suite: `python3 tests/integration/db_harness.py`

---

## Rollback Plan (If Issues Occur)

### Step 1: Revert Migration
```bash
alembic downgrade 0002_onchain_runtime
```

### Step 2: Verify Tables Dropped
```bash
docker-compose exec trading-runtime-destructor python3 -c "
from sqlalchemy import inspect, create_engine
import os

db_url = os.environ.get('DATABASE_URL', 'postgresql://trading_db/trading_system')
engine = create_engine(db_url)

with engine.connect() as conn:
    inspector = inspect(engine)
    tables = sorted(inspector.get_table_names())
    
p3_tables_remaining = [t for t in tables if any(x in t.lower() for x in ['price_', 'approval_', 'hypothesis_', 'regime_'])]

if not p3_tables_remaining:
    print('✅ P3 tables successfully dropped')
else:
    print(f'⚠ P3 tables still exist: {p3_tables_remaining}')
"
```

---

**Migration Date:** 2026-05-27  
**Target Machine:** destroyer (ThinkPad T14)  
**Status:** Ready to execute via `alembic revision --autogenerate` then `alembic upgrade head`
