<<<<<<< HEAD
# Database Migration Guide

**Project**: Portfolio Management Trading System  
**Component**: SQLAlchemy + Alembic  
**Status**: ✅ Baseline Established — P0 Complete

---

## Overview

This guide documents the database migration process for the trading system. The system uses **SQLAlchemy ORM** with **Alembic** for version-controlled migrations.

### Key Principles

1. **Migration First**: Always use Alembic migrations, never `Base.metadata.create_all` in production
2. **Reversible**: All migrations must be upgradeable and rollbackable via downgrade() functions
3. **Documented**: Every schema change includes rationale and impact analysis
4. **Tested**: Migrations are validated on fresh databases before deployment

---

## Current Baseline (P0 Phase Complete)

### Schema Revision: `0003_baseline_production_schema`

This baseline migration establishes the complete production-ready schema including:

| Component | Tables Count | Description |
|-----------|--------------|-------------|
| Core Trading System | 15 | portfolios, orders, fills, strategies, approvals, audit |
| Onchain Runtime (P1.4) | 3 | rpc_health, dex_aggregator_pools, onchain_events |
| Plaid Integration (P1.1) | 6 | plaid_items, accounts, transactions, webhooks, credentials |

### Core Tables

| Table | Purpose | Primary Keys | Notes |
|-------|---------|--------------|-------|
| `portfolios` | Portfolio positions | `id` | Daily snapshots with NAV tracking |
| `portfolio_sleeves` | Allocation groups | `id` | Sleeve weights for multi-sleeve portfolios |
| `strategy_configs` | Strategy definitions | `strategy_id` | Lifecycle + paper/live mode toggle |
| `strategy_runs` | Task execution queue | `task_id` | Queued → running → completed state machine |
| `orders` | Order placements | `order_id` (UUID) | Paper/live orders with risk metadata |
| `fills` | Trade executions | `fill_id` (UUID) | P&L attribution with slippage tracking |
| `strategy_allocations` | Portfolio weights | `id` | Rebalancing targets per strategy |
| `capital_buckets` | Sub-accounts | `id` | Risk-managed capital allocation |
| `approvals` | Governance requests | `approval_id` | Strategy/trade approval workflow |
| `audit_events` | Compliance trail | `id` | Immutable system of record |
| `alerts` | System notifications | `alert_id` | Severity-aware monitoring events |
| `incidents` | Error tracking | `incident_id` | Severity + status lifecycle |
| `exchange_states` | Broker health | `id` | API trust scoring per venue |
| `market_data_feeds` | WS subscription health | `id` | Latency/drop rate monitoring |

### Onchain Runtime Tables (P1.4)

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `rpc_health` | Endpoint monitoring | `endpoint_url`, `network_name`, `latency_ms` |
| `dex_aggregator_pools` | Multi-chain pool listings | `pool_address`, `total_value_usd` |
| `onchain_events` | Event ingestion buffer | `event_id`, `block_number`, `topics_raw` |

### Plaid Integration Tables (P1.1)

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `plaid_credentials` | Encrypted vault | `access_token_encrypted`, `refresh_token_encrypted` |
| `plaid_items` | Linked accounts | `item_id`, `consent_state`, `institution_name` |
| `plaid_accounts` | Holdings/snapshots | `account_type`, `cash_available`, `market_value` |
| `plaid_transactions` | Investment activities | `transaction_id`, `amount_raw`, `categorization_name` |
| `plaid_webhooks` | Security events | `event_type`, `description` |

---

## Verification Commands
=======
# Migration Guide — Trading System Database Schema

## Overview

This document describes the database schema migrations for the portfolio management trading system.

**Status**: ✅ Baseline migration committed and validated

## Baseline Revision (`0001_initial.py`)

The initial schema was generated from SQLAlchemy models and covers 15 core tables:

### Core Tables

| Table | Purpose | Key Columns |
|-------|---------|-------------|
| `portfolios` | Portfolio state and metrics | id, name, objective, nav, available_capital, locked_capital, realized_pnl, unrealized_pnl, liquidity_score, capital_efficiency, created_at, updated_at |
| `portfolio_sleeves` | Sleeve allocations per portfolio | id, portfolio_id (FK), name, weight |
| `strategy_configs` | Strategy metadata and flags | strategy_id, strategy_type, status, paper_mode, live_supported, replay_supported, backtest_supported, risk_mode_hint, capital_bucket, enabled, config_json, created_at, updated_at |
| `strategy_runs` | Strategy execution runs | task_id, strategy_id (FK), status, mode, queued_at, started_at, completed_at, error_message |
| `orders` | Order lifecycle tracking | id, order_id (unique), preview_id, strategy_id (FK), portfolio_id (FK), sleeve_id, product_id, side, size, remaining_size, price, notional, order_type, status, maker_taker_expectation, queue_age_s, risk_mode, reduce_only, created_at, updated_at |
| `strategy_allocations` | Capital allocation per strategy | id, portfolio_id (FK), strategy_id, weight |
| `fills` | Fill tracking from orders | id, fill_id (unique), order_id (FK), product_id, side, price, fee_basis_points, quantity_executed, commission_amount |
| `audit_logs` | Audit trail for operations | id, portfolio_id (FK), actor_id, event_type, event_data, created_at |
| `alerts` | Alert notifications | id, portfolio_id (FK), strategy_id, severity, message, resolved, created_at |
| `incidents` | Incident tracking | id, severity, status, description, affected_entity, remediation_notes, created_at, resolved_at |
| `market_data_snapshots` | Market data caching | id, product_id, quote_type, bid_price, ask_price, last_price, volume_24h, high_24h, low_24h, open_24h, timestamp |
| `market_book_snapshots` | Order book L2 snapshots | id, product_id, quote_type, sequence, bids (JSON), asks (JSON), timestamp |
| `exchange_state` | Coinbase API state tracking | id, account_id (FK), exchange_state (JSON), sync_timestamp, trust_score |
| `market_data_feed_health` | Feed health monitoring | id, product_id, last_heartbeat, latency_ms, error_count, status |
| `order_book_levels` | Aggregated order book levels | id, product_id, price_level, side, quantity, total_quantity |

## Upgrade Procedure

### From Fresh Database
>>>>>>> b5e23b51 (Added falcon updates)

```bash
cd trading_system

<<<<<<< HEAD
# Check migration history (should show 0001 → 0002 → 0003)
alembic history --length 5

# Verify current revision state
alembic current

# Upgrade fresh database to baseline
DATABASE_URL="postgresql://user:***@localhost/trading_db" alembic upgrade head
```

### Fresh Database Migration Test

```bash
docker run -d --name trading-migration-test \
  -e POSTGRES_DB=trading_system_migration_test \
  -e POSTGRES_USER=migrate_user \
  -e POSTGRES_PASSWORD=*** \
  postgres:15-alpine

DATABASE_URL="postgresql://migrate_user:***@localhost/trading_system_migration_test" alembic upgrade head

psql -c "\dt+" trading_system_migration_test

docker stop trading-migration-test
```

This baseline migration establishes the complete production-ready schema including:

| Component | Tables Count | Description |
|-----------|--------------|-------------|
| Core Trading System | 15 | portfolios, orders, fills, strategies, approvals, audit |
| Onchain Runtime (P1.4) | 3 | rpc_health, dex_aggregator_pools, onchain_events |
| Plaid Integration (P1.1) | 6 | plaid_items, accounts, transactions, webhooks, credentials |

### Core Tables

| Table | Purpose | Primary Keys | Notes |
|-------|---------|--------------|-------|
| `portfolios` | Portfolio positions | `id` | Daily snapshots with NAV tracking |
| `portfolio_sleeves` | Allocation groups | `id` | Sleeve weights for multi-sleeve portfolios |
| `strategy_configs` | Strategy definitions | `strategy_id` | Lifecycle + paper/live mode toggle |
| `strategy_runs` | Task execution queue | `task_id` | Queued → running → completed state machine |
| `orders` | Order placements | `order_id` (UUID) | Paper/live orders with risk metadata |
| `fills` | Trade executions | `fill_id` (UUID) | P&L attribution with slippage tracking |
| `strategy_allocations` | Portfolio weights | `id` | Rebalancing targets per strategy |
| `capital_buckets` | Sub-accounts | `id` | Risk-managed capital allocation |
| `approvals` | Governance requests | `approval_id` | Strategy/trade approval workflow |
| `audit_events` | Compliance trail | `id` | Immutable system of record |
| `alerts` | System notifications | `alert_id` | Severity-aware monitoring events |
| `incidents` | Error tracking | `incident_id` | Severity + status lifecycle |
| `exchange_states` | Broker health | `id` | API trust scoring per venue |
| `market_data_feeds` | WS subscription health | `id` | Latency/drop rate monitoring |

### Onchain Runtime Tables (P1.4)

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `rpc_health` | Endpoint monitoring | `endpoint_url`, `network_name`, `latency_ms` |
| `dex_aggregator_pools` | Multi-chain pool listings | `pool_address`, `total_value_usd` |
| `onchain_events` | Event ingestion buffer | `event_id`, `block_number`, `topics_raw` |

### Plaid Integration Tables (P1.1)

| Table | Purpose | Key Fields |
|-------|---------|------------|
| `plaid_credentials` | Encrypted vault | `access_token_encrypted`, `refresh_token_encrypted` |
| `plaid_items` | Linked accounts | `item_id`, `consent_state`, `institution_name` |
| `plaid_accounts` | Holdings/snapshots | `account_type`, `cash_available`, `market_value` |
| `plaid_transactions` | Investment activities | `transaction_id`, `amount_raw`, `categorization_name` |
| `plaid_webhooks` | Security events | `event_type`, `description` |

---

## Establishing Baseline (Already Complete)

### Verification Commands
=======
# Create fresh database
psql -f <(cat << 'EOF'
DROP DATABASE IF EXISTS trading_system_test;
CREATE DATABASE trading_system_test;
EOF
)

# Run migrations
alembic upgrade head

# Verify current head
alembic current
```

### From Existing Database (Without Migrations)

If you have an existing database without Alembic metadata:
>>>>>>> b5e23b51 (Added falcon updates)

```bash
cd trading_system

<<<<<<< HEAD
# Check migration history (should show 0001 → 0002 → 0003)
alembic history --length 5

# Verify current revision state
alembic current

# Upgrade fresh database to baseline
DATABASE_URL="postgresql://user:pass@localhost/trading_db" alembic upgrade head
```

### Fresh Database Migration Test

```bash
# Start PostgreSQL test container
docker run -d --name trading-migration-test \
  -e POSTGRES_DB=trading_system_migration_test \
  -e POSTGRES_USER=migrate_user \
  -e POSTGRES_PASSWORD=migrate_pass \
  postgres:15-alpine

# Run migrations on test DB
DATABASE_URL="postgresql://migrate_user:migrate_pass@localhost/trading_system_migration_test" alembic upgrade head

# Verify all tables created
psql -c "\dt+" trading_system_migration_test

# Stop container
docker stop trading-migration-test
```

---

## Schema Change Management (Post-Baseline)

### Creating New Migration

```bash
cd trading_system

# Generate migration from SQLAlchemy models
alembic revision --autogenerate -m "description_of_change"

# Review generated file in alembic/versions/*.py
# Add data migrations, index creation, or constraint changes manually

# Create commit with migration
git add -A
git commit -m "migration: description_of_change"
```

### Migration Best Practices

- **Never use `Base.metadata.create_all`** — only Alembic for production
- **Document rationale** in migration docstring (revision message)
- **Preserve NOT NULL constraints** where business-critical
- **Add indexes** for query performance (don't defer to ORM)
- **Use backward-compatible changes** first, rename/drop later
- **Test on fresh database** before deployment

### Schema Change Guidelines

| Operation | When | Notes |
|-----------|------|-------|
| Add column | Anytime | nullable=True by default, add constraints in new migration |
| Drop column | Rare | require data extraction/migration first |
| Alter type | Careful | use `postgresql_drop_default` + drop old column + create new with new type |
| Add index | Frequently | before queries become slow; document query patterns |
| Remove index | Rarely | verify query performance first |
| Drop table | Never in prod | requires data migration to alternative schema first |

---

## Production Deployment Checklist

Before deploying any migration:

- [ ] Migration file reviewed and committed
- [ ] Data integrity checks verified (row counts, constraints)
- [ ] Backup created pre-migration
- [ ] Rollback plan documented (downgrade function tested)
- [ ] Documentation updated in `docs/MIGRATION_GUIDE.md`
- [ ] Migration smoke test passed on staging environment

---

## Migration History Commands

```bash
# View migration timeline
alembic history --verbose

# Show current revision state
alembic current

# Skip next N migrations (if needed)
alembic upgrade +10

# Rollback last migration
alembic downgrade -1

# Full rollback to specific revision
alembic downgrade 0002
```

---

## Database Backup Strategy

```bash
# Pre-migration backup
pg_dump trading_system_db > backup_pre_migration_$(date +%Y%m%d_%H%M%S).sql.gz

# Post-migration verification
psql -c "SELECT COUNT(*) FROM portfolios;"  # Verify row counts unchanged
psql -c "SELECT revision, down_revision FROM alembic_version;"  # Confirm current revision
```

---

## Known Issues & Limitations

1. **alembic not in PATH**: Install via `pip install alembic` or add to virtual environment
2. **Database connection required**: Set `DATABASE_URL` environment variable
3. **Docker alternative**: Use test containers for migration validation

---

## Support & References

- **Project Repository**: `/home/falcon/git/portfolio-management/trading_system/`
- **Migration Files**: `trading_system/alembic/versions/*.py`
- **Alembic Config**: `trading_system/alembic/env.py`
- **Baseline Revision**: `0003_baseline_production_schema`

---

*Last Updated*: 2026-05-27  
*Phase Status*: P0 Baseline Complete ✅
=======
# Option 1: Generate migration from existing schema
alembic revision --autogenerate -m "migration_from_scratch"

# Review generated migration, then apply
alembic upgrade head

# Or use create_all as last resort (not recommended for production)
python3 -c "from storage.postgres.models import Base; Base.metadata.create_all(engine)"
```

## Rollback Procedure

To rollback to previous revision:

```bash
cd trading_system
alembic downgrade -1  # Rollback one revision
alembic downgrade 0  # Rollback all revisions (empty database)
```

## CI Migration Checks

Add to `.github/workflows/ci.yml`:

```yaml
- name: Run migrations on fresh DB
  run: |
    psql -c "DROP DATABASE IF EXISTS trading_system_ci; CREATE DATABASE trading_system_ci;"
    alembic -c trading_system/alembic.ini upgrade head -p trading_system_ci
    
- name: Verify migration head
  run: alembic current --env-file=trading_system/.env
  
- name: Migration smoke test
  run: pytest tests/migrations/test_smoke.py -q
```

## Adding New Migrations

When adding new tables or modifying schema:

```bash
cd trading_system
alembic revision --autogenerate -m "description"

# Review generated migration in alembic/versions/YYYY_*.py
# Manually verify column types, constraints, and foreign keys match SQLAlchemy models
# If models changed but not reflected, edit migration manually or use autogenerate again
alembic upgrade head  # Apply migration locally before committing
```

## Production Deployment Checklist

- [ ] Review migration script for idempotency (can run multiple times)
- [ ] Test rollback procedure works as expected
- [ ] Document new columns and their purposes
- [ ] Add migration smoke tests
- [ ] Commit migration to repository
- [ ] Update `MIGRATION_GUIDE.md` with new table/column documentation

## Environment Variables

Set in production `.env`:

```bash
DATABASE_URL=postgresql://user:password@host:5432/trading_system
ALEMBIC_MIGRATION_ENVIRONMENT=production
```

## Monitoring & Alerts

Add to monitoring dashboard:

- `alembic current` on startup (verify migration is at head)
- Alert if `alembic current` != expected head
- Log migration duration on first apply after deploy
>>>>>>> b5e23b51 (Added falcon updates)
