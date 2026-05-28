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

```bash
cd trading_system

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

```bash
cd trading_system

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
