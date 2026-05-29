# P0.3 Durable State and Migration Groundwork

## Scope

This implementation moves the operator API away from purely in-memory state and introduces a repository layer for strategies, backtests, approvals, positions, audit events, and kill-switch state.

It also adds the first SQL migration file that defines the Postgres target schema.

## What changed

### Runtime repository layer

Added:

```text
packages/storage/src/operatorStore.mjs
```

Store implementations:

- `MemoryOperatorStore` for tests and non-persistent execution.
- `FileOperatorStore` for local durable runtime state.

Default runtime path:

```text
data/operator-state.json
```

Override with:

```bash
OPERATOR_STATE_PATH=/path/to/operator-state.json pnpm api
```

Disable file persistence with:

```bash
OPERATOR_STATE_DISABLED=true pnpm api
```

### API wiring

`apps/api/src/server.mjs` uses the store abstraction. Writes to strategies, backtests, approvals, audit, and kill-switch state are persisted when a durable store is active.

### Readiness behavior

`/ready` remains fail-closed. With memory storage, readiness includes:

```text
database_persistence_not_enabled
```

With file-backed storage, this changes to:

```text
sql_database_migrations_pending
```

With Postgres storage selected, readiness reports migration state and remains fail-closed until later production blockers are removed.

### Migration baseline

Added:

```text
packages/storage/src/migrations/001_operator_state.sql
```

Tables:

- `strategies`
- `backtest_runs`
- `approvals`
- `positions`
- `audit_events`
- `operator_flags`

### Validation

Added:

```text
scripts/validate-migrations.mjs
```

Run directly:

```bash
pnpm migrations:validate
```

It is also included in:

```bash
pnpm build
```

### Postgres follow-up

See:

```text
docs/P0_POSTGRES_STORE_AND_MIGRATIONS.md
```

That follow-up adds:

- `PostgresOperatorStore`
- `operatorStoreFactory.mjs`
- `scripts/migrate-postgres.mjs`
- `pnpm migrations:dry-run`
- `pnpm migrations:up`

## Safety posture

This does **not** enable production/live trading.

The following remain true:

- live execution routes are still forbidden
- `/ready` still returns 503
- file-backed state is for local/dev operator continuity
- Postgres state is for durable operator storage, not live-trading certification
- broker, exchange, and onchain execution remain disabled

## Local usage

```bash
pnpm install
pnpm test
pnpm build
pnpm api
```

Then open:

```text
http://localhost:3000/
```

After creating strategies/backtests/approvals in the UI, inspect:

```bash
cat data/operator-state.json | jq .
```

Restart `pnpm api`; the state should still be present.

## Next storage slice

The next logical implementation after the Postgres adapter is integration testing against the `docker-compose.yml` Postgres service and row-level repository operations for concurrent operators.
