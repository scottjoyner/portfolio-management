# P0.3 Durable State and Migration Groundwork

## Scope

This implementation moves the operator API away from purely in-memory state and introduces a repository layer for strategies, backtests, approvals, positions, audit events, and kill-switch state.

It also adds the first SQL migration file that defines the Postgres target schema for the next storage implementation slice.

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

`apps/api/src/server.mjs` now uses the store abstraction. Writes to strategies, backtests, approvals, audit, and kill-switch state are persisted when the file store is active.

### Readiness behavior

`/ready` remains fail-closed. With memory storage, readiness includes:

```text
database_persistence_not_enabled
```

With file-backed storage, this changes to:

```text
sql_database_migrations_pending
```

That means local durability is available, but production is still blocked until the Postgres repository implementation is complete.

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

## Safety posture

This does **not** enable production/live trading.

The following remain true:

- live execution routes are still forbidden
- `/ready` still returns 503
- SQL migrations are defined but not yet executed by the app
- file-backed state is for local/dev operator continuity, not production-grade DB storage
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

The next logical implementation is a Postgres-backed repository:

1. Add migration runner.
2. Apply `001_operator_state.sql` on startup or via CLI.
3. Implement `PostgresOperatorStore`.
4. Add integration tests against the `docker-compose.yml` Postgres service.
5. Change readiness from `sql_database_migrations_pending` to a DB connectivity/migration status check.
