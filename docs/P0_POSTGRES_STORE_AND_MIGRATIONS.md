# P0.3 Postgres Store and Migration Runner

## Scope

This slice adds the first SQL-backed storage path for the operator API.

It builds on the file-backed local state work by adding:

- `PostgresOperatorStore`
- `operatorStoreFactory.mjs`
- `psql`-based migration runner
- dry-run migration validation in `pnpm build`
- fake-client tests for Postgres mapping behavior

## Safety posture

This still does **not** make the system production-ready.

Live trading remains blocked. The Postgres store is for durable operator state only. Production readiness still requires:

- auth/RBAC
- realistic backtesting
- paper/shadow execution
- reconciliation
- deployment hardening
- observability
- incident runbooks
- broker/venue adapter certification

## Migration commands

Validate migration files:

```bash
pnpm migrations:validate
```

Preview SQL that would run:

```bash
pnpm migrations:dry-run
```

Apply migrations using `psql`:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb pnpm migrations:up
```

The migration runner creates and uses:

```sql
schema_migrations(version text primary key, applied_at timestamptz)
```

## Local Postgres workflow

Start Postgres:

```bash
docker compose up -d postgres
```

Apply migrations:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb pnpm migrations:up
```

Run API with Postgres-backed operator state:

```bash
OPERATOR_STORE=postgres \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
pnpm api
```

Open:

```text
http://localhost:3000/
```

## Store selection

Default:

```text
FileOperatorStore -> data/operator-state.json
```

Memory mode:

```bash
OPERATOR_STATE_DISABLED=true pnpm api
```

Postgres mode:

```bash
OPERATOR_STORE=postgres DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb pnpm api
```

## Readiness behavior

`/ready` remains HTTP 503.

Storage-specific blockers:

| Store | Blocker |
|---|---|
| memory | `database_persistence_not_enabled` |
| file | `sql_database_migrations_pending` |
| postgres without migrations | `sql_database_migrations_not_ready` |
| postgres with migrations | still blocked by `ui_api_contract_only` and `real_execution_disabled` |

## Runtime dependency note

`PostgresOperatorStore` dynamically imports the `pg` package. The tests use an injected fake client to keep CI dependency-free.

Before using Postgres mode in a real runtime, add the `pg` dependency to the workspace or run in an environment where `pg` is available.

## Next logical implementation

1. Add `pg` to the workspace dependencies.
2. Add integration tests against the `docker-compose.yml` Postgres service.
3. Add endpoint-level persistence tests using `OPERATOR_STORE=postgres`.
4. Add a startup guard that can optionally run migrations before accepting traffic.
5. Move from full-state rewrite to row-level repository operations for concurrent operators.
