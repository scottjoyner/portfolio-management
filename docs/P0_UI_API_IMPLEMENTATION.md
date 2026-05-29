# P0 UI/API Implementation Slice

## Scope

This slice starts P0.1 and P0.2 from `TODO.md` by adding a dependency-free operator console and API contract scaffold.

It intentionally does **not** enable live trading, broker execution, onchain execution, real Plaid credentials, or production-grade SQL persistence.

## Added operator routes

| Route | Method | Purpose |
|---|---|---|
| `/` | GET | Serves the operator console. |
| `/ui/*` | GET | Serves static UI assets. |
| `/health` | GET | Basic service health. |
| `/ready` | GET | Fail-closed readiness status. |
| `/metrics` | GET | Basic operator metrics. |
| `/api/operator/summary` | GET | UI dashboard summary. |
| `/api/strategies` | GET/POST | List and create strategy definitions. |
| `/api/backtests` | GET/POST | List and run deterministic demo backtests. |
| `/api/approvals` | GET/POST | List and request strategy approvals. |
| `/api/positions` | GET | List positions. |
| `/api/audit` | GET | List audit events. |
| `/api/kill-switch` | POST | Toggle kill switch state. |
| `/api/execution/live/*` | Any | Explicitly forbidden. |

## UI sections

- Strategy lifecycle
- Backtest runs
- Approval requests
- Risk controls / kill switch
- Audit trail
- Readiness summary

## Safety posture

`/ready` remains non-production-ready by design. It reports blockers such as:

- `ui_api_contract_only`
- `database_persistence_not_enabled` when memory storage is active
- `sql_database_migrations_pending` when file-backed durable state is active
- `real_execution_disabled`
- `live_mode_not_certified` when applicable
- `kill_switch_enabled` when applicable

Live execution routes return HTTP 403 with `live_execution_disabled`.

## Tests

`tests/operator-api.test.mjs` covers:

- fail-closed readiness
- strategy creation
- deterministic backtest creation
- approval request flow
- explicit live-execution block
- kill-switch audit and readiness behavior
- storage-mode blocker reporting

`tests/operator-store.test.mjs` covers:

- memory store mutation behavior
- file-backed state persistence across store instances
- state normalization for missing collections

## Run locally

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

## Runtime state

Default local state path:

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

## Next implementation slices

1. Implement Postgres-backed repository using `packages/storage/src/migrations/001_operator_state.sql`.
2. Add OpenAPI schema and typed request/response contracts.
3. Implement real strategy parameter validation and versioning.
4. Replace deterministic demo backtests with the Python backtest service.
5. Add authentication and role-based permissions.
