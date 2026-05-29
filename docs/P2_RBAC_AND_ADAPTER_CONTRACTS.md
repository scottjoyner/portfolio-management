# P2 RBAC, Security Validation, and Adapter Contracts

This slice continues P2 production hardening after the Postgres/paper execution hardening work.

It still does **not** enable live trading.

## Operator roles

The API now supports role-scoped operator tokens when auth is required.

| Environment variable | Role | Intended use |
|---|---|---|
| `OPERATOR_ADMIN_TOKEN` | `admin` | Full operator/admin access to mock/paper workflows. |
| `OPERATOR_AUTH_TOKEN` | `admin` | Backward-compatible admin token. |
| `OPERATOR_PAPER_TOKEN` | `paper` | Paper/backtest/approval request/paper execution actions only. |
| `OPERATOR_READONLY_TOKEN` | `readonly` | Read-only UI/API inspection. |

Auth is required when either condition is true:

```text
OPERATOR_AUTH_REQUIRED=true
MODE=live
```

`MODE=live` still does not enable live trading. Live execution routes remain blocked.

## Role permissions

### readonly

Allowed:

- `GET`, `HEAD`, `OPTIONS`

Denied:

- all mutating routes

### paper

Allowed:

- read routes
- `POST /api/backtests/run`
- `POST /api/approvals/request`
- `POST /api/paper-executions`
- `POST /api/paper-executions/:id/stop`
- `POST /api/paper-executions/:id/signal`
- `POST /api/kill-switch/stop-paper`

Denied:

- strategy lifecycle mutation
- production/live execution
- admin-only operations

### admin

Allowed:

- all current mock/paper operator routes

Still denied:

- live execution routes

## Security validation

Added:

```bash
pnpm security:validate
```

The security validator checks:

- required `pg` runtime dependency exists
- committed private key blocks
- obvious GitHub tokens
- obvious AWS access keys
- suspicious generic secret assignments

It is intentionally lightweight and does not replace a dedicated secret scanner such as Gitleaks or GitHub Advanced Security.

## CI coverage

The CI workflow now runs:

```bash
pnpm security:validate
```

alongside test, build, API contract validation, migration validation, and Postgres smoke testing.

## Adapter contracts

Added fail-closed adapter contracts in:

```text
packages/adapters/src/contracts.mjs
```

Key contract points:

- `ReadOnlyMarketDataAdapter` defines read-only discovery/quote/history expectations.
- `BrokerExecutionAdapter` defines preview/paper/live execution shape.
- `FailClosedExecutionAdapter` allows preview and paper order submission.
- `FailClosedExecutionAdapter.submitLiveOrder()` always throws `live_execution_disabled`.

This gives future broker/venue implementations a safe contract to implement before any real connector is certified.

## Tests

Added:

- `tests/operator-rbac.test.mjs`
- `tests/adapter-contracts.test.mjs`

Existing tests continue to verify:

- live execution routes remain blocked
- CSRF and CORS behavior
- paper fill path remains paper-only

## Remaining P2 work

- Replace JSON-flag product persistence with row-level Postgres operations.
- Add historical market data adapters and replay backtesting.
- Add dedicated secret scanning such as Gitleaks.
- Add full RBAC policy file / policy engine if more roles are needed.
- Implement real adapter contract tests before adding any live connector.
