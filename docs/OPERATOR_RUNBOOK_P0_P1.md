# Operator Runbook — Mock/Paper Operator Product

This runbook is for the P0/P1 operator workflow plus the first P2 paper-execution hardening slice. It does not certify production or live trading.

## Start local mock/paper mode

```bash
pnpm install
pnpm test
pnpm build
pnpm api
```

Open:

```text
http://localhost:3000/
```

Default local state path:

```text
data/operator-state.json
```

## Start with local auth enabled

```bash
OPERATOR_AUTH_REQUIRED=true \
OPERATOR_AUTH_TOKEN=dev-secret \
pnpm api
```

Use the UI through a browser proxy that adds the bearer token, or call API routes directly:

```bash
curl -H 'Authorization: Bearer dev-secret' http://localhost:3000/api/operator/summary
```

## Start Postgres-backed mode

Prerequisites:

- PostgreSQL client `psql`
- Runtime `pg` dependency available to Node
- Local Postgres service running

Start local services:

```bash
docker compose up -d postgres
```

Apply migrations:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb pnpm migrations:up
```

Run API:

```bash
OPERATOR_STORE=postgres \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
pnpm api
```

Run opt-in Postgres integration smoke test:

```bash
RUN_POSTGRES_INTEGRATION=true \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
pnpm test:integration:postgres
```

## Operator workflow

1. Confirm `/health` responds.
2. Open the UI.
3. Review accounts and instruments.
4. Review strategy templates.
5. Create a strategy from a template.
6. Run a backtest.
7. Review metrics and report artifact data.
8. Request approval.
9. Approve the request.
10. Start paper execution.
11. Send a paper signal and review preview/fill/reconciliation output.
12. Stop paper execution.
13. Review audit trail.
14. Test kill switch.

## API smoke commands

```bash
curl http://localhost:3000/health
curl http://localhost:3000/ready
curl http://localhost:3000/api/accounts
curl http://localhost:3000/api/instruments
curl http://localhost:3000/api/strategy-templates
curl http://localhost:3000/api/operator/summary
```

Create strategy from template:

```bash
curl -X POST http://localhost:3000/api/strategies/from-template \
  -H 'Content-Type: application/json' \
  -d '{"templateId":"template-ema-crossover","name":"Runbook EMA"}'
```

Run backtest:

```bash
curl -X POST http://localhost:3000/api/backtests/run \
  -H 'Content-Type: application/json' \
  -d '{"strategyId":"strategy-ema-cross-v1","initialCapitalUsd":100000,"feeBps":5,"slippageBps":10}'
```

Request approval:

```bash
curl -X POST http://localhost:3000/api/approvals/request \
  -H 'Content-Type: application/json' \
  -d '{"strategyId":"strategy-ema-cross-v1","tier":"canary"}'
```

Approve request:

```bash
curl -X POST http://localhost:3000/api/approvals/approval-demo-001/decision \
  -H 'Content-Type: application/json' \
  -d '{"status":"approved","reviewer":"runbook"}'
```

Start paper execution:

```bash
curl -X POST http://localhost:3000/api/paper-executions \
  -H 'Content-Type: application/json' \
  -d '{"strategyId":"strategy-ema-cross-v1","accountId":"acct-paper-primary"}'
```

Send paper signal for preview/fill/reconciliation:

```bash
curl -X POST http://localhost:3000/api/paper-executions/paper-001/signal \
  -H 'Content-Type: application/json' \
  -d '{"signal":{"symbol":"BTC-USD","side":"buy","quantity":0.1,"price":50000,"feeBps":5,"slippageBps":10}}'
```

Use the actual paper execution ID returned from `POST /api/paper-executions` in place of `paper-001`.

Expected response includes:

- `preview`
- `fill`
- `reconciliation`

## Safety checks

Live routes must remain blocked:

```bash
curl -X POST http://localhost:3000/api/execution/live/orders \
  -H 'Content-Type: application/json' \
  -d '{"side":"buy"}'
```

Expected result:

```json
{
  "ok": false,
  "error": "live_execution_disabled"
}
```

Readiness remains fail-closed:

```bash
curl http://localhost:3000/ready
```

Expected result: HTTP 503 with production blockers.

## Recovery

Stop all paper sessions:

```bash
curl -X POST http://localhost:3000/api/kill-switch/stop-paper
```

Enable global kill switch:

```bash
curl -X POST http://localhost:3000/api/kill-switch \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"reason":"operator_runbook_test"}'
```

Disable global kill switch:

```bash
curl -X POST http://localhost:3000/api/kill-switch \
  -H 'Content-Type: application/json' \
  -d '{"enabled":false,"reason":"operator_runbook_test_complete"}'
```
