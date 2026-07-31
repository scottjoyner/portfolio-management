# Operator Runbook — Supervised Paper Operations

This runbook covers daily Portfolio OS operation, guarded paper execution, local/OpenRouter intelligence routing, incident response, and the bot-versus-agent competition. It does **not** certify live trading.

Deployment, backup, and rollback procedures are maintained in [`DEPLOYMENT_ROLLBACK_RUNBOOK.md`](./DEPLOYMENT_ROLLBACK_RUNBOOK.md). Release gates are defined in [`RELEASE_READINESS_MATRIX.md`](./RELEASE_READINESS_MATRIX.md).

## 1. Prerequisites

- Node.js 22
- npm 10
- Python 3.12
- PostgreSQL 17 for the production-paper store
- Docker Engine and Compose for the canonical deployment
- one or more OpenAI-compatible local inference endpoints when local inference is required

Install from the committed locks:

```bash
npm ci --ignore-scripts
python -m pip install --upgrade pip
python -m pip install -r deploy/requirements.venv-lock.txt
```

## 2. Validate before starting

```bash
npm test
npm run build
npm run operational:validate
npm run runtime-artifacts:validate
```

For a real PostgreSQL target:

```bash
DATABASE_URL=<database-url> npm run migrations:up
DATABASE_URL=<database-url> npm run test:integration:postgres
```

Generated ledgers, state, health snapshots, reports, and backups must remain outside source control.

## 3. Local mock mode

```bash
MODE=mock \
OPERATOR_AUTH_REQUIRED=false \
npm run api
```

Open `http://localhost:3000/`.

Mock mode is suitable for UI and workflow development only. It is not production-paper certification.

## 4. Authenticated browser session

Production-paper mode requires bearer authentication and CSRF protection. The shipped UI includes a same-origin operator-session control.

Enter:

- an admin/operator bearer token;
- the CSRF token for mutating requests.

The browser keeps both values only in the current tab's `sessionStorage`. Closing the tab clears them. The UI must never receive or store `OPENROUTER_API_KEY`.

Available API roles:

- `OPERATOR_ADMIN_TOKEN`: full operator access;
- `OPERATOR_AUTH_TOKEN`: admin-compatible operator token;
- `OPERATOR_PAPER_TOKEN`: read access plus explicitly allowed paper workflows;
- `OPERATOR_READONLY_TOKEN`: GET/HEAD/OPTIONS only.

Direct API example:

```bash
curl \
  -H 'Authorization: Bearer <operator-token>' \
  http://localhost:3000/api/operator/summary
```

Mutating request example:

```bash
curl -X PUT http://localhost:3000/api/economics/intelligence/policy \
  -H 'Authorization: Bearer <operator-token>' \
  -H 'X-CSRF-Token: <csrf-token>' \
  -H 'Content-Type: application/json' \
  -d '{
    "mode":"local_only",
    "remoteSpendCapUsdPerDay":2,
    "remoteSpendCapUsdPerRequest":0.25,
    "minimumRemoteValueCoverage":3,
    "fallbackToLocalOnRemoteBlock":true
  }'
```

## 5. PostgreSQL-backed development mode

Apply migrations before starting the API:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
npm run migrations:up
```

Start the API:

```bash
MODE=mock \
OPERATOR_STORE=postgres \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
npm run api
```

Run the real database proof:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
npm run test:integration:postgres
```

The test applies to a migrated database and verifies normalized execution persistence, process-boundary hydration, append-only events, and durable worker leases.

## 6. First-minute checklist

Open **Today** and confirm:

1. the operator session is authenticated;
2. connection and refresh indicators are current;
3. mode is paper, not live;
4. storage is PostgreSQL for production-paper operation;
5. feed and market evidence are fresh;
6. open-position and active-execution counts are plausible;
7. no unexplained critical item appears in the attention queue;
8. competition ranking is either valid or explicitly blocked;
9. the economic worker container heartbeat is healthy;
10. the intelligence-routing card matches the intended deployment capability and policy.

Never interpret a missing value as zero.

## 7. Daily operating workflow

### Today

Review:

- system brief and safety strip;
- open positions;
- active, failed, or unsettled executions;
- paid-agent epoch cost;
- competition validity and standing;
- signal-to-settlement funnel;
- recent activity and warnings.

### Execution

For every active or failed record verify:

- owner and strategy;
- opportunity and economic-decision lineage;
- model quote and actual-cost state when paid intelligence was used;
- approval, submission, order, fill, and settlement stages;
- idempotency keys and version conflicts;
- error or rejection reason;
- append-only event history.

Do not retry a failed execution until venue state and the normalized database agree that no order filled.

### Positions

Compare operator-store positions with the separate competition books. These are different sources and must not be assumed reconciled. Missing cost basis or mark remains unavailable.

### Decisions

Review:

- recommendation and confidence;
- evidence freshness;
- gross expected value;
- execution, model, and uncertainty costs;
- net executable edge;
- intelligence-purchase decision;
- post-reconciliation trade decision.

A pre-call model decision never authorizes a trade.

### Competition

Before relying on the leader confirm:

- a shared epoch exists;
- both books and the snapshot are fresh;
- agent accounting version is current;
- open positions are marked;
- agent operating cost is included;
- `valid_for_ranking` is true.

### Risk & System

Review source-labelled readiness, PostgreSQL migrations, worker heartbeat, audit integrity, local fleet health, remote capability, and all unresolved warnings.

## 8. Intelligence routing

The Economics view exposes three persisted policies.

### Local fleet only

- blocks every remote quote and provider call;
- uses configured LM Studio/llama.cpp nodes;
- remains the fail-closed default even when OpenRouter credentials exist.

### Economic auto-selection

- compares a remote quote only when remote execution is enabled at deployment;
- requires quantified or sufficiently observed expected decision improvement;
- enforces per-request and daily spend caps;
- enforces the minimum value-coverage multiple;
- falls back to a healthy local route when the remote comparison is blocked and fallback is enabled.

### OpenRouter eligible

- allows an explicit remote request within hard caps;
- still requires a valid intelligence-purchase decision;
- still requires provider usage reconciliation;
- still requires a new post-reconciliation decision before paper execution.

Remote inference requires both:

```text
REMOTE_LLM_EXECUTION_ENABLED=true
OPENROUTER_API_KEY=<host-managed-secret>
```

The UI policy cannot override those deployment controls.

Before enabling OpenRouter:

1. independently confirm account credit and provider status;
2. keep a healthy local fallback;
3. begin with a small daily and per-request cap;
4. ensure unresolved `usage_pending` rows are zero or understood;
5. run one bounded paper-only request;
6. verify provider-reported actual cost;
7. verify the estimate-based decision is superseded;
8. create a fresh post-reconciliation decision.

Disable remote inference when cost cannot be reconciled, credentials may be compromised, retries become repetitive, or actual cost exceeds policy.

## 9. Worker health and maintenance

The economic worker writes an atomic heartbeat to `ECONOMIC_WORKER_HEALTH_FILE`. Compose marks it unhealthy when:

- the heartbeat is stale;
- the last run failed;
- the process is stopping.

Inspect locally:

```bash
npm run economics:health
```

Inspect in Compose:

```bash
docker compose --env-file /secure/path/portfolio.env \
  -f docker-compose.production.yml ps
```

A degraded maintenance cycle may remain process-healthy while exposing warnings. A thrown run failure must make process health false.

## 10. Fair competition epoch

Starting a new epoch archives and resets the active Hermes competition ledger. Stop both trading writers first.

Preconditions:

1. stop EventTrader;
2. stop the Hermes agent;
3. confirm the bot paper state exists and is fresh;
4. close all bot positions;
5. confirm the paid-agent cost ledger is readable;
6. back up additional operator artifacts outside the repository.

Start:

```bash
python scripts/start_competition_epoch.py --yes
python scripts/competition_scoreboard.py --print-json
```

Do not restart writers until both commands complete successfully.

## 11. Core smoke checks

```bash
curl http://localhost:3000/health
curl http://localhost:3000/ready
curl http://localhost:3000/ready/production-paper
curl http://localhost:3000/metrics.prom
```

Authenticated probes:

```bash
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/system-truth
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/competition
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/positions
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/executions
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/execution/events
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/opportunities
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/agents/costs
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/economics/intelligence/nodes
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/economics/intelligence/policy
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/audit/verify
```

Run the complete deployed smoke:

```bash
PORTFOLIO_BASE_URL=http://127.0.0.1:3000 \
OPERATOR_ADMIN_TOKEN=<operator-token> \
npm run smoke:production-paper
```

## 12. Budgeted research workflow

Paid requests must remain linked to an opportunity, expected-value hypothesis, budget, model quote, pricing snapshot, and economic decision.

Request and approve a budget through the UI or guarded API. Budget approval alone does not make an uneconomic model call or trade executable.

A remote research job must inherit locality, provider, model, quote, and decision from the selected remote quote; omission must not silently convert it back to local.

## 13. Guarded paper execution

Create or select an approved opportunity and strategy, then use only the guarded paper path.

Before approval verify:

- evidence is fresh;
- actual model usage is reconciled;
- the economic decision is newer than reconciliation;
- execution cost and forecast remain valid;
- normalized execution lineage is complete;
- the kill switch is not active.

Inspect:

```bash
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/executions
curl -H 'Authorization: Bearer <operator-token>' http://localhost:3000/api/execution/events
```

## 14. Safety controls

Stop all paper sessions:

```bash
curl -X POST http://localhost:3000/api/kill-switch/stop-paper \
  -H 'Authorization: Bearer <operator-token>' \
  -H 'X-CSRF-Token: <csrf-token>'
```

Confirm live execution remains rejected. The canonical environment must keep:

```text
LIVE_TRADING=false
LIVE_TRADING_ENABLED=false
COINBASE_DRY_RUN=true
ALLOW_POLYMARKET_ORDER_SUBMISSION=false
ALLOW_LIVE_SETTLEMENT_REDEMPTION=false
```

## 15. Incident response

### API or readiness failure

- stop new actions;
- activate the paper kill switch when available;
- inspect `/ready/production-paper` blockers and logs;
- verify PostgreSQL and migrations;
- restart the API once;
- roll back when durable state or policy does not hydrate correctly.

### Worker heartbeat failure

- deny new paid-research approvals;
- inspect health JSON, logs, and `runtime_jobs`;
- confirm leases recover without duplicate execution;
- restart once;
- roll back when health remains stale or jobs duplicate.

### Local fleet outage

- keep the policy local-only unless remote use was deliberately deployed and budgeted;
- verify DNS, endpoint health, exact model ID, context, and concurrency;
- never silently substitute another model;
- pause research when no approved healthy route exists.

### OpenRouter outage or cap exhaustion

- switch to local-only or retain economic-auto with local fallback;
- do not raise caps to clear the incident;
- review committed cost, actual cost, and `usage_pending`;
- retry only after provider status and credits are independently confirmed.

### Unreconciled `usage_pending`

- block dependent paid execution decisions;
- preserve provider request/generation IDs;
- retry reconciliation idempotently;
- never create a second cost row or billable provider call for the same reservation.

### Duplicate execution suspected

- activate the paper kill switch;
- compare normalized execution, order, fill, and event records with the venue;
- inspect every idempotency key;
- do not retry until the authoritative state is known.

## 16. Shutdown

1. stop Hermes and EventTrader through their supervisor;
2. stop the economic worker and verify lease release/recovery;
3. stop the API;
4. preserve runtime state and logs outside source control;
5. run `npm run runtime-artifacts:validate` before committing;
6. never add runtime ledgers or backups to Git.
