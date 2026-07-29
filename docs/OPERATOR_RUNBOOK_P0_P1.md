# Operator Runbook — Daily Paper/Guarded Operations

This runbook covers the Portfolio OS daily operator console, guarded paper execution, paid-agent cost monitoring, and the bot-versus-agent competition. It does not certify live trading.

## 1. Local prerequisites

- Node.js 22
- pnpm 9.12.3
- Python 3.12
- Linux recommended for `flock`-based writer safety
- PostgreSQL only when running the Postgres-backed operator store

Install:

```bash
pnpm install
python -m pip install --upgrade pip
python -m pip install -r deploy/requirements.venv-lock.txt
```

## 2. Validate before starting

```bash
pnpm runtime-artifacts:validate
pnpm test
pnpm build
```

The runtime-artifact check must pass before trusting the working tree. Generated ledgers, health snapshots, state, and backups must remain outside source control.

## 3. Start local mock/paper mode

```bash
MODE=mock \
OPERATOR_AUTH_REQUIRED=false \
pnpm api
```

Open:

```text
http://localhost:3000/
```

Default local state:

```text
data/operator-state.json
```

The web UI and API are served by `apps/api/src/server.p1.mjs`.

## 4. Start with local authentication

```bash
MODE=mock \
OPERATOR_AUTH_REQUIRED=true \
OPERATOR_AUTH_TOKEN='replace-with-a-strong-token' \
pnpm api
```

Direct API example:

```bash
curl \
  -H 'Authorization: Bearer replace-with-a-strong-token' \
  http://localhost:3000/api/operator/summary
```

Available roles:

- `OPERATOR_ADMIN_TOKEN`: full operator access.
- `OPERATOR_AUTH_TOKEN`: admin-compatible operator token.
- `OPERATOR_PAPER_TOKEN`: read access plus approved paper workflows.
- `OPERATOR_READONLY_TOKEN`: GET/HEAD/OPTIONS only.

The static browser UI needs an authenticated same-origin session or a trusted local proxy that adds the bearer token when auth is enabled.

## 5. Start Postgres-backed mode

Start PostgreSQL:

```bash
docker compose up -d postgres
```

Apply migrations:

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
pnpm migrations:up
```

Run the API:

```bash
MODE=mock \
OPERATOR_STORE=postgres \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
pnpm api
```

Run the opt-in integration smoke test:

```bash
RUN_POSTGRES_INTEGRATION=true \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/arb \
pnpm test:integration:postgres
```

## 6. First-minute operator checklist

Open **Today** and verify:

1. Connection is green and refresh time is recent.
2. Trading mode is expected.
3. Execution status is source-labelled and not unknown.
4. Feed freshness is acceptable.
5. Warning banners are understood.
6. Open-position and active-execution counts are plausible.
7. The attention queue contains no unexplained critical item.
8. Competition is either valid or explicitly blocked.

Do not interpret a missing value as zero.

## 7. Normal daily workflow

### Today

Use the default page to identify what changed and what needs attention.

Review:

- system brief;
- safety strip;
- open positions;
- active or failed executions;
- paid-agent epoch cost;
- competition standing;
- signal-to-settlement funnel;
- recent activity.

### Execution

Filter active or failed records. Expand each execution and verify:

- owner and strategy;
- decision/approval/submission/fill/settlement stages;
- order and fill counts;
- entry/target/stop evidence;
- error or rejection reason;
- event history.

Before retrying a failed execution, confirm that a duplicate order did not fill or settle elsewhere.

### Positions

Review operator-store positions and quote enrichment. Missing cost basis or mark remains unavailable.

Compare the operator row count with the separate competition-book position count. They are different sources and are not automatically reconciled.

### Decisions

Review opportunities before execution:

- recommendation and confidence;
- rationale;
- gross expected value;
- agent/model cost;
- net expected value;
- approval state.

A positive expected value is not realized P&L.

### Competition

Review ranking validity before the leader. Confirm:

- shared epoch exists;
- books and snapshot are fresh;
- agent accounting version is 2;
- both books are marked and comparable;
- agent operating cost is included.

### Agent

Review cost coverage, break-even gap, daily budget, pending approvals, and promotion evidence.

### Risk & System

Review source-labelled health and any unresolved warnings. Restore authoritative evidence rather than substituting local estimates.

## 8. Start a fair competition epoch

Starting a new epoch archives/resets the active Hermes competition ledger. Stop both trading writers first.

### Preconditions

1. Stop EventTrader.
2. Stop the Hermes agent.
3. Confirm the bot paper state exists and is fresh.
4. Close all bot positions.
5. Confirm `data/agent_cost_ledger.json` is readable.
6. Back up any additional operator artifacts needed outside the managed archives.

### Start

```bash
python scripts/start_competition_epoch.py --yes
```

The command refuses to run when:

- the EventTrader writer lock is held;
- the bot state is stale, missing, or invalid;
- the bot has open positions;
- bot equity is unavailable;
- the paid-agent cost baseline is unavailable.

It writes:

```text
data/competition_epoch.json
```

It archives prior files under:

```text
data/legacy_agent_ledgers/
data/competition_epochs/
```

Generate the first post-reset snapshot:

```bash
python scripts/competition_scoreboard.py --print-json
```

The snapshot is written to:

```text
data/competition_state.json
```

Restart the bot and agent only after the epoch command completes successfully.

## 9. Competition verification commands

Print the current normalized scorecard:

```bash
python scripts/competition_scoreboard.py --print-json
```

Inspect the API form:

```bash
curl http://localhost:3000/api/competition
```

A valid response must show:

```json
{
  "standings": {
    "valid_for_ranking": true
  }
}
```

When `valid_for_ranking` is false, use the response `warnings` array. Do not manually edit the snapshot to clear a warning.

## 10. API smoke checks

```bash
curl http://localhost:3000/health
curl http://localhost:3000/ready
curl http://localhost:3000/ready/production-paper
curl http://localhost:3000/api/system-truth
curl http://localhost:3000/api/competition
curl http://localhost:3000/api/positions
curl http://localhost:3000/api/executions
curl http://localhost:3000/api/execution/events
curl http://localhost:3000/api/opportunities
curl http://localhost:3000/api/activity-feed
curl http://localhost:3000/api/agents/costs
curl http://localhost:3000/api/market-data/live-quotes
```

With authentication enabled, add:

```bash
-H 'Authorization: Bearer replace-with-a-strong-token'
```

## 11. Guarded opportunity and research workflow

Request an explicit paid-research budget:

```bash
curl -X POST http://localhost:3000/api/agents/budget-approvals \
  -H 'Content-Type: application/json' \
  -d '{
    "agentId":"market-research-agent",
    "marketScope":"BTC-USD",
    "projectedCost":5,
    "projectedTokens":25000,
    "requestedBy":"operator",
    "reason":"Bounded follow-up research for an existing opportunity"
  }'
```

Approve using the returned ID:

```bash
curl -X POST http://localhost:3000/api/agents/budget-approvals/<id>/decision \
  -H 'Content-Type: application/json' \
  -d '{
    "status":"approved",
    "reviewer":"operator",
    "approvedCostLimit":5,
    "approvedTokenLimit":25000,
    "reason":"Bounded research approved"
  }'
```

Paid requests should remain linked to an opportunity, expected-value hypothesis, budget, and lineage record.

## 12. Guarded paper execution workflow

Create or select an approved opportunity/strategy through the operator API, then create an execution plan and submit it only through the guarded paper path.

Inspect:

```bash
curl http://localhost:3000/api/executions
curl http://localhost:3000/api/execution/events
```

The UI is the preferred lifecycle view because it groups records, orders, fills, and events by execution ID.

## 13. Safety checks

### Confirm live execution remains blocked

```bash
curl -X POST http://localhost:3000/api/execution/live/orders \
  -H 'Content-Type: application/json' \
  -d '{"side":"buy"}'
```

Expected:

```json
{
  "ok": false,
  "error": "live_execution_disabled"
}
```

### Verify audit integrity

```bash
curl http://localhost:3000/api/audit/verify
```

### Verify source control hygiene

```bash
pnpm runtime-artifacts:validate
```

## 14. Incident response

### Failed or rejected execution

1. Open **Execution** and filter `Failed / rejected`.
2. Expand the newest record.
3. Determine the last completed lifecycle stage.
4. Read error/rejection evidence and event history.
5. Check for duplicate or partially filled venue orders.
6. Review the kill switch and System Truth.
7. Retry only after the failure mode is understood.

### Stale System Truth

1. Open **Risk & System**.
2. Identify the stale source.
3. Restore the health/feed publisher.
4. Wait for a fresh snapshot.
5. Do not copy operator-store values into the health snapshot manually.

### Competition ranking blocked

1. Read `/api/competition` warnings.
2. Confirm `data/competition_epoch.json` exists.
3. Confirm agent and bot state are fresh.
4. Confirm all open positions have marks.
5. Confirm agent ledger accounting version 2 and ranking eligibility.
6. Republish the scoreboard.

### Paid-agent cost spike

1. Pause or deny new budget approvals.
2. Review `/api/agents/costs` and recent opportunities.
3. Verify each cost row has a decision/market purpose.
4. Compare gross P&L and cost coverage.
5. Do not increase trading risk merely to chase cost break-even.

## 15. Recovery controls

Stop all operator paper sessions:

```bash
curl -X POST http://localhost:3000/api/kill-switch/stop-paper
```

Enable global kill switch:

```bash
curl -X POST http://localhost:3000/api/kill-switch \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"reason":"operator_incident"}'
```

Disable only after the incident is resolved:

```bash
curl -X POST http://localhost:3000/api/kill-switch \
  -H 'Content-Type: application/json' \
  -d '{"enabled":false,"reason":"operator_incident_resolved"}'
```

## 16. Shutdown

1. Stop the Hermes and EventTrader processes through their supervisor.
2. Stop the Node API.
3. Preserve runtime state locally.
4. Run `pnpm runtime-artifacts:validate` before committing repository changes.
5. Never add runtime ledgers or backups to Git.
