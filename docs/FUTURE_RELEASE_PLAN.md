# Future Release Plan: Production-Hardening Roadmap

This plan defines the next implementation sequence after the P2 opportunity/risk persistence work. It is intentionally conservative: repair and validation come before new features.

Live trading remains blocked. All work below is for paper/review workflows until a separate live-certification release exists.

## Current baseline assumptions

The system now has these major product surfaces:

- API-backed operator dashboard.
- Strategy templates and deterministic backtest scaffolding.
- Paper execution flow with live execution blocked.
- Opportunity review feed.
- Research jobs and agent cost ledger.
- Explicit budget approval workflow.
- Connector-driven market snapshot ingestion and opportunity generation scaffolds.
- P2 Postgres row repository for market snapshots, budget approvals, research jobs, opportunities, risk breakdowns, and agent cost ledger.

## P0 — Repair, parse, and validation baseline

### P0.1 Repair merged P2 store if needed

PR #25 had a conflict-resolution issue in `packages/storage/src/postgresOperatorStoreP2.mjs`. A cleanup commit was pushed to the feature branch after the PR was already merged, so the next implementation PR must verify whether current `main` includes the cleanup.

Acceptance criteria:

- `packages/storage/src/postgresOperatorStoreP2.mjs` has one coherent `load()` method.
- `load()` reads `marketDataSnapshots`, `budgetApprovals`, `researchJobs`, `opportunities`, `riskBreakdowns`, and `agentCostLedger` exactly once.
- `upsertOpportunity`, `upsertRiskBreakdown`, and `upsertOpportunityBundle` are each defined once and have valid method boundaries.
- No conflict markers or duplicated splice artifacts remain.

### P0.2 Add module parse smoke tests

Add a lightweight smoke test that imports critical modules directly.

Target modules:

- `apps/api/src/server.p1.mjs`
- `apps/api/src/operatorRouter.mjs`
- `apps/api/src/opportunityFlows.mjs`
- `apps/api/src/opportunityGenerator.mjs`
- `packages/storage/src/opportunityRowRepository.mjs`
- `packages/storage/src/postgresOperatorStoreP2.mjs`
- `packages/connectors/src/marketDataAdapters.mjs`

Acceptance criteria:

- Syntax or duplicate-export issues fail CI immediately.
- The smoke test does not require a live database.

### P0.3 Re-check existing validation commands

Acceptance criteria:

```bash
pnpm test
pnpm build
pnpm api:validate
pnpm migrations:validate
```

All should run without syntax failures.

## P1 — Browser-level operator workflow testing

### P1.1 Add browser e2e harness

Add Playwright or a similarly lightweight browser test harness.

Acceptance criteria:

- Starts the API/web server in paper/dev mode.
- Opens the dashboard.
- Verifies core panels render: Overview, Opportunities, Polymarket, Agents, Risk, Approvals, Audit.
- Verifies live execution is visibly blocked.

### P1.2 Test connector-to-opportunity workflow from UI

Acceptance criteria:

- Trigger connector ingest or generation from an operator action or test helper.
- Verify opportunity cards appear.
- Verify risk breakdown and net expected value render.
- Verify Polymarket/watch candidates remain review-only.

### P1.3 Test budget approval workflow from UI

Acceptance criteria:

- Request a budget approval from the Agents UI.
- Approve the budget approval.
- Create/request a research job that uses `budgetApprovalId`.
- Verify the job row displays the approval ID.
- Verify the audit feed includes request and approval events.

## P2 — Complete targeted Postgres persistence migration

### P2.1 Route-level transaction helper

Current targeted hooks are opportunistic. Add a route-level helper for Postgres-backed stores that persists a result bundle in one explicit transaction.

Acceptance criteria:

- `budgetApproval`, `job`, `ledger`, `opportunity`, and `riskBreakdown` artifacts can be saved atomically.
- On failure, no partial target-table writes remain.
- Memory/file stores still use existing behavior.

### P2.2 Move selected routes off full-state save for Postgres

Target routes:

- `POST /api/agents/budget-approvals`
- `POST /api/agents/budget-approvals/:id/decision`
- `POST /api/agents/jobs`
- `POST /api/opportunities`
- `POST /api/opportunities/generate-from-connectors`
- `POST /api/opportunities/:id/request-research`

Acceptance criteria:

- Postgres mode persists these through targeted rows.
- File/memory mode remains unchanged.
- Tests prove behavior for both store families.

### P2.3 Add readiness checks for product tables

Acceptance criteria:

- `/ready/production-paper` verifies required P2 tables exist.
- Missing opportunity workflow tables block production-paper readiness.
- The readiness response names missing tables explicitly.

## P3 — Real connector adapter scaffolding

### P3.1 Connector registry

Add a registry that loads adapters based on environment configuration.

Example env shape:

```bash
MARKET_CONNECTORS=paper-crypto,polymarket-watch
POLYMARKET_ENABLED=false
COINBASE_MARKET_DATA_ENABLED=false
```

Acceptance criteria:

- Demo/static adapters remain default.
- Real adapters are opt-in.
- Missing credentials disable real adapters without crashing.

### P3.2 Real market data adapter interface

Define adapter contract:

```ts
listSnapshots(): Promise<MarketDataSnapshot[]>
health(): Promise<ConnectorHealth>
```

Acceptance criteria:

- Each adapter reports health, freshness, and error state.
- Connector failures are captured as ingest errors and surfaced in the UI/API.

### P3.3 Polymarket discovery scaffold

Acceptance criteria:

- Adds an adapter stub for market discovery.
- Produces normalized prediction-market snapshots.
- Does not submit orders.
- Includes tests using static fixture responses.

### P3.4 Historical replay/backtest adapter scaffold

Acceptance criteria:

- Adds an interface for replay data sources.
- Can replay deterministic fixture candles/events.
- Links generated opportunity candidates to replay/backtest evidence.
- Does not require paid data providers.

## P4 — Risk engine and opportunity quality gates

### P4.1 Backtest-required gates

Acceptance criteria:

- Opportunities cannot move from `needs_review` to `approved` unless required evidence is present, unless an explicit override is supplied.
- Overrides are audited and displayed in the opportunity detail.

### P4.2 Research cost drag model

Acceptance criteria:

- Opportunity expected value includes accumulated research/model cost attribution.
- Cost-per-opportunity is visible in UI and API.
- Repeated research requests degrade net expected value.

### P4.3 Approval policy matrix

Acceptance criteria:

- Configurable policy for required approvals by risk tier.
- Higher capital-at-risk opportunities require stricter review.
- Prediction-market candidates remain separately labeled and review-only.

## P5 — Operator UI productization

### P5.1 Replace demo buttons with form-driven workflows

Acceptance criteria:

- Budget approval request form selects agent, market scope, projected tokens, projected cost, and reason.
- Connector generation controls show source, status, and errors.
- Opportunity review actions require a reason.

### P5.2 UI state quality

Acceptance criteria:

- Loading, error, empty, and success states exist for every panel.
- Refreshes do not create duplicate opportunities.
- Tables remain usable with more than 100 records.

## P6 — Live trading certification boundary

This is intentionally outside the next release. Before live execution can be enabled, the project needs a separate live-certification design and review.

Minimum future requirements:

- exchange-specific sandbox certification
- order preview/reconciliation hardening
- position/risk limit enforcement
- kill-switch drills
- audit immutability validation
- deployment rollback plan
- manual sign-off gate

## Suggested next PR order

1. `fix-p2-store-and-parse-smoke-tests`
2. `browser-e2e-operator-workflow`
3. `postgres-route-transaction-helper`
4. `connector-registry-and-env-gates`
5. `polymarket-discovery-scaffold`
6. `historical-replay-adapter-scaffold`
7. `risk-gates-and-approval-policy`
8. `operator-ui-form-workflows`

## Definition of done for each future PR

Every implementation PR should include:

- tests for new behavior
- docs/API contract update when routes change
- explicit safety posture
- no hidden live execution enablement
- no UI-only placeholders unless paired with tracked TODOs and disabled state
- clear validation commands in the PR body
