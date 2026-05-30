# UI End-to-End Testing

This document describes the current dependency-free UI and operator-flow e2e coverage.

## Scope

The UI e2e pass validates two things:

1. The expanded dashboard assets expose the intended trading-bot operator surfaces.
2. The HTTP API can execute the full paper operator workflow end-to-end.

Live trading remains blocked.

## Tests

### Static dashboard asset coverage

```text
tests/e2e-ui-assets.test.mjs
```

Validates that the UI includes:

- Overview
- Portfolio
- Live Markets
- Strategies
- Backtesting
- Opportunities
- Polymarket
- Agents
- Risk
- Approvals
- Executions/Paper
- Audit

Also validates opportunity/risk/cost fields:

- total money risked
- max loss
- potential upside
- gross expected value
- net expected value
- agent research cost
- model inference cost

### HTTP operator workflow coverage

```text
tests/e2e-operator-flow.test.mjs
```

Starts the API in-process with a memory store and validates:

1. `/` serves the dashboard HTML.
2. `/ui/app.js` serves the dashboard app.
3. `/ui/dashboard-data.js` serves scaffold market/opportunity/agent data.
4. `/api/operator/summary` returns current state.
5. Strategy is created from a template.
6. Backtest is run.
7. Approval is requested.
8. Approval is approved.
9. Paper execution is started.
10. Paper signal is filled.
11. Position is created.
12. Audit records the paper fill.
13. Live execution route remains blocked.
14. JSON metrics work.
15. Prometheus-style metrics work at `/metrics.prom`.

## Run commands

```bash
pnpm test
pnpm build
```

Targeted test commands:

```bash
node --test tests/e2e-ui-assets.test.mjs
node --test tests/e2e-operator-flow.test.mjs
```

## Browser/manual smoke

Start the app:

```bash
pnpm api
```

Open:

```text
http://localhost:3000/
```

Manual checks:

- Dashboard loads.
- Navigation links jump to each section.
- Overview cards populate.
- Portfolio accounts populate.
- Market scaffold rows render.
- Opportunity cards show risk/cost/EV values.
- Polymarket card renders and keeps live orders blocked.
- Agents tab shows token/model cost placeholders.
- Strategy/backtest/approval buttons still work.
- Paper execution can be started/stopped.
- Audit verification panel renders.

## Current limitations

This is not Playwright/Cypress browser automation yet. It is dependency-free HTTP and static-asset e2e coverage.

Future browser e2e should add:

- Playwright install and CI job.
- Real DOM interaction tests.
- Accessibility checks.
- Screenshot artifacts.
- Approval-feed interaction tests.
- Polymarket opportunity card tests.
- Agent budget/approval tests.
