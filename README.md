# Portfolio OS

Portfolio OS is a guarded trading research and paper-execution system built around two cooperating but independently measured competitors:

- **Deterministic bot:** the EventTraderV4 strategy and execution stack.
- **Paid agent:** the Hermes model-backed trader, including attributable model/API and compute cost.

The operator console is designed to answer a practical daily question: **what are the bot, agent, and execution engine doing right now, why are they doing it, and is the resulting evidence trustworthy?**

> **Safety status:** live order execution is not certified. The supported operator workflow is paper/guarded, and incomplete or stale evidence fails closed.

## What the system provides

### Daily trading operations console

The default interface is organized around day-to-day operation rather than a collection of unrelated dashboards:

- **Today:** plain-language system brief, safety state, open exposure, active executions, operator attention queue, signal-to-settlement funnel, and recent activity.
- **Execution:** expandable execution lifecycles with owner, strategy, symbol, side, notional, approval, submission, fills, settlement, errors, and event history.
- **Positions:** source-labelled holdings, quantities, cost basis and marks when available, market value, and unrealized P&L.
- **Decisions:** bot and agent recommendations, confidence, rationale, gross expected value, research/model cost, net expected value, and approval state.
- **Competition:** shared-epoch bot-versus-agent results after fees and paid-agent operating cost.
- **Agent:** inference economics, budget approvals, cost coverage, learning evidence, and promotion controls.
- **Risk & System:** source-labelled trading mode, execution authority, feed freshness, paper-book status, exposure, drawdown, and warnings.

### Trading and research capabilities

- Coinbase-oriented market data and guarded execution adapters.
- Deterministic strategy evaluation and backtesting.
- Opportunity, risk, approval, and agent-research workflows.
- Execution planning, preview, reconciliation, settlement, and audit events.
- Agent cost attribution and explicit research budget approvals.
- Durable learning lineage and challenger promotion/rollback metadata.
- Runtime state validation that rejects generated ledgers and backups from source control.

## Trust and accounting model

The interface never names a competition winner unless both competitors have fresh, comparable evidence from the same competition epoch.

### Bot equity

```text
bot_equity = paper_cash + marked_unrealized_pnl
```

Realized P&L is already reflected in paper cash and is not added a second time.

### Agent accounting v2

The Hermes ledger uses one consistent contract for long and short positions:

```text
notional = reserved_margin × leverage
quantity = notional ÷ entry_price
price_pnl = signed_price_change × quantity
net_trade_pnl = price_pnl - entry_fee - exit_fee
```

Leverage changes quantity once. It is not multiplied into P&L a second time.

### Cost-adjusted competition score

```text
normalized_agent_net_equity = normalized_agent_gross_equity - post_epoch_model_and_compute_cost
normalized_bot_net_equity   = normalized_bot_gross_equity
leader                       = higher valid normalized net equity
```

Missing epochs, stale books, invalid accounting versions, unavailable marks, or ledger-integrity failures block ranking.

## Quick start

### Prerequisites

- Node.js 22
- pnpm 9.12.3
- Python 3.12
- Linux is recommended for the local file-locking safety controls

### Install

```bash
pnpm install
python -m pip install --upgrade pip
python -m pip install -r deploy/requirements.venv-lock.txt
```

### Validate

```bash
pnpm runtime-artifacts:validate
pnpm test
pnpm build
```

### Run the local paper operator console

```bash
MODE=mock OPERATOR_AUTH_REQUIRED=false pnpm api
```

Open:

```text
http://localhost:3000/
```

The default local operator state is written to:

```text
data/operator-state.json
```

Runtime state is intentionally ignored by Git and must not be committed.

## Start an authenticated local console

```bash
OPERATOR_AUTH_REQUIRED=true \
OPERATOR_AUTH_TOKEN='replace-with-a-strong-token' \
MODE=mock \
pnpm api
```

Protected API requests use:

```http
Authorization: Bearer replace-with-a-strong-token
```

Read-only and paper-scoped roles are also supported through the corresponding operator tokens. See [`docs/API_CONTRACT_P0_P1.md`](docs/API_CONTRACT_P0_P1.md).

## Start a fair bot-versus-agent competition epoch

Starting an epoch is destructive to the active agent competition ledger because the existing ledger is archived and replaced with a clean accounting-v2 book.

Before starting:

1. Stop EventTrader and the Hermes agent.
2. Confirm the bot book is fresh.
3. Close all bot positions; the epoch starter requires a flat bot book.
4. Confirm the paid-agent cost ledger is readable.

Then run:

```bash
python scripts/start_competition_epoch.py --yes
python scripts/competition_scoreboard.py --print-json
```

The epoch starter:

- obtains the bot writer lock or refuses to proceed;
- archives and resets the Hermes ledger;
- snapshots bot equity, agent equity, and agent cost baselines;
- writes `data/competition_epoch.json`;
- invalidates the previous competition snapshot.

The console will show **No trustworthy winner yet** until a fresh valid competition snapshot is published.

## Important read APIs

| Route | Purpose |
|---|---|
| `GET /api/system-truth` | Source-labelled mode, feed, service, paper-book, and execution truth. |
| `GET /api/competition` | Shared-epoch cost-adjusted competition state and ranking validity. |
| `GET /api/positions` | Operator positions, accounts, capital in play, and source label. |
| `GET /api/executions` | Execution records and current lifecycle status. |
| `GET /api/execution/events` | Execution-engine event history. |
| `GET /api/opportunities` | Pre-trade bot and agent decisions. |
| `GET /api/activity-feed` | Aggregated audit, opportunity, and execution activity. |
| `GET /api/agents/costs` | Paid-agent cost ledger and summary. |
| `GET /api/market-data/live-quotes` | Current observed quotes used to enrich position display. |

## Repository layout

```text
apps/
  api/                    Node operator API and System Truth/competition routes
  web/                    Daily trading operations console
coinbase/                 Coinbase trading, strategy, and execution components
packages/                 Shared storage, execution, adapters, audit, and config
scripts/                  Scoreboard, accounting, epoch, validation, and operations tools
tests/                    Node and Python regression/integration tests
trading_system/           Broader Python research and trading platform
docs/                     Architecture, API contracts, runbooks, and deployment guidance
data/                     Local runtime state only; generated files are ignored
```

## CI model

Merge-critical checks cover:

- tracked runtime-artifact validation;
- web build and daily-operations UI contracts;
- accounting-v2 and competition-epoch regression tests;
- the complete Node test suite;
- focused coverage for the accounting and competition modules.

The broad legacy Python suite and performance benchmarks run as visible diagnostic jobs. They currently include environment-dependent and historical tests that are useful for debt tracking but are not permitted to hide failures in the merge-critical UI and accounting gates.

## Documentation

- [`docs/DAILY_OPERATOR_GUIDE.md`](docs/DAILY_OPERATOR_GUIDE.md) — page-by-page daily use and incident workflow.
- [`docs/COMPETITION_CONSOLE_ARCHITECTURE.md`](docs/COMPETITION_CONSOLE_ARCHITECTURE.md) — data flow, trust boundaries, accounting, and UI architecture.
- [`docs/OPERATOR_RUNBOOK_P0_P1.md`](docs/OPERATOR_RUNBOOK_P0_P1.md) — local startup, auth, competition epoch, and recovery commands.
- [`docs/API_CONTRACT_P0_P1.md`](docs/API_CONTRACT_P0_P1.md) — current operator API surface.
- [`docs/PRODUCTION_DEPLOYMENT_CHECKLIST.md`](docs/PRODUCTION_DEPLOYMENT_CHECKLIST.md) — deployment readiness checks.

## Current limitations

- Live trading remains uncertified and blocked by default.
- Coinbase balance sync may not provide cost basis, so position P&L can remain unavailable.
- Competition snapshot publication still needs to be supervised continuously in deployed environments.
- The legacy Python dashboard remains in the repository until endpoint and operator-workflow parity is proven.
- Browser-level end-to-end tests against a deployed console remain a merge/deployment follow-up.

## License

MIT License — see `LICENSE`.
