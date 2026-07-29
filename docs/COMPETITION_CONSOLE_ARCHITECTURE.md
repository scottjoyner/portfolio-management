# Daily Trading Operations and Competition Architecture

## Purpose

Portfolio OS is an operator-facing paper/guarded trading platform with two independently measured competitors:

- **Deterministic bot:** EventTraderV4 and its canonical paper book.
- **Paid agent:** the Hermes model-backed trader using accounting v2 and attributable model/API/compute costs.

The product has two related but distinct goals:

1. Make the current trading, decision, position, and execution state understandable during daily operation.
2. Run a trustworthy shared-epoch competition that measures whether paid model reasoning produces enough incremental value to cover its cost and outperform the deterministic bot.

The daily operations goal is primary in the UI. The competition is a focused analytical view and never overrides system or execution safety.

## Architectural principles

### Source-labelled truth

Every displayed value should identify or preserve its source. The UI must not silently replace missing canonical data with a convenient local estimate.

Examples:

- System mode and paper-book health come from the System Truth snapshot.
- Positions come from the operator store and are labelled as such.
- Competition results come from the generated competition snapshot.
- Execution lifecycle data comes from the execution engine/store.
- Paid-agent costs come from the agent cost ledger and shared-epoch baseline.

### Fail closed

Unknown, stale, inconsistent, or unmarked data does not become zero and does not become healthy. Ranking and execution claims remain blocked until the required evidence is available.

### Separate decision, execution, and outcome

The data model treats these as separate stages:

```text
market evidence
  → opportunity / signal
  → risk and approval decision
  → execution plan
  → order submission
  → fill
  → settlement / reconciliation
  → realized outcome
  → learning evidence
```

A signal is not an order, a submitted order is not a fill, and a fill is not necessarily settled.

### Cost-aware intelligence

The paid agent is evaluated after attributable cost. Model use is not free alpha and cannot be compared to the deterministic bot on gross equity alone.

## Runtime components

### Operator API

Primary entry point:

```text
apps/api/src/server.p1.mjs
```

Responsibilities:

- authentication and role authorization;
- CSRF/security headers;
- operator-store loading;
- System Truth and competition snapshot routes;
- read-only positions route;
- delegation to operator workflows and execution routes;
- request logging and metrics.

### Daily operations web console

Files:

```text
apps/web/src/index.html
apps/web/src/app.js
apps/web/src/styles.css
```

The console is intentionally dependency-light and polls the operator API directly. It refreshes every five seconds while visible and refreshes when the browser tab becomes visible again.

### Operator store

Core state includes:

- accounts and positions;
- strategies, backtests, and approvals;
- opportunities and risk breakdowns;
- research jobs, budgets, approvals, and agent cost rows;
- paper executions and execution records;
- audit events and kill-switch state.

The default local file is:

```text
data/operator-state.json
```

It is runtime state, not source code, and is ignored by Git.

### System Truth publisher

`apps/api/src/systemTruth.mjs` reads source-labelled runtime evidence, including `data/system-health.json` when fresh and valid. It intentionally does not infer canonical paper-book or execution status from operator state when the health snapshot is unavailable.

### Competition scoreboard

```text
scripts/competition_scoreboard.py
```

The scoreboard reads:

- the accounting-v2 Hermes ledger;
- the canonical EventTraderV4 paper state;
- the agent cost ledger;
- the shared competition epoch manifest.

It writes:

```text
data/competition_state.json
```

The API reads that file through `apps/api/src/competitionSnapshot.mjs` and independently revalidates freshness, accounting version, eligibility, and comparable starting capital.

### Competition epoch starter

```text
scripts/start_competition_epoch.py
```

This guarded command:

1. obtains the EventTrader writer lock or refuses to run;
2. requires a fresh valid and flat bot book;
3. reads the current paid-agent cost baseline;
4. archives and resets the Hermes ledger to accounting v2;
5. archives the previous epoch manifest;
6. records bot, agent, and cost baselines;
7. writes `data/competition_epoch.json`;
8. removes the old competition snapshot.

### Hermes accounting v2

```text
scripts/hermes_agent_accounting.py
scripts/hermes_agent_trader.py
```

All active long, short, add, partial-close, close, fee, and mark-to-market operations use one accounting contract. Legacy ledgers are not automatically upgraded; they fail closed and must be archived/reset.

### Learning lineage and challengers

```text
scripts/learning_lineage.py
scripts/challenger_manager.py
```

These components persist the evidence required to connect decisions and outcomes to versioned learning proposals. Promotion remains guarded by evaluation and rollback metadata rather than recent-trade anecdotes.

## Daily UI information architecture

### Today

The default page contains:

- a plain-language current-state brief;
- trading mode, execution, feed, and competition safety strip;
- open-position, active-execution, agent-cost, and race KPIs;
- prioritized operator attention queue;
- signal-to-settlement pipeline;
- compact position preview;
- recent activity timeline.

The page is a summary. Detailed conclusions should be verified in the corresponding execution, positions, decisions, competition, agent, or system view.

### Execution

The Execution page groups each record into one expandable lifecycle rather than a flat ledger row.

It exposes:

- execution owner and strategy/source;
- execution ID, symbol, side, notional, and confidence;
- decision/approval/submission/fill/settlement progression;
- order and fill counts;
- entry, target, and stop evidence;
- errors and rejection reasons;
- execution event history.

### Positions

The Positions page uses:

```text
GET /api/positions
GET /api/market-data/live-quotes
```

Operator positions are enriched with current quotes only when the inputs are valid. Missing cost basis remains unavailable, so the UI does not invent unrealized P&L.

### Decisions

The Decisions page renders normalized opportunities with:

- source agent or strategy;
- recommendation and confidence;
- persisted rationale;
- gross expected value;
- attributable model/research cost;
- net expected value;
- approval state.

### Competition

The Competition page contains the normalized agent and bot scorecards, validity evidence, epoch reference, cost coverage, break-even gap, and after-cost alpha.

### Agent

The Agent page separates:

- economics and budget controls;
- cost coverage and break-even;
- learning lineage;
- challenger promotion requirements.

### Risk & System

The final page displays source-labelled system health, paper-book truth, execution decision, feed/cache/service freshness, drawdowns, known exposure, and unresolved warnings.

## API contracts used by the console

| Route | Source and purpose |
|---|---|
| `GET /api/system-truth` | Canonical source-labelled runtime health and execution truth. |
| `GET /api/competition` | Validated cost-adjusted shared-epoch scorecard. |
| `GET /api/positions` | Operator-store positions, accounts, and capital in play. |
| `GET /api/executions` | Execution records. |
| `GET /api/execution/events` | Execution-engine lifecycle events. |
| `GET /api/opportunities` | Pre-trade decisions and expected-value evidence. |
| `GET /api/activity-feed` | Aggregated audit, execution, and opportunity activity. |
| `GET /api/agents/costs` | Agent cost rows and summary. |
| `GET /api/agents/budgets` | Paid-agent budget policies. |
| `GET /api/agents/budget-approvals` | Explicit spend approvals. |
| `GET /api/market-data/live-quotes` | Current observed quote enrichment. |

## Competition accounting

### Bot

```text
raw_bot_equity = paper_cash + marked_unrealized_pnl
normalized_bot_pnl = raw_bot_equity - epoch_bot_equity_baseline
normalized_bot_equity = normalized_starting_capital + normalized_bot_pnl
```

Realized P&L is already included in `paper_cash`.

### Agent

```text
notional = margin × leverage
quantity = notional ÷ entry_price
price_pnl = signed_price_change × quantity
trade_net_pnl = price_pnl - entry_fee - exit_fee
```

For competition normalization:

```text
normalized_agent_gross_pnl = raw_agent_equity - epoch_agent_equity_baseline
post_epoch_agent_cost = current_agent_cost - epoch_agent_cost_baseline
normalized_agent_net_equity = normalized_starting_capital
                              + normalized_agent_gross_pnl
                              - post_epoch_agent_cost
```

### Ranking validity

A ranking is valid only when:

1. A shared epoch manifest exists.
2. Agent and bot data refer to the same epoch.
3. The competition snapshot and books are fresh.
4. The Hermes ledger uses accounting version 2.
5. The Hermes ledger is explicitly ranking eligible.
6. Both normalized starting-capital values are finite and equal.
7. Open positions have current marks.
8. Agent cost is available and attributable after the epoch baseline.
9. No ledger-integrity blocker is active.

## Position truth boundaries

`GET /api/positions` returns operator-store positions. It does not claim to be the canonical EventTrader or Hermes ledger.

The UI can calculate market value when quantity and mark are valid. It can calculate unrealized P&L when quantity, mark, and cost basis are valid. Otherwise the field remains unavailable.

Competition open-position counts come from the competition books and are displayed separately from operator-store rows.

## Execution authority boundary

The console displays:

```text
Local operator execution (non-canonical)
```

This label is intentional. The Node operator workflow can model and execute guarded paper lifecycles, but it does not certify live venue authority. Live routes remain blocked unless a separate certification and safety program is completed.

## Persistence and runtime-artifact policy

Generated state is ignored and rejected by CI, including:

- operator state;
- competition snapshots and epochs;
- Hermes and agent-cost ledgers;
- learning lineage state;
- paper trader state and backups;
- watchdog, health, analytics, and cache outputs.

Source-like files that happen to live under `data/`, such as `data/feed_cache.py`, require an exact validator allowlist entry rather than a broad directory exemption.

## CI architecture

### Blocking merge-critical gates

- runtime-artifact validator;
- web build contract;
- accounting-v2 tests;
- competition scoreboard and epoch tests;
- rebalancer fallback tests;
- complete Node test suite;
- focused coverage for the changed accounting and competition modules.

### Diagnostic jobs

The broad legacy Python suite and performance benchmarks remain visible but non-blocking while environment-dependent and stale-contract failures are tracked separately. Diagnostic failures must not be presented as passing, but they also must not prevent the merge-critical UI/accounting tests from running.

## Completed repairs from the original audit

- Bot realized P&L double counting repaired.
- Missing marks block ranking.
- Paid-agent operating cost included in the score.
- `/api/system-truth` restored.
- Active web build validator restored and expanded.
- Hermes leverage-squared accounting replaced by accounting v2.
- Legacy Hermes history made ranking ineligible.
- Shared competition epoch added.
- Runtime state removed from source control and guarded in CI.
- Learning lineage and challenger infrastructure added.
- Daily operator UI replaced the competition-first homepage.
- Source-labelled positions API and position view added.

## Remaining deployment work

1. Supervise scoreboard publication after relevant ledger mutations.
2. Run a deployed browser-level visual and interaction test with representative data.
3. Add durable end-to-end correlation IDs from opportunity through settlement and outcome.
4. Expand source-labelled position reconciliation across operator, bot, agent, and venue books.
5. Retire or reduce the legacy Python dashboard after endpoint and operator-workflow parity is demonstrated.
6. Resolve and reclassify the remaining broad legacy Python diagnostic failures.
