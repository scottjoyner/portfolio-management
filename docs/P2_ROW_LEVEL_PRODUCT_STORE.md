# P2 Row-Level Product Store

This slice replaces the remaining P1 product-layer JSON flag persistence with row-level Postgres table operations.

## Why this matters

The P0/P1 implementation persisted these arrays through `operator_flags` JSON values:

- accounts
- instruments
- strategy templates
- paper executions

That was acceptable for a bootstrap/mock-paper product layer, but it is not a production-ready persistence model because it prevents row-level indexing, constraint validation, incremental writes, reconciliation queries, and table-specific migration checks.

## What changed

`PostgresOperatorStoreP1` now uses row tables for the product layer:

| Product entity | Table |
|---|---|
| accounts | `accounts` |
| instruments | `instruments` |
| strategy templates | `strategy_templates` |
| paper executions | `paper_executions` |

`operator_flags` remains available for operator-level flags such as the kill switch.

## Repository layer

`OperatorRowRepository` now includes typed methods for:

- `listAccounts()`
- `upsertAccount()`
- `listInstruments()`
- `upsertInstrument()`
- `listStrategyTemplates()`
- `upsertStrategyTemplate()`
- `listPaperExecutions()`
- `upsertPaperExecution()`
- `replaceProductLayer()`

`replaceProductLayer()` deletes and reloads product rows in dependency-safe order:

1. `paper_executions`
2. `accounts`
3. `instruments`
4. `strategy_templates`

Then it inserts accounts, instruments, templates, and paper executions.

## Current behavior

`PostgresOperatorStoreP1.save()` still delegates the core operator state save to `PostgresOperatorStore.save()` for:

- strategies
- backtests
- approvals
- positions
- audit events
- kill switch

Then it writes the P1 product layer to row tables.

## Status

This is a hardening step toward true row-level repositories. It removes product-layer JSON flag storage, but the core save path still performs full table replacement for non-product entities. Future P2 work should replace that broader full-state rewrite with targeted row-level mutation APIs.

## Validation

Tests updated:

- `tests/postgres-p1-store.test.mjs`
- `tests/operator-row-repository.test.mjs`

Assertions include:

- product rows are loaded from actual product tables
- accounts and paper executions persist through row tables
- old product-layer `operator_flags` lookup is not used
- product-layer deletion order is dependency-safe
- `getStatus().productLayer` reports `p1-row-tables`

## Live trading

No live trading behavior changes. Live routes remain blocked.
