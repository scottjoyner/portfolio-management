# Prediction-Market Arbitrage / Portfolio Management System

## Production readiness status

**Not ready for production or live trading.**

This repository is currently a safety-first mock/paper scaffold plus an expanding portfolio-management planning baseline. Live trading must remain disabled until the UI, API, persistence, strategy lifecycle, backtesting, approvals, reconciliation, observability, and deployment controls are complete.

See:

- `TODO.md` for the prioritized P0/P1/P2/P3 backlog.
- `docs/PRODUCTION_READINESS_REVIEW_2026_05_29.md` for the deployment review.
- `docs/ARCHITECTURE.md` for the target operator workflow and service-boundary decisions.

## Safety warning

Paper trading is the default. Live trading is disabled by default and requires explicit configuration plus runtime confirmation. Do not connect real-money credentials or execute live orders until the release gates are implemented and certified.

## Quickstart: mock/paper scaffold

```bash
pnpm install
pnpm lint
pnpm typecheck
pnpm test
pnpm build
pnpm cli doctor --mode mock
pnpm cli discover --mode mock
pnpm cli match:propose --mode mock
pnpm cli arb:scan --mode mock
pnpm cli arb:paper --mode mock
```

## Current capability snapshot

| Capability | Current status |
|---|---|
| Mock CLI | Partial deterministic demo responses. |
| Node API | Minimal `/health`, `/metrics`, and generic responses. |
| Web UI | Not implemented. |
| Strategy lifecycle | Python scaffold only; persistence and validation incomplete. |
| Backtesting | Prototype only; not a realistic certification engine. |
| Plaid/account data | Mock/scaffold responses only. |
| Live trading | Blocked by design and not certified. |

## Live trading checklist

Live trading remains blocked until all of these are true:

1. `PAPER_TRADING=false`
2. `LIVE_TRADING=true`
3. `REQUIRE_MANUAL_APPROVAL=true`
4. Market pair or strategy status is approved.
5. Compliance gate passed.
6. Risk checks approved.
7. Runtime confirmation supplied.
8. Database-backed reconciliation is healthy.
9. Kill switch is tested from UI/API/worker paths.
10. CI and staging certification pass without waived failures.

## Known risks

- Cross-venue execution is non-atomic.
- Partial fills can create temporary unhedged exposure.
- Similar wording markets may still resolve differently.
- Strategy and backtest modules are not yet wired into a complete UI/API workflow.
- Account, Plaid, broker, exchange, and onchain integrations are not production-certified.
