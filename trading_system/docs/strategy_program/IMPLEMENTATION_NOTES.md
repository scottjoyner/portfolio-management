# IMPLEMENTATION_NOTES

- Unified the 100-strategy contract with explicit `mapped_implementation` links to real strategy classes where present.
- Kept non-destructive adapter behavior for research-only items while enforcing honest readiness metadata.
- Added capital orchestration logic with tier caps, drawdown/quality scaling, and reserve-aware deployable budgeting.
- Added richer per-strategy metadata for data quality gates, downgrade criteria, take-profit/treasury behavior, smoke scenarios, and portability/fragility/ops complexity scoring fields consumed by catalog adapters.
- Generic catalog adapters now enforce warmup/edge/latency/stale-data/risk-halt gating in signal generation to reduce unrealistic paper/live behavior drift.
