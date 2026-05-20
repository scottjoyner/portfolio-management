# Backtesting Realism and Portability-to-Live (Third Pass)

This pass targets optimistic simulation bias and portability scoring.

## Where simulation is usually optimistic

- under-modeled feed/order round-trip latency
- overestimated queue fill probability
- stale quote assumptions that are too favorable
- cancel/replace churn not penalized for message pressure
- rejection/outage rates treated as near-zero

## Realism penalty model

Implemented in `analytics/metrics/live_transfer.py`:

- `latency_penalty`
- `fill_optimism_penalty`
- `stale_quote_penalty`
- `turnover_penalty`
- `rejection_penalty`
- `outage_penalty`

The total penalty contributes to:

- strategy `fragility_score`
- `live_transfer_confidence`
- realism-adjusted `expected_live_return`

## Validation report

`apps/backtester/runner.py` now writes:

- summary report (`--output`)
- validation ranking report (`--validation-report`)

The validation report ranks all registered strategies by expected live-transfer confidence and includes total realism penalties.
