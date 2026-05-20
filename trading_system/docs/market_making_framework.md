# Market Making Stack (Second Pass)

This pass hardens maker execution around queue realism, toxic-flow detection, quote fade policy, and cancel/replace pressure control.

## Components

- `market_data/microstructure/features.py`
  - top-of-book microprice and imbalance features
  - VPIN-like toxic flow estimator
- `execution/queue_model/models.py`
  - queue-time and fill-probability estimate
  - expected fill size
  - stale quote decay
  - adverse selection estimate (bps)
- `execution/maker_engine/engine.py`
  - volatility + toxicity dynamic spread
  - inventory skew in bps
  - ladder generation with size penalty by adverse-selection
  - fade criteria by age, toxicity, and microprice drift
  - cancel/replace pressure tracking
  - inventory drift notional diagnostics
- `apps/replay_engine/runner.py`
  - fixture-driven replay path focused on maker diagnostics

## Operational diagnostics

Replay output includes:

- toxic hit count
- fade event count
- average cancel/replace pressure
- average inventory drift notional

## Benchmarks

Use `python benchmarks/maker_path_benchmark.py` to profile quote-ladder throughput.
