# Expert Stack Evolution (Iteration)

This iteration expands the platform toward a research + production stack with:

- risk modes and explicit high-risk gating
- trust-score-driven exchange state reconciliation
- queue-position-aware replay/backtest modeling
- compute backend router (NumPy/CuPy/PyTorch fallback)
- expanded strategy catalog (50+)
- experiment tracking manifests
- control-plane endpoints for strategy catalog and reconciliation diagnostics

## Layered Architecture

- `apps/`: api, worker, backtester, replay_engine
- `core/`: config, models, timing/latency trace
- `exchange/coinbase/reconciliation`: state hardening and forensics export
- `execution/queue_model`: queue/fill probability estimation
- `compute/feature_pipelines`: backend abstraction with CPU fallback
- `strategies/`: legacy + advanced catalog
- `research/experiment_tracking`: deterministic run manifesting

## Latency Attribution

`LatencyTrace` captures monotonic nanosecond timestamps and exports microsecond stage deltas:

1. feed receive
2. normalization
3. feature compute
4. strategy decision
5. risk approval
6. submit
7. ack/fill

## Safety and Governance

- `RESEARCH_ONLY` mode cannot place live risk.
- `UNTRUSTED` exchange trust score blocks risk-increasing orders while allowing reduce-only.
- High-risk modes require explicit runtime enablement.
- Per-mode max order notional/open-order/capital ceilings are enforced.

## Backtesting/Replay Enhancements

Backtester now emits:

- basic performance metrics
- queue-model outputs
- maker/taker participation estimate
- fragility and portability scores
- reproducible JSON output artifact
- experiment manifest entry (`artifacts/experiments.jsonl`)

## Known limitations

- Queue model is intentionally simple and should be calibrated with venue-specific data.
- GPU backends are optional; current code performs runtime fallback rather than static capability probing.
- Some advanced strategies are metadata-complete spec strategies pending product-specific alpha kernels.
