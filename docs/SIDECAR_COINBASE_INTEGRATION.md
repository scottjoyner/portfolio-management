# Sidecar to Coinbase Research Integration

This integration keeps the local trader sidecar as a research/reporting layer and keeps Coinbase order routing inside the existing Coinbase stack.

## Flow

```text
local-trader-agent backtest
  -> HTML / CSV / manifest artifacts
  -> local_trader_agent.export.to_coinbase_research_payload()
  -> coinbase.src.sidecar_adapter.research_record_from_manifest()
  -> coinbase.src.import_sidecar_results.import_manifest_paths()
  -> StrategyRanking state
  -> Coinbase orchestrator can use ranking and normal risk controls
```

## Safety boundary

The sidecar does not place orders. It exports research evidence. Coinbase execution remains behind `ExecutionOrchestrator`, `TradeMode`, and existing risk checks.

## Export a sidecar manifest

```python
from local_trader_agent.export import export_manifest_file

export_manifest_file(
    "sidecars/local-trader-agent/workspace/reports/BTC-USD_rsi.manifest.json",
    product_id="BTC-USD",
    strategy_name="sidecar_rsi_cross",
)
```

## Import evidence into ranking

```python
from coinbase.src.import_sidecar_results import import_manifest_paths

import_manifest_paths([
    "sidecars/local-trader-agent/workspace/reports/BTC-USD_rsi.manifest.json",
])
```

## Coinbase-compatible strategy adapter

`coinbase/src/strategies/sidecar_rsi.py` exposes `SidecarRSICrossStrategy`, a `BaseStrategy` implementation that emits `BracketSetup` objects using the same RSI-cross semantics as the sidecar. The orchestrator still handles sizing, ranking, risk checks, and mode selection.
