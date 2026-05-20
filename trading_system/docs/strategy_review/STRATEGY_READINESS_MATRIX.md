# Strategy Readiness Matrix

| Strategy | Unit-tested | Backtest-ready | Replay-ready | Paper-ready | Live-safe | Major blockers | Realism caveats |
|---|---:|---:|---:|---:|---:|---|---|
| Core CEX scaffold strategies | yes | partial | partial | yes | limited | no native order intent/sizing in strategy classes | score-driven simplification |
| GenericSpecStrategy catalog rows | yes (registry) | no | no | metadata-only | no | implementation absent | conceptual only |
| Maker engine + MM strategies | yes | yes | yes | yes | guarded | calibration and exchange microstructure fit | queue/fill model approximation |
| Onchain LP overlays | yes | partial | yes | yes | guarded | full route/gas uncertainty in production | heuristic parameterization |
| Hybrid hedge + treasury sweep | yes | partial | yes | yes | guarded | operational sequencing dependencies | confidence/slippage models are simplified |

