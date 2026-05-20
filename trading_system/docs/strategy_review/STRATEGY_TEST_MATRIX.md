# Strategy Test Matrix

| Strategy area | Unit | Integration | Replay | Backtest smoke | Config validation | Explainability | Risk gating | Gaps |
|---|---|---|---|---|---|---|---|---|
| Registry + metadata | yes (`test_strategy_registry.py`) | limited | n/a | n/a | partial | partial | n/a | no full API/UI exposure tests |
| New interface contract | yes (`test_strategy_contract.py`) | n/a | n/a | n/a | yes | partial | n/a | no per-family deep scenario tests yet |
| Risk engine | yes (`test_risk_engine.py`) | indirect | n/a | n/a | n/a | n/a | yes | limited stress-path coverage |
| Market making engine | yes | yes | yes | yes | n/a | limited | partial | fill realism remains simplified |
| Onchain LP/Hedge | yes | partial | yes | yes | partial | partial | yes | live gas/MEV data realism |

