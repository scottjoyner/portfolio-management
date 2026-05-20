# Strategy Dependency Map

## Core strategy stack dependencies

- **Data feeds**: `market_data/*` normalized into per-strategy `market_state` dictionaries.
- **Risk hooks**: `risk/engine.py` evaluates downstream `OrderIntent` constraints.
- **Execution hooks**: `execution/*` modules provide maker engine, hybrid hedging, and routing primitives.
- **Registry hooks**: `strategies/registry/registry.py` is canonical strategy loader.
- **Tests**: `tests/unit/test_strategy_registry.py`, `tests/unit/test_strategy_contract.py` plus family-specific tests.

## Per-family mapping (condensed)

| Family | Required features | Risk/sizing hook | Execution hook | Analytics/reporting | Config/tests/docs |
|---|---|---|---|---|---|
| Trend/MeanRev/Vol | score + family-specific indicators + warmup flag | `RiskEngine.evaluate` with mode hints from metadata | currently indirect; order intent creation external | strategy signal fields (`tags`, `features`) | `strategies/*/*.py`, unit contract tests, `docs/strategies/*.md` |
| Market Making | spread/book/inventory | `MARKET_MAKING_PRO` mode hints | maker engine + queue model | maker benchmark + replay fixtures | `tests/unit/test_maker_engine.py`, `tests/sim/test_replay_maker_fixture.py` |
| Execution algos | participation, arrival price, score | normal risk mode limits | execution router and smart execution packages | performance metrics in analytics package | `docs/strategies/vwap_twap.md` |
| StatArb/BasisCarry | spread, hedge ratio, funding/basis | aggressive/derivatives mode hints | sequencing and hedge planner | attribution available via analytics package; sparse direct wiring | `docs/strategies/pairs.md`, `docs/strategies/basis_carry.md` |
| Onchain LP overlays | vol regime, pool state, route costs, token safety | policy engine + approval gates + contract/token guard | onchain swap execution and bridge settlement | IL/Fee decomposition docs and tests | `tests/lp/*`, `tests/route_sim/*`, `docs/onchain_mm/*` |
| Treasury/Hybrid Hedge | realized pnl, delta, orderbook depth, route edge | capital buckets + explicit approval packet flow | hybrid sequencing + hedge planner | live transfer + attribution metrics | `tests/unit/test_hybrid_hedge_and_profit.py` |
