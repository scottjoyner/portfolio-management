# Changelog: Strategy Refinement Pass

## Reviewed

- All modules under `strategies/*` and `onchain/strategies/*`.
- Strategy registry and key risk/test wiring.

## Material code changes

- Introduced normalized strategy interface models (`StrategyMetadata`, `StrategyConfig`, richer `StrategySignal`).
- Added `BaseSignalStrategy` to reduce duplicated threshold logic and add cooldown/disable behavior.
- Refactored 14 core CEX strategies to use standardized metadata/config and explicit data requirements.
- Added registry duplicate ID protection and metadata index helper.
- Added unit tests covering config validation, cooldown behavior, and metadata contract.

## Docs added

- Full strategy review bundle under `docs/strategy_review/`.

## Behavioral changes

- Strategies now require declared input keys before emitting signals.
- Strategies now enforce cooldown windows and score safety caps.
- Metadata now includes mode/readiness hints consistently for refactored strategies.

