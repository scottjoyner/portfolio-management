# Strategy Interface Standard

This pass standardizes around `strategies/base/interfaces.py` + `strategies/base/simple.py`.

## Required contract

- `metadata() -> dict`
- `generate_signal(market_state) -> StrategySignal | None`
- `explain_trade(signal) -> str`

## Standardized fields

- `StrategySignal`: `strategy_id`, `product_id`, `score`, `reason`, `confidence`, `warmup_passed`, `tags`, `features`.
- `StrategyMetadata`: type/status/products/data requirements/mode support/risk mode hint/capital bucket.
- `StrategyConfig`: threshold, cooldown, warmup period, max score safety cap, enabled flag.

## Operational hooks now explicit

- `required_inputs()`
- `supports_mode(mode)`
- `in_cooldown()`
- `is_disabled(market_state)`
- `approvals_required()`

## Backwards compatibility

- Existing strategy classes still satisfy legacy method names.
- Registry API (`load_strategies`) preserved.
- Advanced spec catalog left intact for metadata coverage.

