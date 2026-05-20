# STRATEGY_CONFIG_MATRIX

All strategies share a typed schema with these controls:

- enable/disable
- supported_products
- risk_tier
- max_capital_fraction
- sizing_model
- entry_threshold
- exit_threshold
- stop/take-profit fields
- cooldown and approval thresholds
- backtest/paper/live flags
- telemetry flags

Validation rejects negative sizes, invalid thresholds, unsupported products, and live enablement without required safety gates.
