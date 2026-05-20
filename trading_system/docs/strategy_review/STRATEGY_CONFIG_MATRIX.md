# Strategy Config Matrix

## Shared typed config

All scaffolded CEX strategies now use `StrategyConfig` with validation:

- Required: `threshold`, `max_abs_score`, `enabled`
- Optional: `cooldown_seconds`, `warmup_period`
- Validation: rejects unsafe `max_abs_score < 0.5`, negative cooldown, out-of-range thresholds.

## Dangerous defaults reviewed

- Prior implicit defaults accepted arbitrary score magnitudes and had no cooldown.
- New default `max_abs_score=10` and per-strategy cooldowns reduce runaway churn.

## Mode support summary

| Strategy family | Replay | Backtest | Paper | Live approval | Live auto |
|---|---|---|---|---|---|
| Trend/Vol/MeanRev | yes | yes | yes | possible | limited |
| Market making | yes | yes | yes | required | limited |
| Ensemble/Rotation | yes | yes | yes | not default | no |
| StatArb/BasisCarry | yes | yes | yes | required | no |
| Onchain LP/Hedge/Treasury | yes | yes | yes | yes | guarded |

