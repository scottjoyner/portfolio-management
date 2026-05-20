from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class CrossSectionalRelativeStrengthStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="CrossSectionalRelativeStrengthStrategy",
                strategy_type="relative_strength",
                live_supported=True,
                data_requirements=["product_id", "score", "universe_rank", "warmup_complete"],
                risk_mode_hint="NORMAL",
            ),
            config=StrategyConfig(threshold=0.25, cooldown_seconds=15, warmup_period=100),
        )
