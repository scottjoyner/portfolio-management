from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class MeanReversionZScoreStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="MeanReversionZScoreStrategy",
                strategy_type="mean_reversion",
                live_supported=True,
                data_requirements=["product_id", "score", "zscore", "warmup_complete"],
                risk_mode_hint="NORMAL",
            ),
            config=StrategyConfig(threshold=0.15, cooldown_seconds=2, warmup_period=50),
        )
