from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class VwapTwapExecutionStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VwapTwapExecutionStrategy",
                strategy_type="execution",
                live_supported=True,
                data_requirements=["product_id", "score", "arrival_price", "participation", "warmup_complete"],
                risk_mode_hint="NORMAL",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=1, warmup_period=1),
        )
