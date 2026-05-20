from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class LongHorizonDcaStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="LongHorizonDcaStrategy",
                strategy_type="accumulation",
                live_supported=True,
                data_requirements=["product_id", "score", "drawdown", "warmup_complete"],
                risk_mode_hint="ULTRA_CONSERVATIVE",
                capital_bucket="ACCUMULATION",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=60, warmup_period=1_000),
        )
