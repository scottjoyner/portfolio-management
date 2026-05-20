from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class LiquidityVacuumSnapbackStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="LiquidityVacuumSnapbackStrategy",
                strategy_type="special",
                live_supported=False,
                data_requirements=["product_id", "score", "liquidity_gap", "warmup_complete"],
                risk_mode_hint="AGGRESSIVE",
            ),
            config=StrategyConfig(threshold=0.35, cooldown_seconds=2, warmup_period=10),
        )
