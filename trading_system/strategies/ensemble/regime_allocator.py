from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class RegimeSwitchingEnsembleAllocator(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RegimeSwitchingEnsembleAllocator",
                strategy_type="ensemble",
                live_supported=False,
                data_requirements=["product_id", "score", "regime", "strategy_quality", "warmup_complete"],
                risk_mode_hint="NORMAL",
            ),
            config=StrategyConfig(threshold=0.3, cooldown_seconds=30, warmup_period=200),
        )
