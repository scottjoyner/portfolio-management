from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class GridRebalanceCaptureStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="GridRebalanceCaptureStrategy",
                strategy_type="mean_reversion",
                live_supported=False,
                data_requirements=["product_id", "score", "mid", "inventory", "warmup_complete"],
                risk_mode_hint="ULTRA_CONSERVATIVE",
                capital_bucket="MARKET_MAKING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=1, warmup_period=10),
        )
