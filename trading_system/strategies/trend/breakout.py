from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class TrendFollowingBreakoutStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="TrendFollowingBreakoutStrategy",
                strategy_type="trend",
                live_supported=True,
                data_requirements=["product_id", "score", "close", "high", "warmup_complete"],
                risk_mode_hint="NORMAL",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=5, warmup_period=20),
        )
