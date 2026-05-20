from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class VolatilityBreakoutStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolatilityBreakoutStrategy",
                strategy_type="volatility",
                live_supported=True,
                data_requirements=["product_id", "score", "atr", "realized_vol", "warmup_complete"],
                risk_mode_hint="AGGRESSIVE",
            ),
            config=StrategyConfig(threshold=0.25, cooldown_seconds=4, warmup_period=40),
        )
