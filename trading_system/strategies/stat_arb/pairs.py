from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class PairsTradingStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="PairsTradingStrategy",
                strategy_type="stat_arb",
                live_supported=False,
                data_requirements=["product_id", "score", "spread", "hedge_ratio", "warmup_complete"],
                risk_mode_hint="AGGRESSIVE",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=10, warmup_period=100),
        )
