from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class BasisCarryDerivativesStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="BasisCarryDerivativesStrategy",
                strategy_type="carry",
                live_supported=False,
                data_requirements=["product_id", "score", "basis", "funding_rate", "warmup_complete"],
                risk_mode_hint="DERIVATIVES_EXPERT",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=30, warmup_period=100),
        )
