from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class AdaptiveSpreadMMStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="AdaptiveSpreadMMStrategy",
                strategy_type="market_making",
                live_supported=True,
                data_requirements=["product_id", "score", "spread_bps", "volatility", "warmup_complete"],
                risk_mode_hint="MARKET_MAKING_PRO",
                capital_bucket="MARKET_MAKING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=0.2, warmup_period=5),
        )
