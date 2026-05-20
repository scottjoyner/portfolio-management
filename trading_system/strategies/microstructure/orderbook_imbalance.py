from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class OrderBookImbalanceStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="OrderBookImbalanceStrategy",
                strategy_type="microstructure",
                live_supported=True,
                data_requirements=["product_id", "score", "imbalance", "best_bid", "best_ask", "warmup_complete"],
                risk_mode_hint="LAB_HFT",
            ),
            config=StrategyConfig(threshold=0.35, cooldown_seconds=0.5, warmup_period=20),
        )
