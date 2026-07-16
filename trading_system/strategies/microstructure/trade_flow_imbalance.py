from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class TradeFlowImbalanceStrategy(BaseSignalStrategy):
    """Trade-flow imbalance: net buy-volume minus sell-volume, normalized.

    Reads per-tick aggressive buy/sell volumes and emits a signal when the
    normalized flow imbalance crosses the configured threshold.
    """

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="TradeFlowImbalanceStrategy",
                strategy_type="microstructure",
                live_supported=True,
                data_requirements=[
                    "product_id",
                    "buy_volume",
                    "sell_volume",
                    "warmup_complete",
                ],
                risk_mode_hint="LAB_HFT",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.3, cooldown_seconds=0.5, warmup_period=20),
        )

    def generate_signal(self, market_state: dict):
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        missing = self.required_inputs() - set(market_state)
        if missing:
            return None

        buy_volume = float(market_state.get("buy_volume", 0.0))
        sell_volume = float(market_state.get("sell_volume", 0.0))
        denom = buy_volume + sell_volume
        if denom <= 0.0:
            return None

        imbalance = (buy_volume - sell_volume) / denom

        if abs(imbalance) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        score = imbalance
        return self._build_signal(market_state, score, imbalance, buy_volume, sell_volume)

    def _now(self) -> float:
        from time import monotonic

        return monotonic()

    def _build_signal(self, market_state, score, imbalance, buy_volume, sell_volume):
        from trading_system.strategies.base.interfaces import StrategySignal

        direction = "BUY" if score > 0 else "SELL"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"trade flow imbalance={imbalance:.3f} ({direction} pressure)",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "imbalance": imbalance,
                "buy_volume": buy_volume,
                "sell_volume": sell_volume,
            },
        )
