from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class VpinProxyStrategy(BaseSignalStrategy):
    """VPIN (volume-synchronized probability of informed trading) proxy.

    Volume is bucketed into up-tick vs down-tick buckets; the volume
    imbalance ratio over a trailing window estimates order-flow toxicity.
    Extreme toxicity (very high VPIN) precedes imminent directional moves,
    so we emit a signal in the direction of the dominant volume pressure.
    """

    def __init__(self, window: int = 50, toxicity_threshold: float = 0.6) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VpinProxyStrategy",
                strategy_type="microstructure",
                data_requirements=[
                    "product_id",
                    "closes",
                    "volumes",
                    "warmup_complete",
                ],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=toxicity_threshold, cooldown_seconds=20.0, warmup_period=window),
        )
        self.window = window
        self.toxicity_threshold = toxicity_threshold

    def generate_signal(self, market_state: dict):
        from trading_system.strategies.base.interfaces import StrategySignal

        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        missing = self.required_inputs() - set(market_state)
        if missing:
            return None

        closes = list(market_state.get("closes", []))
        volumes = list(market_state.get("volumes", []))
        if len(closes) < self.window + 1 or len(volumes) < self.window + 1:
            return None

        buy_vol = 0.0
        sell_vol = 0.0
        for i in range(1, len(closes)):
            v = float(volumes[i])
            if closes[i] > closes[i - 1]:
                buy_vol += v
            elif closes[i] < closes[i - 1]:
                sell_vol += v

        total = buy_vol + sell_vol
        if total <= 0.0:
            return None

        vpin = abs(buy_vol - sell_vol) / total
        if vpin <= self.toxicity_threshold:
            return None

        direction = 1.0 if buy_vol >= sell_vol else -1.0
        score = direction * min(1.0, vpin)
        confidence = min(1.0, vpin)
        self._last_emit_ts = self._now()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"vpin={vpin:.3f} toxicity extreme, informed pressure {direction:+.0f}",
            confidence=confidence,
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "vpin": vpin,
                "buy_volume": buy_vol,
                "sell_volume": sell_vol,
            },
        )

    def _now(self) -> float:
        from time import monotonic

        return monotonic()
