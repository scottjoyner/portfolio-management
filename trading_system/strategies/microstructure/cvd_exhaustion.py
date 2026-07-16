from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class CvdExhaustionStrategy(BaseSignalStrategy):
    """Cumulative volume delta (CVD) exhaustion reversal.

    Tracks the running cumulative delta of buy vs sell volume. When the
    normalized CVD reaches an extreme (magnitude above threshold) it signals
    exhaustion and a likely mean-reverting snapback in the opposite direction.
    """

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="CvdExhaustionStrategy",
                strategy_type="microstructure",
                live_supported=True,
                data_requirements=[
                    "product_id",
                    "cumulative_delta",
                    "delta_scale",
                    "warmup_complete",
                ],
                risk_mode_hint="LAB_HFT",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.6, cooldown_seconds=2.0, warmup_period=30),
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

        cumulative_delta = float(market_state.get("cumulative_delta", 0.0))
        delta_scale = float(market_state.get("delta_scale", 0.0))
        if delta_scale <= 0.0:
            return None

        normalized = cumulative_delta / delta_scale
        if abs(normalized) <= self.config.threshold:
            return None

        # Exhaustion: extreme CVD reverses, so signal opposite to the delta.
        score = -normalized

        self._last_emit_ts = self._now()
        return self._build_signal(market_state, score, normalized)

    def _now(self) -> float:
        from time import monotonic

        return monotonic()

    def _build_signal(self, market_state, score, normalized):
        from trading_system.strategies.base.interfaces import StrategySignal

        direction = "BUY" if score > 0 else "SELL"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"CVD exhaustion normalized={normalized:.3f} -> fade ({direction})",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "normalized_cvd": normalized,
            },
        )
