from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class SpreadCompressionStrategy(BaseSignalStrategy):
    """Spread compression: abnormally tight spread vs recent baseline.

    A collapsing bid/ask spread often precedes a liquidity-driven move.
    Emits a directional signal using book pressure when spread_bps falls
    below the compression threshold relative to a provided baseline.
    """

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="SpreadCompressionStrategy",
                strategy_type="microstructure",
                live_supported=True,
                data_requirements=[
                    "product_id",
                    "spread_bps",
                    "baseline_spread_bps",
                    "book_pressure",
                    "warmup_complete",
                ],
                risk_mode_hint="LAB_HFT",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.3, cooldown_seconds=1.0, warmup_period=20),
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

        spread_bps = float(market_state.get("spread_bps", 0.0))
        baseline = float(market_state.get("baseline_spread_bps", 0.0))
        if baseline <= 0.0:
            return None

        compression = (baseline - spread_bps) / baseline
        book_pressure = float(market_state.get("book_pressure", 0.0))
        if book_pressure == 0.0 and spread_bps >= baseline:
            return None

        score = compression * book_pressure

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        return self._build_signal(market_state, score, compression, book_pressure)

    def _now(self) -> float:
        from time import monotonic

        return monotonic()

    def _build_signal(self, market_state, score, compression, book_pressure):
        from trading_system.strategies.base.interfaces import StrategySignal

        direction = "BUY" if score > 0 else "SELL"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"spread compression={compression:.3f} book_pressure={book_pressure:.3f} ({direction})",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "compression": compression,
                "book_pressure": book_pressure,
            },
        )
