from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class StablecoinFlowProxyStrategy(BaseSignalStrategy):
    """Stablecoin-flow / sentiment proxy via volume z-score anomaly.

    Detects anomalous volume (z-score) combined with price direction.
    High positive volume z-score with up-move => inflow sentiment (BUY);
    high positive volume z-score with down-move => outflow sentiment (SELL).
    """

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="StablecoinFlowProxyStrategy",
                strategy_type="microstructure",
                live_supported=False,
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=1.0, cooldown_seconds=1.0, warmup_period=12),
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

        closes = list(market_state.get("closes", []))
        volumes = list(market_state.get("volumes", []))
        if len(volumes) < 4 or len(closes) != len(volumes):
            return None

        window = volumes[:-1]
        mean = sum(window) / len(window)
        var = sum((v - mean) ** 2 for v in window) / len(window)
        std = var ** 0.5
        if std <= 0:
            return None

        z = (volumes[-1] - mean) / std
        ret = (closes[-1] - closes[-2]) / closes[-2] if len(closes) >= 2 and closes[-2] != 0 else 0.0

        anomaly = z * (1.0 if ret >= 0 else -1.0)

        if abs(anomaly) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        score = max(-1.0, min(1.0, anomaly / 3.0))
        direction = "BUY" if score > 0 else "SELL"
        return self._build_signal(market_state, score, anomaly, z)

    def _now(self) -> float:
        from time import monotonic

        return monotonic()

    def _build_signal(self, market_state, score, anomaly, z):
        from trading_system.strategies.base.interfaces import StrategySignal

        direction = "BUY" if score > 0 else "SELL"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"stablecoin-flow anomaly={anomaly:.3f} ({direction})",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"volume_z": z, "anomaly": anomaly},
        )
