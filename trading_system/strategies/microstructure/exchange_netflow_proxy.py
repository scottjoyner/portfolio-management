from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class ExchangeNetflowProxyStrategy(BaseSignalStrategy):
    """Exchange-netflow proxy via return magnitude vs volume spike.

    Large down-move on huge volume => outflow / distribution (SELL).
    Large up-move on huge volume => inflow / accumulation (BUY).
    """

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ExchangeNetflowProxyStrategy",
                strategy_type="microstructure",
                live_supported=False,
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.25, cooldown_seconds=1.0, warmup_period=10),
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
        if len(closes) < 3 or len(volumes) < 3 or len(closes) != len(volumes):
            return None

        lookback = min(self.config.warmup_period or 10, len(closes))
        avg_vol = sum(volumes[:-1]) / max(1, len(volumes) - 1)
        last_vol = volumes[-1]
        if avg_vol <= 0:
            return None

        base_close = closes[-lookback] if lookback > 0 else closes[0]
        ret = (closes[-1] - base_close) / base_close if base_close != 0 else 0.0
        vol_spike = last_vol / avg_vol

        pressure = ret * vol_spike

        if abs(pressure) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        score = max(-1.0, min(1.0, pressure))
        direction = "BUY" if score > 0 else "SELL"
        return self._build_signal(market_state, score, pressure, ret, vol_spike)

    def _now(self) -> float:
        from time import monotonic

        return monotonic()

    def _build_signal(self, market_state, score, pressure, ret, vol_spike):
        from trading_system.strategies.base.interfaces import StrategySignal

        direction = "BUY" if score > 0 else "SELL"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"netflow pressure={pressure:.3f} ({direction})",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"return": ret, "vol_spike": vol_spike},
        )
