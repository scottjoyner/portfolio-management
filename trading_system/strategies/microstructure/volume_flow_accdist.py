from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


class VolumeFlowAccDistStrategy(BaseSignalStrategy):
    """Volume-flow accumulation/distribution proxy.

    Accumulation when price rises on rising volume; distribution when price
    falls on rising volume. Signals on sustained accumulation (positive)
    or distribution (negative) using recent closes + volumes bars.
    """

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolumeFlowAccDistStrategy",
                strategy_type="microstructure",
                live_supported=False,
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=1.0, warmup_period=10),
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

        lookback = min(self.config.warmup_period or 6, len(closes))
        recent_c = closes[-lookback:]
        recent_v = volumes[-lookback:]

        baseline_v = recent_v[:-1]
        avg_vol = sum(baseline_v) / len(baseline_v)
        if avg_vol <= 0:
            return None

        acc = 0.0
        for i in range(1, len(recent_c)):
            ret = recent_c[i] - recent_c[i - 1]
            vol_ratio = recent_v[i] / avg_vol if avg_vol > 0 else 1.0
            acc += (1.0 if ret >= 0 else -1.0) * (vol_ratio - 1.0)

        norm = acc / len(recent_c)
        if abs(norm) <= self.config.threshold:
            return None

        self._last_emit_ts = self._now()
        score = max(-1.0, min(1.0, norm))
        direction = "BUY" if score > 0 else "SELL"
        return self._build_signal(market_state, score, norm, acc)

    def _now(self) -> float:
        from time import monotonic

        return monotonic()

    def _build_signal(self, market_state, score, norm, acc):
        from trading_system.strategies.base.interfaces import StrategySignal

        direction = "BUY" if score > 0 else "SELL"
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"vol-flow acc/dist={norm:.3f} ({direction} pressure)",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"acc_dist": acc, "normalized": norm},
        )
