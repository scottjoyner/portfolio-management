from __future__ import annotations

import math

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


class ReturnDispersionFearGreedStrategy(BaseSignalStrategy):
    """Return-dispersion fear/greed index proxied from realized volatility.

    Rolling std of recent returns is the dispersion. Calm (low dispersion) =
    greed (bullish bias); volatility spikes = fear (bearish bias). Emits a
    contrarian-leaning score on regime flips between calm and turbulent states.
    """

    def __init__(self, window: int = 30, calm_z: float = 0.7, fear_z: float = 2.0) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ReturnDispersionFearGreed",
                strategy_type="sentiment",
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.15, cooldown_seconds=300, warmup_period=window),
        )
        self.window = max(5, window)
        self.calm_z = calm_z
        self.fear_z = fear_z
        self._baseline_vol: float | None = None
        self._regime: str = "unknown"

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self.window + 1:
            return None

        window_closes = closes[-(self.window + 1):]
        rets = [window_closes[i] / window_closes[i - 1] - 1.0 for i in range(1, len(window_closes))]
        disp = math.sqrt(sum(r * r for r in rets) / len(rets)) + 1e-12

        if self._baseline_vol is None:
            self._baseline_vol = disp
            return None
        self._baseline_vol = 0.95 * self._baseline_vol + 0.05 * disp

        ratio = disp / (self._baseline_vol + 1e-12)
        new_regime = "calm" if ratio < self.calm_z else ("fear" if ratio > self.fear_z else "normal")

        score = 0.0
        reason = ""
        if new_regime != self._regime and new_regime != "unknown":
            if new_regime == "calm":
                score = 0.6
                reason = f"vol dispersion ratio={ratio:.2f} < {self.calm_z}: calm/greed bullish"
            elif new_regime == "fear":
                score = -0.6
                reason = f"vol dispersion ratio={ratio:.2f} > {self.fear_z}: fear bearish"
            else:
                score = 0.2 if self._regime == "fear" else -0.2
                reason = f"dispersion normalizing from {self._regime} (ratio={ratio:.2f})"
            self._regime = new_regime
        else:
            self._regime = new_regime
            return None

        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"dispersion_ratio": round(ratio, 3), "regime": new_regime},
        )
