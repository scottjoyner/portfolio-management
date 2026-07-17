"""
On-chain GPU/flow proxy strategy (derivatives proxy).

No on-chain feed exists here, so we infer net flow pressure from traded volume
and realized price impact:

* Volume z-score: standardized recent volume vs its trailing mean/std. Large
  positive z -> abnormal participation (capitulation or blow-off).
* Price-impact proxy: |return| / volume (normalized). High impact on low volume
  signals thin, exhausted move; high impact on high volume signals absorption.
* Net flow pressure = sign(return) * volume_z. Strong one-sided flow with high
  volume = trend conviction; extreme volume z combined with low price-impact and
  a stretched move = exhaustion -> fade the direction.

Extreme exhaustion (very high volume z + very low impact + stretched return)
biases a contrarian fade. Pure Python, deterministic. Returns None gracefully
when required fields are missing so it never crashes in the replay/backtest
harness.
"""
from __future__ import annotations

from math import sqrt
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class OnchainFlowProxyStrategy(BaseSignalStrategy):
    def __init__(
        self,
        lookback: int = 20,
        volume_z_threshold: float = 2.0,
        impact_floor: float = 1e-6,
    ) -> None:
        self._lookback = lookback
        self._volume_z_threshold = volume_z_threshold
        self._impact_floor = impact_floor
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="OnchainFlowProxyStrategy",
                strategy_type="derivatives_proxy",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "volumes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=60, warmup_period=lookback),
        )

    @staticmethod
    def _mean(xs: list[float]) -> float:
        return sum(xs) / len(xs) if xs else 0.0

    @staticmethod
    def _stdev(xs: list[float], mean: float) -> float:
        if len(xs) < 2:
            return 0.0
        var = sum((x - mean) ** 2 for x in xs) / (len(xs) - 1)
        return sqrt(var)

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes")
        volumes = market_state.get("volumes")
        if not closes or not volumes or len(closes) < self._lookback + 1:
            return None

        window = closes[-(self._lookback + 1):]
        v_window = volumes[-self._lookback:]
        last_close = window[-1]
        if last_close <= 0:
            return None

        ret = (window[-1] - window[-2]) / window[-2] if window[-2] != 0 else 0.0
        recent_vol = v_window[-1] if v_window else 0.0
        vol_mean = self._mean(v_window)
        vol_sd = self._stdev(v_window, vol_mean)
        vol_z = (recent_vol - vol_mean) / vol_sd if vol_sd > 0 else 0.0

        # Price-impact proxy = |return| / volume (higher = thinner, exhausted).
        impact = abs(ret) / recent_vol if recent_vol > self._impact_floor else 0.0

        # Net flow pressure: direction * standardized volume.
        pressure = (1.0 if ret > 0 else -1.0) * vol_z

        # Exhaustion fade: extreme volume + very low impact + stretched move.
        raw = 0.0
        if vol_z >= self._volume_z_threshold:
            if impact < 0.5 * self._impact_floor + 1e-4 and abs(ret) > 0.0:
                raw = -pressure  # exhausted one-sided blow-off -> fade
            else:
                raw = pressure  # sustained conviction flow -> ride

        if raw == 0.0:
            return None

        score = max(-1.0, min(1.0, raw * min(1.0, abs(vol_z) / (self._volume_z_threshold * 2.0))))
        if abs(score) < self.config.threshold:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=f"vol_z={vol_z:.2f} impact={impact:.6f} ret={ret:.4f}",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "volume_z": vol_z,
                "price_impact": impact,
                "flow_pressure": pressure,
            },
        )
