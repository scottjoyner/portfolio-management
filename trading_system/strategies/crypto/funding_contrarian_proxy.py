"""
Funding-rate contrarian proxy (crypto microstructure).

The system has NO live funding-rate / open-interest feed here, so this strategy
proxies "crowded" directional positioning from price/volume microstructure:

* Extreme, sustained recent return momentum combined with elevated volume is
  treated as a crowd-direction proxy. When price has moved heavily one-sided,
  the crowd is likely leaning that way -> fade (contrarian).
* If the optional `funding_rate` field is supplied by the caller, it is used
  directly: a positive extreme (longs paying shorts) biases a fade of longs.

Pure Python, deterministic. Returns None gracefully when required proxy fields
are missing so it never crashes in the replay/backtest harness.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class FundingRateContrarianProxyStrategy(BaseSignalStrategy):
    def __init__(
        self,
        lookback: int = 20,
        return_threshold: float = 0.06,
        volume_multiplier: float = 1.5,
        funding_threshold: float = 0.0001,
    ) -> None:
        self._lookback = lookback
        self._return_threshold = return_threshold
        self._volume_multiplier = volume_multiplier
        self._funding_threshold = funding_threshold
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="FundingProxyContrarianStrategy",
                strategy_type="crypto_microstructure",
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
    def _sma(xs: list[float], n: int) -> float:
        if len(xs) < n:
            return 0.0
        return sum(xs[-n:]) / n

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
        ret = (window[-1] - window[0]) / window[0] if window[0] != 0 else 0.0
        # Baseline volume = earlier portion of the lookback window; recent = last few bars.
        baseline = volumes[: len(volumes) - self._lookback]
        baseline = volumes if not baseline else baseline
        baseline_vol = self._sma(baseline, min(self._lookback, len(baseline)))
        recent_vol = self._sma(volumes[-min(5, len(volumes)):], min(5, len(volumes)))
        vol_spike = recent_vol / baseline_vol if baseline_vol > 0 else 0.0

        # Optional direct funding-rate input: positive extreme -> fade long (SELL).
        funding = market_state.get("funding_rate")
        funding_signal = 0.0
        if funding is not None:
            if funding >= self._funding_threshold:
                funding_signal = -1.0
            elif funding <= -self._funding_threshold:
                funding_signal = 1.0

        # Proxy crowd direction from sustained one-sided move + volume confirmation.
        crowded_long = ret >= self._return_threshold and vol_spike >= self._volume_multiplier
        crowded_short = ret <= -self._return_threshold and vol_spike >= self._volume_multiplier

        raw = 0.0
        if crowded_long:
            raw = -1.0
        elif crowded_short:
            raw = 1.0
        raw = raw if funding_signal == 0.0 else funding_signal

        if raw == 0.0:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        score = raw * min(1.0, abs(ret) / (self._return_threshold * 2.0))
        reason = (
            f"crowded {'long' if raw < 0 else 'short'} fade: ret={ret:.4f} "
            f"vol_spike={vol_spike:.2f} funding={funding}"
        )

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=reason,
            confidence=min(1.0, max(0.0, abs(score))),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "return": ret,
                "volume_spike": vol_spike,
                "funding_rate": funding if funding is not None else 0.0,
            },
        )
