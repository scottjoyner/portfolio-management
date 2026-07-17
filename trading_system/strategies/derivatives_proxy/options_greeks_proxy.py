"""
Options-greeks proxy strategy (derivatives proxy).

The system has NO live options chain, so we proxy option-greek intuition from
realized price dynamics:

* Realized vol = rolling std of log-returns, used as an "implied-ish" vol proxy.
* Pseudo-delta: position of price within a Bollinger band (scaled 0..1).
  Deep-in-the-band (extreme pseudo-delta near 0 or 1) implies an overstretched
  move -> fade (sell when overextended long, buy when overextended short).
  This is a curvature/gamma-style fade of extremes rather than trend-following.
* Vol-risk-premium overlay: when realized vol is far below a proxy "implied"
  (a smoothed, lagged-high vol estimate), the market is "calm vs expectation"
  and we lean mean-reversion. The optional `funding_rate` may refine the
  directional bias (positive funding biases a fade of longs).

Pure Python, deterministic. Returns None gracefully when required fields are
absent so it never crashes in the replay/backtest harness.
"""
from __future__ import annotations

from math import log, sqrt
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class OptionsGreeksProxyStrategy(BaseSignalStrategy):
    def __init__(
        self,
        lookback: int = 30,
        band_mult: float = 2.0,
        vol_lag: int = 30,
        vrp_threshold: float = 0.35,
        funding_threshold: float = 0.0001,
    ) -> None:
        self._lookback = lookback
        self._band_mult = band_mult
        self._vol_lag = vol_lag
        self._vrp_threshold = vrp_threshold
        self._funding_threshold = funding_threshold
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="OptionsGreeksProxyStrategy",
                strategy_type="derivatives_proxy",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "warmup_complete"],
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

    def _realized_vol(self, closes: list[float]) -> float:
        if len(closes) < 3:
            return 0.0
        log_rets = [log(closes[i] / closes[i - 1]) for i in range(1, len(closes))]
        return self._stdev(log_rets, self._mean(log_rets))

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes")
        if not closes or len(closes) < self._lookback + 1:
            return None

        window = closes[-(self._lookback + 1):]
        rv = self._realized_vol(window)
        if rv <= 0:
            return None

        # Bollinger band over closes; pseudo-delta = normalized position in band.
        series = window[1:]
        mid = self._mean(series)
        sd = self._stdev(series, mid)
        band = sd * self._band_mult
        if band <= 0:
            return None
        pseudo_delta = (series[-1] - (mid - band)) / (2.0 * band)  # 0..1
        pseudo_delta = max(0.0, min(1.0, pseudo_delta))

        # Curvature/gamma fade of extremes: extreme pseudo-delta -> reverse.
        curvature = (0.5 - pseudo_delta) * 2.0  # +1 deep short, -1 deep long

        # Vol-risk-premium overlay: realized far below a lagged "implied" proxy.
        vrp = 0.0
        if len(closes) >= self._lookback + self._vol_lag + 1:
            lagged = self._realized_vol(closes[: -(self._vol_lag + 1)][-(self._lookback + 1):])
            if lagged > 0:
                vrp = (lagged - rv) / lagged
        vrp_bias = 0.0
        if vrp >= self._vrp_threshold:
            vrp_bias = curvature  # calm market vs expectation -> fade extremes

        raw = curvature
        if vrp_bias != 0.0:
            raw = raw + 0.5 * vrp_bias

        # Optional direct funding-rate input refines directional fade.
        funding = market_state.get("funding_rate")
        if funding is not None:
            if funding >= self._funding_threshold:
                raw = raw - 0.3  # crowded long -> lean short
            elif funding <= -self._funding_threshold:
                raw = raw + 0.3  # crowded short -> lean long

        if abs(raw) < 0.1:
            return None

        score = max(-1.0, min(1.0, raw * max(0.0, min(1.0, abs(curvature)))))
        if abs(score) < self.config.threshold:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=(
                f"pseudo_delta={pseudo_delta:.3f} curvature={curvature:.3f} "
                f"vrp={vrp:.3f} funding={funding}"
            ),
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "realized_vol": rv,
                "pseudo_delta": pseudo_delta,
                "vol_risk_premium": vrp,
            },
        )
