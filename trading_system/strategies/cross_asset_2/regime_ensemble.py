"""
Regime-conditioned ensemble strategy (cross_asset-2).

Detects market regime from the `closes` series using two cheap proxies:
  * Hurst exponent (rescaled-range / variance-ratio style) -> trending vs mean-reverting.
  * Realized-vol percentile -> calm vs volatile.

A regime label (TREND / RANGE / VOLATILE) then dynamically weights three internal
sub-signals computed from the same series:
  * EMA trend (fast EMA vs slow EMA).
  * RSI-style momentum (Wilder-ish over/under extension).
  * Bollinger-band position (z within band).

Weights shift by regime: TREND favors EMA; RANGE favors Bollinger/mean-reversion;
VOLATILE down-weights trend and favors RSI extremes. The blended signed score is
emitted.  Pure Python, deterministic, math only; returns None on insufficient data.
"""
from __future__ import annotations

from math import log, sqrt
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata, StrategySignal


class RegimeConditionedEnsembleStrategy(BaseSignalStrategy):
    def __init__(
        self,
        window: int = 60,
        fast_ema: int = 12,
        slow_ema: int = 26,
        rsi_period: int = 14,
        boll_period: int = 20,
        boll_mult: float = 2.0,
    ) -> None:
        self._window = window
        self._fast_ema = fast_ema
        self._slow_ema = slow_ema
        self._rsi_period = rsi_period
        self._boll_period = boll_period
        self._boll_mult = boll_mult
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RegimeConditionedEnsembleStrategy",
                strategy_type="cross_asset_2",
                live_supported=False,
                replay_supported=True,
                backtest_supported=True,
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=120, warmup_period=window),
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

    @staticmethod
    def _ema_series(xs: list[float], period: int) -> list[float]:
        if not xs:
            return []
        k = 2.0 / (period + 1.0)
        out = [xs[0]]
        for x in xs[1:]:
            out.append(x * k + out[-1] * (1.0 - k))
        return out

    def _hurst(self, xs: list[float]) -> float:
        n = len(xs)
        if n < 12:
            return 0.5
        lags = [2, 4, 8, 16, 32]
        lags = [l for l in lags if l < n]
        if len(lags) < 2:
            return 0.5
        rs_vals = []
        for lag in lags:
            # Split into non-overlapping sub-series, compute mean range / std.
            segs = n // lag
            if segs < 1:
                continue
            rs_sum = 0.0
            for s in range(segs):
                sub = xs[s * lag:(s + 1) * lag]
                mean = self._mean(sub)
                cum = 0.0
                cumdev = []
                for v in sub:
                    cum += v - mean
                    cumdev.append(cum)
                rng = max(cumdev) - min(cumdev)
                sd = self._stdev(sub, mean)
                if sd > 0:
                    rs_sum += rng / sd
            rs_vals.append((log(lag), log(rs_sum / segs)))
        if len(rs_vals) < 2:
            return 0.5
        # Slope of log(lag) vs log(RS) via least squares.
        mx = self._mean([a for a, _ in rs_vals])
        my = self._mean([b for _, b in rs_vals])
        num = sum((a - mx) * (b - my) for a, b in rs_vals)
        den = sum((a - mx) ** 2 for a, _ in rs_vals)
        return max(0.0, min(1.0, my + (num / den) * (mx - mx))) if den > 0 else 0.5

    def _regime(self, closes: list[float]) -> str:
        n = len(closes)
        if n < self._window:
            return "RANGE"
        win = closes[-self._window:]
        hurst = self._hurst(win)
        # Realized vol percentile vs trailing window.
        log_rets = [log(win[i] / win[i - 1]) for i in range(1, len(win))]
        rv = self._stdev(log_rets, self._mean(log_rets))
        prior = closes[-(self._window * 2):-self._window]
        if prior:
            prets = [log(prior[i] / prior[i - 1]) for i in range(1, len(prior))]
            prv = self._stdev(prets, self._mean(prets))
            vol_ratio = rv / prv if prv > 0 else 1.0
        else:
            vol_ratio = 1.0

        if vol_ratio > 1.35:
            return "VOLATILE"
        if hurst > 0.55:
            return "TREND"
        if hurst < 0.45:
            return "RANGE"
        return "TREND" if vol_ratio > 1.0 else "RANGE"

    def _weights(self, regime: str) -> tuple[float, float, float]:
        if regime == "TREND":
            return (0.55, 0.25, 0.20)
        if regime == "RANGE":
            return (0.20, 0.20, 0.60)
        # VOLATILE: favor RSI extremes, down-weight trend.
        return (0.15, 0.55, 0.30)

    def _ema_signal(self, closes: list[float]) -> float:
        if len(closes) < self._slow_ema + 1:
            return 0.0
        fe = self._ema_series(closes, self._fast_ema)[-1]
        se = self._ema_series(closes, self._slow_ema)[-1]
        if se == 0:
            return 0.0
        diff = (fe - se) / se
        return max(-1.0, min(1.0, diff * 20.0))

    def _rsi_signal(self, closes: list[float]) -> float:
        p = self._rsi_period
        if len(closes) < p + 1:
            return 0.0
        gains = 0.0
        losses = 0.0
        for i in range(-p, 0):
            d = closes[i] - closes[i - 1]
            if d >= 0:
                gains += d
            else:
                losses -= d
        if losses == 0:
            rsi = 100.0
        else:
            rs = (gains / p) / (losses / p)
            rsi = 100.0 - 100.0 / (1.0 + rs)
        # Map 0..100 to roughly -1..1 around neutral 50.
        return max(-1.0, min(1.0, (50.0 - rsi) / 50.0))

    def _boll_signal(self, closes: list[float]) -> float:
        if len(closes) < self._boll_period:
            return 0.0
        win = closes[-self._boll_period:]
        mid = self._mean(win)
        sd = self._stdev(win, mid)
        if sd <= 0:
            return 0.0
        pos = (closes[-1] - mid) / (self._boll_mult * sd)
        return max(-1.0, min(1.0, -pos))  # mean-reversion: fade extremes

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes")
        if not closes or len(closes) < self._window + 1:
            return None

        regime = self._regime(closes)
        w_ema, w_rsi, w_boll = self._weights(regime)
        s_ema = self._ema_signal(closes)
        s_rsi = self._rsi_signal(closes)
        s_boll = self._boll_signal(closes)
        score = max(-1.0, min(1.0, w_ema * s_ema + w_rsi * s_rsi + w_boll * s_boll))

        if abs(score) < self.config.threshold:
            return None

        product_id = str(market_state.get("product_id", "BTC-USD"))
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=product_id,
            score=score,
            reason=f"regime={regime} w=(ema{self._weights(regime)[0]:.2f},rsi{self._weights(regime)[1]:.2f},boll{self._weights(regime)[2]:.2f})",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, regime],
            features={"regime": regime, "ema_sig": s_ema, "rsi_sig": s_rsi, "boll_sig": s_boll},
        )
