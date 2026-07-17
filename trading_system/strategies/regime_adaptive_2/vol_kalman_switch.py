from __future__ import annotations

import math
from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1.0 - k)
    return ema


def _gaussian_pdf(x: float, mu: float, sigma: float) -> float:
    sigma = max(sigma, 1e-9)
    z = (x - mu) / sigma
    return math.exp(-0.5 * z * z) / (sigma * math.sqrt(2.0 * math.pi))


class VolRegimeKalmanSwitchStrategy(BaseSignalStrategy):
    """Hidden 2-state volatility switching model, trading the inferred state.

    A latent binary volatility regime (calm vs. turbulent) is tracked with a
    recursive Bayesian (Kalman-style) filter over squared log-returns.  Two
    Gaussian emission models -- one low-variance ("calm"), one high-variance
    ("turbulent") -- are calibrated from the lower/upper halves of the realised
    volatility distribution.  A sticky 2x2 transition matrix produces a
    forward-predicted regime belief, then the observed return updates it via
    Bayes' rule (the discrete analogue of a Kalman measurement update).

    Trading logic:
      * calm regime  -> trend-follow the smoothed drift (momentum works when
        vol is low and moves are orderly);
      * turbulent regime -> fade the latest move (mean-revert violent spikes).
    Conviction scales with how confidently the filter sits in one regime
    (|belief - 0.5|) times the normalised drift/spike magnitude.
    """

    def __init__(self, window: int = 60) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="VolRegimeKalmanSwitch",
                strategy_type="regime_adaptive_kalman",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.12, cooldown_seconds=25, warmup_period=window),
        )
        self.window = window
        # sticky transition matrix: P(stay in same regime) = 0.9
        self._p_stay = 0.9

    def _log_returns(self, closes: list[float]) -> list[float]:
        out: list[float] = []
        for i in range(1, len(closes)):
            a, b = closes[i - 1], closes[i]
            if a > 0 and b > 0:
                out.append(math.log(b / a))
        return out

    def _filter_belief(self, sq: list[float], var_lo: float, var_hi: float) -> float:
        """Return P(turbulent) after running the recursive filter over sq."""
        sig_lo = math.sqrt(max(var_lo, 1e-12))
        sig_hi = math.sqrt(max(var_hi, 1e-12))
        belief = 0.5  # P(turbulent)
        for s in sq:
            x = math.sqrt(s)  # |return| magnitude as the observation
            # --- predict (transition) ---
            pred_turb = belief * self._p_stay + (1.0 - belief) * (1.0 - self._p_stay)
            pred_calm = 1.0 - pred_turb
            # --- update (measurement / Bayes) ---
            like_turb = _gaussian_pdf(x, 0.0, sig_hi)
            like_calm = _gaussian_pdf(x, 0.0, sig_lo)
            num = like_turb * pred_turb
            den = num + like_calm * pred_calm
            belief = num / den if den > 1e-300 else pred_turb
            belief = min(1.0 - 1e-6, max(1e-6, belief))
        return belief

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if len(closes) < self.window:
            return None

        window = closes[-self.window:]
        rets = self._log_returns(window)
        if len(rets) < 20:
            return None

        sq = [r * r for r in rets]
        ordered = sorted(sq)
        half = len(ordered) // 2
        var_lo = _mean(ordered[:half]) or 1e-12
        var_hi = _mean(ordered[half:]) or (var_lo * 4.0)
        if var_hi <= var_lo:
            var_hi = var_lo * 4.0

        p_turb = self._filter_belief(sq, var_lo, var_hi)

        # smoothed drift (calm regime signal) and latest spike (turbulent signal)
        drift = _ema(rets, 10)
        rstd = math.sqrt(_mean(sq)) + 1e-12
        norm_drift = max(-1.0, min(1.0, drift / rstd))
        last_spike = max(-1.0, min(1.0, rets[-1] / (rstd + 1e-12) / 3.0))

        regime_conf = abs(p_turb - 0.5) * 2.0  # in [0, 1]

        if p_turb >= 0.5:
            # turbulent: fade the last move
            raw = -last_spike
            regime = "turbulent"
            drive = min(1.0, abs(last_spike))
        else:
            # calm: follow the drift
            raw = norm_drift
            regime = "calm"
            drive = min(1.0, abs(norm_drift))

        score = max(-1.0, min(1.0, raw * regime_conf))
        if abs(score) <= self.config.threshold:
            return None

        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=(
                f"kalman regime={regime} P(turb)={p_turb:.3f} "
                f"drift={norm_drift:.3f} spike={last_spike:.3f}"
            ),
            confidence=min(1.0, regime_conf * (0.5 + 0.5 * drive)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status, regime],
            features={
                "p_turbulent": round(p_turb, 4),
                "regime_conf": round(regime_conf, 4),
                "var_lo": round(var_lo, 10),
                "var_hi": round(var_hi, 10),
                "norm_drift": round(norm_drift, 4),
            },
        )
