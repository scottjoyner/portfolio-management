from time import monotonic

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _highest(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return max(values[-period:])


def _lowest(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return min(values[-period:])


def _choppiness(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1 or len(highs) < period + 1 or len(lows) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    if len(trs) < period:
        return None
    atr_sum = sum(trs[-period:])
    if atr_sum <= 0:
        return None
    hh = max(highs[-period:])
    ll = min(lows[-period:])
    if hh <= ll:
        return None
    import math

    ci = 100.0 * math.log10(atr_sum / (hh - ll)) / math.log10(period)
    return ci


class DonchianChoppinessVolBreakoutStrategy(BaseSignalStrategy):
    """Donchian channel breakout gated by a Choppiness-index trend filter (CI < 50)."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="DonchianChoppinessVolBreakoutStrategy",
                strategy_type="volatility",
                live_supported=True,
                data_requirements=["product_id", "score", "close", "high", "low", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=10, warmup_period=60),
        )

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        closes = market_state.get("closes") or []
        close = market_state.get("close")
        if close is None and closes:
            close = closes[-1]

        if not highs or not lows or close is None:
            score = float(market_state.get("score", 0.0))
            if score <= self.config.threshold:
                return None
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, 0.0, "fallback score path")

        # breakout vs the prior 20-bar channel (exclude the current bar)
        if len(highs) < 21 or len(lows) < 21:
            return None
        hh = _highest(highs[:-1], 20)
        ll = _lowest(lows[:-1], 20)
        if hh is None or ll is None:
            return None

        ci = _choppiness(highs, lows, closes, 14)
        if ci is None:
            return None
        # only trade directional breakouts when market is trending (low choppiness)
        if ci >= 50.0:
            return None

        if close > hh:
            score = min(1.0, (close - hh) / (hh - ll + 1e-9) + 0.3)
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, close, f"Donchian breakout up close {close:.2f} > {hh:.2f} (CI={ci:.1f})")
        if close < ll:
            score = min(1.0, (ll - close) / (hh - ll + 1e-9) + 0.3)
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, close, f"Donchian breakout down close {close:.2f} < {ll:.2f} (CI={ci:.1f})")
        return None

    def _mk_signal(self, score: float, close: float, reason: str) -> StrategySignal:
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(self.metadata_model.products[0]),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=True,
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"close": close, "choppiness_filtered": True},
        )
