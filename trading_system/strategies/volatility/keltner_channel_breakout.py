from time import monotonic

from trading_system.strategies.base.interfaces import StrategySignal

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
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
    return sum(trs[-period:]) / period


class KeltnerVolBreakoutStrategy(BaseSignalStrategy):
    """Keltner channel breakout: close beyond EMA(20) +/- ATR(14)*2 signals expansion."""

    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="KeltnerVolBreakoutStrategy",
                strategy_type="volatility",
                live_supported=True,
                data_requirements=["product_id", "score", "close", "high", "low", "warmup_complete"],
                risk_mode_hint="AGGRESSIVE",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=5, warmup_period=40),
        )

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        close = market_state.get("close")
        if close is None and closes:
            close = closes[-1]

        if not closes or close is None:
            score = float(market_state.get("score", 0.0))
            if score <= self.config.threshold:
                return None
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, 0.0, "fallback score path")

        ema = _sma(closes, 20)
        atr = _atr(highs, lows, closes, 14)
        if ema is None or atr is None:
            return None

        upper = ema + 2.0 * atr
        lower = ema - 2.0 * atr
        if close > upper:
            score = min(1.0, (close - upper) / (atr + 1e-9))
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, close, f"close {close:.2f} > Keltner upper {upper:.2f}")
        if close < lower:
            score = min(1.0, (lower - close) / (atr + 1e-9))
            self._last_emit_ts = monotonic()
            return self._mk_signal(score, close, f"close {close:.2f} < Keltner lower {lower:.2f}")
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
            features={"close": close, "atr_based": True},
        )
