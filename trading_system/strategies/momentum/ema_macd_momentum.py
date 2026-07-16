from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _ema(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1.0 - k)
    return ema


def _sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _macd_histogram(closes: list[float]) -> float | None:
    if len(closes) < 35:
        return None
    macd_series = []
    for i in range(len(closes)):
        e12 = _ema(closes[: i + 1], 12)
        e26 = _ema(closes[: i + 1], 26)
        if e12 is None or e26 is None:
            macd_series.append(None)
        else:
            macd_series.append(e12 - e26)
    while macd_series and macd_series[0] is None:
        macd_series.pop(0)
    if len(macd_series) < 9:
        return None
    macd_line = macd_series[-1]
    signal_line = _ema(macd_series, 9)
    if signal_line is None:
        return macd_line
    return macd_line - signal_line


class EmaMacdMomentumStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="EmaMacdMomentumStrategy",
                strategy_type="momentum",
                live_supported=True,
                data_requirements=["product_id", "close", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.2, cooldown_seconds=30, warmup_period=50),
        )

    def generate_signal(self, market_state: dict) -> "object":
        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        closes = market_state.get("closes") or []
        if not closes and "close" in market_state:
            closes = [market_state["close"]]
        if len(closes) < 35:
            return None

        ema12 = _ema(closes, 12)
        ema26 = _ema(closes, 26)
        hist = _macd_histogram(closes)
        if ema12 is None or ema26 is None or hist is None:
            return None

        score = 0.0
        if ema12 > ema26:
            score += 0.5
        if hist > 0:
            score += 0.5 * min(1.0, abs(hist) / (abs(ema26) + 1e-9))
        else:
            score -= 0.5 * min(1.0, abs(hist) / (abs(ema26) + 1e-9))

        if score <= self.config.threshold:
            return None

        self._last_emit_ts = __import__("time").monotonic()
        confidence = min(1.0, max(0.0, abs(score)))
        return self._build_signal(market_state, score, ema12, ema26, hist)

    def _build_signal(self, market_state, score, ema12, ema26, hist):
        from trading_system.strategies.base.interfaces import StrategySignal

        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"EMA12={ema12:.2f}>{ema26:.2f}=EMA26 MACD_hist={hist:.4f} (bullish momentum)",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "ema12": round(ema12, 4),
                "ema26": round(ema26, 4),
                "macd_hist": round(hist, 6),
            },
        )
