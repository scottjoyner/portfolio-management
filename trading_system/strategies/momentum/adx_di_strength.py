from __future__ import annotations

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata


def _true_ranges(highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
    trs = []
    for i in range(1, len(closes)):
        prev_close = closes[i - 1]
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - prev_close),
            abs(lows[i] - prev_close),
        )
        trs.append(tr)
    return trs


def _wilder_smooth(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    smoothed = sum(values[:period]) / period
    for v in values[period:]:
        smoothed = (smoothed * (period - 1) + v) / period
    return smoothed


def _adx_di(highs: list[float], lows: list[float], closes: list[float], period: int = 14):
    if len(closes) < period + 1:
        return None, None, None
    plus_dm = []
    minus_dm = []
    for i in range(1, len(closes)):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm.append(max(up, 0.0) if up > down else 0.0)
        minus_dm.append(max(down, 0.0) if down > up else 0.0)
    trs = _true_ranges(highs, lows, closes)
    atr = _wilder_smooth(trs, period)
    if atr is None or atr == 0:
        return None, None, None
    dx_list = []
    plus_di_prev = _wilder_smooth(plus_dm, period) or 0.0
    minus_di_prev = _wilder_smooth(minus_dm, period) or 0.0
    tr_smooth = _wilder_smooth(trs, period) or 1e-9
    plus_di = 100.0 * plus_di_prev / tr_smooth
    minus_di = 100.0 * minus_di_prev / tr_smooth
    for i in range(period, len(trs)):
        pdi = plus_dm[i] / trs[i] * 100.0 if trs[i] else 0.0
        mdi = minus_dm[i] / trs[i] * 100.0 if trs[i] else 0.0
        plus_di_prev = (plus_di_prev * (period - 1) + pdi) / period
        minus_di_prev = (minus_di_prev * (period - 1) + mdi) / period
        dx_list.append(100.0 * abs(plus_di_prev - minus_di_prev) / (plus_di_prev + minus_di_prev + 1e-9))
    adx = _wilder_smooth(dx_list, period)
    if adx is None:
        return None, None, None
    return adx, plus_di, minus_di


class AdxDiStrengthStrategy(BaseSignalStrategy):
    def __init__(self) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="AdxDiStrengthStrategy",
                strategy_type="trend",
                live_supported=True,
                data_requirements=["product_id", "highs", "lows", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.3, cooldown_seconds=30, warmup_period=60),
        )

    def generate_signal(self, market_state: dict) -> "object":
        from trading_system.strategies.base.interfaces import StrategySignal
        import time

        disabled, _ = self.is_disabled(market_state)
        if disabled:
            return None
        if self.in_cooldown():
            return None

        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        closes = market_state.get("closes") or []
        if len(closes) < 30:
            return None

        adx, plus_di, minus_di = _adx_di(highs, lows, closes, period=14)
        if adx is None or plus_di is None or minus_di is None:
            return None
        if adx < 25:
            return None

        if plus_di > minus_di:
            score = 0.5 + 0.5 * min(1.0, (plus_di - minus_di) / 100.0)
        else:
            score = -(0.5 + 0.5 * min(1.0, (minus_di - plus_di) / 100.0))

        if score <= self.config.threshold:
            return None

        time_monotonic = time.monotonic()
        self._last_emit_ts = time_monotonic
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"ADX={adx:.1f}>25 +DI={plus_di:.1f} -DI={minus_di:.1f} (trend strength)",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={
                "adx": round(adx, 2),
                "plus_di": round(plus_di, 2),
                "minus_di": round(minus_di, 2),
            },
        )
