"""ADX-weak-range fade.

When ADX < 20 the market is in a weak / non-trending range. Inside such ranges
price excursions to Bollinger extremes are high-probability mean-reversion
setups. Only fade extreme band touches while ADX confirms the lack of trend;
stand aside when the trend is strong (ADX >= 20).
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_c.indicators import adx, bollinger


class AdxWeakRangeFadeStrategy(BaseSignalStrategy):
    def __init__(self, period: int = 20, num_std: float = 2.5, adx_period: int = 14, adx_thr: float = 25.0) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="AdxWeakRangeFadeStrategy",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + adx_period + 1),
        )
        self.period = period
        self.num_std = num_std
        self.adx_period = adx_period
        self.adx_thr = adx_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        need = self.period + self.adx_period + 1
        if len(closes) < need or len(highs) < need or len(lows) < need:
            return None
        a = adx(highs, lows, closes, self.adx_period)
        if a is None or a >= self.adx_thr:
            return None
        price = closes[-1]
        mean, upper, lower = bollinger(closes, self.period, self.num_std)
        if mean is None or upper is None or lower is None or mean <= 0:
            return None
        score = 0.0
        reason = ""
        if price < lower:
            pen = (lower - price) / (lower + 1e-9)
            score = min(1.0, 0.4 + pen * 4.0)
            reason = f"ADX={a:.1f}<{self.adx_thr:.0f} range; price={price:.2f} < lower={lower:.2f} fade"
        elif price > upper:
            pen = (price - upper) / (upper + 1e-9)
            score = -min(1.0, 0.4 + pen * 4.0)
            reason = f"ADX={a:.1f}<{self.adx_thr:.0f} range; price={price:.2f} > upper={upper:.2f} fade"
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason,
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"adx": round(a, 2), "lower": round(lower, 2), "upper": round(upper, 2)},
        )
