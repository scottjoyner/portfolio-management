"""Relative Volatility Index regime-adaptive (RelativeVolatilityIndex).

RVI gauges volatility direction (upside vs downside volatility). In trending
regimes we follow the volatility direction (rising upside vol => long bias);
in ranging regimes we fade RVI extremes (overbought/oversold volatility).
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import _stdev, adapt_to_regime


class RelativeVolatilityIndex(BaseSignalStrategy):
    def __init__(self, period: int = 14, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RelativeVolatilityIndex",
                strategy_type="volatility_direction",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period * 2 + regime_period),
        )
        self.period = period
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        need = self.period * 2 + self.regime_period
        if len(closes) < need:
            return None
        ups = 0.0
        downs = 0.0
        for i in range(len(closes) - self.period, len(closes)):
            win = closes[max(0, i - self.period + 1): i + 1]
            sd = _stdev(win)
            if closes[i] >= closes[i - 1]:
                ups += sd
            else:
                downs += sd
        if ups + downs <= 1e-12:
            return None
        rvi = 100.0 * ups / (ups + downs)
        follow = (rvi - 50.0) / 50.0
        score = adapt_to_regime(follow, closes, self.regime_period, self.regime_thr)
        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"RVI={rvi:.1f} regime-adaptive",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"rvi": round(rvi, 2)},
        )
