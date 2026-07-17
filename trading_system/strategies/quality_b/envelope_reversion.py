"""Moving Average Envelope regime-adaptive (EnvelopeReversion).

EMA envelope of +-band%. In ranging regimes we mean-revert (fade excursions
outside the band); in trending regimes we follow the breakout (trade the
direction of the envelope breach). Signed score scales with penetration depth.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import adapt_to_regime, ema_last


class EnvelopeReversion(BaseSignalStrategy):
    def __init__(self, period: int = 20, band_pct: float = 0.03, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="EnvelopeReversion",
                strategy_type="mean_reversion",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=period + regime_period),
        )
        self.period = period
        self.band_pct = band_pct
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        if len(closes) < self.period + self.regime_period:
            return None
        ema = ema_last(closes, self.period)
        if ema is None or ema <= 0:
            return None
        price = closes[-1]
        upper = ema * (1.0 + self.band_pct)
        lower = ema * (1.0 - self.band_pct)
        # base = mean-reversion signal (fade the breach)
        base = 0.0
        reason = ""
        if price > upper:
            pen = (price - upper) / (upper + 1e-9)
            base = -min(1.0, pen * 10.0)
            reason = f"price={price:.2f} > upper={upper:.2f} envelope-fade"
        elif price < lower:
            pen = (lower - price) / (lower + 1e-9)
            base = min(1.0, pen * 10.0)
            reason = f"price={price:.2f} < lower={lower:.2f} envelope-fade"
        else:
            return None
        score = adapt_to_regime(base, closes, self.regime_period, self.regime_thr)
        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason + " regime-adaptive",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"ema": round(ema, 2), "upper": round(upper, 2), "lower": round(lower, 2)},
        )
