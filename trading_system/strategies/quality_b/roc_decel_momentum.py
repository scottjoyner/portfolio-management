"""Price Rate-of-Change regime-adaptive momentum (RocDecelMomentum).

In trending regimes we follow ROC momentum (long when ROC>0, short when ROC<0),
optionally fading only when momentum is decelerating in a ranging regime. In
ranges we fade ROC extremes. The deceleration filter softens entries near
exhaustion.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import adapt_to_regime, roc


class RocDecelMomentum(BaseSignalStrategy):
    def __init__(self, period: int = 12, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="RocDecelMomentum",
                strategy_type="momentum",
                data_requirements=["product_id", "closes", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.02, cooldown_seconds=0.0, warmup_period=period * 3 + regime_period),
        )
        self.period = period
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        need = self.period * 3 + self.regime_period
        if len(closes) < need:
            return None
        roc_now = roc(closes, self.period)
        roc_prev = roc(closes[:-1], self.period)
        if roc_now is None or roc_prev is None:
            return None
        accel = roc_now - roc_prev
        follow = 0.0
        reason = ""
        if roc_now > 0:
            follow = min(1.0, roc_now * 5.0)
            reason = f"ROC={roc_now:.4f} up"
        elif roc_now < 0:
            follow = -min(1.0, -roc_now * 5.0)
            reason = f"ROC={roc_now:.4f} down"
        else:
            return None
        score = adapt_to_regime(follow, closes, self.regime_period, self.regime_thr)
        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=reason + f" accel={accel:.4f} regime-adaptive",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"roc": round(roc_now, 5), "accel": round(accel, 5)},
        )
