"""Elder's Bull/Bear power regime-adaptive (ElderPowerTrend).

Bull/Bear Power vs EMA(13). In trending regimes we follow the power (long when
Bull Power positive and rising, short when Bear Power negative and falling); in
ranging regimes we fade the extremes (overbought/oversold). Distinct from the
Rust `elder_ray` id.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import adapt_to_regime, ema_last


class ElderPowerTrend(BaseSignalStrategy):
    def __init__(self, ema_period: int = 13, smooth: int = 3, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="ElderPowerTrend",
                strategy_type="trend_power",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=ema_period + smooth + regime_period),
        )
        self.ema_period = ema_period
        self.smooth = smooth
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self.ema_period + self.smooth + self.regime_period or len(highs) < len(closes) or len(lows) < len(closes):
            return None
        ema = ema_last(closes, self.ema_period)
        if ema is None or ema <= 0:
            return None
        bull = highs[-1] - ema
        bear = lows[-1] - ema
        prev_bull = highs[-2] - ema_last(closes[:-1], self.ema_period)
        prev_bear = lows[-2] - ema_last(closes[:-1], self.ema_period)
        price = closes[-1]
        follow = 0.0
        reason = ""
        if bull > 0 and price > ema and bull > prev_bull:
            follow = min(1.0, bull / ema * 20.0)
            reason = f"BullPower={bull:.2f} rising"
        elif bear < 0 and price < ema and bear < prev_bear:
            follow = -min(1.0, -bear / ema * 20.0)
            reason = f"BearPower={bear:.2f} falling"
        if follow == 0.0:
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
            reason=reason + " regime-adaptive",
            confidence=min(1.0, abs(score)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"bull_power": round(bull, 2), "bear_power": round(bear, 2), "ema": round(ema, 2)},
        )
