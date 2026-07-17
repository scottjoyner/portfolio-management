"""Aroon-based regime-adaptive trend composite (AroonTrendComposite).

Aroon-Up/Down oscillator. In trending regimes we follow the trend (long when
Aroon-Up >> Down, short when Down >> Up); in ranging regimes we fade the
extremes (mean-reversion of trend exhaustion). Distinct from the Rust `aroon` id.
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import adapt_to_regime, highest, lowest


class AroonTrendComposite(BaseSignalStrategy):
    def __init__(self, period: int = 25, gap: float = 30.0, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="AroonTrendComposite",
                strategy_type="trend_strength",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.1, cooldown_seconds=0.0, warmup_period=period + regime_period),
        )
        self.period = period
        self.gap = gap
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self.period + self.regime_period or len(highs) < len(closes) or len(lows) < len(closes):
            return None
        wh = highest(highs, self.period)
        wl = lowest(lows, self.period)
        if wh is None or wl is None:
            return None
        hi_idx = lo_idx = len(highs) - 1
        for i in range(len(highs) - 1, len(highs) - self.period - 1, -1):
            if highs[i] == wh:
                hi_idx = i
                break
        for i in range(len(lows) - 1, len(lows) - self.period - 1, -1):
            if lows[i] == wl:
                lo_idx = i
                break
        aroon_up = 100.0 * (self.period - (len(highs) - 1 - hi_idx)) / self.period
        aroon_down = 100.0 * (self.period - (len(lows) - 1 - lo_idx)) / self.period
        if abs(aroon_up - aroon_down) < self.gap:
            return None
        osc = (aroon_up - aroon_down) / 100.0
        follow = max(-1.0, min(1.0, osc))
        score = adapt_to_regime(follow, closes, self.regime_period, self.regime_thr)
        score = max(-1.0, min(1.0, score))
        if abs(score) <= self.config.threshold:
            return None
        self._last_emit_ts = monotonic()
        return StrategySignal(
            strategy_id=self.strategy_id,
            product_id=str(market_state.get("product_id", "BTC-USD")),
            score=score,
            reason=f"AroonUp={aroon_up:.1f} AroonDown={aroon_down:.1f} osc={osc:.2f} regime-adaptive",
            confidence=min(1.0, abs(osc)),
            warmup_passed=bool(market_state.get("warmup_complete", True)),
            tags=[self.metadata_model.strategy_type, self.metadata_model.status],
            features={"aroon_up": round(aroon_up, 2), "aroon_down": round(aroon_down, 2)},
        )
