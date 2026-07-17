"""Low-volatility breakout regime-adaptive (NarrowRangeBreakout).

Find the narrowest of the last 7 bars' true ranges (NR7) — a volatility
contraction. In trending regimes we follow the thrust out of the narrow range
(long on upside thrust, short on downside); in ranging regimes we fade the
thrust (mean-reversion after volatility expansions).
"""
from __future__ import annotations

from time import monotonic

from strategies.base import BaseSignalStrategy, StrategyConfig, StrategyMetadata
from trading_system.strategies.base.interfaces import StrategySignal
from trading_system.strategies.quality_b.indicators import adapt_to_regime


class NarrowRangeBreakout(BaseSignalStrategy):
    def __init__(self, lookback: int = 7, thrust: float = 1.0, regime_period: int = 30, regime_thr: float = 0.3) -> None:
        super().__init__(
            metadata=StrategyMetadata(
                strategy_id="NarrowRangeBreakout",
                strategy_type="volatility_breakout",
                data_requirements=["product_id", "closes", "highs", "lows", "warmup_complete"],
                risk_mode_hint="NORMAL",
                capital_bucket="ACTIVE_TRADING",
            ),
            config=StrategyConfig(threshold=0.05, cooldown_seconds=0.0, warmup_period=lookback + regime_period),
        )
        self.lookback = lookback
        self.thrust = thrust
        self.regime_period = regime_period
        self.regime_thr = regime_thr

    def generate_signal(self, market_state: dict) -> StrategySignal | None:
        if self.is_disabled(market_state)[0] or self.in_cooldown():
            return None
        closes = market_state.get("closes") or []
        highs = market_state.get("highs") or []
        lows = market_state.get("lows") or []
        if len(closes) < self.lookback + self.regime_period or len(highs) < len(closes) or len(lows) < len(closes):
            return None
        trs = []
        for i in range(len(closes) - self.lookback, len(closes)):
            h = highs[i]
            l = lows[i]
            pc = closes[i - 1]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        if not trs:
            return None
        nr_idx_local = trs.index(min(trs))
        nr_idx = len(closes) - self.lookback + nr_idx_local
        nr_high = highs[nr_idx]
        nr_low = lows[nr_idx]
        nr_range = nr_high - nr_low
        if nr_range <= 1e-9:
            return None
        price = closes[-1]
        base = 0.0
        reason = ""
        if price > nr_high:
            pen = (price - nr_high) / nr_range
            if pen > self.thrust:
                # base = mean-reversion fade of the thrust
                base = -min(1.0, pen / self.thrust * 0.5)
                reason = f"thrust above NR7 high={nr_high:.2f} fade"
        elif price < nr_low:
            pen = (nr_low - price) / nr_range
            if pen > self.thrust:
                base = min(1.0, pen / self.thrust * 0.5)
                reason = f"thrust below NR7 low={nr_low:.2f} fade"
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
            features={"nr_high": round(nr_high, 2), "nr_low": round(nr_low, 2), "nr_range": round(nr_range, 2)},
        )
