from __future__ import annotations
import math
from typing import List, Optional

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


class DCAAccumulationStrategy(BaseStrategy):
    def __init__(self, base_interval_bars: int = 96,
                 volatility_boost: float = 1.5,
                 max_boost: float = 4.0,
                 lookback_vol: int = 50,
                 min_hold_bars: int = 200):
        self.base_interval = base_interval_bars
        self.vol_boost = volatility_boost
        self.max_boost = max_boost
        self.lookback_vol = lookback_vol
        self.min_hold_bars = min_hold_bars
        self._name = "dca_accumulate"
        self._bars_since_last_buy: int = 0
        self._total_bars: int = 0
        self._last_buy_price: Optional[float] = None
        self._last_buy_bar: int = 0

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        self._total_bars += 1
        self._bars_since_last_buy += 1

        if len(bars) < self.lookback_vol:
            return None

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        if self._bars_since_last_buy < self.base_interval:
            return None

        current = closes[-1]
        vol_ratio = self._volatility_ratio(closes)
        price_drop_pct = self._price_drop_from_peak(closes)
        below_sma = self._below_sma(closes, 200)

        boost = 1.0
        reasons = []

        if vol_ratio > self.vol_boost:
            boost = min(self.max_boost, vol_ratio)
            reasons.append(f"vol_boost={vol_ratio:.1f}x")

        if price_drop_pct < -0.05:
            drop_boost = min(2.0, abs(price_drop_pct) * 20)
            boost = max(boost, drop_boost)
            reasons.append(f"dip={price_drop_pct:.1%}")

        if below_sma:
            boost *= 1.5
            reasons.append("below_sma200")

        avg_vol = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else bar.volume
        vol_low = bar.volume < avg_vol * 0.5
        if vol_low:
            boost *= 0.7
            reasons.append("low_vol")

        boost = min(self.max_boost, max(1.0, boost))
        confidence = min(0.6, boost * 0.15)

        if self._last_buy_price:
            bars_held = self._total_bars - self._last_buy_bar
            if bars_held < self.min_hold_bars:
                return None

        stop = current - atr * 4.0
        target = current + atr * 2.0
        rr = abs(target - current) / max(abs(current - stop), 1e-9)

        reason_str = "DCA: " + ", ".join(reasons) if reasons else "DCA: scheduled"

        self._bars_since_last_buy = 0
        self._last_buy_price = current
        self._last_buy_bar = self._total_bars

        return BracketSetup(
            direction=Direction.LONG, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(confidence, 3),
            reason=reason_str, strategy_name=self._name, atr=atr,
            metadata={
                "boost": round(boost, 2),
                "vol_ratio": round(vol_ratio, 2),
                "price_drop_pct": round(price_drop_pct, 4),
                "interval": self.base_interval,
            },
        )

    def _volatility_ratio(self, closes: List[float]) -> float:
        if len(closes) < self.lookback_vol + 1:
            return 0.0
        returns = [(closes[i] - closes[i-1]) / max(closes[i-1], 1e-9)
                   for i in range(-self.lookback_vol, 0)]
        recent_returns = [(closes[i] - closes[i-1]) / max(closes[i-1], 1e-9)
                          for i in range(-10, 0)]
        hist_vol = math.sqrt(sum(r*r for r in returns) / len(returns)) if returns else 0.001
        recent_vol = math.sqrt(sum(r*r for r in recent_returns) / len(recent_returns)) if recent_returns else 0.001
        if hist_vol < 0.0001:
            return 0.0
        return recent_vol / hist_vol

    def _price_drop_from_peak(self, closes: List[float]) -> float:
        if len(closes) < 20:
            return 0.0
        peak = max(closes[-50:]) if len(closes) >= 50 else max(closes)
        return (closes[-1] - peak) / max(peak, 1e-9)

    def _below_sma(self, closes: List[float], period: int) -> bool:
        if len(closes) < period:
            return False
        sma = sum(closes[-period:]) / period
        return closes[-1] < sma

    @staticmethod
    def _estimate_atr(closes: List[float], highs: List[float],
                       lows: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 0.0
        tr_vals = []
        for i in range(1, min(period + 1, len(closes))):
            tr = max(highs[-i] - lows[-i],
                     abs(highs[-i] - closes[-i-1]),
                     abs(lows[-i] - closes[-i-1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0
