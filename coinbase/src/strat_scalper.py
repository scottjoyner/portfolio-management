from __future__ import annotations
import math
from typing import List, Optional

from .protocols import Direction, Bar, BracketSetup, BaseStrategy
from typing import Tuple


class VolatilityScalperStrategy(BaseStrategy):
    def __init__(self, atr_period: int = 14,
                 expansion_entry: float = 2.5,
                 contraction_entry: float = 0.5,
                 lookback_vol: int = 50,
                 bb_period: int = 20, bb_std: float = 2.0):
        self.atr_period = atr_period
        self.expansion_threshold = expansion_entry
        self.contraction_threshold = contraction_entry
        self.lookback_vol = lookback_vol
        self.bb_period = bb_period
        self.bb_std = bb_std
        self._name = "vol_scalper"

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        if len(bars) < max(self.bb_period * 2, self.lookback_vol):
            return None

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]
        atr = self._estimate_atr(closes, highs, lows, self.atr_period)
        if atr <= 0:
            return None

        avg_atr = self._estimate_atr(closes, highs, lows, self.atr_period * 3)
        if avg_atr <= 0:
            return None

        vol_ratio = atr / avg_atr
        current = closes[-1]

        bb_mid, bb_upper, bb_lower = self._bollinger(closes, self.bb_period, self.bb_std)
        rsi_val = self._rsi(closes, 14)
        avg_vol = sum(volumes[-10:]) / 10 if len(volumes) >= 10 else bar.volume

        if vol_ratio >= self.expansion_threshold:
            if bb_upper > 0 and current > bb_upper:
                direction = Direction.SHORT
                stop = current + atr * 1.2
                target = bb_mid
                conf = min(0.6, (vol_ratio / 3.0) * 0.5 + 0.3)
                reason = f"VSCALP: expansion fade upper ({vol_ratio:.1f}x)"
            elif bb_lower > 0 and current < bb_lower:
                direction = Direction.LONG
                stop = current - atr * 1.2
                target = bb_mid
                conf = min(0.6, (vol_ratio / 3.0) * 0.5 + 0.3)
                reason = f"VSCALP: expansion fade lower ({vol_ratio:.1f}x)"
            else:
                return None

        elif vol_ratio <= self.contraction_threshold:
            squeeze_bars = sum(1 for i in range(-5, 0) if
                               self._estimate_atr(closes[i-5:i], highs[i-5:i], lows[i-5:i], 5) /
                               max(self._estimate_atr(closes[:i], highs[:i], lows[:i], 20), 1e-9) < 0.6)
            if squeeze_bars >= 3:
                if rsi_val < 40:
                    direction = Direction.LONG
                    stop = current - atr * 2.0
                    target = current + atr * 3.0
                    conf = 0.45
                    reason = f"VSCALP: squeeze breakout long rsi={rsi_val:.0f}"
                elif rsi_val > 60:
                    direction = Direction.SHORT
                    stop = current + atr * 2.0
                    target = current - atr * 3.0
                    conf = 0.45
                    reason = f"VSCALP: squeeze breakout short rsi={rsi_val:.0f}"
                else:
                    direction = Direction.LONG if current < bb_mid else Direction.SHORT
                    stop = current - atr * 1.5 if direction == Direction.LONG else current + atr * 1.5
                    target = current + atr * 2.0 if direction == Direction.LONG else current - atr * 2.0
                    conf = 0.35
                    reason = f"VSCALP: squeeze direction bias"
            else:
                return None
        else:
            candle_range = (bar.high - bar.low) / max(current, 1e-9) * 10000
            avg_range = atr / max(current, 1e-9) * 10000
            range_ratio = candle_range / max(avg_range, 1e-9)
            if range_ratio < 0.3:
                return None
            if rsi_val < 30:
                direction = Direction.LONG
                stop = current - atr * 1.0
                target = current + atr * 1.5
                conf = 0.4
                reason = f"VSCALP: oversold scalp rsi={rsi_val:.0f}"
            elif rsi_val > 70:
                direction = Direction.SHORT
                stop = current + atr * 1.0
                target = current - atr * 1.5
                conf = 0.4
                reason = f"VSCALP: overbought scalp rsi={rsi_val:.0f}"
            else:
                return None

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < 1.0:
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(min(conf, 0.7), 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={"vol_ratio": round(vol_ratio, 2),
                      "bb_position": "upper" if bb_upper > 0 and current > bb_upper else
                                     "lower" if bb_lower > 0 and current < bb_lower else "middle"},
        )

    def _bollinger(self, closes: List[float], period: int, std_mult: float
                    ) -> Tuple[float, float, float]:
        if len(closes) < period:
            return (closes[-1], closes[-1], closes[-1]) if closes else (0, 0, 0)
        recent = closes[-period:]
        mean = sum(recent) / period
        var = sum((x - mean) ** 2 for x in recent) / period
        std = math.sqrt(var)
        return (mean, mean + std_mult * std, mean - std_mult * std)

    @staticmethod
    def _rsi(closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i-1] for i in range(-period, 0)]
        gains = sum(d for d in deltas if d > 0)
        losses = sum(abs(d) for d in deltas if d < 0)
        if losses == 0:
            return 100.0
        rs = (gains / period) / (losses / period)
        return 100.0 - (100.0 / (1.0 + rs))

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
