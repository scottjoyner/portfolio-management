from __future__ import annotations
import math
from typing import List, Optional

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


class MomentumAccelerationStrategy(BaseStrategy):
    def __init__(self, mom_period: int = 14, accel_period: int = 7,
                 lookback: int = 50, entry_threshold: float = 0.01,
                 acceleration_threshold: float = 0.005):
        self.mom_period = mom_period
        self.accel_period = accel_period
        self.lookback = lookback
        self.entry_threshold = entry_threshold
        self.accel_threshold = acceleration_threshold
        self._name = "mom_accel"

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        if len(bars) < self.lookback + self.mom_period + self.accel_period:
            return None

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        momentum = self._compute_momentum(closes, self.mom_period)
        acceleration = self._compute_acceleration(momentum, self.accel_period)

        if len(momentum) < 5 or len(acceleration) < 3:
            return None

        current_mom = momentum[-1]
        prev_mom = momentum[-2]
        current_accel = acceleration[-1]
        prev_accel = acceleration[-2]
        current_price = closes[-1]

        vol_confirm = self._volume_confirmation(volumes, 10)
        rsi_val = self._rsi(closes, 14)

        if current_mom > self.entry_threshold and current_accel > self.accel_threshold:
            if prev_accel <= self.accel_threshold:
                direction = Direction.LONG
                conf = min(0.65, current_accel * 20 + 0.4)
                reason = (f"MOM^2: accelerating bullish "
                          f"mom={current_mom:.3f} accel={current_accel:.4f}")
                if vol_confirm:
                    conf = min(0.75, conf + 0.1)
                    reason += " (vol confirm)"
                elif rsi_val > 70:
                    conf *= 0.7
                    reason += " (overbought)"
                stop = current_price - atr * 2.0
                target = current_price + atr * 3.0
            else:
                return None

        elif current_mom < -self.entry_threshold and current_accel < -self.accel_threshold:
            if prev_accel >= -self.accel_threshold:
                direction = Direction.SHORT
                conf = min(0.65, abs(current_accel) * 20 + 0.4)
                reason = (f"MOM^2: accelerating bearish "
                          f"mom={current_mom:.3f} accel={current_accel:.4f}")
                if vol_confirm:
                    conf = min(0.75, conf + 0.1)
                    reason += " (vol confirm)"
                elif rsi_val < 30:
                    conf *= 0.7
                    reason += " (oversold)"
                stop = current_price + atr * 2.0
                target = current_price - atr * 3.0
            else:
                return None

        elif current_mom > self.entry_threshold and current_accel < -self.accel_threshold:
            if prev_accel >= -self.accel_threshold:
                direction = Direction.SHORT
                conf = 0.5
                reason = (f"MOM^2: bearish divergence "
                          f"mom={current_mom:.3f} accel={current_accel:.4f}")
                stop = current_price + atr * 1.5
                target = current_price - atr * 2.5
            else:
                return None

        elif current_mom < -self.entry_threshold and current_accel > self.accel_threshold:
            if prev_accel <= self.accel_threshold:
                direction = Direction.LONG
                conf = 0.5
                reason = (f"MOM^2: bullish divergence "
                          f"mom={current_mom:.3f} accel={current_accel:.4f}")
                stop = current_price - atr * 1.5
                target = current_price + atr * 2.5
            else:
                return None
        else:
            return None

        rr = abs(target - current_price) / max(abs(current_price - stop), 1e-9)
        if rr < 1.2:
            return None

        return BracketSetup(
            direction=direction, entry_price=current_price,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(conf, 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={
                "momentum": round(current_mom, 5),
                "acceleration": round(current_accel, 6),
                "vol_confirm": vol_confirm,
            },
        )

    def _compute_momentum(self, closes: List[float], period: int) -> List[float]:
        if len(closes) < period + 1:
            return []
        momentum = []
        for i in range(period, len(closes)):
            ret = (closes[i] - closes[i - period]) / max(closes[i - period], 1e-9)
            momentum.append(ret)
        return momentum

    def _compute_acceleration(self, momentum: List[float], period: int) -> List[float]:
        if len(momentum) < period + 1:
            return []
        acceleration = []
        for i in range(period, len(momentum)):
            accel = (momentum[i] - momentum[i - period]) / max(period, 1)
            acceleration.append(accel)
        return acceleration

    def _volume_confirmation(self, volumes: List[float], period: int) -> bool:
        if len(volumes) < period + 1:
            return False
        avg_vol = sum(volumes[-period-1:-1]) / period
        return volumes[-1] > avg_vol * 1.2

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
