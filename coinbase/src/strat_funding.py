from __future__ import annotations
import math
from typing import List, Optional

from .protocols import Direction, Bar, BracketSetup, BaseStrategy, InstrumentType


class FundingRateCaptureStrategy(BaseStrategy):
    def __init__(self, min_funding_rate_bps: float = 0.5,
                 max_funding_rate_bps: float = 5.0,
                 lookback_bars: int = 96,
                 atr_stop_mult: float = 1.5,
                 min_confidence: float = 0.35):
        self.min_funding = min_funding_rate_bps
        self.max_funding = max_funding_rate_bps
        self.lookback = lookback_bars
        self.atr_stop = atr_stop_mult
        self.min_confidence = min_confidence
        self._name = "funding_capture"
        self._funding_history: List[float] = []

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        if len(bars) < 30:
            return None

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        funding_rate = self._estimate_funding_rate(bars)
        if funding_rate is None:
            return None

        self._funding_history.append(funding_rate)
        if len(self._funding_history) > self.lookback:
            self._funding_history.pop(0)

        avg_funding = sum(self._funding_history) / len(self._funding_history) if self._funding_history else 0.0
        funding_bps = funding_rate * 10000
        current = closes[-1]

        vol_regime = self._detect_vol_regime(closes, highs, lows)
        trend = self._detect_trend(closes)

        if funding_bps > self.min_funding:
            if funding_bps > self.max_funding:
                return None
            if vol_regime != "high":
                direction = Direction.SHORT
                conf = min(0.65, (funding_bps / self.max_funding) * 0.5 + 0.3)
                reason = f"FUND: short positive funding={funding_bps:.1f}bps avg={avg_funding*10000:.1f}bps"
                if trend == "uptrend":
                    conf *= 0.7
                    reason += " (trend headwind)"
                stop = current + atr * self.atr_stop
                target = current - atr * 2.0
                rr = abs(target - current) / max(abs(current - stop), 1e-9)
                if rr >= 1.2 and conf >= self.min_confidence:
                    return BracketSetup(
                        direction=direction, entry_price=current,
                        stop_price=round(stop, 4), target_price=round(target, 4),
                        risk_reward=round(rr, 2), confidence=round(conf, 3),
                        reason=reason, strategy_name=self._name, atr=atr,
                        instrument_type=InstrumentType.PERP_FUTURES,
                        leverage=1.0,
                        metadata={"funding_rate_bps": round(funding_bps, 2),
                                  "avg_funding_bps": round(avg_funding * 10000, 2)},
                    )

        elif funding_bps < -self.min_funding:
            if abs(funding_bps) > self.max_funding:
                return None
            if vol_regime != "high":
                direction = Direction.LONG
                conf = min(0.65, (abs(funding_bps) / self.max_funding) * 0.5 + 0.3)
                reason = f"FUND: long negative funding={funding_bps:.1f}bps avg={avg_funding*10000:.1f}bps"
                if trend == "downtrend":
                    conf *= 0.7
                    reason += " (trend headwind)"
                stop = current - atr * self.atr_stop
                target = current + atr * 2.0
                rr = abs(target - current) / max(abs(current - stop), 1e-9)
                if rr >= 1.2 and conf >= self.min_confidence:
                    return BracketSetup(
                        direction=direction, entry_price=current,
                        stop_price=round(stop, 4), target_price=round(target, 4),
                        risk_reward=round(rr, 2), confidence=round(conf, 3),
                        reason=reason, strategy_name=self._name, atr=atr,
                        instrument_type=InstrumentType.PERP_FUTURES,
                        leverage=1.0,
                        metadata={"funding_rate_bps": round(funding_bps, 2),
                                  "avg_funding_bps": round(avg_funding * 10000, 2)},
                    )

        return None

    def _estimate_funding_rate(self, bars: List[Bar]) -> Optional[float]:
        if len(bars) < 3:
            return None
        recent = bars[-3:]
        differences = []
        for i in range(1, len(recent)):
            open_diff = recent[i].open - recent[i-1].close
            close_diff = recent[i].close - recent[i].open
            differences.append(abs(close_diff - open_diff))
        avg_diff = sum(differences) / len(differences) if differences else 0.0
        base_vol = self._estimate_atr(
            [b.close for b in bars], [b.high for b in bars], [b.low for b in bars]
        )
        if base_vol <= 0 or base_vol == 0:
            return 0.0001
        funding_rate = avg_diff / max(base_vol, 1e-9) * 0.001
        funding_rate = max(-0.001, min(0.001, funding_rate))
        return funding_rate

    def _detect_vol_regime(self, closes: List[float], highs: List[float],
                            lows: List[float]) -> str:
        if len(closes) < 20:
            return "normal"
        atr_current = self._estimate_atr(closes[-10:], highs[-10:], lows[-10:])
        atr_history = self._estimate_atr(closes, highs, lows)
        if atr_history <= 0:
            return "normal"
        ratio = atr_current / atr_history
        if ratio > 1.5:
            return "high"
        elif ratio < 0.5:
            return "low"
        return "normal"

    def _detect_trend(self, closes: List[float]) -> str:
        if len(closes) < 20:
            return "neutral"
        ma20 = sum(closes[-20:]) / 20
        ma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else ma20
        if ma20 > ma50 * 1.01:
            return "uptrend"
        elif ma20 < ma50 * 0.99:
            return "downtrend"
        return "neutral"

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
