from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Optional, Dict
from enum import Enum

from .protocols import Direction, Bar, BracketSetup, BaseStrategy, InstrumentType


class TradingMode(Enum):
    SCALP = "scalp"
    SWING = "swing"
    TREND = "trend"
    HOLD = "hold"


MODE_PROFILES = {
    TradingMode.SCALP: {
        "stop_atr": 0.8, "target_atr": 1.2, "min_rr": 1.0,
        "max_holding_bars": 5, "max_positions_per_product": 3,
        "description": "Quick flips, tight stops, high volume",
    },
    TradingMode.SWING: {
        "stop_atr": 1.5, "target_atr": 2.5, "min_rr": 1.5,
        "max_holding_bars": 48, "max_positions_per_product": 2,
        "description": "Multi-bar holds, moderate stops, balanced",
    },
    TradingMode.TREND: {
        "stop_atr": 2.5, "target_atr": 5.0, "min_rr": 2.0,
        "max_holding_bars": 500, "max_positions_per_product": 1,
        "description": "Trend following, wide stops, low volume per trade",
    },
    TradingMode.HOLD: {
        "stop_atr": 5.0, "target_atr": 10.0, "min_rr": 1.5,
        "max_holding_bars": 5000, "max_positions_per_product": 1,
        "description": "Accumulation regime, very wide stops",
    },
}


class AdaptiveModeSelector:
    def __init__(self, switch_cooldown_bars: int = 24):
        self.current_mode = TradingMode.SWING
        self._mode_history: List[TradingMode] = []
        self._bars_since_switch = 0
        self._cooldown = switch_cooldown_bars
        self._last_fg: float = 50.0
        self._last_regime: str = "unknown"

    def update(self, regime: str, volatility_bps: float,
               fear_greed_value: float = 50.0,
               adx: float = 25.0, trend_strength: float = 0.0):
        self._bars_since_switch += 1
        self._last_fg = fear_greed_value
        self._last_regime = regime

        if self._bars_since_switch < self._cooldown:
            return self.current_mode

        new_mode = self._select_mode(regime, volatility_bps, fear_greed_value, adx, trend_strength)
        if new_mode != self.current_mode:
            self._mode_history.append(self.current_mode)
            if len(self._mode_history) > 10:
                self._mode_history = self._mode_history[-10:]
            self.current_mode = new_mode
            self._bars_since_switch = 0

        return self.current_mode

    def _select_mode(self, regime: str, vol_bps: float,
                      fg: float, adx: float, trend: float) -> TradingMode:
        if fg < 20 or fg > 80:
            return TradingMode.TREND
        if "uptrend" in regime or "downtrend" in regime:
            if vol_bps < 30:
                return TradingMode.TREND
            elif vol_bps < 80:
                return TradingMode.SWING
            else:
                return TradingMode.SCALP
        if "ranging" in regime or "low_vol" in regime:
            return TradingMode.SCALP
        if "high_vol" in regime:
            return TradingMode.SCALP
        if adx < 20:
            return TradingMode.SCALP
        if adx > 35:
            return TradingMode.TREND
        return TradingMode.SWING

    def profile(self) -> Dict:
        return MODE_PROFILES[self.current_mode]

    def summary(self) -> Dict:
        return {
            "mode": self.current_mode.value,
            "description": MODE_PROFILES[self.current_mode]["description"],
            "stop_atr": MODE_PROFILES[self.current_mode]["stop_atr"],
            "target_atr": MODE_PROFILES[self.current_mode]["target_atr"],
            "min_rr": MODE_PROFILES[self.current_mode]["min_rr"],
            "max_holding_bars": MODE_PROFILES[self.current_mode]["max_holding_bars"],
        }


class AdaptiveScalpSwingStrategy(BaseStrategy):
    def __init__(self, mode_selector: Optional[AdaptiveModeSelector] = None):
        self.mode_selector = mode_selector or AdaptiveModeSelector()
        self._name = "adaptive_mode"

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

        current = closes[-1]
        profile = self.mode_selector.profile()

        vol_bps = atr / max(current, 1e-9) * 10000
        rsi_val = self._rsi(closes, 14)
        avg_vol = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else bar.volume
        vol_spike = bar.volume > avg_vol * 2.0

        mode = self.mode_selector.current_mode

        if mode == TradingMode.SCALP:
            return self._scalp_signal(current, atr, rsi_val, vol_bps, vol_spike, profile)
        elif mode == TradingMode.SWING:
            return self._swing_signal(current, atr, rsi_val, vol_bps, profile, closes)
        elif mode == TradingMode.TREND:
            return self._trend_signal(current, atr, rsi_val, profile, closes)
        else:
            return None

    def _scalp_signal(self, current: float, atr: float, rsi: float,
                       vol_bps: float, vol_spike: bool,
                       profile: Dict) -> Optional[BracketSetup]:
        if vol_spike:
            return None
        if vol_bps > 100:
            return None
        if rsi < 30:
            direction = Direction.LONG
            stop = current - atr * profile["stop_atr"]
            target = current + atr * profile["target_atr"]
            conf = 0.4
            reason = f"SCALP: oversold rsi={rsi:.0f}"
        elif rsi > 70:
            direction = Direction.SHORT
            stop = current + atr * profile["stop_atr"]
            target = current - atr * profile["target_atr"]
            conf = 0.4
            reason = f"SCALP: overbought rsi={rsi:.0f}"
        else:
            return None

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < profile["min_rr"]:
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(conf, 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={"mode": "scalp", "rsi": round(rsi, 1)},
        )

    def _swing_signal(self, current: float, atr: float, rsi: float,
                       vol_bps: float, profile: Dict,
                       closes: List[float]) -> Optional[BracketSetup]:
        ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else current
        if current > ma20 and rsi < 45:
            direction = Direction.LONG
            stop = current - atr * profile["stop_atr"]
            target = current + atr * profile["target_atr"]
            conf = 0.5
            reason = f"SWING: pullback long rsi={rsi:.0f}"
        elif current < ma20 and rsi > 55:
            direction = Direction.SHORT
            stop = current + atr * profile["stop_atr"]
            target = current - atr * profile["target_atr"]
            conf = 0.5
            reason = f"SWING: rally short rsi={rsi:.0f}"
        else:
            if rsi < 35:
                direction = Direction.LONG
                stop = current - atr * profile["stop_atr"]
                target = current + atr * profile["target_atr"]
                conf = 0.45
                reason = f"SWING: oversold rsi={rsi:.0f}"
            elif rsi > 65:
                direction = Direction.SHORT
                stop = current + atr * profile["stop_atr"]
                target = current - atr * profile["target_atr"]
                conf = 0.45
                reason = f"SWING: overbought rsi={rsi:.0f}"
            else:
                return None

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < profile["min_rr"]:
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(conf, 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={"mode": "swing", "rsi": round(rsi, 1)},
        )

    def _trend_signal(self, current: float, atr: float, rsi: float,
                       profile: Dict, closes: List[float]) -> Optional[BracketSetup]:
        if len(closes) < 50:
            return None
        ma50 = sum(closes[-50:]) / 50
        ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else ma50
        trend = "up" if ma20 > ma50 else "down"

        if trend == "up":
            if rsi < 45:
                direction = Direction.LONG
                stop = ma50 - atr * 1.0
                target = current + atr * profile["target_atr"]
                conf = 0.55
                reason = f"TREND: uptrend pullback rsi={rsi:.0f}"
            else:
                return None
        else:
            if rsi > 55:
                direction = Direction.SHORT
                stop = ma50 + atr * 1.0
                target = current - atr * profile["target_atr"]
                conf = 0.55
                reason = f"TREND: downtrend rally rsi={rsi:.0f}"
            else:
                return None

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < profile["min_rr"]:
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=round(conf, 3),
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={"mode": "trend", "trend": trend, "rsi": round(rsi, 1)},
        )

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
