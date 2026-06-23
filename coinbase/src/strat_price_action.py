from __future__ import annotations
import math
from dataclasses import dataclass
from typing import List, Optional, Tuple
from collections import deque

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


@dataclass
class SwingPoint:
    index: int
    price: float
    kind: str  # "high" or "low"
    strength: int = 1


@dataclass
class SupportResistanceLevel:
    price: float
    kind: str  # "support" or "resistance"
    touches: int = 1
    strength: float = 1.0


class PriceActionSRStrategy(BaseStrategy):
    def __init__(self, swing_lookback: int = 10, min_touches: int = 2,
                 sr_window: int = 50):
        self.swing_lookback = swing_lookback
        self.min_touches = min_touches
        self.sr_window = sr_window
        self._name = "price_action_sr"

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        if len(bars) < 30:
            return None
        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]

        swings = self._detect_swing_points(highs, lows)
        levels = self._build_sr_levels(swings, closes)
        atr = self._estimate_atr(closes, highs, lows)

        near_support = self._nearest_level(bar.close, levels, "support", atr)
        near_resistance = self._nearest_level(bar.close, levels, "resistance", atr)

        setup = self._check_break_retest(bars, swings, levels, atr)
        if setup:
            return setup

        setup = self._check_bounce(bar, history, near_support, near_resistance, atr)
        if setup:
            return setup

        setup = self._check_trendline_break(bars, swings, atr)
        if setup:
            return setup

        return None

    def _detect_swing_points(self, highs: List[float], lows: List[float],
                              lookback: Optional[int] = None) -> List[SwingPoint]:
        lb = lookback or self.swing_lookback
        swings = []
        for i in range(lb, len(highs) - lb):
            if all(highs[i] > highs[j] for j in range(i - lb, i)) and \
               all(highs[i] > highs[j] for j in range(i + 1, i + lb + 1)):
                strength = sum(1 for j in range(i - lb, i + lb + 1)
                               if highs[i] >= highs[j])
                swings.append(SwingPoint(i, highs[i], "high", strength))
            if all(lows[i] < lows[j] for j in range(i - lb, i)) and \
               all(lows[i] < lows[j] for j in range(i + 1, i + lb + 1)):
                strength = sum(1 for j in range(i - lb, i + lb + 1)
                               if lows[i] <= lows[j])
                swings.append(SwingPoint(i, lows[i], "low", strength))
        return swings

    def _build_sr_levels(self, swings: List[SwingPoint],
                          closes: List[float]) -> List[SupportResistanceLevel]:
        levels = []
        tolerance = (max(closes[-20:]) - min(closes[-20:])) / max(closes[-20:], 1e-9) * 0.02

        grouped: List[Tuple[float, str, List[SwingPoint]]] = []
        for sw in swings:
            merged = False
            for i, (price, kind, group) in enumerate(grouped):
                if kind == sw.kind and abs(price - sw.price) / max(price, 1e-9) < tolerance:
                    group.append(sw)
                    new_price = sum(s.price for s in group) / len(group)
                    grouped[i] = (new_price, kind, group)
                    merged = True
                    break
            if not merged:
                grouped.append((sw.price, sw.kind, [sw]))

        for price, kind, group in grouped:
            touches = len(group)
            if touches >= self.min_touches:
                avg_strength = sum(s.strength for s in group) / len(group)
                levels.append(SupportResistanceLevel(
                    price=price,
                    kind="support" if kind == "low" else "resistance",
                    touches=touches,
                    strength=avg_strength * touches,
                ))

        return levels

    def _check_break_retest(self, bars: List[Bar], swings: List[SwingPoint],
                             levels: List[SupportResistanceLevel],
                             atr: float) -> Optional[BracketSetup]:
        if len(bars) < 5 or not levels:
            return None
        current_close = bars[-1].close
        for lvl in levels:
            dist_pct = abs(current_close - lvl.price) / max(lvl.price, 1e-9)
            if dist_pct > atr / max(current_close, 1e-9) * 3:
                continue
            prev_close = bars[-2].close
            prev_prev_close = bars[-3].close if len(bars) > 2 else prev_close

            if lvl.kind == "resistance":
                if prev_close < lvl.price and current_close > lvl.price:
                    retest = any(abs(b.close - lvl.price) / max(lvl.price, 1e-9) < 0.005
                                 for b in bars[-3:])
                    if retest:
                        direction = Direction.LONG
                        stop = lvl.price - atr * 1.5
                        target = current_close + atr * 3.0
                    else:
                        continue
                elif prev_close > lvl.price and current_close < lvl.price:
                    retest = any(abs(b.close - lvl.price) / max(lvl.price, 1e-9) < 0.005
                                 for b in bars[-3:])
                    if retest:
                        direction = Direction.SHORT
                        stop = lvl.price + atr * 1.5
                        target = current_close - atr * 3.0
                    else:
                        continue
                else:
                    continue
            elif lvl.kind == "support":
                if prev_close > lvl.price and current_close < lvl.price:
                    retest = any(abs(b.close - lvl.price) / max(lvl.price, 1e-9) < 0.005
                                 for b in bars[-3:])
                    if retest:
                        direction = Direction.SHORT
                        stop = lvl.price + atr * 1.5
                        target = current_close - atr * 3.0
                    else:
                        continue
                elif prev_close < lvl.price and current_close > lvl.price:
                    retest = any(abs(b.close - lvl.price) / max(lvl.price, 1e-9) < 0.005
                                 for b in bars[-3:])
                    if retest:
                        direction = Direction.LONG
                        stop = lvl.price - atr * 1.5
                        target = current_close + atr * 3.0
                    else:
                        continue
                else:
                    continue
            else:
                continue

            rr = abs(target - current_close) / max(abs(current_close - stop), 1e-9)
            if rr < 1.2:
                continue
            return BracketSetup(
                direction=direction, entry_price=current_close,
                stop_price=stop, target_price=target,
                risk_reward=rr, confidence=min(0.7, lvl.strength * 0.1),
                reason=f"SR: {lvl.kind} break-retest ({lvl.touches}t)",
                strategy_name=self._name, atr=atr,
            )
        return None

    def _check_bounce(self, bar: Bar, history: List[Bar],
                       near_support: Optional[Tuple[float, float]],
                       near_resistance: Optional[Tuple[float, float]],
                       atr: float) -> Optional[BracketSetup]:
        closes = [b.close for b in history] + [bar.close]
        if len(closes) < 14:
            return None
        rsi = self._rsi(closes, 14)

        if near_support and rsi < 40:
            price, strength = near_support
            entry = bar.close
            stop = price - atr * 1.2
            target = entry + atr * 2.5
            rr = abs(target - entry) / max(abs(entry - stop), 1e-9)
            if rr >= 1.5:
                return BracketSetup(
                    direction=Direction.LONG, entry_price=entry,
                    stop_price=stop, target_price=target,
                    risk_reward=rr, confidence=min(0.65, strength * 0.08),
                    reason=f"SR: support bounce @{price:.2f}",
                    strategy_name=self._name, atr=atr,
                )

        if near_resistance and rsi > 60:
            price, strength = near_resistance
            entry = bar.close
            stop = price + atr * 1.2
            target = entry - atr * 2.5
            rr = abs(target - entry) / max(abs(entry - stop), 1e-9)
            if rr >= 1.5:
                return BracketSetup(
                    direction=Direction.SHORT, entry_price=entry,
                    stop_price=stop, target_price=target,
                    risk_reward=rr, confidence=min(0.65, strength * 0.08),
                    reason=f"SR: resistance reject @{price:.2f}",
                    strategy_name=self._name, atr=atr,
                )
        return None

    def _check_trendline_break(self, bars: List[Bar], swings: List[SwingPoint],
                                atr: float) -> Optional[BracketSetup]:
        if len(swings) < 4 or len(bars) < 20:
            return None
        highs_sw = [s for s in swings if s.kind == "high"][-3:]
        lows_sw = [s for s in swings if s.kind == "low"][-3:]
        current_close = bars[-1].close

        if len(highs_sw) >= 2:
            slope = (highs_sw[-1].price - highs_sw[-2].price) / max(highs_sw[-1].index - highs_sw[-2].index, 1)
            trendline_price = highs_sw[-1].price + slope * (len(bars) - 1 - highs_sw[-1].index)
            if slope < 0 and current_close > trendline_price:
                dist_pct = (current_close - trendline_price) / max(current_close, 1e-9)
                if dist_pct < atr / max(current_close, 1e-9) * 2:
                    direction = Direction.LONG
                    stop = min(s.price for s in lows_sw[-2:]) if lows_sw else current_close * 0.97
                    target = current_close + atr * 3.0
                    rr = abs(target - current_close) / max(abs(current_close - stop), 1e-9)
                    if rr >= 1.5:
                        return BracketSetup(
                            direction=direction, entry_price=current_close,
                            stop_price=stop, target_price=target,
                            risk_reward=rr, confidence=0.5,
                            reason="SR: downtrend line break",
                            strategy_name=self._name, atr=atr,
                        )

        if len(lows_sw) >= 2:
            slope = (lows_sw[-1].price - lows_sw[-2].price) / max(lows_sw[-1].index - lows_sw[-2].index, 1)
            trendline_price = lows_sw[-1].price + slope * (len(bars) - 1 - lows_sw[-1].index)
            if slope > 0 and current_close < trendline_price:
                dist_pct = (trendline_price - current_close) / max(current_close, 1e-9)
                if dist_pct < atr / max(current_close, 1e-9) * 2:
                    direction = Direction.SHORT
                    stop = max(s.price for s in highs_sw[-2:]) if highs_sw else current_close * 1.03
                    target = current_close - atr * 3.0
                    rr = abs(target - current_close) / max(abs(current_close - stop), 1e-9)
                    if rr >= 1.5:
                        return BracketSetup(
                            direction=direction, entry_price=current_close,
                            stop_price=stop, target_price=target,
                            risk_reward=rr, confidence=0.5,
                            reason="SR: uptrend line break",
                            strategy_name=self._name, atr=atr,
                        )
        return None

    @staticmethod
    def _nearest_level(price: float, levels: List[SupportResistanceLevel],
                        kind: str, atr: float) -> Optional[Tuple[float, float]]:
        candidates = [l for l in levels if l.kind == kind]
        if not candidates:
            return None
        threshold = atr * 2.0
        nearest = min(candidates, key=lambda l: abs(l.price - price))
        if abs(nearest.price - price) < threshold:
            return (nearest.price, nearest.strength)
        return None

    @staticmethod
    def _rsi(closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        deltas = [closes[i] - closes[i - 1] for i in range(-period, 0)]
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
                     abs(highs[-i] - closes[-i - 1]),
                     abs(lows[-i] - closes[-i - 1]))
            tr_vals.append(tr)
        return sum(tr_vals) / len(tr_vals) if tr_vals else 0.0
