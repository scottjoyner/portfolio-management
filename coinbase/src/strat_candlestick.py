from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .protocols import Direction, Bar, BracketSetup, BaseStrategy


@dataclass
class PatternDef:
    name: str
    direction: Direction
    min_confidence: float = 0.5
    requires_confirmation: bool = False


class CandlestickPatternStrategy(BaseStrategy):
    def __init__(self, min_body_ratio: float = 0.3,
                 max_wick_body_ratio: float = 2.0,
                 confirmation_bars: int = 1):
        self.min_body = min_body_ratio
        self.max_wick = max_wick_body_ratio
        self.confirmation_bars = confirmation_bars
        self._name = "candlestick"

    def name(self) -> str:
        return self._name

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        if len(bars) < 5:
            return None
        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        pattern = self._detect_pattern(bars)
        if pattern is None:
            return None

        name, direction, confidence = pattern
        current = bars[-1].close

        if direction == Direction.LONG:
            stop = min(b.low for b in bars[-3:]) if len(bars) >= 3 else current * 0.96
            target = current + atr * 2.5
        else:
            stop = max(b.high for b in bars[-3:]) if len(bars) >= 3 else current * 1.04
            target = current - atr * 2.5

        if direction == Direction.LONG:
            stop = min(stop, current - atr * 0.8)
        else:
            stop = max(stop, current + atr * 0.8)

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < 1.2:
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=min(0.75, confidence),
            reason=f"CP: {name}", strategy_name=self._name, atr=atr,
        )

    def _detect_pattern(self, bars: List[Bar]) -> Optional[Tuple[str, Direction, float]]:
        results = []

        single = self._check_single_patterns(bars[-1])
        if single:
            results.append(single)

        if len(bars) >= 2:
            two = self._check_two_bar_patterns(bars[-2], bars[-1])
            if two:
                results.append(two)

        if len(bars) >= 3:
            three = self._check_three_bar_patterns(bars[-3], bars[-2], bars[-1])
            if three:
                results.append(three)

        if not results:
            return None
        return max(results, key=lambda r: r[2])

    def _check_single_patterns(self, bar: Bar) -> Optional[Tuple[str, Direction, float]]:
        body = abs(bar.close - bar.open)
        upper_wick = bar.high - max(bar.open, bar.close)
        lower_wick = min(bar.open, bar.close) - bar.low
        total_range = bar.high - bar.low

        if total_range == 0:
            return None

        body_ratio = body / total_range
        upper_ratio = upper_wick / total_range
        lower_ratio = lower_wick / total_range

        if body_ratio < 0.1 and upper_wick > 0 and lower_wick > 0:
            prev_close = None
            return ("doji", Direction.LONG, 0.35) if bar.close > bar.open else ("doji", Direction.SHORT, 0.35)

        if body_ratio < self.min_body and upper_ratio > self.max_wick * body_ratio:
            if upper_ratio > 0.6:
                return ("shooting_star", Direction.SHORT, 0.55)
            return ("inverted_hammer", Direction.LONG, 0.45)

        if body_ratio < self.min_body and lower_ratio > self.max_wick * body_ratio:
            if lower_ratio > 0.6:
                return ("hammer", Direction.LONG, 0.55)
            return ("hanging_man", Direction.SHORT, 0.45)

        if body_ratio > 0.9:
            if bar.close > bar.open:
                return ("marubozu_bull", Direction.LONG, 0.50)
            return ("marubozu_bear", Direction.SHORT, 0.50)

        if body_ratio < 0.3 and upper_ratio < 0.1 and lower_ratio < 0.1:
            return ("spinning_top", Direction.LONG if bar.close > bar.open else Direction.SHORT, 0.30)

        return None

    def _check_two_bar_patterns(self, prev: Bar, curr: Bar) -> Optional[Tuple[str, Direction, float]]:
        prev_body = abs(prev.close - prev.open)
        curr_body = abs(curr.close - curr.open)
        prev_range = prev.high - prev.low
        curr_range = curr.high - curr.low

        if prev_range == 0 or curr_range == 0:
            return None

        prev_body_ratio = prev_body / prev_range
        curr_body_ratio = curr_body / curr_range

        if prev.close < prev.open and curr.close > curr.open:
            if curr.open < prev.close and curr.close > prev.open:
                return ("bullish_engulfing", Direction.LONG, 0.70)
            if curr.close > prev_range * 0.5 + prev.low:
                return ("piercing_line", Direction.LONG, 0.60)

        if prev.close > prev.open and curr.close < curr.open:
            if curr.open > prev.close and curr.close < prev.open:
                return ("bearish_engulfing", Direction.SHORT, 0.70)
            if curr.close < prev_range * 0.5 + prev.low:
                return ("dark_cloud_cover", Direction.SHORT, 0.60)

        if prev_body_ratio > 0.6:
            if curr_body_ratio < 0.2 and curr.close > prev.close:
                return ("bullish_harami", Direction.LONG, 0.50)
            if curr_body_ratio < 0.2 and curr.close < prev.close:
                return ("bearish_harami", Direction.SHORT, 0.50)

        return None

    def _check_three_bar_patterns(self, b1: Bar, b2: Bar, b3: Bar) -> Optional[Tuple[str, Direction, float]]:
        b1_body = abs(b1.close - b1.open)
        b2_body = abs(b2.close - b2.open)
        b3_body = abs(b3.close - b3.open)

        b1_bear = b1.close < b1.open
        b3_bull = b3.close > b3.open
        b1_bull = b1.close > b1.open
        b3_bear = b3.close < b3.open

        if b1_bear and b3_bull and b2_body < b1_body and b2_body < b3_body:
            if b2.low < b1.low and b2.low < b3.low:
                return ("morning_star", Direction.LONG, 0.75)

        if b1_bull and b3_bear and b2_body < b1_body and b2_body < b3_body:
            if b2.high > b1.high and b2.high > b3.high:
                return ("evening_star", Direction.SHORT, 0.75)

        if b1_bear and b3_bull:
            if b3.close > b1.open and b3.low > b1.low:
                return ("three_bar_reversal_bull", Direction.LONG, 0.55)

        if b1_bull and b3_bear:
            if b3.close < b1.open and b3.high < b1.high:
                return ("three_bar_reversal_bear", Direction.SHORT, 0.55)

        up_count = sum(1 for b in [b1, b2, b3] if b.close > b.open)
        if up_count == 3:
            bodies = [abs(b.close - b.open) for b in [b1, b2, b3]]
            if all(bodies[i] >= bodies[i-1] * 0.8 for i in range(1, 3)):
                return ("three_white_soldiers", Direction.LONG, 0.65)

        down_count = sum(1 for b in [b1, b2, b3] if b.close < b.open)
        if down_count == 3:
            bodies = [abs(b.close - b.open) for b in [b1, b2, b3]]
            if all(bodies[i] >= bodies[i-1] * 0.8 for i in range(1, 3)):
                return ("three_black_crows", Direction.SHORT, 0.65)

        return None

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
