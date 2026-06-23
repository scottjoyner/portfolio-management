from __future__ import annotations
import math
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .protocols import Direction, Bar, BracketSetup, BaseStrategy, InstrumentType


class GridTradingStrategy(BaseStrategy):
    def __init__(self, grid_levels: int = 5, grid_spread_bps: float = 50.0,
                 range_bars: int = 20, max_positions: int = 3):
        self.grid_levels = grid_levels
        self.grid_spread = grid_spread_bps
        self.range_bars = range_bars
        self.max_positions = max_positions
        self._name = "grid_trade"
        self._grid_center: Optional[float] = None
        self._active_grids: List[Dict] = []

    def name(self) -> str:
        return self._name

    def set_product_id(self, product_id: str):
        self._current_pid = product_id

    def on_bar(self, bar: Bar, history: List[Bar]) -> Optional[BracketSetup]:
        bars = history + [bar]
        if len(bars) < self.range_bars:
            return None

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        atr = self._estimate_atr(closes, highs, lows)
        if atr <= 0:
            return None

        if self._grid_center is None:
            self._grid_center = closes[-1]

        range_high = max(highs[-self.range_bars:])
        range_low = min(lows[-self.range_bars:])
        range_pct = (range_high - range_low) / max(range_low, 1e-9)

        if range_pct > atr / max(closes[-1], 1e-9) * 6:
            self._grid_center = closes[-1]

        grid_step = self._grid_center * (self.grid_spread / 10000)
        current = closes[-1]

        dist_from_center = abs(current - self._grid_center) / max(self._grid_center, 1e-9)
        if dist_from_center > atr / max(closes[-1], 1e-9) * 4:
            return None

        rsi_val = self._rsi(closes, 14)
        avg_vol = sum(b.volume for b in bars[-10:]) / 10
        vol_spike = bar.volume > avg_vol * 3.0
        if vol_spike:
            return None

        grid_floor = self._grid_center - grid_step * self.grid_levels
        grid_ceil = self._grid_center + grid_step * self.grid_levels

        if current < grid_floor + grid_step * 0.5:
            direction = Direction.LONG
            stop = current - atr * 1.5
            target = self._grid_center
            reason = f"GRID: buy near floor ({current:.2f} < {grid_floor:.2f})"
        elif current > grid_ceil - grid_step * 0.5:
            direction = Direction.SHORT
            stop = current + atr * 1.5
            target = self._grid_center
            reason = f"GRID: sell near ceiling ({current:.2f} > {grid_ceil:.2f})"
        elif rsi_val < 30:
            direction = Direction.LONG
            stop = current - atr * 1.2
            target = min(current + atr * 2.0, grid_ceil)
            reason = f"GRID: oversold bounce rsi={rsi_val:.0f}"
        elif rsi_val > 70:
            direction = Direction.SHORT
            stop = current + atr * 1.2
            target = max(current - atr * 2.0, grid_floor)
            reason = f"GRID: overbought reject rsi={rsi_val:.0f}"
        else:
            for level in range(1, self.grid_levels + 1):
                buy_level = self._grid_center - grid_step * level
                sell_level = self._grid_center + grid_step * level
                if current <= buy_level + grid_step * 0.3:
                    direction = Direction.LONG
                    stop = current - atr * 1.0
                    target = buy_level + grid_step
                    reason = f"GRID: level {level} buy @ {buy_level:.2f}"
                    break
                elif current >= sell_level - grid_step * 0.3:
                    direction = Direction.SHORT
                    stop = current + atr * 1.0
                    target = sell_level - grid_step
                    reason = f"GRID: level {level} sell @ {sell_level:.2f}"
                    break
            else:
                return None

        rr = abs(target - current) / max(abs(current - stop), 1e-9)
        if rr < 1.0:
            return None

        return BracketSetup(
            direction=direction, entry_price=current,
            stop_price=round(stop, 4), target_price=round(target, 4),
            risk_reward=round(rr, 2), confidence=0.35,
            reason=reason, strategy_name=self._name, atr=atr,
            metadata={
                "grid_center": round(self._grid_center, 2),
                "grid_step": round(grid_step, 2),
                "grid_floor": round(grid_floor, 2),
                "grid_ceil": round(grid_ceil, 2),
            },
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
