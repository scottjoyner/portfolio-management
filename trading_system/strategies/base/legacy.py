"""Legacy strategy building blocks retained under an unambiguous module name."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class OHLCVBar:
    """OHLCV bar with timestamp."""

    timestamp: int
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None

    def __post_init__(self) -> None:
        if self.open is not None:
            assert (self.high or 0) >= (self.low or 0), "high must be >= low"


def compute_sma(data: list[OHLCVBar], period: int) -> list[float]:
    """Compute a simple moving average."""

    if not data or len(data) < period:
        return []
    sma = []
    for i in range(len(data)):
        start_idx = max(0, i - period + 1)
        window = data[start_idx:i + 1]
        prices = [bar.close for bar in window if bar.close is not None]
        sma.append(sum(prices) / len(prices) if prices else None)
    return sma


def compute_ema(data: list[OHLCVBar], period: int) -> list[float]:
    """Compute an exponential moving average."""

    if not data or len(data) < period:
        return []
    window = data[:min(period, len(data))]
    first_sma = sum(bar.close for bar in window if bar.close is not None) / max(len(window), 1)
    ema = [first_sma]
    multiplier = 2 / (period + 1)
    for bar in data[period:]:
        last_ema = ema[-1]
        price = bar.close or last_ema
        ema.append((price - last_ema) * multiplier + last_ema)
    return ema


def compute_z_score(data: list[float]) -> list[float]:
    """Compute z-scores from price data."""

    if not data or len(data) < 2:
        return []
    mean = sum(data) / len(data)
    variance = sum((value - mean) ** 2 for value in data) / len(data)
    std = math.sqrt(variance)
    if std == 0:
        return [0.0] * len(data)
    return [(value - mean) / std for value in data]


class BaseStrategy:
    """Backward-compatible strategy base used by older strategy modules."""

    def setup(self, ohlcv_data: list[OHLCVBar]) -> None:
        pass

    def on_bar(self, bar: OHLCVBar) -> tuple[Optional[bool], Optional[float]]:
        return None, None

    def is_position_open(self) -> bool:
        return getattr(self, "_position_size", 0) != 0

    def close_position(self) -> None:
        self._position_size = 0


__all__ = [
    "BaseStrategy",
    "OHLCVBar",
    "compute_sma",
    "compute_ema",
    "compute_z_score",
]
