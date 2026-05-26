from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class Candle:
    product_id: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "product_id": self.product_id,
            "timestamp": self.timestamp.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trades": self.trades,
        }


class CandleAggregator:
    def __init__(self, product_id: str, window_seconds: int = 60) -> None:
        self.product_id = product_id
        self.window_seconds = window_seconds
        self._candles: list[Candle] = []
        self._current: Candle | None = None

    def _round_timestamp(self, ts: datetime) -> datetime:
        total_seconds = int(ts.timestamp())
        rounded = total_seconds - (total_seconds % self.window_seconds)
        return datetime.fromtimestamp(rounded, tz=timezone.utc)

    def ingest_trade(self, price: float, size: float, timestamp: datetime | None = None) -> Candle | None:
        ts = timestamp or datetime.now(timezone.utc)
        bucket_ts = self._round_timestamp(ts)

        if self._current is None or self._current.timestamp != bucket_ts:
            if self._current is not None:
                self._candles.append(self._current)
            self._current = Candle(
                product_id=self.product_id,
                timestamp=bucket_ts,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=size,
                trades=1,
            )
            return None

        self._current.high = max(self._current.high, price)
        self._current.low = min(self._current.low, price)
        self._current.close = price
        self._current.volume += size
        self._current.trades += 1
        return None

    def get_candles(self, count: int = 100) -> list[Candle]:
        result = list(self._candles)
        if self._current:
            result.append(self._current)
        return result[-count:]

    def clear(self) -> None:
        self._candles.clear()
        self._current = None
