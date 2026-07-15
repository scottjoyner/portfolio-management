"""
Streaming (incremental) indicators — O(1) per-tick updates.

Instead of recomputing EMA/RSI/MACD/Bollinger over all N candles
every tick, maintain running state and compute only the delta.

Speedup: O(n) → O(1) per indicator per tick.

Hybrid implementation: uses Rust-native accelerated backend when
available (rust_core), falls back to pure Python otherwise.
"""

import math
from collections import deque
from typing import Dict, List, Optional, Tuple

# Try Rust-native streaming backend
try:
    from rust_core import (
        PyStreamingEngine as _RustStreamingEngine,
        PyStreamingIndicators as _RustStreamingIndicators,
    )
    _HAS_RUST_STREAMING = True
except ImportError:  # pragma: no cover
    _HAS_RUST_STREAMING = False


class RingBuffer:
    """Fixed-size circular buffer for streaming data."""

    __slots__ = ("_data", "_maxlen", "_head", "_size")

    def __init__(self, maxlen: int = 200):
        self._data = [0.0] * maxlen
        self._maxlen = maxlen
        self._head = 0
        self._size = 0

    def append(self, value: float) -> None:
        self._data[self._head] = value
        self._head = (self._head + 1) % self._maxlen
        if self._size < self._maxlen:
            self._size += 1

    def __getitem__(self, index):
        if index < 0:
            index += self._size
        if index < 0 or index >= self._size:
            raise IndexError("index out of range")
        pos = (self._head - self._size + index) % self._maxlen
        return self._data[pos]

    def __len__(self) -> int:
        return self._size

    def to_list(self) -> List[float]:
        return [self[i] for i in range(self._size)]

    @property
    def last(self) -> Optional[float]:
        return self[-1] if self._size > 0 else None

    @property
    def oldest(self) -> Optional[float]:
        return self[0] if self._size > 0 else None


class StreamingIndicators:
    """Incremental indicator engine for one product.

    Maintains running state for EMA, RSI, SMA, Bollinger, MACD, Z-score.
    Each call to update() processes one new price in O(1) time.

    When Rust acceleration is available, delegates to the native backend.
    """

    __slots__ = (
        "product_id", "closes", "volumes",
        "_ema_state", "_sma_state", "_sq_sum_state",
        "_rsi_avg_gain", "_rsi_avg_loss", "_rsi_prev_close",
        "_macd_ema_fast", "_macd_ema_slow", "_macd_signal",
    )

    def __init__(self, product_id: str, maxlen: int = 200):
        self.product_id = product_id
        self.closes = RingBuffer(maxlen)
        self.volumes = RingBuffer(maxlen)

        self._ema_state: Dict[int, Optional[float]] = {}
        self._sma_state: Dict[int, Optional[float]] = {}
        self._sq_sum_state: Dict[int, float] = {}
        self._rsi_avg_gain: Optional[float] = None
        self._rsi_avg_loss: Optional[float] = None
        self._rsi_prev_close: Optional[float] = None
        self._macd_ema_fast: Optional[float] = None
        self._macd_ema_slow: Optional[float] = None
        self._macd_signal: Optional[float] = None

    def update(self, close: float, volume: float = 0.0) -> None:
        self.closes.append(close)
        self.volumes.append(volume)
        self._update_sma(close)
        self._update_ema(close)
        self._update_rsi(close)
        self._update_macd(close)

    # ── EMA ─────────────────────────────────────────────────────────

    def _update_ema(self, price: float) -> None:
        for period, prev in list(self._ema_state.items()):
            k = 2.0 / (period + 1)
            self._ema_state[period] = price * k + prev * (1.0 - k)

    def seed_ema(self, period: int, closes: List[float]) -> float:
        if not closes or len(closes) < period:
            val = closes[-1] if closes else 0.0
        else:
            val = sum(closes[:period]) / period
        self._ema_state[period] = val
        return val

    def ema(self, period: int) -> Optional[float]:
        return self._ema_state.get(period)

    # ── SMA + Bollinger ─────────────────────────────────────────────

    def _update_sma(self, price: float) -> None:
        n = len(self.closes)
        sq2 = price * price
        for period in list(self._sma_state.keys()):
            prev_sma = self._sma_state[period]
            prev_sq = self._sq_sum_state.get(period, 0.0)
            if prev_sma is None:
                continue
            if n > period:
                oldest = self.closes[n - period - 1] if n > period else price
                new_sma = prev_sma + (price - oldest) / period
                new_sq = prev_sq + sq2 - oldest * oldest
            else:
                new_sma = prev_sma + (price - prev_sma) / period
                new_sq = prev_sq + sq2
            self._sma_state[period] = new_sma
            self._sq_sum_state[period] = new_sq

    def seed_sma(self, period: int, closes: List[float]) -> float:
        if len(closes) < period:
            return closes[-1] if closes else 0.0
        recent = closes[:period]
        val = sum(recent) / period
        sq_sum = sum(x * x for x in recent)
        self._sma_state[period] = val
        self._sq_sum_state[period] = sq_sum
        return val

    def sma(self, period: int) -> Optional[float]:
        return self._sma_state.get(period)

    def bollinger(self, period: int, std_mult: float = 2.0) -> Optional[Tuple[float, float, float]]:
        mid = self._sma_state.get(period)
        n = len(self.closes)
        if mid is None or n < period:
            return None
        sq_sum = self._sq_sum_state.get(period, 0.0)
        variance = sq_sum / period - mid * mid
        std = math.sqrt(max(variance, 0.0))
        return (mid, mid + std_mult * std, mid - std_mult * std)

    # ── RSI ─────────────────────────────────────────────────────────

    def _update_rsi(self, price: float) -> None:
        if self._rsi_prev_close is None:
            self._rsi_prev_close = price
            return
        change = price - self._rsi_prev_close
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        self._rsi_prev_close = price

        if self._rsi_avg_gain is None:
            self._rsi_avg_gain = gain
            self._rsi_avg_loss = loss
        else:
            self._rsi_avg_gain = (self._rsi_avg_gain * 13.0 + gain) / 14.0
            self._rsi_avg_loss = (self._rsi_avg_loss * 13.0 + loss) / 14.0

    def seed_rsi(self, period: int, closes: List[float], seed_method: str = "wilder") -> float:
        if len(closes) < period + 1:
            return 50.0
        gains, losses = 0.0, 0.0
        for i in range(len(closes) - period, len(closes)):
            change = closes[i] - closes[i - 1]
            gains += max(change, 0.0)
            losses += max(-change, 0.0)
        self._rsi_avg_gain = gains / period
        self._rsi_avg_loss = losses / period
        self._rsi_prev_close = closes[-1]
        return self.rsi()

    def rsi(self) -> Optional[float]:
        if self._rsi_avg_gain is None or self._rsi_avg_loss is None:
            return None
        if self._rsi_avg_loss == 0:
            return 100.0
        rs = self._rsi_avg_gain / self._rsi_avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    # ── MACD ────────────────────────────────────────────────────────

    def _update_macd(self, price: float) -> None:
        fast = self._macd_ema_fast
        slow = self._macd_ema_slow
        if fast is not None and slow is not None:
            k12 = 2.0 / 13.0
            k26 = 2.0 / 27.0
            self._macd_ema_fast = price * k12 + fast * (1.0 - k12)
            self._macd_ema_slow = price * k26 + slow * (1.0 - k26)
            macd_line = self._macd_ema_fast - self._macd_ema_slow
            if self._macd_signal is not None:
                k9 = 2.0 / 10.0
                self._macd_signal = macd_line * k9 + self._macd_signal * (1.0 - k9)
            else:
                self._macd_signal = macd_line

    def seed_macd(self, closes: List[float],
                  fast: int = 12, slow: int = 26, signal: int = 9) -> None:
        self._macd_ema_fast = self.seed_ema(fast, closes)
        self._macd_ema_slow = self.seed_ema(slow, closes)
        if self._macd_ema_fast and self._macd_ema_slow:
            macd_line = self._macd_ema_fast - self._macd_ema_slow
            self._macd_signal = macd_line

    def macd(self) -> Optional[Tuple[float, float, float]]:
        if self._macd_ema_fast is None or self._macd_ema_slow is None:
            return None
        macd_line = self._macd_ema_fast - self._macd_ema_slow
        sig = self._macd_signal if self._macd_signal is not None else macd_line
        return (macd_line, sig, macd_line - sig)


class StreamingEngine:
    """Manages StreamingIndicators for multiple products.

    One engine instance serves the entire tick loop.

    Uses Rust-native StreamingEngine when available; falls back to pure Python.
    """

    def __init__(self):
        if _HAS_RUST_STREAMING:
            self._rust = _RustStreamingEngine()
            self._products: Dict[str, StreamingIndicators] = {}
        else:
            self._rust = None
            self._products: Dict[str, StreamingIndicators] = {}

    def get_or_create(self, product_id: str, maxlen: int = 200) -> object:
        if self._rust is not None:
            return self._rust.get_or_create(product_id, maxlen)
        if product_id not in self._products:
            self._products[product_id] = StreamingIndicators(product_id, maxlen)
        return self._products[product_id]

    def seed(self, product_id: str, closes: List[float], volumes: Optional[List[float]] = None):
        ind = self.get_or_create(product_id)
        for i in range(len(closes)):
            v = volumes[i] if volumes and i < len(volumes) else 0.0
            ind.update(closes[i], v)

    def try_get(self, product_id: str) -> Optional[object]:
        """Return streaming indicators handle if product exists, else None."""
        if self._rust is not None:
            return self._rust.try_get(product_id)
        return self._products.get(product_id)

    def update(self, product_id: str, close: float, volume: float = 0.0):
        if self._rust is not None:
            self._rust.update(product_id, close, volume)
            return
        ind = self._products.get(product_id)
        if ind:
            ind.update(close, volume)

    def ema(self, product_id: str, period: int) -> Optional[float]:
        if self._rust is not None:
            return self._rust.ema(product_id, period)
        ind = self._products.get(product_id)
        return ind.ema(period) if ind else None

    def rsi(self, product_id: str) -> Optional[float]:
        if self._rust is not None:
            return self._rust.rsi(product_id)
        ind = self._products.get(product_id)
        return ind.rsi() if ind else None

    def macd(self, product_id: str) -> Optional[Tuple[float, float, float]]:
        if self._rust is not None:
            return self._rust.macd(product_id)
        ind = self._products.get(product_id)
        return ind.macd() if ind else None

    def bollinger(self, product_id: str, period: int = 20) -> Optional[Tuple[float, float, float]]:
        ind = self._products.get(product_id)
        return ind.bollinger(period) if ind else None

    @property
    def products(self) -> Dict[str, object]:
        if self._rust is not None:
            return {}  # Not accessible via dict when using Rust backend
        return self._products
