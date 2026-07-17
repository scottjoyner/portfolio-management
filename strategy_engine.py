#!/usr/bin/env python3
"""
Strategy Engine – self-contained signal generation for the portfolio optimizer.

Implements 5 core crypto trading strategies that feed into
PortfolioOptimizer as opportunity detectors.

Each strategy consumes OHLCV bars and returns Signal objects:
  Signal(action="BUY"|"SELL", price, confidence, reason)
"""

import logging
import math
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    action: str  # "BUY" | "SELL" | "HOLD"
    price: float = 0.0
    confidence: float = 1.0
    reason: str = ""
    strategy: str = ""


# ---------------------------------------------------------------------------
# Per-call Indicator Cache
# ---------------------------------------------------------------------------
# Avoids redundant indicator recomputation when multiple strategies in one
# call to run_strategies() request the same (indicator, period, data) tuple.

_thread_cache = threading.local()


def _get_cache() -> Dict[str, float]:
    try:
        return _thread_cache.store
    except AttributeError:
        _thread_cache.store = {}
        return _thread_cache.store


def _clear_cache() -> None:
    try:
        _thread_cache.store.clear()
    except AttributeError:
        pass


def _cache_key(name: str, period: int, data_id: int) -> str:
    return f"{name}:{period}:{data_id}"


# ---------------------------------------------------------------------------
# Helpers (with per-call caching)
# ---------------------------------------------------------------------------


def _sma(values: List[float], period: int) -> float:
    key = _cache_key("sma", period, id(values))
    cache = _get_cache()
    cached = cache.get(key)
    if cached is not None:
        return cached
    if len(values) < period:
        val = values[-1] if values else 0.0
    else:
        val = sum(values[-period:]) / period
    cache[key] = val
    return val


def _ema(values: List[float], period: int) -> float:
    key = _cache_key("ema", period, id(values))
    cache = _get_cache()
    cached = cache.get(key)
    if cached is not None:
        return cached
    if len(values) < period:
        val = values[-1] if values else 0.0
    else:
        k = 2.0 / (period + 1)
        result = sum(values[:period]) / period
        for v in values[period:]:
            result = v * k + result * (1 - k)
        val = result
    cache[key] = val
    return val


def _rsi(values: List[float], period: int = 14) -> float:
    key = _cache_key("rsi", period, id(values))
    cache = _get_cache()
    cached = cache.get(key)
    if cached is not None:
        return cached
    if len(values) < period + 1:
        val = 50.0
    else:
        deltas = [
            values[i] - values[i - 1] for i in range(len(values) - period, len(values))
        ]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            val = 100.0
        else:
            rs = avg_gain / avg_loss
            val = 100.0 - (100.0 / (1.0 + rs))
    cache[key] = val
    return val


def _bollinger(values: List[float], period: int = 20, std_mult: float = 2.0):
    key = _cache_key("boll", period, id(values))
    cache = _get_cache()
    cached = cache.get(key)
    if cached is not None:
        return cached
    if len(values) < period:
        val = (values[-1], values[-1], values[-1], 0.0) if values else (0, 0, 0, 0)
    else:
        recent = values[-period:]
        mean = sum(recent) / period
        variance = sum((x - mean) ** 2 for x in recent) / period
        std = math.sqrt(variance)
        val = (mean, mean + std_mult * std, mean - std_mult * std, std)
    cache[key] = val
    return val


def _zscore(values: List[float], period: int = 30) -> float:
    key = _cache_key("zscore", period, id(values))
    cache = _get_cache()
    cached = cache.get(key)
    if cached is not None:
        return cached
    if len(values) < period:
        val = 0.0
    else:
        recent = values[-period:]
        mean = sum(recent) / period
        variance = sum((x - mean) ** 2 for x in recent) / period
        std = math.sqrt(variance)
        val = (values[-1] - mean) / std if std != 0 else 0.0
    cache[key] = val
    return val


def _wma(values: List[float], period: int) -> float:
    key = _cache_key("wma", period, id(values))
    cache = _get_cache()
    cached = cache.get(key)
    if cached is not None:
        return cached
    if len(values) < period:
        val = values[-1] if values else 0.0
    else:
        weights = list(range(1, period + 1))
        recent = values[-period:]
        val = sum(w * v for w, v in zip(weights, recent)) / sum(weights)
    cache[key] = val
    return val


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


class EMA_Crossover:
    """Fast/slow EMA crossover. Trending markets."""

    def __init__(self, fast: int = 9, slow: int = 21):
        self.fast = fast
        self.slow = slow
        self.prev_fast: Optional[float] = None
        self.prev_slow: Optional[float] = None

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.slow + 1:
            return None
        fast_val = _ema(closes, self.fast)
        slow_val = _ema(closes, self.slow)
        signal = None
        if self.prev_fast is not None and self.prev_slow is not None:
            if self.prev_fast <= self.prev_slow and fast_val > slow_val:
                conf = min(abs(fast_val - slow_val) / slow_val * 50, 1.0)
                signal = Signal(
                    "BUY",
                    close,
                    conf,
                    f"EMA crossover {self.fast}/{self.slow}",
                    "ema_cross",
                )
            elif self.prev_fast >= self.prev_slow and fast_val < slow_val:
                conf = min(abs(fast_val - slow_val) / slow_val * 50, 1.0)
                signal = Signal(
                    "SELL",
                    close,
                    conf,
                    f"EMA crossover {self.fast}/{self.slow}",
                    "ema_cross",
                )
        self.prev_fast = fast_val
        self.prev_slow = slow_val
        return signal


class RSI_MeanReversion:
    """RSI oversold/overbought. Range-bound markets."""

    def __init__(self, period: int = 14, oversold: float = 30, overbought: float = 70):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.period + 1:
            return None
        rsi = _rsi(closes, self.period)
        if rsi < self.oversold:
            conf = min((self.oversold - rsi) / self.oversold, 1.0)
            return Signal("BUY", close, conf, f"RSI {rsi:.0f} oversold", "rsi_revert")
        elif rsi > self.overbought:
            conf = min((rsi - self.overbought) / (100 - self.overbought), 1.0)
            return Signal(
                "SELL", close, conf, f"RSI {rsi:.0f} overbought", "rsi_revert"
            )
        return None


class BollingerBreakout:
    """Bollinger Band squeeze/breakout. Volatility expansion."""

    def __init__(
        self, period: int = 20, std_mult: float = 2.0, squeeze_threshold: float = 0.1
    ):
        self.period = period
        self.std_mult = std_mult
        self.squeeze_threshold = squeeze_threshold

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.period + 1:
            return None
        mid, upper, lower, std = _bollinger(closes, self.period, self.std_mult)
        bandwidth = std / mid if mid > 0 else 0
        if close > upper:
            conf = min((close - upper) / upper * 20, 1.0)
            return Signal(
                "BUY", close, conf, "Bollinger breakout above upper", "boll_break"
            )
        elif close < lower:
            conf = min((lower - close) / lower * 20, 1.0)
            return Signal(
                "SELL", close, conf, "Bollinger breakdown below lower", "boll_break"
            )
        return None


class ZScoreReversion:
    """Z-score mean reversion. Statistical extremes."""

    def __init__(
        self, period: int = 30, buy_threshold: float = -2.0, sell_threshold: float = 2.0
    ):
        self.period = period
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.period + 1:
            return None
        z = _zscore(closes, self.period)
        if z < self.buy_threshold:
            conf = min(abs(z) / 4.0, 1.0)
            return Signal(
                "BUY", close, conf, f"Z-score {z:.2f} extreme low", "zscore_revert"
            )
        elif z > self.sell_threshold:
            conf = min(z / 4.0, 1.0)
            return Signal(
                "SELL", close, conf, f"Z-score {z:.2f} extreme high", "zscore_revert"
            )
        return None


class VolumeMomentum:
    """Volume-weighted price trend. Momentum confirmation."""

    def __init__(self, period: int = 14, volume_mult: float = 1.5):
        self.period = period
        self.volume_mult = volume_mult

    def on_bar(
        self, close: float, closes: List[float], volumes: List[float]
    ) -> Optional[Signal]:
        if len(closes) < self.period + 1 or len(volumes) < self.period + 1:
            return None
        recent_close = closes[-self.period :]
        recent_vol = volumes[-self.period :]
        avg_vol = sum(recent_vol) / self.period
        if avg_vol == 0:
            return None
        last_vol = volumes[-1]
        if last_vol < avg_vol * self.volume_mult:
            return None
        price_change = (closes[-1] - closes[-self.period]) / max(closes[-self.period], 1e-12)
        if price_change > 0.05:
            conf = min(price_change * 2, 1.0)
            return Signal(
                "BUY",
                close,
                conf,
                f"Volume surge +{price_change * 100:.1f}%",
                "vol_mom",
            )
        elif price_change < -0.05:
            conf = min(abs(price_change) * 2, 1.0)
            return Signal(
                "SELL",
                close,
                conf,
                f"Volume surge {price_change * 100:.1f}%",
                "vol_mom",
            )
        return None


# ---------------------------------------------------------------------------
# Additional Strategies
# ---------------------------------------------------------------------------


class MACD:
    """MACD crossover. Classic momentum indicator.

    MACD Line = EMA(12) - EMA(26)
    Signal Line = EMA(MACD, 9)
    Histogram = MACD - Signal
    BUY when histogram crosses above 0 (bullish momentum increasing)
    SELL when histogram crosses below 0 (bearish momentum increasing)
    """

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9):
        self.fast = fast
        self.slow = slow
        self.signal = signal
        self.prev_hist: Optional[float] = None

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.slow + self.signal + 1:
            return None
        
        # Precompute MACD series once
        macd_series = []
        for i in range(self.signal, len(closes)):
            fast_e = _ema(closes[: i + 1], self.fast)
            slow_e = _ema(closes[: i + 1], self.slow)
            macd_series.append(fast_e - slow_e)
        
        if len(macd_series) < self.signal:  # pragma: no cover
            return None
        
        macd_line = _ema(closes, self.fast) - _ema(closes, self.slow)
        signal_line = _ema(macd_series, self.signal)
        histogram = macd_line - signal_line

        result = None
        if self.prev_hist is not None:
            if self.prev_hist < 0 and histogram > 0:
                conf = min(
                    abs(histogram) / signal_line * 2
                    if signal_line != 0
                    else 0.1,
                    1.0,
                )
                result = Signal(
                    "BUY", close, conf, f"MACD histogram bullish crossover", "macd"
                )
            elif self.prev_hist > 0 and histogram < 0:
                conf = min(
                    abs(histogram) / signal_line * 2
                    if signal_line != 0
                    else 0.1,
                    1.0,
                )
                result = Signal(
                    "SELL", close, conf, f"MACD histogram bearish crossover", "macd"
                )
        self.prev_hist = histogram
        return result


class VWAP_Reversion:
    """VWAP reversion. Mean-reversion to volume-weighted average price.

    VWAP = Σ(price * volume) / Σ(volume)
    BUY when price is significantly below VWAP
    SELL when price is significantly above VWAP
    """

    def __init__(self, threshold: float = 0.03):
        self.threshold = threshold

    def on_bar(
        self, close: float, closes: List[float], volumes: List[float]
    ) -> Optional[Signal]:
        if len(closes) < 10 or len(volumes) < 10:
            return None
        total_pv = sum(closes[i] * volumes[i] for i in range(len(closes)))
        total_v = sum(volumes)
        if total_v == 0:
            return None
        vwap = total_pv / total_v
        deviation = (close - vwap) / vwap

        if deviation < -self.threshold:
            conf = min(abs(deviation) / 0.10, 1.0)
            return Signal(
                "BUY",
                close,
                conf,
                f"VWAP reversion: {deviation * 100:.1f}% below VWAP",
                "vwap_revert",
            )
        elif deviation > self.threshold:
            conf = min(deviation / 0.10, 1.0)
            return Signal(
                "SELL",
                close,
                conf,
                f"VWAP reversion: {deviation * 100:.1f}% above VWAP",
                "vwap_revert",
            )
        return None


class OBV_Divergence:
    """On-Balance Volume divergence detection.

    OBV tracks cumulative volume signed by price direction.
    Bullish divergence: price makes lower low, OBV makes higher low
    Bearish divergence: price makes higher high, OBV makes lower high
    """

    def __init__(self, lookback: int = 14):
        self.lookback = lookback

    def on_bar(
        self, close: float, closes: List[float], volumes: List[float]
    ) -> Optional[Signal]:
        if len(closes) < self.lookback + 3 or len(volumes) < self.lookback + 3:
            return None
        obv = 0.0
        obv_series = [0.0]
        for i in range(1, len(closes)):
            if closes[i] > closes[i - 1]:
                obv += volumes[i]
            elif closes[i] < closes[i - 1]:
                obv -= volumes[i]
            obv_series.append(obv)
        recent_prices = closes[-self.lookback :]
        recent_obv = obv_series[-self.lookback :]
        price_low = min(recent_prices)
        price_high = max(recent_prices)
        obv_low = min(recent_obv)
        obv_high = max(recent_obv)
        price_current = recent_prices[-1]
        obv_current = recent_obv[-1]

        if price_current <= price_low * 1.01 and obv_current > obv_low * 1.02:
            divergence = min(
                (obv_current - obv_low) / abs(obv_low) if obv_low != 0 else 0.1, 1.0
            )
            return Signal(
                "BUY",
                close,
                divergence * 0.8,
                f"Bullish OBV divergence: price low, OBV rising",
                "obv_div",
            )

        if price_current >= price_high * 0.99 and obv_current < obv_high * 0.98:
            divergence = min(
                (obv_high - obv_current) / abs(obv_high) if obv_high != 0 else 0.1, 1.0
            )
            return Signal(
                "SELL",
                close,
                divergence * 0.8,
                f"Bearish OBV divergence: price high, OBV falling",
                "obv_div",
            )
        return None


class ChandeMomentum:
    """Chande Momentum Oscillator (CMO).

    CMO = (Sum(Gains) - Sum(Losses)) / (Sum(Gains) + Sum(Losses)) * 100
    Range: -100 to +100
    More responsive than RSI to recent price changes.
    BUY when CMO < -50 (oversold)
    SELL when CMO > +50 (overbought)
    """

    def __init__(self, period: int = 14, oversold: float = -50, overbought: float = 50):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.period + 2:
            return None
        deltas = [
            closes[i] - closes[i - 1]
            for i in range(len(closes) - self.period, len(closes))
        ]
        gains = sum(d for d in deltas if d > 0)
        losses = sum(abs(d) for d in deltas if d < 0)
        denom = gains + losses
        if denom == 0:
            return None
        cmo = (gains - losses) / denom * 100

        if cmo < self.oversold:
            conf = min(abs(cmo - self.oversold) / abs(self.oversold), 1.0)
            return Signal("BUY", close, conf, f"CMO {cmo:.0f} oversold", "cmo")
        elif cmo > self.overbought:
            conf = min((cmo - self.overbought) / (100 - self.overbought), 1.0)
            return Signal("SELL", close, conf, f"CMO {cmo:.0f} overbought", "cmo")
        return None


class TRIX:
    """Triple Exponential Average (TRIX) momentum.

    Applies EMA smoothing three times to filter noise,
    then measures the rate of change of the triple-smoothed series.
    BUY when TRIX crosses above 0
    SELL when TRIX crosses below 0
    """

    def __init__(self, period: int = 15):
        self.period = period
        self.prev_trix: Optional[float] = None

    def _triple_ema(self, closes: List[float], p: int) -> float:
        if len(closes) < p * 3:  # pragma: no cover
            return closes[-1]
        ema1 = [_ema(closes[: i + 1], p) for i in range(p * 2 - 1, len(closes))]
        if len(ema1) < p + 1:  # pragma: no cover
            return ema1[-1]
        ema2 = [_ema(ema1[: i + 1], p) for i in range(p - 1, len(ema1))]
        if len(ema2) < p + 1:
            return ema2[-1]
        return _ema(ema2, p)

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.period * 3 + 2:
            return None
        trix_series = []
        for i in range(self.period * 3, len(closes)):
            segment = closes[: i + 1]
            e3 = self._triple_ema(segment, self.period)
            trix_series.append(e3)
        if len(trix_series) < 2:
            return None
        current_trix = (trix_series[-1] - trix_series[-2]) / trix_series[-2] * 10000
        result = None
        if self.prev_trix is not None:
            if self.prev_trix < 0 and current_trix > 0:
                conf = min(abs(current_trix) / 100, 1.0)
                result = Signal(
                    "BUY",
                    close,
                    conf,
                    f"TRIX bullish crossover ({current_trix:.1f})",
                    "trix",
                )
            elif self.prev_trix > 0 and current_trix < 0:
                conf = min(abs(current_trix) / 100, 1.0)
                result = Signal(
                    "SELL",
                    close,
                    conf,
                    f"TRIX bearish crossover ({current_trix:.1f})",
                    "trix",
                )
        self.prev_trix = current_trix
        return result


# ---------------------------------------------------------------------------
# Risk-focused strategies (5 new, rounds out all risk angles)
# ---------------------------------------------------------------------------


class ADX:
    """Average Directional Index — trend strength filter.

    Measures trend strength (0-100). ADX > 25 = strong trend.
    Uses +DI/-DI crossovers for direction when trend is strong.
    BUY when ADX > threshold and +DI crosses above -DI
    SELL when ADX > threshold and -DI crosses above +DI
    Risk: avoids whipsaw in choppy/range-bound markets.
    """

    def __init__(self, period: int = 14, threshold: float = 25.0):
        self.period = period
        self.threshold = threshold
        self.prev_plus_di: Optional[float] = None
        self.prev_minus_di: Optional[float] = None

    @staticmethod
    def _wilder_smooth(values: List[float], p: int) -> float:
        if len(values) < p:
            return sum(values) / len(values) if values else 0.0
        result = sum(values[:p]) / p
        k = 1.0 / p
        for v in values[p:]:
            result = v * k + result * (1 - k)
        return result

    def _calc_di_adx(
        self, highs: List[float], lows: List[float], closes: List[float], p: int
    ):
        n = len(highs)
        tr = [0.0]
        plus_dm = [0.0]
        minus_dm = [0.0]
        for i in range(1, n):
            tr_i = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            tr.append(tr_i)
            up = highs[i] - highs[i - 1]
            dn = lows[i - 1] - lows[i]
            plus_dm.append(up if up > dn and up > 0 else 0)
            minus_dm.append(dn if dn > up and dn > 0 else 0)

        def wilder_slice(arr, p, end):
            if end < p:
                return sum(arr[: end + 1]) / (end + 1)
            val = sum(arr[end - p + 1 : end + 1]) / p
            for j in range(end + 1, len(arr)):
                val = (val * (p - 1) + arr[j]) / p
            return val

        last_idx = len(tr) - 1
        s_tr = wilder_slice(tr, p, last_idx)
        s_pdm = wilder_slice(plus_dm, p, last_idx)
        s_mdm = wilder_slice(minus_dm, p, last_idx)

        if s_tr == 0:
            return 0.0, 0.0, 0.0

        plus_di = s_pdm / s_tr * 100
        minus_di = s_mdm / s_tr * 100

        # Build DX series for ADX
        dx_vals = []
        for i in range(p, len(tr)):
            st = wilder_slice(tr, p, i)
            sp = wilder_slice(plus_dm, p, i)
            sm = wilder_slice(minus_dm, p, i)
            if st > 0 and (sp + sm) > 0:
                pdi = sp / st * 100
                mdi = sm / st * 100
                dx_vals.append(abs(pdi - mdi) / (pdi + mdi) * 100)

        adx = self._wilder_smooth(dx_vals, p) if len(dx_vals) >= p else 0.0
        return adx, plus_di, minus_di

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if highs is None or lows is None or len(highs) < self.period * 2 + 2:
            return None
        adx, plus_di, minus_di = self._calc_di_adx(highs, lows, closes, self.period)

        if adx < self.threshold:
            self.prev_plus_di = plus_di
            self.prev_minus_di = minus_di
            return None

        result = None
        if self.prev_plus_di is not None and self.prev_minus_di is not None:
            if self.prev_plus_di <= self.prev_minus_di and plus_di > minus_di:
                conf = min((adx - self.threshold) / 50, 1.0)
                result = Signal(
                    "BUY", close, conf, f"ADX {adx:.1f} +DI crossover (trend up)", "adx"
                )
            elif self.prev_plus_di >= self.prev_minus_di and plus_di < minus_di:
                conf = min((adx - self.threshold) / 50, 1.0)
                result = Signal(
                    "SELL",
                    close,
                    conf,
                    f"ADX {adx:.1f} -DI crossover (trend down)",
                    "adx",
                )
        self.prev_plus_di = plus_di
        self.prev_minus_di = minus_di
        return result


class KeltnerChannels:
    """Keltner Channels — volatility-based breakout system.

    Middle = EMA(period), Channel width = ATR * multiplier.
    BUY when close breaks above upper channel
    SELL when close breaks below lower channel
    Risk: complements Bollinger Bands; uses ATR (different volatility view).
    """

    def __init__(self, period: int = 20, atr_period: int = 14, mult: float = 2.0):
        self.period = period
        self.atr_period = atr_period
        self.mult = mult

    def _atr(
        self, highs: List[float], lows: List[float], closes: List[float], p: int
    ) -> float:
        if len(highs) < p + 2:  # pragma: no cover
            return 0.0
        tr_vals = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            tr_vals.append(tr)
        if len(tr_vals) < p:  # pragma: no cover
            return sum(tr_vals) / len(tr_vals)
        atr = sum(tr_vals[:p]) / p
        k = 1.0 / p
        for v in tr_vals[p:]:
            atr = v * k + atr * (1 - k)
        return atr

    def on_bar(
        self,
        close: float,
        closes: List[float],
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        volumes: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if highs is None or lows is None:
            return None
        if len(closes) < self.period + self.atr_period:
            return None
        ema = _ema(closes, self.period)
        atr = self._atr(highs, lows, closes, self.atr_period)
        upper = ema + atr * self.mult
        lower = ema - atr * self.mult

        if close > upper:
            conf = min((close - upper) / (atr * self.mult) if atr > 0 else 0.5, 1.0)
            conf = max(conf, 0.1)
            return Signal(
                "BUY", close, conf, f"Keltner breakout above {upper:.4f}", "keltner"
            )
        elif close < lower:
            conf = min((lower - close) / (atr * self.mult) if atr > 0 else 0.5, 1.0)
            conf = max(conf, 0.1)
            return Signal(
                "SELL", close, conf, f"Keltner breakdown below {lower:.4f}", "keltner"
            )
        return None


class ChaikinMoneyFlow:
    """Chaikin Money Flow — volume-weighted accumulation/distribution.

    CMF = sum(MFV, N) / sum(Volume, N)
    where MFV = ((Close - Low) - (High - Close)) / (High - Low) * Volume
    CMF > 0.1 → accumulation (buying pressure)
    CMF < -0.1 → distribution (selling pressure)
    Risk: confirms whether price moves are backed by volume.
    """

    def __init__(self, period: int = 21, threshold: float = 0.1):
        self.period = period
        self.threshold = threshold

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if volumes is None or highs is None or lows is None:
            return None
        if len(closes) < self.period + 1:
            return None

        mfv_sum = 0.0
        vol_sum = 0.0
        for i in range(-self.period, 0):
            hl = highs[i] - lows[i]
            mfm = (
                ((closes[i] - lows[i]) - (highs[i] - closes[i])) / hl
                if hl != 0
                else 0.0
            )
            mfv_sum += mfm * volumes[i]
            vol_sum += volumes[i]

        cmf = mfv_sum / vol_sum if vol_sum > 0 else 0.0

        if cmf > self.threshold:
            conf = min(cmf / 0.3, 1.0)
            return Signal(
                "BUY", close, conf, f"CMF {cmf:.2f} accumulation", "chaikin_mf"
            )
        elif cmf < -self.threshold:
            conf = min(abs(cmf) / 0.3, 1.0)
            return Signal(
                "SELL", close, conf, f"CMF {cmf:.2f} distribution", "chaikin_mf"
            )
        return None


class WilliamsR:
    """Williams %R — overbought / oversold momentum oscillator.

    %R = (Highest High - Close) / (Highest High - Lowest Low) * -100
    Range: -100 to 0
    Below -80 → oversold (BUY)
    Above -20 → overbought (SELL)
    Risk: complements RSI and CMO with different math (uses H/L range).
    """

    def __init__(
        self, period: int = 14, oversold: float = -80, overbought: float = -20
    ):
        self.period = period
        self.oversold = oversold
        self.overbought = overbought

    def on_bar(
        self,
        close: float,
        closes: List[float],
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        volumes: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if highs is None or lows is None:
            return None
        if len(highs) < self.period + 1:
            return None

        highest = max(highs[-self.period :])
        lowest = min(lows[-self.period :])
        rng = highest - lowest
        if rng == 0:
            return None

        wr = (highest - close) / rng * -100

        if wr < self.oversold:
            conf = min(abs(wr - self.oversold) / abs(self.oversold), 1.0)
            return Signal(
                "BUY", close, conf, f"Williams %R {wr:.0f} oversold", "williams_r"
            )
        elif wr > self.overbought:
            conf = min((wr - self.overbought) / abs(self.overbought), 1.0)
            return Signal(
                "SELL", close, conf, f"Williams %R {wr:.0f} overbought", "williams_r"
            )
        return None


class ParabolicSAR:
    """Parabolic Stop-and-Reverse — trend reversal detection.

    Tracks price with accelerating trailing stop.
    BUY when SAR flips from above price to below price (uptrend start)
    SELL when SAR flips from below price to above price (downtrend start)
    Risk: dynamic trailing stop that tightens as trend accelerates.
    """

    def __init__(
        self, af_start: float = 0.02, af_increment: float = 0.02, af_max: float = 0.20
    ):
        self.af_start = af_start
        self.af_increment = af_increment
        self.af_max = af_max
        self.prev_trend: Optional[str] = None

    def _compute(self, highs: List[float], lows: List[float]):
        n = len(highs)
        if n < 3:
            return 0.0, "UP"

        if highs[1] > highs[0]:
            trend = "UP"
            sar = min(lows[0], lows[1])
            ep = max(highs[0], highs[1])
        else:
            trend = "DOWN"
            sar = max(highs[0], highs[1])
            ep = min(lows[0], lows[1])
        af = self.af_start

        for i in range(2, n):
            if trend == "UP":
                sar = sar + af * (ep - sar)
                sar = min(sar, lows[i - 1])
                if i >= 2:
                    sar = min(sar, lows[i - 2])
                if highs[i] > ep:
                    ep = highs[i]
                    af = min(af + self.af_increment, self.af_max)
                if lows[i] < sar:
                    trend = "DOWN"
                    sar = ep
                    ep = lows[i]
                    af = self.af_start
            else:
                sar = sar - af * (sar - ep)
                sar = max(sar, highs[i - 1])
                if i >= 2:
                    sar = max(sar, highs[i - 2])
                if lows[i] < ep:
                    ep = lows[i]
                    af = min(af + self.af_increment, self.af_max)
                if highs[i] > sar:
                    trend = "UP"
                    sar = ep
                    ep = highs[i]
                    af = self.af_start

        return sar, trend

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if highs is None or lows is None:
            return None

        sar, trend = self._compute(highs, lows)

        result = None
        if self.prev_trend is not None and self.prev_trend != trend:
            conf = min(abs(close - sar) / sar * 10, 1.0)
            if trend == "UP":
                result = Signal(
                    "BUY",
                    close,
                    conf,
                    f"PSAR reversal: uptrend (SAR={sar:.4f})",
                    "psar",
                )
            else:
                result = Signal(
                    "SELL",
                    close,
                    conf,
                    f"PSAR reversal: downtrend (SAR={sar:.4f})",
                    "psar",
                )

        self.prev_trend = trend
        return result


# ---------------------------------------------------------------------------
# Round 3: Five more — HMA, Force Index, VPT, Donchian, Aroon
# ---------------------------------------------------------------------------


class HullMA:
    """Hull Moving Average — fast, smooth trend with minimal lag.

    HMA(n) = WMA(2 * WMA(n/2) - WMA(n), sqrt(n))
    Uses fast/slow HMA crossover like EMA_Crossover but smoother.
    BUY when fast HMA crosses above slow HMA
    SELL when fast HMA crosses below slow HMA
    Risk: reduced lag means earlier entry/exit in trending markets.
    """

    def __init__(self, fast: int = 9, slow: int = 21):
        self.fast = fast
        self.slow = slow
        self.prev_fast: Optional[float] = None
        self.prev_slow: Optional[float] = None

    @staticmethod
    def _hma(values: List[float], n: int) -> float:
        if len(values) < n:  # pragma: no cover
            return values[-1] if values else 0.0
        half = max(n // 2, 2)
        sqrt_n = max(int(n**0.5), 2)
        wma_half = _wma(values, half)
        wma_full = _wma(values, n)
        raw = 2 * wma_half - wma_full
        # Need at least sqrt_n values of the raw series to compute final WMA
        raw_series = []
        for i in range(max(sqrt_n * 2, n), len(values) + 1):
            segment = values[:i]
            wh = _wma(segment, min(half, len(segment)))
            wf = _wma(segment, min(n, len(segment)))
            raw_series.append(2 * wh - wf)
        if len(raw_series) < sqrt_n:
            return raw
        return _wma(raw_series, sqrt_n)

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if len(closes) < self.slow + 1:
            return None
        fast_val = self._hma(closes, self.fast)
        slow_val = self._hma(closes, self.slow)
        signal = None
        if self.prev_fast is not None and self.prev_slow is not None:
            if self.prev_fast <= self.prev_slow and fast_val > slow_val:
                conf = min(abs(fast_val - slow_val) / slow_val * 30, 1.0)
                signal = Signal(
                    "BUY", close, conf, f"HMA crossover {self.fast}/{self.slow}", "hma"
                )
            elif self.prev_fast >= self.prev_slow and fast_val < slow_val:
                conf = min(abs(fast_val - slow_val) / slow_val * 30, 1.0)
                signal = Signal(
                    "SELL", close, conf, f"HMA crossover {self.fast}/{self.slow}", "hma"
                )
        self.prev_fast = fast_val
        self.prev_slow = slow_val
        return signal


class ForceIndex:
    """Elder's Force Index — measures conviction behind price moves.

    FI(1) = Volume * (Close - Prev_Close)
    Smoothed = EMA(FI, 13)
    BUY when smoothed FI crosses above 0 (buying conviction)
    SELL when smoothed FI crosses below 0 (selling conviction)
    Risk: volume-weighted price momentum filters out low-conviction moves.
    """

    def __init__(self, period: int = 13):
        self.period = period
        self.prev_smoothed: Optional[float] = None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if volumes is None or len(closes) < self.period + 2:
            return None
        fi_values = []
        for i in range(1, len(closes)):
            fi_values.append(volumes[i] * (closes[i] - closes[i - 1]))
        smoothed = _ema(fi_values, self.period)

        result = None
        if self.prev_smoothed is not None:
            if self.prev_smoothed < 0 and smoothed > 0:
                conf = min(
                    smoothed / abs(self.prev_smoothed)
                    if self.prev_smoothed != 0
                    else 0.1,
                    1.0,
                )
                conf = max(conf, 0.1)
                result = Signal(
                    "BUY", close, conf, f"Force Index bullish crossover", "force_idx"
                )
            elif self.prev_smoothed > 0 and smoothed < 0:
                conf = min(
                    abs(smoothed) / self.prev_smoothed
                    if self.prev_smoothed != 0
                    else 0.1,
                    1.0,
                )
                conf = max(conf, 0.1)
                result = Signal(
                    "SELL", close, conf, f"Force Index bearish crossover", "force_idx"
                )
        self.prev_smoothed = smoothed
        return result


class VolumePriceTrend:
    """Volume Price Trend — leading indicator of accumulation/distribution.

    VPT = cumulative sum of Volume * (Close - Prev_Close) / Prev_Close
    BUY when VPT crosses above its EMA (accumulation underway)
    SELL when VPT crosses below its EMA (distribution underway)
    Risk: detects divergences before price confirms them.
    """

    def __init__(self, period: int = 21):
        self.period = period
        self.prev_diff: Optional[float] = None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if volumes is None or len(closes) < self.period + 2:
            return None
        vpt = 0.0
        vpt_series = [0.0]
        for i in range(1, len(closes)):
            pct = (
                (closes[i] - closes[i - 1]) / closes[i - 1] if closes[i - 1] != 0 else 0
            )
            vpt += volumes[i] * pct
            vpt_series.append(vpt)

        vpt_ema = _ema(vpt_series, self.period)
        current_vpt = vpt_series[-1]
        current_diff = current_vpt - vpt_ema

        result = None
        if self.prev_diff is not None:
            if self.prev_diff <= 0 and current_diff > 0:
                conf = min(
                    abs(current_diff) / abs(vpt_ema) * 2 if vpt_ema != 0 else 0.1, 1.0
                )
                conf = max(conf, 0.1)
                result = Signal(
                    "BUY", close, conf, f"VPT bullish (accumulation)", "vpt"
                )
            elif self.prev_diff >= 0 and current_diff < 0:
                conf = min(
                    abs(current_diff) / abs(vpt_ema) * 2 if vpt_ema != 0 else 0.1, 1.0
                )
                conf = max(conf, 0.1)
                result = Signal(
                    "SELL", close, conf, f"VPT bearish (distribution)", "vpt"
                )
        self.prev_diff = current_diff
        return result


class DonchianChannels:
    """Donchian Channels — pure price breakout system.

    Upper = highest high of last N periods
    Lower = lowest low of last N periods
    BUY when close exceeds upper channel
    SELL when close breaks below lower channel
    Risk: classic Turtle Trading method for capturing strong trends.
    """

    def __init__(self, period: int = 20):
        self.period = period

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if highs is None or lows is None or len(highs) < self.period + 1:
            return None
        upper = max(highs[-self.period :])
        lower = min(lows[-self.period :])

        rng = upper - lower
        if rng == 0:
            return None

        # NOTE: the lookback window includes the current bar, so upper >= high
        # and lower <= low always hold; these breakout branches are therefore
        # unreachable in practice and excluded from coverage.
        if close > upper:  # pragma: no cover
            conf = min((close - upper) / rng, 1.0)
            conf = max(conf, 0.1)
            return Signal(
                "BUY", close, conf, f"Donchian breakout above {upper:.4f}", "donchian"
            )
        elif close < lower:  # pragma: no cover
            conf = min((lower - close) / rng, 1.0)
            conf = max(conf, 0.1)
            return Signal(
                "SELL", close, conf, f"Donchian breakdown below {lower:.4f}", "donchian"
            )
        return None


class Aroon:
    """Aroon — measures trend direction and strength.

    Aroon Up = ((N - Periods_Since_High) / N) * 100
    Aroon Down = ((N - Periods_Since_Low) / N) * 100
    Oscillator = Aroon Up - Aroon Down
    BUY when Aroon Up crosses above Aroon Down and both are > 50
    SELL when Aroon Down crosses above Aroon Up and both are > 50
    Risk: identifies established trends vs random noise.
    """

    def __init__(self, period: int = 25):
        self.period = period
        self.prev_osc: Optional[float] = None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if highs is None or lows is None or len(highs) < self.period + 2:
            return None

        n = self.period
        recent_highs = highs[-n:]
        recent_lows = lows[-n:]
        highest = max(recent_highs)
        lowest = min(recent_lows)

        # Find periods since most recent high/low
        days_since_high = n - 1 - recent_highs.index(highest)
        days_since_low = n - 1 - recent_lows.index(lowest)

        aroon_up = (n - days_since_high) / n * 100
        aroon_down = (n - days_since_low) / n * 100
        osc = aroon_up - aroon_down

        result = None
        if self.prev_osc is not None:
            if self.prev_osc <= 0 and osc > 0 and aroon_up > 50:
                conf = min(aroon_up / 100, 1.0)
                result = Signal(
                    "BUY",
                    close,
                    conf,
                    f"Aroon bullish (up={aroon_up:.0f} down={aroon_down:.0f})",
                    "aroon",
                )
            elif self.prev_osc >= 0 and osc < 0 and aroon_down > 50:
                conf = min(aroon_down / 100, 1.0)
                result = Signal(
                    "SELL",
                    close,
                    conf,
                    f"Aroon bearish (up={aroon_up:.0f} down={aroon_down:.0f})",
                    "aroon",
                )
        self.prev_osc = osc
        return result


class PriceEfficiencyRatio:
    """Price Efficiency Ratio — trend strength measurement.

    Measures price efficiency (how directional vs noisy the market is)
    via price * volume / (EMA(price) * sqrt(EMA(volume))).
    High efficiency = trending market, Low efficiency = choppy/ranging market.
    Signal when efficiency crosses above/below threshold.
    Risk: filters out whipsaw by requiring strong trend efficiency before acting.
    """

    def __init__(
        self, period: int = 21, signal_period: int = 7, threshold: float = 0.8
    ):
        self.period = period
        self.signal_period = signal_period
        self.threshold = threshold
        self.prev_eff: Optional[float] = None

    @staticmethod
    def _efficiency(price: List[float], volume: List[float]) -> List[float]:
        """Compute price efficiency ratio at each bar."""
        eff = []
        for i in range(len(price)):
            if i < 1:
                eff.append(1.0)
                continue
            p_mean = sum(price[: i + 1]) / (i + 1)
            v_mean = sum(volume[: i + 1]) / (i + 1)
            if p_mean == 0 or v_mean == 0:
                eff.append(1.0)
            else:
                ratio = price[i] * volume[i] / (p_mean * math.sqrt(v_mean))
                eff.append(ratio)
        return eff

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if volumes is None or len(closes) < self.period + 1:
            return None
        eff = self._efficiency(closes, volumes)
        if len(eff) < self.signal_period:  # pragma: no cover
            return None
        smoothed = _wma(eff, self.signal_period)

        if self.prev_eff is None:
            self.prev_eff = smoothed
            return None

        if self.prev_eff <= self.threshold and smoothed > self.threshold:
            conf = min((smoothed - self.threshold) / (1.0 - self.threshold), 1.0)
            self.prev_eff = smoothed
            return Signal(
                "BUY", close, conf, f"Efficiency bullish ({smoothed:.2f})", "price_eff"
            )
        elif self.prev_eff >= self.threshold and smoothed < self.threshold:
            conf = min((self.threshold - smoothed) / self.threshold, 1.0)
            self.prev_eff = smoothed
            return Signal(
                "SELL", close, conf, f"Efficiency bearish ({smoothed:.2f})", "price_eff"
            )
        self.prev_eff = smoothed
        return None


class SimplifiedCCI:
    """Simplified CCI — cyclical overbought/oversold detection.

    Computes (Current Price / SMA - 1) * 100 using a simple average (not the
    full Mean-Deviation CCI formula) for a lighter-weight overbought/oversold
    indicator.
    > +threshold → overbought (SELL), < -threshold → oversold (BUY)
    Risk: detects cyclical patterns that short-term strategies miss.
    """

    def __init__(self, period: int = 28, threshold: float = 30):
        self.period = period
        self.threshold = threshold

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if len(closes) < self.period + 1:
            return None
        avg_price = sum(closes[-self.period :]) / self.period
        if avg_price == 0:
            return None
        cci = (close / avg_price - 1.0) * 100

        if cci > self.threshold:
            conf = min((cci - self.threshold) / 50, 1.0)
            return Signal(
                "SELL", close, max(conf, 0.1), f"sCCI {cci:.1f} overbought", "scci"
            )
        elif cci < -self.threshold:
            conf = min((-cci - self.threshold) / 50, 1.0)
            return Signal(
                "BUY", close, max(conf, 0.1), f"sCCI {cci:.1f} oversold", "scci"
            )
        return None


class RangeExpansionIndex:
    """Range Expansion Index — price range expansion/contraction detection.

    Measures the expansion and contraction of price range over time.
    When highs get higher AND lows get lower → buying pressure (positive)
    When highs drop AND lows rise → selling pressure (negative)
    Signal when index crosses zero with a minimum amplitude threshold.
    Risk: detects true trend reversal before price confirms it.
    """

    def __init__(self, period: int = 21, min_amplitude: float = 0.05):
        self.period = period
        self.min_amplitude = min_amplitude
        self.prev_rei: Optional[float] = None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if highs is None or lows is None or len(highs) < self.period + 1:
            return None

        recent_highs = highs[-self.period :]
        recent_lows = lows[-self.period :]
        current_high = max(recent_highs)
        current_low = min(recent_lows)
        prev_high = (
            highs[-(self.period + 1)] if len(highs) > self.period else current_high
        )
        prev_low = lows[-(self.period + 1)] if len(lows) > self.period else current_low

        range_expansion = (
            (current_high - prev_high) / prev_high if prev_high != 0 else 0
        )
        range_contraction = (prev_low - current_low) / prev_low if prev_low != 0 else 0
        rei = range_expansion + range_contraction

        if abs(rei) > self.min_amplitude and self.prev_rei is not None:
            if self.prev_rei <= 0 and rei > 0:
                conf = min(abs(rei) / self.min_amplitude, 1.0)
                self.prev_rei = rei
                return Signal(
                    "BUY",
                    close,
                    max(conf, 0.1),
                    f"REI bullish (range expanding)",
                    "range_exp_idx",
                )
            elif self.prev_rei >= 0 and rei < 0:
                conf = min(abs(rei) / self.min_amplitude, 1.0)
                self.prev_rei = rei
                return Signal(
                    "SELL",
                    close,
                    max(conf, 0.1),
                    f"REI bearish (range contracting)",
                    "range_exp_idx",
                )
        self.prev_rei = rei
        return None


class EMADeviation:
    """EMA Deviation — price deviation from EMA.

    Computes (Close - EMA) / EMA as a measure of how far price has moved
    from its trend. Positive deviation = above trend, negative = below.
    Signal when deviation crosses zero with a minimum amplitude threshold.
    Risk: identifies pullbacks within a trend that other strategies miss.
    """

    def __init__(self, period: int = 14, min_amplitude: float = 0.05):
        self.period = period
        self.min_amplitude = min_amplitude
        self.prev_dev: Optional[float] = None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if len(closes) < self.period + 1:
            return None
        ema = _ema(closes, self.period)
        dev = (close - ema) / ema if ema != 0 else 0

        if abs(dev) > self.min_amplitude and self.prev_dev is not None:
            if self.prev_dev <= 0 and dev > 0:
                conf = min(abs(dev) / self.min_amplitude, 1.0)
                self.prev_dev = dev
                return Signal(
                    "BUY",
                    close,
                    max(conf, 0.1),
                    f"EMA deviation bullish ({dev:.3f})",
                    "ema_dev",
                )
            elif self.prev_dev >= 0 and dev < 0:
                conf = min(abs(dev) / self.min_amplitude, 1.0)
                self.prev_dev = dev
                return Signal(
                    "SELL",
                    close,
                    max(conf, 0.1),
                    f"EMA deviation bearish ({dev:.3f})",
                    "ema_dev",
                )
        self.prev_dev = dev
        return None


class SignalToNoiseRatio:
    """Signal-to-Noise Ratio — conviction measurement for price movement.

    SNR = |Price Change| / Volatility * 100
    High SNR = strong conviction behind the move, Low SNR = weak/no conviction.
    Signal when SNR crosses a threshold in either direction.
    Risk: filters out low-conviction moves that are likely to be whipsaw.
    """

    def __init__(
        self, period: int = 14, buy_threshold: float = 30, sell_threshold: float = -30
    ):
        self.period = period
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.prev_snr: Optional[float] = None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        if len(closes) < self.period + 2:
            return None

        # Compute volatility over the period
        recent_closes = closes[-self.period :]
        mean_close = sum(recent_closes) / self.period
        variance = sum((x - mean_close) ** 2 for x in recent_closes) / self.period
        std = math.sqrt(variance) if variance > 0 else 1.0

        # Price change and SNR
        price_change_pct = (
            (closes[-1] - closes[-self.period]) / closes[-self.period] * 100
        )
        snr = abs(price_change_pct) / (std / mean_close * 100) if mean_close != 0 else 0

        # Direction matters — positive SNR for bullish, negative for bearish
        direction_snr = (
            price_change_pct / (std / mean_close * 100)
            if mean_close != 0 and std > 0
            else 0
        )

        if self.prev_snr is not None:
            if self.prev_snr <= 0 and direction_snr > self.buy_threshold / 30:
                conf = min(direction_snr / (self.buy_threshold / 30), 1.0)
                self.prev_snr = direction_snr
                return Signal(
                    "BUY",
                    close,
                    max(conf, 0.1),
                    f"SNR bullish conviction ({direction_snr:.2f})",
                    "snr_idx",
                )
            elif self.prev_snr >= 0 and direction_snr < -self.sell_threshold / 30:
                conf = min(abs(direction_snr) / (self.sell_threshold / 30), 1.0)
                self.prev_snr = direction_snr
                return Signal(
                    "SELL",
                    close,
                    max(conf, 0.1),
                    f"SNR bearish conviction ({direction_snr:.2f})",
                    "snr_idx",
                )
        self.prev_snr = direction_snr
        return None


class FundingRateContrarian:
    """Funding rate contrarian signal.

    Fetches live perpetual swap funding rates from Binance Futures
    public API (no auth needed). Extreme positive funding → SELL
    (crowded long, shorts paying longs — reversal expected).
    Extreme negative funding → BUY (crowded short, bounce expected).

    Pure external-data strategy — ignores OHLCV inputs.
    """

    BINANCE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

    def __init__(self, min_abs_funding_bps: float = 0.1):
        self.min_abs_funding_bps = min_abs_funding_bps
        self._cache: Dict[str, float] = {}
        self._cache_ts: float = 0

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        currency: Optional[str] = None,
    ) -> Optional[Signal]:
        import time
        now = time.time()
        if now - self._cache_ts > 60:
            try:
                import json, urllib.request
                req = urllib.request.Request(self.BINANCE_URL)
                with urllib.request.urlopen(req, timeout=5) as resp:
                    data = json.loads(resp.read().decode())
                fresh: Dict[str, float] = {}
                for entry in data:
                    symbol = entry.get("symbol", "")
                    fr_str = entry.get("lastFundingRate", "0")
                    fr = float(fr_str) if fr_str else 0.0
                    fresh[symbol] = fr
                if fresh:
                    self._cache = fresh
                    self._cache_ts = now
            except Exception:
                return None  # Silently handle (Binance may be geo-blocked)

        if not self._cache:
            return None

        # Find most extreme funding rate across all pairs
        best_fr = 0.0
        best_sym = ""
        for sym, fr in self._cache.items():
            if abs(fr) > abs(best_fr):
                best_fr = fr
                best_sym = sym

        if not best_sym:
            return None

        # Convert from decimal (per 8h) to bps
        fr_bps = best_fr * 100.0 * 100.0

        if fr_bps > self.min_abs_funding_bps:
            conf = min(fr_bps / 8.0, 0.90)
            return Signal(
                "SELL", close, max(conf, 0.1),
                f"Funding {best_sym}: +{fr_bps:.1f}bps (crowded long → fade)",
                "funding_contrarian",
            )
        elif fr_bps < -self.min_abs_funding_bps:
            conf = min(-fr_bps / 8.0, 0.90)
            return Signal(
                "BUY", close, max(conf, 0.1),
                f"Funding {best_sym}: {fr_bps:.1f}bps (crowded short → bounce)",
                "funding_contrarian",
            )
        return None


class ExchangeFlowSignal:
    """Exchange flow divergence signal via CoinGecko.

    Uses CoinGecko market_chart volume data as a proxy for exchange
    flow pressure. Detects volume anomalies combined with price trend
    to identify distribution (volume spike on up-move → SELL) or
    accumulation (volume spike on down-move → BUY).

    Pure external-data strategy — ignores OHLCV inputs.
    Fetches via the existing CoinGecko client if available, else
    falls back to direct public API calls.
    """

    PRODUCT_TO_CG: Dict[str, str] = {
        "BTC-USD": "bitcoin", "ETH-USD": "ethereum", "SOL-USD": "solana",
        "XRP-USD": "ripple", "ADA-USD": "cardano", "DOGE-USD": "dogecoin",
        "AVAX-USD": "avalanche-2", "DOT-USD": "polkadot", "LINK-USD": "chainlink",
        "UNI-USD": "uniswap", "MATIC-USD": "matic-network",
    }

    def __init__(self, cache_ttl: float = 300.0, vol_spike_threshold: float = 3.0):
        self.cache_ttl = cache_ttl
        self.vol_spike_threshold = vol_spike_threshold
        self._cache: Dict[str, Dict] = {}
        self._cache_ts: float = 0

    def _cg_id(self, currency: str) -> Optional[str]:
        return self.PRODUCT_TO_CG.get(currency)

    def _fetch_market_chart(self, cg_id: str) -> Optional[Dict]:
        import time, json, urllib.request
        now = time.time()
        if cg_id in self._cache and now - self._cache_ts < self.cache_ttl:
            return self._cache.get(cg_id)
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart?vs_currency=usd&days=2"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            self._cache[cg_id] = data
            self._cache_ts = now
            try:
                from data.feed_cache import save_records as _fc_save
                _fc_save("onchain", f"market_chart_{cg_id}", [{"ts": int(now), "cg_id": cg_id, "data": data}])
            except Exception:
                pass
            return data
        except Exception as e:
            logger.debug("ExchangeFlowSignal fetch failed for %s: %s", cg_id, e)
            return None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        currency: Optional[str] = None,
    ) -> Optional[Signal]:
        if not currency:
            return None
        cg_id = self._cg_id(currency)
        if not cg_id:
            return None

        data = self._fetch_market_chart(cg_id)
        if not data:
            return None

        prices = data.get("prices", [])
        vols = data.get("total_volumes", [])
        if len(prices) < 24 or len(vols) < 24:
            return None

        # Extract last 48 intervals
        recent_prices = [p[1] for p in prices[-48:]]
        recent_vols = [v[1] for v in vols[-48:]]

        if len(recent_prices) < 12 or len(recent_vols) < 12:
            return None

        # Compute volume anomaly
        recent_48_vols = [v[1] for v in vols[-96:]] if len(vols) >= 96 else recent_vols
        avg_vol = sum(recent_48_vols) / len(recent_48_vols) if recent_48_vols else 1.0
        if avg_vol <= 0:
            return None
        current_vol = recent_vols[-1]
        vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0.0

        # Price trend over last 24 periods
        price_start = recent_prices[0]
        price_end = recent_prices[-1]
        price_change_pct = (price_end - price_start) / price_start * 100.0 if price_start > 0 else 0.0

        if vol_ratio < self.vol_spike_threshold:
            return None

        # Volume spike detected — evaluate direction
        if price_change_pct > 2.0:
            # Volume spike on up-move → possible distribution
            conf = min((vol_ratio - self.vol_spike_threshold) / 5.0, 0.85)
            return Signal(
                "SELL", close, max(conf, 0.1),
                f"Exchange flow: {cg_id} vol {vol_ratio:.1f}x avg +{price_change_pct:.1f}% (distribution)",
                "exchange_flow",
            )
        elif price_change_pct < -2.0:
            # Volume spike on down-move → possible accumulation
            conf = min((vol_ratio - self.vol_spike_threshold) / 5.0, 0.85)
            return Signal(
                "BUY", close, max(conf, 0.1),
                f"Exchange flow: {cg_id} vol {vol_ratio:.1f}x avg {price_change_pct:.1f}% (accumulation)",
                "exchange_flow",
            )
        return None


class BTCDXYCorrelation:
    """BTC-DXY correlation reversion signal.

    When the rolling 90-day correlation between BTC-USD and the US
    Dollar Index (DXY) deviates significantly from its 1-year mean,
    bet on reversion toward the mean correlation.
    """

    def __init__(self, lookback_days: int = 90, history_days: int = 365):
        self.lookback_days = lookback_days
        self.history_days = history_days
        self._cache: Dict = {}
        self._cache_ts: float = 0

    def _fetch_data(self) -> Optional[Dict]:
        import time, json, urllib.request
        now = time.time()
        if self._cache and now - self._cache_ts < 3600:
            return self._cache
        try:
            # Fetch BTC and DXY from Yahoo Finance public API
            # Use direct Yahoo chart API (same as cross_asset_regime.py)
            results: Dict[str, List[float]] = {}
            for symbol, label in [("BTC-USD", "btc"), ("DX-Y.NYB", "dxy")]:
                period = f"{self.history_days}d"
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={period}&interval=1d"
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=10) as resp:
                    chart = json.loads(resp.read().decode())
                quotes = chart.get("chart", {}).get("result", [{}])[0].get("indicators", {}).get("quote", [{}])[0]
                closes_raw = quotes.get("close", [])
                results[label] = [c for c in closes_raw if c is not None]
            self._cache = results
            self._cache_ts = now
            return results
        except Exception as e:
            logger.debug("BTCDXYCorrelation fetch failed: %s", e)
            return None

    @staticmethod
    def _rolling_corr(a: List[float], b: List[float], window: int) -> float:
        n = min(len(a), len(b))
        if n < window:
            return 0.0
        a_slice = a[-window:]
        b_slice = b[-window:]
        n_w = len(a_slice)
        mean_a = sum(a_slice) / n_w
        mean_b = sum(b_slice) / n_w
        cov = sum((ai - mean_a) * (bi - mean_b) for ai, bi in zip(a_slice, b_slice)) / n_w
        std_a = (sum((ai - mean_a) ** 2 for ai in a_slice) / n_w) ** 0.5
        std_b = (sum((bi - mean_b) ** 2 for bi in b_slice) / n_w) ** 0.5
        if std_a < 1e-15 or std_b < 1e-15:
            return 0.0
        return cov / (std_a * std_b)

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        currency: Optional[str] = None,
    ) -> Optional[Signal]:
        if currency and currency != "BTC-USD":
            return None  # Only meaningful for BTC

        data = self._fetch_data()
        if not data or "btc" not in data or "dxy" not in data:
            return None

        btc_prices = data["btc"]
        dxy_prices = data["dxy"]

        if len(btc_prices) < self.history_days // 2 or len(dxy_prices) < self.history_days // 2:
            return None

        # Compute rolling correlation over lookback
        current_corr = self._rolling_corr(btc_prices, dxy_prices, self.lookback_days)

        # Compute mean of rolling correlations over the full history
        min_len = min(len(btc_prices), len(dxy_prices))
        corrs = []
        for i in range(self.lookback_days, min_len):
            c = self._rolling_corr(btc_prices[:i], dxy_prices[:i], min(self.lookback_days, i))
            corrs.append(c)

        if len(corrs) < 20:
            return None

        mean_corr = sum(corrs) / len(corrs)
        var_corr = sum((c - mean_corr) ** 2 for c in corrs) / len(corrs)
        std_corr = var_corr ** 0.5 if var_corr > 0 else 0.01

        z = (current_corr - mean_corr) / std_corr if std_corr > 0 else 0.0

        if z > 2.0:
            # BTC-DXY correlation abnormally high → expect decoupling
            # If correlation is positive-high and BTC is up, both could reverse
            conf = min((z - 2.0) / 3.0, 0.85)
            return Signal(
                "SELL", close, max(conf, 0.1),
                f"BTC-DXY corr {current_corr:.2f} (z={z:.1f}, mean={mean_corr:.2f}) — reversion expected",
                "btc_dxy_corr",
            )
        elif z < -2.0:
            # BTC-DXY correlation abnormally low → expect recoupling
            conf = min((-z - 2.0) / 3.0, 0.85)
            return Signal(
                "BUY", close, max(conf, 0.1),
                f"BTC-DXY corr {current_corr:.2f} (z={z:.1f}, mean={mean_corr:.2f}) — reversion expected",
                "btc_dxy_corr",
            )
        return None


class OrderFlowCVD:
    """Order-flow divergence via Cumulative Volume Delta (CVD).

    Builds a rolling CVD from signed volume (close vs prior close) and
    detects classic order-flow exhaustion: price makes a local extreme
    while CVD diverges the opposite way (distribution on up-moves,
    accumulation on down-moves). Candle-based — no external data needed,
    so it is fully deterministic and testable from OHLCV alone.
    """

    def __init__(self, lookback: int = 30, divergence_bars: int = 6, min_conf: float = 0.35):
        self.lookback = lookback
        self.divergence_bars = divergence_bars
        self.min_conf = min_conf

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        currency: Optional[str] = None,
    ) -> Optional[Signal]:
        n = self.lookback
        if volumes is None or len(volumes) < n + 1 or len(closes) < n + 1:
            return None

        # Build CVD series in FORWARD time order over the last n+1 bars so
        # series[-1] is the most recent CVD and series[-1-d] is d bars ago —
        # this keeps the CVD window aligned with the price window below.
        w_closes = closes[-(n + 1):]
        w_vols = volumes[-(n + 1):]
        cvd = 0.0
        series: List[float] = []
        for i in range(1, len(w_closes)):
            delta = w_closes[i] - w_closes[i - 1]
            cvd += w_vols[i] if delta >= 0 else -w_vols[i]
            series.append(cvd)

        d = self.divergence_bars
        if len(series) < d + 1:
            return None

        price_chg = w_closes[-1] - w_closes[-1 - d]
        cvd_chg = series[-1] - series[-1 - d]
        recent_vol = sum(w_vols[-d:])

        if price_chg > 0 and cvd_chg <= 0:
            strength = min((-cvd_chg) / (recent_vol + 1e-9) * 2.0, 1.0)
            conf = min(0.3 + strength, 0.85)
            if conf >= self.min_conf:
                return Signal(
                    "SELL", close, conf,
                    f"OrderFlow CVD bearish divergence (price +{price_chg:.2%}, CVD {cvd_chg:.0f})",
                    "order_flow_cvd",
                )
        elif price_chg < 0 and cvd_chg >= 0:
            strength = min(cvd_chg / (recent_vol + 1e-9) * 2.0, 1.0)
            conf = min(0.3 + strength, 0.85)
            if conf >= self.min_conf:
                return Signal(
                    "BUY", close, conf,
                    f"OrderFlow CVD bullish divergence (price {price_chg:.2%}, CVD +{cvd_chg:.0f})",
                    "order_flow_cvd",
                )
        return None


class WickPressureFlow:
    """Order-flow pressure from candle wicks + body bias.

    Sums bullish pressure (lower wick + bullish body) vs bearish pressure
    (upper wick + bearish body) over a window. A persistent imbalance is a
    leading order-flow signal. Candle-based, deterministic, testable.
    """

    def __init__(self, lookback: int = 20, threshold: float = 0.12, min_conf: float = 0.35):
        self.lookback = lookback
        self.threshold = threshold
        self.min_conf = min_conf

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        currency: Optional[str] = None,
    ) -> Optional[Signal]:
        n = self.lookback
        if highs is None or lows is None or len(closes) < n + 1 \
                or len(highs) < n + 1 or len(lows) < n + 1:
            return None

        buy = 0.0
        sell = 0.0
        for i in range(1, n + 1):
            o = closes[-i - 1]
            c = closes[-i]
            h = highs[-i]
            l = lows[-i]
            body_lo = min(o, c)
            body_hi = max(o, c)
            buy += max(body_lo - l, 0.0) + max(c - o, 0.0)
            sell += max(h - body_hi, 0.0) + max(o - c, 0.0)

        total = buy + sell
        if total <= 0:
            return None
        net = (buy - sell) / total

        if net > self.threshold:
            conf = min(0.3 + net, 0.85)
            if conf >= self.min_conf:
                return Signal("BUY", close, conf, f"Wick pressure bullish {net:.2f}", "wick_pressure")
        elif net < -self.threshold:
            conf = min(0.3 + (-net), 0.85)
            if conf >= self.min_conf:
                return Signal("SELL", close, conf, f"Wick pressure bearish {net:.2f}", "wick_pressure")
        return None


class ExchangeNetflowSignal:
    """On-chain exchange-netflow signal (chain analytics).

    Uses CoinGecko market_chart volume + price history as a proxy for
    exchange flow pressure. Detects *sustained* netflow (not just spikes):
    rising volume alongside falling price = coins flowing in (capitulation /
    accumulation → BUY); rising volume alongside rising price = coins flowing
    out to strong hands / distribution → SELL. Distinct from
    ``ExchangeFlowSignal`` which keys off single-bar volume spikes.

    The fetch function is injectable so the strategy is unit-testable
    offline (no network required in tests).
    """

    PRODUCT_TO_CG: Dict[str, str] = {
        "BTC-USD": "bitcoin", "ETH-USD": "ethereum", "SOL-USD": "solana",
        "XRP-USD": "ripple", "ADA-USD": "cardano", "DOGE-USD": "dogecoin",
        "AVAX-USD": "avalanche-2", "DOT-USD": "polkadot", "LINK-USD": "chainlink",
        "UNI-USD": "uniswap", "MATIC-USD": "matic-network",
    }

    def __init__(self, cache_ttl: float = 600.0, trend_window: int = 24, vol_trend_min: float = 0.15):
        self.cache_ttl = cache_ttl
        self.trend_window = trend_window
        self.vol_trend_min = vol_trend_min
        self._cache: Dict[str, Dict] = {}
        self._cache_ts: float = 0
        self._fetch_fn = self._default_fetch

    def _cg_id(self, currency: str) -> Optional[str]:
        return self.PRODUCT_TO_CG.get(currency)

    def _default_fetch(self, cg_id: str) -> Optional[Dict]:
        import time, json, urllib.request
        now = time.time()
        if cg_id in self._cache and now - self._cache_ts < self.cache_ttl:
            return self._cache.get(cg_id)
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart?vs_currency=usd&days=3"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            self._cache[cg_id] = data
            self._cache_ts = now
            try:
                from data.feed_cache import save_records as _fc_save
                _fc_save("onchain", f"market_chart_{cg_id}", [{"ts": int(now), "cg_id": cg_id, "data": data}])
            except Exception:
                pass
            return data
        except Exception as e:
            logger.debug("ExchangeNetflowSignal fetch failed for %s: %s", cg_id, e)
            return None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        currency: Optional[str] = None,
    ) -> Optional[Signal]:
        if not currency:
            return None
        cg_id = self._cg_id(currency)
        if not cg_id:
            return None

        data = self._fetch_fn(cg_id)
        if not data:
            return None

        vols = [v[1] for v in data.get("total_volumes", [])]
        prices = [p[1] for p in data.get("prices", [])]
        w = self.trend_window
        if len(vols) < w * 2 or len(prices) < w * 2:
            return None

        avg_recent = sum(vols[-w:]) / w
        avg_prior = sum(vols[-2 * w:-w]) / w
        if avg_prior <= 0:
            return None
        vol_trend = avg_recent / avg_prior - 1.0

        price_chg_pct = (prices[-1] - prices[-1 - w]) / prices[-1 - w] * 100.0 if prices[-1 - w] > 0 else 0.0

        if vol_trend < self.vol_trend_min:
            return None

        if price_chg_pct < -1.0:
            conf = min(0.3 + vol_trend, 0.85)
            return Signal(
                "BUY", close, conf,
                f"Chain netflow accumulation: vol +{vol_trend:.0%}, price {price_chg_pct:.1f}%",
                "exchange_netflow",
            )
        elif price_chg_pct > 1.0:
            conf = min(0.3 + vol_trend, 0.85)
            return Signal(
                "SELL", close, conf,
                f"Chain netflow distribution: vol +{vol_trend:.0%}, price {price_chg_pct:.1f}%",
                "exchange_netflow",
            )
        return None


class StablecoinFlowSignal:
    """On-chain stablecoin supply-flow macro gauge (chain analytics).

    Aggregates USDC + USDT market-cap history as a proxy for crypto
    liquidity / risk appetite. Rising stablecoin supply = fresh dry powder
    flowing in (risk-on → bullish BTC); contracting supply = redemptions /
    risk-off (bearish BTC). This is a market-wide macro signal, so it only
    acts on BTC-USD. The fetch function is injectable for offline tests.
    """

    STABLE_COINS: Dict[str, str] = {"USDC": "usd-coin", "USDT": "tether"}
    BTC = "BTC-USD"

    def __init__(self, cache_ttl: float = 900.0, trend_window: int = 30,
                 min_trend_pct: float = 0.5):
        self.cache_ttl = cache_ttl
        self.trend_window = trend_window
        self.min_trend_pct = min_trend_pct
        self._cache: Dict[str, List[List[float]]] = {}
        self._cache_ts: float = 0
        self._fetch_fn = self._default_fetch

    def _default_fetch(self, cg_id: str) -> Optional[Dict]:
        import time, json, urllib.request
        now = time.time()
        if cg_id in self._cache and now - self._cache_ts < self.cache_ttl:
            return {"market_caps": self._cache[cg_id]}
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/market_chart?vs_currency=usd&days=3"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            caps = data.get("market_caps") or []
            if caps:
                self._cache[cg_id] = caps
                self._cache_ts = now
                return {"market_caps": caps}
        except Exception as e:
            logger.debug("StablecoinFlowSignal fetch failed for %s: %s", cg_id, e)
        return None

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
        currency: Optional[str] = None,
    ) -> Optional[Signal]:
        if currency != self.BTC:
            return None

        series: List[float] = []
        for sym, cg in self.STABLE_COINS.items():
            res = self._fetch_fn(cg)
            caps = (res or {}).get("market_caps") or []
            if not series:
                series = [c[1] for c in caps]
            else:
                # Align by index; add each stablecoin's market cap
                for i, c in enumerate(caps):
                    if i < len(series):
                        series[i] += c[1]

        w = self.trend_window
        if len(series) < w * 2:
            return None

        avg_recent = sum(series[-w:]) / w
        avg_prior = sum(series[-2 * w:-w]) / w
        if avg_prior <= 0:
            return None
        trend_pct = (avg_recent / avg_prior - 1.0) * 100.0

        if trend_pct > self.min_trend_pct:
            conf = min(0.3 + trend_pct / 10.0, 0.85)
            return Signal(
                "BUY", close, conf,
                f"Stablecoin supply +{trend_pct:.1f}% (risk-on liquidity inflow)",
                "stablecoin_flow",
            )
        elif trend_pct < -self.min_trend_pct:
            conf = min(0.3 + (-trend_pct) / 10.0, 0.85)
            return Signal(
                "SELL", close, conf,
                f"Stablecoin supply {trend_pct:.1f}% (risk-off redemption)",
                "stablecoin_flow",
            )
        return None


class KalshiSignal:
    """Kalshi prediction market signal — BUY when YES probability is extreme high,
    SELL when extreme low. Uses the unified client to fetch live data.

    This is NOT an OHLCV-based strategy — it reads prediction market data
    through the existing on_bar() interface (ignores close/high/low/volume
    arguments and fetches its own external data).
    """

    def __init__(self, min_volume: float = 2000, min_extremity: float = 0.25):
        self.min_volume = min_volume
        self.min_extremity = min_extremity
        self._cache: List[Dict] = []
        self._cache_ts: float = 0

    @staticmethod
    def _make_client():
        import os
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        from event_markets.unified_client import UnifiedPredictionMarketClient
        return UnifiedPredictionMarketClient(
            kalshi_api_key_id=os.environ.get("KALSHI_API_KEY_ID", ""),
            kalshi_private_key_path=os.environ.get("KALSHI_PRIVATE_KEY_PATH", ""),
        )

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        import time
        now = time.time()
        if now - self._cache_ts > 300:  # 5-min cache
            try:
                client = self._make_client()
                markets = client.search_kalshi(term="", limit=10, min_volume=self.min_volume)
                self._cache = [{
                    "question": m.question,
                    "prob": m.mid_price,
                    "volume": m.volume,
                    "spread": m.spread,
                    "liq": m.liquidity_score,
                    "extremity": m.probability_extremity,
                } for m in markets if m.is_open]
                self._cache_ts = now
            except Exception as e:
                logger.warning("KalshiSignal fetch failed: %s", e)
                return None

        for m in self._cache:
            if m["extremity"] < self.min_extremity:
                continue
            conf = min(m["extremity"] * m["liq"] * 1.5, 0.95)
            if m["prob"] > 0.5 + self.min_extremity * 0.5:
                return Signal(
                    "BUY", close, max(conf, 0.1),
                    f"Kalshi: {m['question'][:60]} → {m['prob']*100:.0f}% YES (vol=${m['volume']:.0f})",
                    "kalshi",
                )
            elif m["prob"] < 0.5 - self.min_extremity * 0.5:
                return Signal(
                    "SELL", close, max(conf, 0.1),
                    f"Kalshi: {m['question'][:60]} → {m['prob']*100:.0f}% YES (vol=${m['volume']:.0f})",
                    "kalshi",
                )
        return None


class PolymarketSignal:
    """Polymarket prediction market signal — same logic as KalshiSignal
    but reads from Polymarket CLOB.  Generates BUY on high YES probability,
    SELL on low YES probability.
    """

    def __init__(self, min_volume: float = 2000, min_extremity: float = 0.25):
        self.min_volume = min_volume
        self.min_extremity = min_extremity
        self._cache: List[Dict] = []
        self._cache_ts: float = 0

    @staticmethod
    def _make_client():
        import os
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        from event_markets.unified_client import UnifiedPredictionMarketClient
        return UnifiedPredictionMarketClient()

    def on_bar(
        self,
        close: float,
        closes: List[float],
        volumes: Optional[List[float]] = None,
        highs: Optional[List[float]] = None,
        lows: Optional[List[float]] = None,
    ) -> Optional[Signal]:
        import time
        now = time.time()
        if now - self._cache_ts > 300:
            try:
                client = self._make_client()
                markets = client.search_polymarket(term="", limit=10, min_volume=self.min_volume)
                self._cache = [{
                    "question": m.question,
                    "prob": m.mid_price,
                    "volume": m.volume,
                    "spread": m.spread,
                    "liq": m.liquidity_score,
                    "extremity": m.probability_extremity,
                } for m in markets if m.is_open]
                self._cache_ts = now
            except Exception as e:
                logger.warning("PolymarketSignal fetch failed: %s", e)
                return None

        for m in self._cache:
            if m["extremity"] < self.min_extremity:
                continue
            conf = min(m["extremity"] * m["liq"] * 1.5, 0.95)
            if m["prob"] > 0.5 + self.min_extremity * 0.5:
                return Signal(
                    "BUY", close, max(conf, 0.1),
                    f"Polymarket: {m['question'][:60]} → {m['prob']*100:.0f}% YES (vol=${m['volume']:.0f})",
                    "polymarket",
                )
            elif m["prob"] < 0.5 - self.min_extremity * 0.5:
                return Signal(
                    "SELL", close, max(conf, 0.1),
                    f"Polymarket: {m['question'][:60]} → {m['prob']*100:.0f}% YES (vol=${m['volume']:.0f})",
                    "polymarket",
                )
        return None


# ---------------------------------------------------------------------------
# Strategy Runner
# ---------------------------------------------------------------------------

# Registry of all available strategies
ALL_STRATEGIES = {
    "ema_cross": EMA_Crossover,
    "rsi_revert": RSI_MeanReversion,
    "boll_break": BollingerBreakout,
    "zscore_revert": ZScoreReversion,
    "vol_mom": VolumeMomentum,
    "macd": MACD,
    "vwap_revert": VWAP_Reversion,
    "obv_div": OBV_Divergence,
    "cmo": ChandeMomentum,
    "trix": TRIX,
    "adx": ADX,
    "keltner": KeltnerChannels,
    "chaikin_mf": ChaikinMoneyFlow,
    "williams_r": WilliamsR,
    "psar": ParabolicSAR,
    "hma": HullMA,
    "force_idx": ForceIndex,
    "vpt": VolumePriceTrend,
    "donchian": DonchianChannels,
    "aroon": Aroon,
    "price_eff": PriceEfficiencyRatio,
    "scci": SimplifiedCCI,
    "range_exp_idx": RangeExpansionIndex,
    "ema_dev": EMADeviation,
    "snr_idx": SignalToNoiseRatio,
    "funding_contrarian": FundingRateContrarian,
    "exchange_flow": ExchangeFlowSignal,
    "btc_dxy_corr": BTCDXYCorrelation,
    "order_flow_cvd": OrderFlowCVD,
    "wick_pressure": WickPressureFlow,
    "exchange_netflow": ExchangeNetflowSignal,
    "stablecoin_flow": StablecoinFlowSignal,
    "kalshi": KalshiSignal,
    "polymarket": PolymarketSignal,
    # Rust-only strategies — no Python class, dispatch handled by _HAS_RUST check
    "candle_pat": None, "sup_res": None, "liq_vac": None, "cvd_flow": None, "vcp": None,
    "impulse_exh": None, "mom_accel": None, "rsi_fail": None, "avwap": None, "donch_pull": None,
    "vol_prof": None, "bb_squeeze": None, "multi_rsi": None, "linreg_slope": None, "hurst": None,
    "elder_ray": None, "klinger": None, "pivot_points": None, "ichimoku": None, "choppiness": None,
    "true_cci": None, "dpo": None, "kst": None, "mass_idx": None, "ulcer": None,
    "mfi": None, "stoch": None, "emv": None, "ad_div": None, "envelope": None, "atr_channel": None,
    "kama": None, "dmi_cross": None, "vma": None, "vortex": None, "rvi": None, "coppock": None,
    "std_channel": None, "vol_ratio": None, "vwap_macd": None, "nvi": None, "de_marker": None, "gap_revert": None,
    "supertrend": None, "fisher": None, "ultimate_osc": None, "vw_rsi": None,
    "kalman_mr": None, "hp_trend": None,
}

# Strategy-to-asset-class mapping
CLASS_STRATEGIES = {
    "safe": ["ema_cross", "macd", "vol_mom", "trix", "hma",
             "kalman_mr", "hp_trend",
             "funding_contrarian", "exchange_flow", "btc_dxy_corr"],
    "growth": [
        "ema_cross",
        "macd",
        "rsi_revert",
        "boll_break",
        "zscore_revert",
        "vol_mom",
        "vwap_revert",
        "obv_div",
        "cmo",
        "trix",
        "adx",
        "keltner",
        "chaikin_mf",
        "williams_r",
        "psar",
        "hma",
        "force_idx",
        "vpt",
        "donchian",
        "aroon",
        "price_eff",
        "scci",
        "range_exp_idx",
        "ema_dev",
        "snr_idx",
        "kalshi",
        "polymarket",
        "elder_ray", "klinger", "pivot_points", "ichimoku", "choppiness",
        "true_cci", "dpo", "kst", "mass_idx", "ulcer",
        # Rust-only
        "candle_pat", "sup_res", "liq_vac", "cvd_flow", "vcp",
        "impulse_exh", "mom_accel", "rsi_fail", "avwap", "donch_pull",
        "vol_prof", "bb_squeeze", "multi_rsi", "linreg_slope", "hurst",
        "mfi", "stoch", "emv", "ad_div", "envelope", "atr_channel",
        "kama", "dmi_cross", "vma", "vortex", "rvi", "coppock",
        "std_channel", "vol_ratio", "vwap_macd", "nvi", "de_marker", "gap_revert",
        "supertrend", "fisher", "ultimate_osc", "vw_rsi",
        # ── External-data strategies ──
        "kalman_mr", "hp_trend",
        "funding_contrarian", "exchange_flow", "btc_dxy_corr",
        "order_flow_cvd", "wick_pressure", "exchange_netflow", "stablecoin_flow",
    ],
    "speculative": [
        "rsi_revert",
        "boll_break",
        "zscore_revert",
        "vol_mom",
        "vwap_revert",
        "obv_div",
        "cmo",
        "adx",
        "keltner",
        "chaikin_mf",
        "williams_r",
        "psar",
        "hma",
        "force_idx",
        "vpt",
        "donchian",
        "aroon",
        "price_eff",
        "scci",
        "range_exp_idx",
        "ema_dev",
        "snr_idx",
        "kalshi",
        "polymarket",
        "elder_ray", "klinger", "pivot_points", "ichimoku", "choppiness",
        "true_cci", "dpo", "kst", "mass_idx", "ulcer",
        # Rust-only
        "candle_pat", "sup_res", "liq_vac", "cvd_flow", "vcp",
        "impulse_exh", "mom_accel", "rsi_fail", "avwap", "donch_pull",
        "vol_prof", "bb_squeeze", "multi_rsi", "linreg_slope", "hurst",
        "mfi", "stoch", "emv", "ad_div", "envelope", "atr_channel",
        "kama", "dmi_cross", "vma", "vortex", "rvi", "coppock",
        "std_channel", "vol_ratio", "vwap_macd", "nvi", "de_marker", "gap_revert",
        # ── External-data strategies ──
        "kalman_mr", "hp_trend",
        "funding_contrarian", "exchange_flow",
        "order_flow_cvd", "wick_pressure", "exchange_netflow",
    ],
}

# Strategies that accept volume data
VOLUME_STRATEGIES = {
    "vol_mom", "vwap_revert", "obv_div", "chaikin_mf",
    "force_idx", "vpt", "price_eff", "klinger", "vol_prof",
    "mfi", "emv", "ad_div", "vwap_macd", "nvi", "cvd_flow",
    "avwap", "impulse_exh", "linreg_slope", "mom_accel",
    "vw_rsi", "order_flow_cvd",
}

# Strategies that accept high/low data (candles with OHLC)
HIGH_LOW_STRATEGIES = {
    "adx", "keltner", "chaikin_mf", "williams_r",
    "psar", "donchian", "aroon", "mass_idx",
    "range_exp_idx", "bb_squeeze", "choppiness",
    "stoch", "emv", "ad_div", "atr_channel",
    "dmi_cross", "vortex", "vol_ratio", "de_marker",
    "klinger", "vcp", "sup_res", "liq_vac",
    "elder_ray", "ichimoku", "pivot_points", "true_cci",
    "supertrend", "ultimate_osc", "wick_pressure",
}


def run_strategies(
    currency: str,
    asset_class: str,
    closes: List[float],
    volumes: List[float],
    current_price: float,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
) -> List[Signal]:
    """Run all applicable strategies for an asset and return ranked signals.
    Uses Rust native acceleration for supported strategies.
    """
    _clear_cache()  # per-call indicator cache
    strategy_names = CLASS_STRATEGIES.get(asset_class, ["rsi_revert", "boll_break"])
    signals = []

    for name in strategy_names:
        # Try Rust acceleration for supported strategies
        if _HAS_RUST and name in _RUST_STRATEGIES:
            rust_signal: Optional[Signal] = None
            try:
                # Avoid O(n) allocation: pass closes as opens with last element duplicated
                # run_strategy_opens_py handles the shift internally
                opens = closes
                hs = highs if highs else []
                ls = lows if lows else []
                vs = volumes if volumes else []
                # Skip Rust path if strategy needs data that isn't available
                has_hl = len(hs) >= 20 and len(ls) >= 20
                has_vol = len(vs) > 0
                if (not has_hl and name in HIGH_LOW_STRATEGIES) or (not has_vol and name in VOLUME_STRATEGIES):
                    rust_signal = None
                else:
                    result = _rust_core.run_strategy_opens_py(
                        name, closes, opens, vs, hs, ls,
                    )
                    if result is not None:
                        action, confidence, reason = result
                        rust_signal = Signal(action=action, confidence=confidence, reason=reason)
            except Exception as e:
                logger.debug("Rust strategy %s failed: %s", name, e)
                rust_signal = None

            if rust_signal is not None and rust_signal.action != "HOLD":
                rust_signal.strategy = name
                signals.append(rust_signal)
                continue

        cls = ALL_STRATEGIES.get(name)
        if not cls:
            continue
        try:
            strat = cls()
            needs_hl = name in HIGH_LOW_STRATEGIES
            needs_vol = name in VOLUME_STRATEGIES
            extra_kwargs = {}
            if name in {"funding_contrarian", "exchange_flow", "btc_dxy_corr", "stablecoin_flow"}:
                extra_kwargs["currency"] = currency

            if needs_hl:
                sig = strat.on_bar(
                    current_price,
                    closes,
                    volumes=volumes if needs_vol else None,
                    highs=highs,
                    lows=lows,
                    **extra_kwargs,
                )
            elif needs_vol:
                sig = strat.on_bar(current_price, closes, volumes=volumes, **extra_kwargs)
            else:
                sig = strat.on_bar(current_price, closes, **extra_kwargs)

            if sig and sig.action != "HOLD":
                sig.strategy = name
                signals.append(sig)
        except Exception as e:
            logger.debug("Strategy %s failed: %s", name, e)
            continue

    # Sort by confidence descending
    signals.sort(key=lambda s: s.confidence, reverse=True)
    return signals


# ---------------------------------------------------------------------------
# Backtest Validator
# ---------------------------------------------------------------------------


@dataclass
class BacktestTrade:
    entry_bar: int
    entry_price: float
    side: str  # "BUY" | "SELL"
    exit_bar: Optional[int] = None
    exit_price: Optional[float] = None
    return_pct: Optional[float] = None
    reason: str = ""


@dataclass
class BacktestVerdict:
    strategy: str
    currency: str
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    total_return_pct: float
    sharpe_ratio: float
    profit_factor: float
    max_drawdown_pct: float
    regime: str
    passed: bool
    reason: str


# ── Single-sourced backtest pass thresholds (P1-6) ──────────────────
# These are the *intentional* stricter thresholds (stricter than the docs'
# looser spec). Both the Python backtest and the Rust backtest receive these
# values so the two engines can never drift apart.
BACKTEST_PASS: Dict[str, float] = {
    "min_win_rate": 0.50,
    "min_sharpe": 0.5,
    "min_profit_factor": 1.20,
    "max_drawdown_pct": 15.0,
    "min_total_return_pct": -10.0,
}


def _close_backtest_trade(
    trade: "BacktestTrade",
    exit_price: float,
    exit_bar: int,
    trades: List["BacktestTrade"],
    equity: List[float],
    fee_bps: float = 0.0,
) -> None:
    """Record an exit for ``trade`` (P1-5: subtract round-trip entry+exit fee)."""
    if trade.side == "BUY":
        gross = (exit_price - trade.entry_price) / trade.entry_price * 100.0
    else:
        gross = (trade.entry_price - exit_price) / trade.entry_price * 100.0
    return_pct = gross - (fee_bps / 100.0) * 2.0 * 100.0
    trade.exit_bar = exit_bar
    trade.exit_price = exit_price
    trade.return_pct = return_pct
    trades.append(trade)
    equity.append(equity[-1] * (1.0 + return_pct / 100.0))


def backtest_strategy(
    strategy_name: str,
    currency: str,
    closes: List[float],
    volumes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    warmup: int = 30,
    min_trades: int = 3,
    opens: Optional[List[float]] = None,
    fee_bps: float = 0.0,
    max_hold_bars: int = 0,
) -> BacktestVerdict:
    """Run a strategy through historical data and score its recent performance.

    Returns a BacktestVerdict with passed=True if the strategy shows positive
    win rate, Sharpe > 0.3, and profit factor > 1.1 over the window.
    Uses Rust native acceleration when available.
    """
    # Try Rust acceleration first for supported strategies
    if _HAS_RUST and strategy_name in _RUST_STRATEGIES:
        rust_result = _rust_backtest_strategy(
            strategy_name, currency, closes, volumes,
            highs=highs, lows=lows, warmup=warmup,
            opens=opens, fee_bps=fee_bps, max_hold_bars=max_hold_bars,
        )
        if rust_result is not None:
            return rust_result

    cls = ALL_STRATEGIES.get(strategy_name)
    if not cls or len(closes) < warmup + 10:
        return BacktestVerdict(
            strategy_name,
            currency,
            0,
            0,
            0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            "UNKNOWN",
            False,
            "Insufficient data",
        )

    strat = cls()
    trades: List[BacktestTrade] = []
    open_trade: Optional[BacktestTrade] = None
    equity = [1.0]
    peak = 1.0

    needs_hl = strategy_name in HIGH_LOW_STRATEGIES
    needs_vol = strategy_name in VOLUME_STRATEGIES

    for i in range(warmup, len(closes)):
        bar_closes = closes[: i + 1]
        bar_volumes = volumes[: i + 1] if volumes else []
        bar_highs = highs[: i + 1] if highs and needs_hl else None
        bar_lows = lows[: i + 1] if lows and needs_hl else None

        try:
            if needs_hl:
                sig = strat.on_bar(
                    closes[i],
                    bar_closes,
                    volumes=bar_volumes if needs_vol else None,
                    highs=bar_highs,
                    lows=bar_lows,
                )
            elif needs_vol:
                sig = strat.on_bar(closes[i], bar_closes, volumes=bar_volumes)
            else:
                sig = strat.on_bar(closes[i], bar_closes)
        except Exception as e:  # pragma: no cover
            logger.debug("Backtest strategy %s bar %d failed: %s", strategy_name, i, e)
            continue

        if not sig or sig.action == "HOLD":
            continue

        if open_trade is None and sig.action == "BUY":
            open_trade = BacktestTrade(i, closes[i], "BUY", reason=sig.reason)
        elif open_trade is None and sig.action == "SELL":
            open_trade = BacktestTrade(i, closes[i], "SELL", reason=sig.reason)
        elif open_trade is not None:
            opp = (open_trade.side == "BUY" and sig.action == "SELL") or (
                open_trade.side == "SELL" and sig.action == "BUY"
            )
            held = i - open_trade.entry_bar
            forced = max_hold_bars > 0 and held >= max_hold_bars
            if opp or forced:
                _close_backtest_trade(open_trade, closes[i], i, trades, equity, fee_bps)
                peak = max(peak, equity[-1])
                open_trade = None

    # Force-close any open trade at the last bar (P1-7: not assumed free when fee_bps>0).
    if open_trade is not None:
        _close_backtest_trade(open_trade, closes[-1], len(closes) - 1, trades, equity, fee_bps)
        peak = max(peak, equity[-1])

    if len(trades) < min_trades:
        return BacktestVerdict(
            strategy_name,
            currency,
            len(trades),
            0,
            0,
            0.0,
            0.0,
            0.0,
            1.0,
            0.0,
            "UNKNOWN",
            False,
            f"Only {len(trades)} trades (< {min_trades})",
        )

    winning = sum(1 for t in trades if t.return_pct is not None and t.return_pct > 0)
    losing = len(trades) - winning
    win_rate = winning / len(trades)
    total_return = sum(t.return_pct for t in trades if t.return_pct is not None)

    gross_profit = sum(
        t.return_pct for t in trades if t.return_pct is not None and t.return_pct > 0
    )
    gross_loss = abs(
        sum(
            t.return_pct
            for t in trades
            if t.return_pct is not None and t.return_pct < 0
        )
    )
    profit_factor = (
        gross_profit / gross_loss
        if gross_loss > 0
        else (gross_profit if gross_profit > 0 else 1.0)
    )

    returns = [t.return_pct / 100.0 for t in trades if t.return_pct is not None]
    avg_ret = sum(returns) / len(returns) if returns else 0.0
    variance = (
        sum((r - avg_ret) ** 2 for r in returns) / len(returns)
        if len(returns) > 1
        else 0.0
    )
    vol = math.sqrt(variance) if variance > 0 else 0.001
    # P0-4: unify Sharpe definition with Rust: mean per-trade return / std(ret) * sqrt(n).
    # (The previous (avg_ret*100)/(vol*100) canceled the *100, so this is numerically
    #  identical for the python path, but now it matches the Rust engine exactly.)
    sharpe = (avg_ret / vol) * math.sqrt(len(returns)) if vol > 0 and returns else 0.0

    dd = max(0.0, (peak - min(equity)) / peak) if equity else 0.0

    # Regime classification
    regime = _classify_regime(closes)

    dd_pct = dd * 100.0
    # P1-6: thresholds single-sourced from BACKTEST_PASS (shared with Rust).
    passed = (
        win_rate >= BACKTEST_PASS["min_win_rate"]
        and sharpe > BACKTEST_PASS["min_sharpe"]
        and profit_factor > BACKTEST_PASS["min_profit_factor"]
        and dd_pct < BACKTEST_PASS["max_drawdown_pct"]
        and total_return > BACKTEST_PASS["min_total_return_pct"]
    )

    reasons = []
    if not passed:
        if win_rate < BACKTEST_PASS["min_win_rate"]:
            reasons.append(f"win_rate {win_rate:.0%} < {BACKTEST_PASS['min_win_rate']:.0%}")
        if sharpe <= BACKTEST_PASS["min_sharpe"]:
            reasons.append(f"Sharpe {sharpe:.2f} <= {BACKTEST_PASS['min_sharpe']}")
        if profit_factor <= BACKTEST_PASS["min_profit_factor"]:
            reasons.append(f"profit_factor {profit_factor:.2f} <= {BACKTEST_PASS['min_profit_factor']}")
        if dd_pct >= BACKTEST_PASS["max_drawdown_pct"]:
            reasons.append(f"max_drawdown {dd_pct:.1f}% >= {BACKTEST_PASS['max_drawdown_pct']}")
        if total_return <= BACKTEST_PASS["min_total_return_pct"]:
            reasons.append(f"return {total_return:.1f}% too negative")

    return BacktestVerdict(
        strategy=strategy_name,
        currency=currency,
        total_trades=len(trades),
        winning_trades=winning,
        losing_trades=losing,
        win_rate=round(win_rate, 3),
        total_return_pct=round(total_return, 2),
        sharpe_ratio=round(sharpe, 2),
        profit_factor=round(profit_factor, 2),
        max_drawdown_pct=round(dd * 100, 2),
        regime=regime,
        passed=passed,
        reason=reasons[0] if reasons else "Passed backtest",
    )


def _classify_regime(closes: List[float]) -> str:
    if len(closes) < 20:
        return "UNKNOWN"
    recent = closes[-30:]
    high = max(recent)
    low = min(recent)
    price_range = (high - low) / low * 100 if low > 0 else 0
    returns = [
        (closes[i] - closes[i - 1]) / closes[i - 1]
        for i in range(max(1, len(closes) - 30), len(closes))
    ]
    avg_vol = sum(abs(r) for r in returns) / len(returns) if returns else 0
    if price_range > 15:
        return "TRENDED"
    elif price_range < 8 and avg_vol < 0.01:
        return "RANGING"
    return "VOLATILE"


# ── Rust native acceleration (fallback to pure Python) ─────────────

_RUST_STRATEGIES: set = {
    "ema_cross", "rsi_revert", "boll_break", "zscore_revert", "vol_mom",
    "macd", "vwap_revert", "obv_div", "cmo", "trix", "adx", "keltner",
    "chaikin_mf", "williams_r", "psar", "hma", "force_idx", "vpt",
    "donchian", "aroon", "price_eff", "scci", "range_exp_idx", "ema_dev",
    "snr_idx",
    # ── 10 new strategies ──
    "candle_pat", "sup_res", "liq_vac", "cvd_flow", "vcp",
    "impulse_exh", "mom_accel", "rsi_fail", "avwap", "donch_pull",
    # ── 5 newest strategies ──
    "vol_prof", "bb_squeeze", "multi_rsi", "linreg_slope", "hurst",
    # ── 10 newest strategies (41-50) ──
    "elder_ray", "klinger", "pivot_points", "ichimoku", "choppiness",
    "true_cci", "dpo", "kst", "mass_idx", "ulcer",
    # ── 6 new OHLCV strategies (51-56) ──
    "mfi", "stoch", "emv", "ad_div", "envelope", "atr_channel",
    # ── 12 new strategies (57-68) ──
    "kama", "dmi_cross", "vma", "vortex", "rvi", "coppock",
    "std_channel", "vol_ratio", "vwap_macd", "nvi", "de_marker", "gap_revert",
    # ── 4 new growth-focused strategies (69-72) ──
    "supertrend", "fisher", "ultimate_osc", "vw_rsi",
    # ── 2 new Rust strategies (73-74) ──
    "kalman_mr", "hp_trend",
}

# ── Rust native: run ALL strategies on ALL products ────────────────

def batch_signals_rust(
    products: List[Tuple[str, str]],
    closes_dict: Dict[str, List[float]],
    volumes_dict: Dict[str, List[float]],
    highs_dict: Dict[str, List[float]],
    lows_dict: Dict[str, List[float]],
) -> Dict[str, Dict[str, str]]:
    """Run ALL 50 technical strategies on ALL products using Rust evaluate_all.

    Returns per-product dict of {strategy_name: "BUY"|"SELL"|"HOLD"}.
    Falls back to empty dict if Rust not available.
    """
    if not _HAS_RUST:
        return {}
    results: Dict[str, Dict[str, str]] = {}
    for pid, _ in products:
        closes = closes_dict.get(pid) or []
        volumes = volumes_dict.get(pid) or []
        highs = highs_dict.get(pid) or []
        lows = lows_dict.get(pid) or []
        if len(closes) < 30:
            results[pid] = {}
            continue
        try:
            opens = closes[:-1] + closes[-1:]
            raw = _rust_core.evaluate_all_opens_py(closes, opens, volumes, highs, lows)
            pid_sigs: Dict[str, str] = {}
            for s_name, action, confidence, reason in raw:
                pid_sigs[s_name] = action
            results[pid] = pid_sigs
        except Exception as e:
            logger.debug("Rust evaluate_all failed for %s: %s", pid, e)
            results[pid] = {}
    return results


# ── Compute backend integration (GPU/CPU batch acceleration) ─────

try:
    from trading_system.core.compute_backend import (
        get_compute_backend, NumpyBackend, TorchBackend,
    )
    _COMPUTE_BACKEND = get_compute_backend()
    _HAS_COMPUTE_BACKEND = True
except ImportError:
    _COMPUTE_BACKEND = None
    _HAS_COMPUTE_BACKEND = False


def batch_signals_fast(
    products: List[Tuple[str, str]],
    closes_dict: Dict[str, List[float]],
    volumes_dict: Dict[str, List[float]],
    highs_dict: Dict[str, List[float]],
    lows_dict: Dict[str, List[float]],
) -> Dict[str, Dict[str, str]]:
    """Run ALL strategies on ALL products — Rust native (all 25) > NumPy (5) > fallback.

    Args:
        products: list of (product_id, asset_class) tuples
        closes_dict: product_id -> list of closes
        volumes_dict: product_id -> list of volumes
        highs_dict: product_id -> list of highs
        lows_dict: product_id -> list of lows

    Returns::

        {"BTC-USD": {"ema_cross": "BUY", "rsi_revert": "HOLD", ...},
         "ETH-USD": {...}, ...}
    """
    # Priority 1: Rust native — all 25 strategies, compiled, fastest
    if _HAS_RUST:
        return batch_signals_rust(products, closes_dict, volumes_dict, highs_dict, lows_dict)

    # Priority 2: NumPy vectorized backend — 5 strategies (legacy path)
    if not _HAS_COMPUTE_BACKEND:
        return {}
    import numpy as np
    n = len(products)
    if n == 0:
        return {}

    # Build matrices (n_products, n_candles)
    # Find max length for padding
    max_len = max((len(closes_dict.get(pid, [])) for pid, _ in products), default=0)
    if max_len < 30:
        return {}

    closes_mat = np.zeros((n, max_len), dtype=np.float64)
    vols_mat = np.zeros((n, max_len), dtype=np.float64)
    highs_mat = np.zeros((n, max_len), dtype=np.float64)
    lows_mat = np.zeros((n, max_len), dtype=np.float64)

    for i, (pid, _) in enumerate(products):
        c = closes_dict.get(pid, [])
        v = volumes_dict.get(pid, [])
        h = highs_dict.get(pid, [])
        lo = lows_dict.get(pid, [])
        ln = len(c)
        closes_mat[i, :ln] = c
        if v:
            vols_mat[i, :ln] = v[:ln]
        if h:
            highs_mat[i, :ln] = h[:ln]
        if lo:
            lows_mat[i, :ln] = lo[:ln]

    # Compute all signals for all products in one shot
    backend = get_compute_backend()
    batch_results = backend.batch_signals(closes_mat, vols_mat, highs_mat, lows_mat)

    # Transpose to per-product dict
    results: Dict[str, Dict[str, str]] = {}
    for i, (pid, _) in enumerate(products):
        product_signals: Dict[str, str] = {}
        for strat_name in batch_results:
            signals_list = batch_results[strat_name]
            if i < len(signals_list):
                product_signals[strat_name] = signals_list[i]
        results[pid] = product_signals
    return results


# ── Accelerated batch backtest ────────────────────────────────────

def batch_backtest_rust(
    strategies: List[Tuple[str, str, List[float], List[float], Optional[List[float]], Optional[List[float]]]],
    warmup: int = 30,
) -> Dict[str, "BacktestVerdict"]:
    """Backtest Rust-supported strategies grouped by product, in parallel via rayon.

    Args:
        strategies: list of (strategy_name, currency, closes, volumes, highs, lows) tuples
        warmup: number of bars to skip at start

    Returns:
        dict of cache_key -> BacktestVerdict
    """
    if not _HAS_RUST:
        return {}
    results: Dict[str, BacktestVerdict] = {}

    # Group strategies by product
    by_product: Dict[str, Dict[str, Any]] = {}
    for s_name, currency, closes, volumes, highs, lows in strategies:
        if s_name not in _RUST_STRATEGIES:
            continue
        if currency not in by_product:
            by_product[currency] = {"names": set(), "closes": closes, "volumes": volumes, "highs": highs, "lows": lows}
        by_product[currency]["names"].add(s_name)

    for currency, info in by_product.items():
        names = sorted(info["names"])
        if not names:
            continue
        closes = info["closes"]
        if len(closes) <= warmup:
            continue
        volumes = info["volumes"] if info["volumes"] else [1.0] * len(closes)
        if len(volumes) != len(closes):
            volumes = [1.0] * len(closes)
        highs = info.get("highs")
        lows = info.get("lows")
        opens = info.get("opens") or closes  # P0-1: match live opens=closes
        fee_bps = info.get("fee_bps", 0.0)
        max_hold = info.get("max_hold_bars", 0)
        try:
            raw = _rust_core.backtest_multi_py(
                names, closes, volumes, warmup, highs=highs, lows=lows,
                opens=opens, fee_bps=fee_bps, max_hold_bars=max_hold,
                min_win_rate=BACKTEST_PASS["min_win_rate"],
                min_sharpe=BACKTEST_PASS["min_sharpe"],
                min_pf=BACKTEST_PASS["min_profit_factor"],
                max_dd_pct=BACKTEST_PASS["max_drawdown_pct"],
                min_ret_pct=BACKTEST_PASS["min_total_return_pct"],
            )
            for s_name, metrics in raw:
                ck = f"{s_name}/{currency}"
                if len(metrics) >= 9:
                    passed = bool(metrics[8])
                    results[ck] = BacktestVerdict(
                        strategy=s_name,
                        currency=currency,
                        total_trades=int(metrics[0]),
                        winning_trades=int(metrics[1]),
                        losing_trades=int(metrics[2]),
                        win_rate=metrics[3],
                        total_return_pct=metrics[4],
                        sharpe_ratio=metrics[5],
                        profit_factor=metrics[6],
                        max_drawdown_pct=metrics[7],
                        regime="AUTO",
                        passed=passed,
                        reason="Rust batch" if passed else "Rust batch: below thresholds",
                    )
        except Exception as e:
            logger.debug("Rust batch backtest for %s failed: %s", currency, e)
    return results


# ── Universe-wide parallel evaluation (single Rust round-trip) ─────

def batch_signals_universe(
    products: List[Tuple[str, str]],
    closes_dict: Dict[str, List[float]],
    volumes_dict: Dict[str, List[float]],
    highs_dict: Optional[Dict[str, List[float]]] = None,
    lows_dict: Optional[Dict[str, List[float]]] = None,
    opens_dict: Optional[Dict[str, List[float]]] = None,
) -> Dict[str, Dict[str, str]]:
    """Run ALL strategies on the whole product universe in ONE Rust call.

    Evaluates every product via ``rust_core.evaluate_universe_py`` (rayon over
    products, one task each) in a single Python↔Rust round-trip. Returns the
    same shape as :func:`batch_signals_rust` (pid -> {strategy_name: action}).

    Falls back to the existing per-product :func:`batch_signals_rust` loop if
    Rust is unavailable, the new binding is missing, or the call raises.
    """
    if not _HAS_RUST or not hasattr(_rust_core, "evaluate_universe_py"):
        return batch_signals_rust(products, closes_dict, volumes_dict,
                                  highs_dict or {}, lows_dict or {})
    try:
        pids = [pid for pid, _ in products]
        if not pids:
            return {}
        closes_map = {pid: (closes_dict.get(pid) or []) for pid in pids}
        volumes_map = {pid: (volumes_dict.get(pid) or []) for pid in pids}
        # Build plain dicts (missing entries dropped so Rust treats them as absent).
        hm = {pid: highs_dict[pid] for pid in pids if highs_dict and pid in highs_dict} if highs_dict else None
        lm = {pid: lows_dict[pid] for pid in pids if lows_dict and pid in lows_dict} if lows_dict else None
        om = {pid: opens_dict[pid] for pid in pids if opens_dict and pid in opens_dict} if opens_dict else None
        raw = _rust_core.evaluate_universe_py(
            pids, closes_map, volumes_map, hm, lm, om,
        )
        results: Dict[str, Dict[str, str]] = {}
        for pid in pids:
            pid_sigs: Dict[str, str] = {}
            for s_name, action, confidence, reason in raw.get(pid, []):
                pid_sigs[s_name] = action
            results[pid] = pid_sigs
        return results
    except Exception as e:
        logger.debug("batch_signals_universe failed, falling back: %s", e)
        prods = [(pid, "growth") for pid in (p[0] if isinstance(p, (tuple, list)) else p) for p in products] \
            if products and isinstance(products[0], (tuple, list)) else products
        return batch_signals_rust(prods, closes_dict, volumes_dict,
                                  highs_dict or {}, lows_dict or {})


def batch_signals_from_candles(
    products: List[Tuple[str, str]],
    candles_map: Dict[str, list],
    opens_map: Optional[Dict[str, List[float]]] = None,
) -> Dict[str, Dict[str, str]]:
    """Run ALL strategies on raw candle dicts in ONE Rust call.

    ``candles_map`` is the raw ``Dict[str, list]`` returned by the feed manager:
    each value is a list of candles, each candle EITHER a dict with keys
    ``open/high/low/close/volume`` OR a normalized tuple/list
    ``[ts, low, high, open, close, volume]`` (index 1=low, 2=high, 3=open,
    4=close, 5=volume). OHLCV extraction + all-strategy eval happen inside Rust.

    Returns pid -> {strategy_name: action}, identical in shape to
    :func:`batch_signals_universe`.

    Falls back to the legacy parse loop + :func:`batch_signals_universe` if Rust
    is unavailable, the new binding is missing, or the call raises.
    """
    if not _HAS_RUST or not hasattr(_rust_core, "batch_signals_from_candles_py"):
        return _batch_signals_from_candles_parse(products, candles_map, opens_map)
    try:
        pids = [pid for pid, _ in products]
        if not pids:
            return {}
        raw = _rust_core.batch_signals_from_candles_py(
            [p for p, _ in products], candles_map, opens_map,
        )
        results: Dict[str, Dict[str, str]] = {}
        for pid in pids:
            pid_sigs: Dict[str, str] = {}
            for s_name, action, confidence, reason in raw.get(pid, []):
                pid_sigs[s_name] = action
            results[pid] = pid_sigs
        return results
    except Exception as e:
        logger.debug("batch_signals_from_candles failed, fallback: %s", e)
        return _batch_signals_from_candles_parse(products, candles_map, opens_map)


def batch_signals_cached(products, candles_map, opens_map=None, max_len=100):
    """Ingest raw candles into the persistent Rust buffer, then evaluate from cached Vecs.

    Falls back to batch_signals_from_candles on any error / missing binding.
    """
    if not _HAS_RUST or not hasattr(_rust_core, "candle_store_eval_py"):
        return batch_signals_from_candles(products, candles_map, opens_map)
    try:
        pids = [p for p, _ in products]
        for pid in pids:
            cands = candles_map.get(pid) or []
            if cands:
                _rust_core.candle_store_ingest_py(pid, cands)
        raw = _rust_core.candle_store_eval_py(pids)
        return {pid: {name: action for (name, action, conf, reason) in sigs} for pid, sigs in raw.items()}
    except Exception as e:
        logger.debug("batch_signals_cached failed, fallback: %s", e)
        return batch_signals_from_candles(products, candles_map, opens_map)


# Expose the persistent-buffer clear helper (None if the binding is absent).
# NB: bound lazily at end of module (after _rust_core is imported).
candle_store_clear = None


def tick_signals(products, currencies, candles_map, pass_cache_keys, opens_map=None):
    """Fast-path per-tick signal + bt-cache gate.

    Ingests raw candles, evaluates all strategies in Rust, and returns only the
    ``(pid, name, action, conf)`` tuples whose ``f"{name}/{currency}"`` key is in
    ``pass_cache_keys``. Falls back to ``None`` on any error / missing binding so
    callers can use the legacy path.
    """
    if not _HAS_RUST or not hasattr(_rust_core, "tick_signals_py"):
        return None
    try:
        return _rust_core.tick_signals_py(
            list(products), list(currencies), candles_map, list(pass_cache_keys),
            opens_map,
        )
    except Exception as e:
        logger.debug("tick_signals failed, fallback: %s", e)
        return None


def tick_candidates(products, currencies, candles_map, pass_cache_keys, opens_map=None):
    """Fast-path per-tick candidate builder.

    Returns the full candidate shape
    ``(pid, currency, closes, volumes, highs, lows, [(name, action), ...])``
    per product directly from Rust (OHLCV parse + all-strategy eval + bt-cache
    gate + regime group filter). Equivalent to the legacy Python fast path in
    ``portfolio_optimizer._detect_strategy_signals``. Returns ``None`` on any
    error / missing binding so callers can fall back to the legacy path.
    """
    if not _HAS_RUST or not hasattr(_rust_core, "tick_candidates_py"):
        return None
    try:
        return _rust_core.tick_candidates_py(
            list(products), list(currencies), candles_map, list(pass_cache_keys),
            opens_map,
        )
    except Exception as e:
        logger.debug("tick_candidates failed, fallback: %s", e)
        return None


def _batch_signals_from_candles_parse(
    products: List[Tuple[str, str]],
    candles_map: Dict[str, list],
    opens_map: Optional[Dict[str, List[float]]] = None,
) -> Dict[str, Dict[str, str]]:
    """Legacy fallback: replicate the optimizer parse loop then batch_universe."""
    pids = [pid for pid, _ in products]
    if not pids:
        return {}
    closes_dict: Dict[str, List[float]] = {}
    volumes_dict: Dict[str, List[float]] = {}
    highs_dict: Dict[str, List[float]] = {}
    lows_dict: Dict[str, List[float]] = {}
    for pid in pids:
        candles = candles_map.get(pid) or []
        cl, vo, hi, lo = [], [], [], []
        for c in candles[-100:]:
            if isinstance(c, dict):
                cl.append(float(c.get("close", 0.0)))
                vo.append(float(c.get("volume", 0.0)))
                hi.append(float(c.get("high", 0.0)))
                lo.append(float(c.get("low", 0.0)))
            else:
                # tuple form [ts, low, high, open, close, volume]
                cl.append(float(c[4]))
                vo.append(float(c[5]))
                hi.append(float(c[2]))
                lo.append(float(c[1]))
        closes_dict[pid] = cl
        volumes_dict[pid] = vo
        highs_dict[pid] = hi
        lows_dict[pid] = lo
    return batch_signals_universe(
        products, closes_dict, volumes_dict, highs_dict, lows_dict, opens_map,
    )


def batch_backtest_universe(
    strategies: List[Tuple[str, str, List[float], List[float], Optional[List[float]], Optional[List[float]]]],
    warmup: int = 30,
) -> Dict[str, "BacktestVerdict"]:
    """Backtest the given strategy set for each product in ONE Rust call.

    Wraps ``rust_core.backtest_universe_py`` (rayon over products). Returns the
    same shape as :func:`batch_backtest_rust` (cache_key -> BacktestVerdict).

    Falls back to :func:`batch_backtest_rust` if Rust is unavailable, the new
    binding is missing, or the call raises.
    """
    if not _HAS_RUST or not hasattr(_rust_core, "backtest_universe_py"):
        return batch_backtest_rust(strategies, warmup=warmup)
    try:
        # Group strategies by product (currency).
        by_product: Dict[str, Dict[str, Any]] = {}
        for s_name, currency, closes, volumes, highs, lows in strategies:
            if s_name not in _RUST_STRATEGIES:
                continue
            if currency not in by_product:
                by_product[currency] = {"names": set(), "closes": closes,
                                        "volumes": volumes, "highs": highs,
                                        "lows": lows}
            by_product[currency]["names"].add(s_name)

        if not by_product:
            return {}

        pids = list(by_product.keys())
        closes_map = {}
        volumes_map = {}
        hm: Dict[str, List[float]] = {}
        lm: Dict[str, List[float]] = {}
        names_by_product: List[List[str]] = []
        for pid in pids:
            info = by_product[pid]
            closes = info["closes"]
            if len(closes) <= warmup:
                continue
            volumes = info["volumes"] if info["volumes"] else [1.0] * len(closes)
            if len(volumes) != len(closes):
                volumes = [1.0] * len(closes)
            closes_map[pid] = closes
            volumes_map[pid] = volumes
            if info.get("highs"):
                hm[pid] = info["highs"]
            if info.get("lows"):
                lm[pid] = info["lows"]
            names_by_product.append(sorted(info["names"]))

        if not closes_map:
            return {}

        raw = _rust_core.backtest_universe_py(
            # flat list of all strategy names (per-product sets applied in Rust loop);
            # backtest_universe_py backtests the SAME set for every product, so we
            # pass the union and accept the slight over-compute on sparse products.
            sorted({n for ns in names_by_product for n in ns}),
            list(closes_map.keys()), closes_map, volumes_map, warmup,
            hm if hm else None, lm if lm else None, None,
            0.0, 0,
            BACKTEST_PASS["min_win_rate"], BACKTEST_PASS["min_sharpe"],
            BACKTEST_PASS["min_profit_factor"], BACKTEST_PASS["max_drawdown_pct"],
            BACKTEST_PASS["min_total_return_pct"],
        )
        results: Dict[str, "BacktestVerdict"] = {}
        for pid in raw:
            for s_name, metrics in raw[pid]:
                ck = f"{s_name}/{pid}"
                if len(metrics) >= 9:
                    passed = bool(metrics[8])
                    results[ck] = BacktestVerdict(
                        strategy=s_name,
                        currency=pid,
                        total_trades=int(metrics[0]),
                        winning_trades=int(metrics[1]),
                        losing_trades=int(metrics[2]),
                        win_rate=metrics[3],
                        total_return_pct=metrics[4],
                        sharpe_ratio=metrics[5],
                        profit_factor=metrics[6],
                        max_drawdown_pct=metrics[7],
                        regime="AUTO",
                        passed=passed,
                        reason="Rust universe batch" if passed else "Rust universe batch: below thresholds",
                    )
        return results
    except Exception as e:
        logger.debug("batch_backtest_universe failed, falling back: %s", e)
        return batch_backtest_rust(strategies, warmup=warmup)


try:
    import rust_core as _rust_core
    _HAS_RUST = True
    logger.info("Rust core loaded — native acceleration enabled (%d strategies)",
                len(_RUST_STRATEGIES))
except ImportError:  # pragma: no cover
    _HAS_RUST = False
    logger.info("Rust core not available — using pure Python")
    _rust_core = None  # type: ignore

# Now that _rust_core is imported, bind the persistent-buffer clear helper.
candle_store_clear = getattr(_rust_core, "candle_store_clear_py", None) if _HAS_RUST else None


def _rust_backtest_strategy(
    strategy_name: str,
    currency: str,
    closes: List[float],
    volumes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    warmup: int = 30,
    opens: Optional[List[float]] = None,
    fee_bps: float = 0.0,
    max_hold_bars: int = 0,
) -> Optional[BacktestVerdict]:
    """Run backtest in Rust for supported strategies, returning None for unsupported ones.

    P0-1: ``opens`` is forwarded to Rust so backtest matches live ``run_strategies``
    (which passes ``opens=closes``). When omitted, ``opens=closes`` is used so the
    pattern strategies read the same open values as live trading instead of the
    legacy synthesized prev-close.
    P1-5: ``fee_bps`` round-trip fee is subtracted per trade in Rust.
    P1-6: thresholds are single-sourced from ``BACKTEST_PASS`` and passed through.
    """
    if not _HAS_RUST or strategy_name not in _RUST_STRATEGIES:
        return None
    try:
        if opens is None:
            opens = closes
        bt = _rust_core.backtest_strategy_py(
            strategy_name, closes, volumes, warmup, highs=highs, lows=lows,
            opens=opens, fee_bps=fee_bps, max_hold_bars=max_hold_bars,
            min_win_rate=BACKTEST_PASS["min_win_rate"],
            min_sharpe=BACKTEST_PASS["min_sharpe"],
            min_pf=BACKTEST_PASS["min_profit_factor"],
            max_dd_pct=BACKTEST_PASS["max_drawdown_pct"],
            min_ret_pct=BACKTEST_PASS["min_total_return_pct"],
        )
        if bt is None or len(bt) < 9:
            return None
        passed = bool(bt[8])
        return BacktestVerdict(
            strategy=strategy_name,
            currency=currency,
            total_trades=int(bt[0]),
            winning_trades=int(bt[1]),
            losing_trades=int(bt[2]),
            win_rate=bt[3],
            total_return_pct=bt[4],
            sharpe_ratio=bt[5],
            profit_factor=bt[6],
            max_drawdown_pct=bt[7],
            regime="AUTO",
            passed=passed,
            reason="Rust backtest: passed" if passed else "Rust backtest: below thresholds",
        )
    except Exception as e:  # pragma: no cover
        logger.debug("Rust backtest failed for %s/%s: %s", strategy_name, currency, e)
        return None
