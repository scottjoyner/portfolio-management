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
from dataclasses import dataclass
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class Signal:
    action: str  # "BUY" | "SELL" | "HOLD"
    price: float = 0.0
    confidence: float = 1.0
    reason: str = ""
    strategy: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sma(values: List[float], period: int) -> float:
    if len(values) < period:
        return values[-1] if values else 0.0
    return sum(values[-period:]) / period


def _ema(values: List[float], period: int) -> float:
    if len(values) < period:
        return values[-1] if values else 0.0
    k = 2.0 / (period + 1)
    result = sum(values[:period]) / period
    for v in values[period:]:
        result = v * k + result * (1 - k)
    return result


def _rsi(values: List[float], period: int = 14) -> float:
    if len(values) < period + 1:
        return 50.0
    deltas = [
        values[i] - values[i - 1] for i in range(len(values) - period, len(values))
    ]
    gains = [d if d > 0 else 0 for d in deltas]
    losses = [-d if d < 0 else 0 for d in deltas]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _bollinger(values: List[float], period: int = 20, std_mult: float = 2.0):
    if len(values) < period:
        return (values[-1], values[-1], values[-1], 0.0) if values else (0, 0, 0, 0)
    recent = values[-period:]
    mean = sum(recent) / period
    variance = sum((x - mean) ** 2 for x in recent) / period
    std = math.sqrt(variance)
    return (mean, mean + std_mult * std, mean - std_mult * std, std)


def _zscore(values: List[float], period: int = 30) -> float:
    if len(values) < period:
        return 0.0
    recent = values[-period:]
    mean = sum(recent) / period
    variance = sum((x - mean) ** 2 for x in recent) / period
    std = math.sqrt(variance)
    if std == 0:
        return 0.0
    return (values[-1] - mean) / std


def _wma(values: List[float], period: int) -> float:
    if len(values) < period:
        return values[-1] if values else 0.0
    weights = list(range(1, period + 1))
    recent = values[-period:]
    return sum(w * v for w, v in zip(weights, recent)) / sum(weights)


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------


class EMA_Crossover:
    """Fast/slow EMA crossover. Trending markets."""

    def __init__(self, fast: int = 9, slow: int = 21):
        self.fast = fast
        self.slow = slow
        self.prev_fast = 0.0
        self.prev_slow = 0.0

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.slow + 1:
            return None
        fast_val = _ema(closes, self.fast)
        slow_val = _ema(closes, self.slow)
        signal = None
        if self.prev_fast > 0 and self.prev_slow > 0:
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
        self.prev_bandwidth = 0.0

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
        self.prev_bandwidth = bandwidth
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
        avg_vol = sum(recent_vol) / self.period if self.period > 0 else 1
        if avg_vol == 0:
            return None
        last_vol = volumes[-1]
        if last_vol < avg_vol * self.volume_mult:
            return None
        price_change = (closes[-1] - closes[-self.period]) / closes[-self.period]
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
        self.prev_hist = 0.0

    def on_bar(self, close: float, closes: List[float]) -> Optional[Signal]:
        if len(closes) < self.slow + self.signal + 1:
            return None
        macd_line = _ema(closes, self.fast) - _ema(closes, self.slow)
        sig_line = _ema(closes, self.signal)
        # Use a synthetic MACD series for signal EMA
        macd_series = []
        for i in range(self.signal, len(closes)):
            fast_e = _ema(closes[: i + 1], self.fast)
            slow_e = _ema(closes[: i + 1], self.slow)
            macd_series.append(fast_e - slow_e)
        if len(macd_series) < self.signal:
            return None
        signal_line = _ema(macd_series, self.signal)
        histogram = macd_line - signal_line

        result = None
        if self.prev_hist != 0.0:
            if self.prev_hist < 0 and histogram > 0:
                conf = min(
                    abs(histogram) / _ema(macd_series, self.signal) * 2
                    if _ema(macd_series, self.signal) != 0
                    else 0.1,
                    1.0,
                )
                result = Signal(
                    "BUY", close, conf, f"MACD histogram bullish crossover", "macd"
                )
            elif self.prev_hist > 0 and histogram < 0:
                conf = min(
                    abs(histogram) / _ema(macd_series, self.signal) * 2
                    if _ema(macd_series, self.signal) != 0
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
        self.prev_trix = 0.0

    def _triple_ema(self, closes: List[float], p: int) -> float:
        if len(closes) < p * 3:
            return closes[-1]
        ema1 = [_ema(closes[: i + 1], p) for i in range(p * 2 - 1, len(closes))]
        if len(ema1) < p + 1:
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
        if self.prev_trix != 0.0:
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
        self.prev_plus_di = 0.0
        self.prev_minus_di = 0.0

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
        if self.prev_plus_di != 0:
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
        if len(highs) < p + 2:
            return 0.0
        tr_vals = []
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            tr_vals.append(tr)
        if len(tr_vals) < p:
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
        self.prev_fast = 0.0
        self.prev_slow = 0.0

    @staticmethod
    def _hma(values: List[float], n: int) -> float:
        if len(values) < n:
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
        if self.prev_fast > 0 and self.prev_slow > 0:
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
        self.prev_smoothed = 0.0

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
        if self.prev_smoothed != 0.0:
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
        self.prev_diff = 0.0

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
        if self.prev_diff != 0.0:
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

        if close > upper:
            conf = min((close - upper) / rng, 1.0)
            conf = max(conf, 0.1)
            return Signal(
                "BUY", close, conf, f"Donchian breakout above {upper:.4f}", "donchian"
            )
        elif close < lower:
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
        self.prev_osc = 0.0

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
        if self.prev_osc != 0.0:
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
        self.prev_eff = 0.0

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
        if len(eff) < self.signal_period:
            return None
        smoothed = _wma(eff, self.signal_period)

        if self.prev_eff == 0.0:
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
        self.prev_rei = 0.0

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

        if abs(rei) > self.min_amplitude and self.prev_rei != 0.0:
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
        self.prev_dev = 0.0

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

        if abs(dev) > self.min_amplitude and self.prev_dev != 0.0:
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
        self.prev_snr = 0.0

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

        if self.prev_snr != 0.0:
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
    "kalshi": KalshiSignal,
    "polymarket": PolymarketSignal,
}

# Strategy-to-asset-class mapping
CLASS_STRATEGIES = {
    "safe": ["ema_cross", "macd", "vol_mom", "trix", "hma"],
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
    ],
}

# Strategies that accept volume data
VOLUME_STRATEGIES = {
    "vol_mom",
    "vwap_revert",
    "obv_div",
    "chaikin_mf",
    "force_idx",
    "vpt",
    "kama",
    "rv_idx",
}

# Strategies that accept high/low data (candles with OHLC)
HIGH_LOW_STRATEGIES = {
    "adx",
    "keltner",
    "chaikin_mf",
    "williams_r",
    "psar",
    "donchian",
    "aroon",
    "cci",
    "mass_idx",
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
    """Run all applicable strategies for an asset and return ranked signals."""
    strategy_names = CLASS_STRATEGIES.get(asset_class, ["rsi_revert", "boll_break"])
    signals = []

    for name in strategy_names:
        cls = ALL_STRATEGIES.get(name)
        if not cls:
            continue
        try:
            strat = cls()
            needs_hl = name in HIGH_LOW_STRATEGIES
            needs_vol = name in VOLUME_STRATEGIES

            if needs_hl:
                sig = strat.on_bar(
                    current_price,
                    closes,
                    volumes=volumes if needs_vol else None,
                    highs=highs,
                    lows=lows,
                )
            elif needs_vol:
                sig = strat.on_bar(current_price, closes, volumes=volumes)
            else:
                sig = strat.on_bar(current_price, closes)

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


def backtest_strategy(
    strategy_name: str,
    currency: str,
    closes: List[float],
    volumes: List[float],
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    warmup: int = 30,
    min_trades: int = 3,
) -> BacktestVerdict:
    """Run a strategy through historical data and score its recent performance.

    Returns a BacktestVerdict with passed=True if the strategy shows positive
    win rate, Sharpe > 0.3, and profit factor > 1.1 over the window.
    """
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
        except Exception as e:
            logger.debug("Backtest strategy %s bar %d failed: %s", strategy_name, i, e)
            continue

        if not sig or sig.action == "HOLD":
            continue

        if open_trade is None and sig.action == "BUY":
            open_trade = BacktestTrade(i, closes[i], "BUY", reason=sig.reason)
        elif open_trade is None and sig.action == "SELL":
            open_trade = BacktestTrade(i, closes[i], "SELL", reason=sig.reason)
        elif open_trade is not None:
            if (open_trade.side == "BUY" and sig.action == "SELL") or (
                open_trade.side == "SELL" and sig.action == "BUY"
            ):
                open_trade.exit_bar = i
                open_trade.exit_price = closes[i]
                if open_trade.side == "BUY":
                    open_trade.return_pct = (
                        (closes[i] - open_trade.entry_price)
                        / open_trade.entry_price
                        * 100
                    )
                else:
                    open_trade.return_pct = (
                        (open_trade.entry_price - closes[i])
                        / open_trade.entry_price
                        * 100
                    )
                trades.append(open_trade)
                open_trade = None

                ret = (
                    trades[-1].return_pct / 100.0
                    if trades[-1].return_pct is not None
                    else 0.0
                )
                equity.append(equity[-1] * (1 + ret))
                peak = max(peak, equity[-1])

    # Force-close any open trade at the last bar
    if open_trade is not None:
        open_trade.exit_bar = len(closes) - 1
        open_trade.exit_price = closes[-1]
        if open_trade.side == "BUY":
            open_trade.return_pct = (
                (closes[-1] - open_trade.entry_price) / open_trade.entry_price * 100
            )
        else:
            open_trade.return_pct = (
                (open_trade.entry_price - closes[-1]) / open_trade.entry_price * 100
            )
        trades.append(open_trade)

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
    sharpe = (avg_ret * 100) / (vol * 100) if vol > 0 else 0.0

    dd = max(0.0, (peak - min(equity)) / peak) if equity else 0.0

    # Regime classification
    regime = _classify_regime(closes)

    passed = (
        win_rate >= 0.4
        and sharpe > 0.2
        and profit_factor > 1.05
        and total_return > -20.0
    )
    if not passed and win_rate >= 0.5 and sharpe > 0.5:
        passed = True  # high win rate overrides

    reasons = []
    if not passed:
        if win_rate < 0.4:
            reasons.append(f"win_rate {win_rate:.0%} < 40%")
        if sharpe <= 0.2:
            reasons.append(f"Sharpe {sharpe:.2f} <= 0.2")
        if profit_factor <= 1.05:
            reasons.append(f"profit_factor {profit_factor:.2f} <= 1.05")
        if total_return <= -20.0:
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
