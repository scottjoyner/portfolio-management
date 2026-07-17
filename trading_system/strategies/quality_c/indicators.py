"""Shared, dependency-free technical indicators for the quality_c strategy package.

Pure stdlib (math only). All functions operate on plain lists of floats and
return None when the input is insufficient, so callers can decide what to do
during warmup.
"""
from __future__ import annotations

import math


def sma(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def stdev(values: list[float], mean: float) -> float:
    if not values:
        return 0.0
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(var)


def ema_last(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    k = 2.0 / (period + 1.0)
    acc = sum(values[:period]) / period
    for v in values[period:]:
        acc = v * k + acc * (1.0 - k)
    return acc


def rsi(values: list[float], period: int = 14) -> float | None:
    if len(values) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        delta = values[-i] - values[-i - 1]
        if delta >= 0:
            gains += delta
        else:
            losses -= delta
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100.0 - (100.0 / (1.0 + rs))


def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1 or len(highs) < period + 1 or len(lows) < period + 1:
        return None
    trs: list[float] = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def stochastic(closes: list[float], highs: list[float], lows: list[float], k_period: int = 14, d_period: int = 3) -> tuple[float | None, float | None]:
    if len(closes) < k_period:
        return None, None
    hh = max(highs[-k_period:])
    ll = min(lows[-k_period:])
    if hh == ll:
        k = 50.0
    else:
        k = 100.0 * (closes[-1] - ll) / (hh - ll)
    # slow %D = SMA of %K
    if len(closes) < k_period + d_period - 1:
        return k, None
    ks: list[float] = []
    for j in range(d_period):
        idx = len(closes) - 1 - j
        if idx - k_period + 1 < 0:
            return k, None
        hhh = max(highs[idx - k_period + 1: idx + 1])
        lll = min(lows[idx - k_period + 1: idx + 1])
        if hhh == lll:
            ks.append(50.0)
        else:
            ks.append(100.0 * (closes[idx] - lll) / (hhh - lll))
    if len(ks) < d_period:
        return k, None
    d = sum(ks) / len(ks)
    return k, d


def bollinger(closes: list[float], period: int, num_std: float):
    if len(closes) < period:
        return None, None, None
    window = closes[-period:]
    mean = sum(window) / period
    sd = stdev(window, mean)
    return mean, mean + num_std * sd, mean - num_std * sd


def keltner(highs: list[float], lows: list[float], closes: list[float], period: int, mult: float):
    mid = ema_last(closes, period)
    if mid is None:
        return None, None, None
    a = atr(highs, lows, closes, period)
    if a is None:
        return None, None, None
    return mid, mid + mult * a, mid - mult * a


def adx(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    n = len(closes)
    if n < 2 * period + 1:
        return None
    plus_dm = [0.0] * n
    minus_dm = [0.0] * n
    tr = [0.0] * n
    for i in range(1, n):
        up = highs[i] - highs[i - 1]
        down = lows[i - 1] - lows[i]
        plus_dm[i] = up if (up > down and up > 0) else 0.0
        minus_dm[i] = down if (down > up and down > 0) else 0.0
        tr[i] = max(highs[i] - lows[i], abs(highs[i] - closes[i - 1]), abs(lows[i] - closes[i - 1]))
    # smoothed sums
    def smooth(arr, p):
        out = sum(arr[1: p + 1]) / p
        for i in range(p + 1, n):
            out = (out * (p - 1) + arr[i]) / p
        return out

    atr_s = smooth(tr, period)
    pdm_s = smooth(plus_dm, period)
    mdm_s = smooth(minus_dm, period)
    if atr_s <= 0:
        return None
    pdi = 100.0 * pdm_s / atr_s
    mdi = 100.0 * mdm_s / atr_s
    dx = 100.0 * abs(pdi - mdi) / (pdi + mdi + 1e-9)
    # ADX = smoothed DX over period
    return dx
