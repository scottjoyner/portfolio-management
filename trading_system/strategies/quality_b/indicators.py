"""Shared pure-Python indicators for the quality_b strategy suite.

Stdlib only. Each function is deterministic and operates on plain lists.
"""
from __future__ import annotations

import math


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def sma(xs: list[float], period: int) -> float | None:
    if len(xs) < period:
        return None
    return _mean(xs[-period:])


def sma_last(xs: list[float], period: int) -> float | None:
    if len(xs) < period:
        return None
    return _mean(xs[-period:])


def ema_last(xs: list[float], period: int) -> float | None:
    if len(xs) < period:
        return None
    k = 2.0 / (period + 1.0)
    prev = _mean(xs[:period])
    for v in xs[period:]:
        prev = v * k + prev * (1.0 - k)
    return prev


def rma(xs: list[float], period: int) -> float | None:
    """Wilder's moving average (smoothed) of a series."""
    if len(xs) < period:
        return None
    prev = _mean(xs[:period])
    for v in xs[period:]:
        prev = (prev * (period - 1) + v) / period
    return prev


def true_range(highs: list[float], lows: list[float], closes: list[float]) -> float | None:
    n = len(closes)
    if n < 2 or len(highs) < n or len(lows) < n:
        return None
    c = closes[-1]
    pc = closes[-2]
    h = highs[-1]
    l = lows[-1]
    return max(h - l, abs(h - pc), abs(l - pc))


def atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1 or len(highs) < period + 1 or len(lows) < period + 1:
        return None
    trs = []
    for i in range(len(closes) - period, len(closes)):
        c = closes[i]
        pc = closes[i - 1]
        h = highs[i]
        l = lows[i]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    return rma(trs, period)


def highest(xs: list[float], period: int) -> float | None:
    if len(xs) < period:
        return None
    return max(xs[-period:])


def lowest(xs: list[float], period: int) -> float | None:
    if len(xs) < period:
        return None
    return min(xs[-period:])


def kaufman_efficiency(closes: list[float], period: int = 10) -> float | None:
    """Kaufman Efficiency Ratio in [-1, 1]: net displacement / path length."""
    if len(closes) < period + 1:
        return None
    window = closes[-(period + 1):]
    net = abs(window[-1] - window[0])
    path = sum(abs(window[i] - window[i - 1]) for i in range(1, len(window)))
    if path <= 1e-12:
        return 0.0
    er = net / path
    return max(-1.0, min(1.0, er))


def regime_trending(closes: list[float], period: int = 30, thr: float = 0.3) -> bool:
    """True when the series is in a trending (directional) regime.

    Uses the magnitude of the Kaufman Efficiency Ratio over a longer window:
    high |ER| => persistent one-sided drift (trend); low |ER| => choppy/mean-
    reverting (range).
    """
    er = kaufman_efficiency(closes, period)
    if er is None:
        return False
    return abs(er) > thr


def adapt_to_regime(follow_score: float, closes: list[float], period: int = 30, thr: float = 0.3) -> float:
    """Regime-adaptive sign selection.

    `follow_score` is the signal that trades WITH the established drift (trend-
    following). In a trending regime we keep it; in a ranging regime we flip it
    to a mean-reversion signal.
    """
    if regime_trending(closes, period, thr):
        return follow_score
    return -follow_score


def roc(closes: list[float], period: int = 10) -> float | None:
    if len(closes) < period + 1:
        return None
    past = closes[-(period + 1)]
    cur = closes[-1]
    if past <= 1e-12:
        return None
    return (cur - past) / past


def stdev_series(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return _stdev(closes[-period:])


def log_returns(closes: list[float]) -> list[float]:
    out = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0 and closes[i] > 0:
            out.append(math.log(closes[i] / closes[i - 1]))
    return out
