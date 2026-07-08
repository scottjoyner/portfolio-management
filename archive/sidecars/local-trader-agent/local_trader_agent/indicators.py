from __future__ import annotations

import pandas as pd


def rsi_wilder(close: pd.Series, period: int = 14) -> pd.Series:
    """Compute Wilder RSI.

    Returns NaN for the warm-up region. The input should be sorted oldest -> newest.
    """
    if period < 2:
        raise ValueError("RSI period must be >= 2")
    close = close.astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    rsi = rsi.astype(float)
    rsi = rsi.fillna(100.0).where(avg_loss == 0, rsi)
    return rsi


def crossed_above(series: pd.Series, threshold: float) -> pd.Series:
    """True when series crosses from <= threshold to > threshold."""
    previous = series.shift(1)
    return (previous <= threshold) & (series > threshold)
