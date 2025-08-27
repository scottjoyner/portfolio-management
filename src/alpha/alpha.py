from __future__ import annotations
import math
import pandas as pd
from typing import Optional, Dict

from .data import compute_atr, rolling_high, rsi

def rr_ratio(entry: float, stop: float, target: float, side: str = "long") -> float:
    if side == "long":
        risk = max(1e-9, entry - stop)
        reward = max(0.0, target - entry)
    else:
        risk = max(1e-9, stop - entry)
        reward = max(0.0, entry - target)
    return reward / risk if risk > 0 else 0.0

def donchian_breakout_setup(df: pd.DataFrame, stop_atr_mult: float = 2.0, target_atr_mult: float = 3.0, lookback: int = 20) -> Optional[Dict]:
    """
    Long-only: if close pierces the 20-day high, set stop = entry - k*ATR, target = entry + m*ATR.
    """
    if len(df) < max(lookback, 50):
        return None
    atr = compute_atr(df).iloc[-1]
    entry = float(df["close"].iloc[-1])
    breakout = float(rolling_high(df, lookback).iloc[-2])  # yesterday's 20D high
    if entry <= breakout:
        return None
    stop = entry - stop_atr_mult * atr
    target = entry + target_atr_mult * atr
    rr = rr_ratio(entry, stop, target, "long")
    return {"side": "buy", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "donchian_breakout"}

def trend_rsi_pullback_setup(df: pd.DataFrame, stop_atr_mult: float = 2.0, target_high_lookback: int = 20) -> Optional[Dict]:
    """
    Uptrend (SMA200 up, close > SMA50); RSI<35 signals pullback entry. Stop = entry - k*ATR, target = 20D high.
    """
    if len(df) < 220:
        return None
    cl = df["close"]
    sma50 = cl.rolling(50).mean().iloc[-1]
    sma200_series = cl.rolling(200).mean()
    sma200 = sma200_series.iloc[-1]
    sma200_prev = sma200_series.iloc[-5]
    if not (cl.iloc[-1] > sma50 and sma200 > sma200_prev):
        return None
    r = rsi(cl).iloc[-1]
    if r >= 35:
        return None
    atr = compute_atr(df).iloc[-1]
    entry = float(cl.iloc[-1])
    stop = entry - stop_atr_mult * atr
    target = float(df["high"].rolling(target_high_lookback).max().iloc[-2])
    # ensure target above entry; if not, fall back to entry + 2*ATR
    if target <= entry:
        target = entry + 2.0 * atr
    rr = rr_ratio(entry, stop, target, "long")
    return {"side": "buy", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "trend_rsi_pullback"}


def donchian_breakdown_setup(df: pd.DataFrame, stop_atr_mult: float = 2.0, target_atr_mult: float = 3.0, lookback: int = 20) -> Optional[Dict]:
    """
    Short-only: if close pierces the 20-day low, set stop = entry + k*ATR, target = entry - m*ATR.
    """
    if len(df) < max(lookback, 50):
        return None
    atr = compute_atr(df).iloc[-1]
    entry = float(df["close"].iloc[-1])
    breakdown = float(df["low"].rolling(lookback).min().iloc[-2])
    if entry >= breakdown:
        return None
    stop = entry + stop_atr_mult * atr
    target = entry - target_atr_mult * atr
    rr = rr_ratio(entry, stop, target, "short")
    return {"side": "sell", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "donchian_breakdown"}

def trend_rsi_rip_setup(df: pd.DataFrame, stop_atr_mult: float = 2.0, target_low_lookback: int = 20) -> Optional[Dict]:
    """
    Downtrend (SMA200 down, close < SMA50); RSI>65 signals rip to sell. Stop = entry + k*ATR, target = 20D low.
    """
    if len(df) < 220:
        return None
    cl = df["close"]
    sma50 = cl.rolling(50).mean().iloc[-1]
    sma200_series = cl.rolling(200).mean()
    sma200 = sma200_series.iloc[-1]
    sma200_prev = sma200_series.iloc[-5]
    if not (cl.iloc[-1] < sma50 and sma200 < sma200_prev):
        return None
    from .data import rsi  # local import
    r = rsi(cl).iloc[-1]
    if r <= 65:
        return None
    atr = compute_atr(df).iloc[-1]
    entry = float(cl.iloc[-1])
    stop = entry + stop_atr_mult * atr
    target = float(df["low"].rolling(target_low_lookback).min().iloc[-2])
    if target >= entry:
        target = entry - 2.0 * atr
    rr = rr_ratio(entry, stop, target, "short")
    return {"side": "sell", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "trend_rsi_rip"}
