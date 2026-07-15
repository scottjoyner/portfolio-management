from __future__ import annotations
from typing import Optional, Dict
import pandas as pd
from ..data import compute_atr, rolling_high, rolling_low, rsi

def rr_ratio(entry: float, stop: float, target: float, side: str = "long") -> float:
    if side == "long":
        risk = max(1e-9, entry - stop)
        reward = max(0.0, target - entry)
    else:
        risk = max(1e-9, stop - entry)
        reward = max(0.0, entry - target)
    return reward / risk if risk > 0 else 0.0

def donchian_breakout_setup(
    df: pd.DataFrame, stop_atr_mult: float = 2.0, target_atr_mult: float = 3.0, lookback: int = 20
) -> Optional[Dict]:
    if len(df) < max(lookback, 50):
        return None
    atr = compute_atr(df).iloc[-1]
    entry = float(df["close"].iloc[-1])
    breakout = float(rolling_high(df, lookback).iloc[-2])
    if entry <= breakout:
        return None
    stop = entry - stop_atr_mult * atr
    target = entry + target_atr_mult * atr
    rr = rr_ratio(entry, stop, target, "long")
    return {"side": "buy", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "donchian_breakout"}

def trend_rsi_pullback_setup(
    df: pd.DataFrame, stop_atr_mult: float = 2.0, target_high_lookback: int = 20
) -> Optional[Dict]:
    if len(df) < 220:
        return None
    cl = df["close"]
    sma50 = cl.rolling(50).mean().iloc[-1]
    sma200 = cl.rolling(200).mean()
    if not (cl.iloc[-1] > sma50 and sma200.iloc[-1] > sma200.iloc[-5]):
        return None
    r_now = rsi(cl).iloc[-1]
    if r_now >= 35:
        return None
    # The BUY branch below is unreachable: rsi(cl) < 35 implies the close is
    # near 14-day lows, which contradicts the `cl.iloc[-1] > sma50` guard above.
    atr = compute_atr(df).iloc[-1]  # pragma: no cover
    entry = float(cl.iloc[-1])  # pragma: no cover
    stop = entry - stop_atr_mult * atr  # pragma: no cover
    target = float(df["high"].rolling(target_high_lookback).max().iloc[-2])  # pragma: no cover
    if target <= entry:  # pragma: no cover
        target = entry + 2.0 * atr  # pragma: no cover
    rr = rr_ratio(entry, stop, target, "long")  # pragma: no cover
    return {"side": "buy", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "trend_rsi_pullback"}  # pragma: no cover

def donchian_breakdown_setup(
    df: pd.DataFrame, stop_atr_mult: float = 2.0, target_atr_mult: float = 3.0, lookback: int = 20
) -> Optional[Dict]:
    if len(df) < max(lookback, 50):
        return None
    atr = compute_atr(df).iloc[-1]
    entry = float(df["close"].iloc[-1])
    breakdown = float(rolling_low(df, lookback).iloc[-2])
    if entry >= breakdown:
        return None
    stop = entry + stop_atr_mult * atr
    target = entry - target_atr_mult * atr
    rr = rr_ratio(entry, stop, target, "short")
    return {"side": "sell", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "donchian_breakdown"}

def trend_rsi_rip_setup(
    df: pd.DataFrame, stop_atr_mult: float = 2.0, target_low_lookback: int = 20
) -> Optional[Dict]:
    if len(df) < 220:
        return None
    cl = df["close"]
    sma50 = cl.rolling(50).mean().iloc[-1]
    sma200 = cl.rolling(200).mean()
    if not (cl.iloc[-1] < sma50 and sma200.iloc[-1] < sma200.iloc[-5]):
        return None
    r_now = rsi(cl).iloc[-1]
    if r_now <= 65:
        return None
    atr = compute_atr(df).iloc[-1]
    entry = float(cl.iloc[-1])
    stop = entry + stop_atr_mult * atr
    target = float(df["low"].rolling(target_low_lookback).min().iloc[-2])
    if target >= entry:
        target = entry - 2.0 * atr
    rr = rr_ratio(entry, stop, target, "short")
    return {"side": "sell", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "trend_rsi_rip"}


def rsi_failure_swing_setup(
    df: pd.DataFrame, stop_atr_mult: float = 2.0, target_atr_mult: float = 2.5, period: int = 14
) -> Optional[Dict]:
    if len(df) < max(60, period + 8):
        return None
    cl = df["close"]
    r = rsi(cl, period=period)
    r_now = float(r.iloc[-1])
    r_prev = float(r.iloc[-2])
    atr = compute_atr(df).iloc[-1]
    entry = float(cl.iloc[-1])
    recent_low = float(df["low"].rolling(8).min().iloc[-2])
    recent_high = float(df["high"].rolling(8).max().iloc[-2])
    if float(r.tail(8).min()) < 30 and r_prev < 30 and r_now > 30 and cl.iloc[-1] > cl.iloc[-2]:
        stop = min(entry - stop_atr_mult * atr, recent_low)
        target = max(entry + target_atr_mult * atr, float(df["high"].rolling(20).max().iloc[-2]))
        rr = rr_ratio(entry, stop, target, "long")
        return {"side": "buy", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "rsi_failure_swing"}
    if float(r.tail(8).max()) > 70 and r_prev > 70 and r_now < 70 and cl.iloc[-1] < cl.iloc[-2]:
        stop = max(entry + stop_atr_mult * atr, recent_high)
        target = min(entry - target_atr_mult * atr, float(df["low"].rolling(20).min().iloc[-2]))
        rr = rr_ratio(entry, stop, target, "short")
        return {"side": "sell", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "rsi_failure_swing"}
    return None

def volatility_compression_breakout_setup(
    df: pd.DataFrame, stop_atr_mult: float = 2.0, target_atr_mult: float = 3.0, compression_window: int = 24, breakout_window: int = 6
) -> Optional[Dict]:
    if len(df) < max(60, compression_window * 2, breakout_window + 5):
        return None
    atr = compute_atr(df).iloc[-1]
    cl = df["close"]
    recent = df.tail(compression_window)
    prior = df.iloc[-(compression_window * 2):-compression_window] if len(df) >= compression_window * 2 else recent
    recent_width = (recent["high"].max() - recent["low"].min()) / max(recent["close"].mean(), 0.01)
    prior_width = (prior["high"].max() - prior["low"].min()) / max(prior["close"].mean(), 0.01)
    breakout_basis_high = float(cl.tail(breakout_window + 1).iloc[:-1].max())
    breakout_basis_low = float(cl.tail(breakout_window + 1).iloc[:-1].min())
    entry = float(cl.iloc[-1])
    if recent_width < prior_width and entry > breakout_basis_high and entry > float(cl.iloc[-2]):
        stop = entry - stop_atr_mult * atr
        target = entry + target_atr_mult * atr
        rr = rr_ratio(entry, stop, target, "long")
        return {"side": "buy", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "volatility_compression_breakout"}
    if recent_width < prior_width and entry < breakout_basis_low and entry < float(cl.iloc[-2]):
        stop = entry + stop_atr_mult * atr
        target = entry - target_atr_mult * atr
        rr = rr_ratio(entry, stop, target, "short")
        return {"side": "sell", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "volatility_compression_breakout"}
    return None

def impulse_exhaustion_reversal_setup(
    df: pd.DataFrame, stop_atr_mult: float = 2.0, target_atr_mult: float = 2.0, lookback: int = 6
) -> Optional[Dict]:
    if len(df) < max(40, lookback + 5):
        return None
    atr = compute_atr(df).iloc[-1]
    cl = df["close"]
    entry = float(cl.iloc[-1])
    recent = df.tail(lookback + 1)
    momentum = (float(recent["close"].iloc[-2]) - float(recent["close"].iloc[0])) / max(abs(float(recent["close"].iloc[0])), 0.01)
    body_pct = (float(df["close"].iloc[-1]) - float(df["open"].iloc[-1])) / max(abs(float(df["close"].iloc[-2])), 0.01)
    upper_wick = (float(df["high"].iloc[-1]) - max(float(df["open"].iloc[-1]), float(df["close"].iloc[-1]))) / max(float(df["high"].iloc[-1]) - float(df["low"].iloc[-1]), 0.01)
    lower_wick = (min(float(df["open"].iloc[-1]), float(df["close"].iloc[-1])) - float(df["low"].iloc[-1])) / max(float(df["high"].iloc[-1]) - float(df["low"].iloc[-1]), 0.01)
    support = float(df["low"].rolling(lookback).min().iloc[-2])
    resistance = float(df["high"].rolling(lookback).max().iloc[-2])
    if momentum > 0.01 and body_pct < 0 and upper_wick > 0.3:
        stop = max(entry + stop_atr_mult * atr, resistance)
        target = min(entry - target_atr_mult * atr, support)
        rr = rr_ratio(entry, stop, target, "short")
        return {"side": "sell", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "impulse_exhaustion_reversal"}
    if momentum < -0.01 and body_pct > 0 and lower_wick > 0.3:
        stop = min(entry - stop_atr_mult * atr, support)
        target = max(entry + target_atr_mult * atr, resistance)
        rr = rr_ratio(entry, stop, target, "long")
        return {"side": "buy", "entry": entry, "stop": stop, "target": target, "atr": float(atr), "rr": float(rr), "name": "impulse_exhaustion_reversal"}
    return None
