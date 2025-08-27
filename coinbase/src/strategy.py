from __future__ import annotations
import numpy as np
import pandas as pd

def annualized_vol(returns: pd.Series, bars_per_year: int) -> float:
    return float(returns.std(ddof=0) * np.sqrt(bars_per_year))

def trend_signal(prices: pd.Series, fast: int = 50, slow: int = 200) -> float:
    if len(prices) < max(fast, slow):
        return 0.0
    f = prices.rolling(fast).mean().iloc[-1]
    s = prices.rolling(slow).mean().iloc[-1]
    return 1.0 if f > s else 0.0

def target_weight(prices: pd.Series, target_ann_vol: float, bars_per_year: int) -> float:
    rets = prices.pct_change().dropna()
    cur_vol = annualized_vol(rets.tail(200), bars_per_year) if len(rets) else 0.0
    if cur_vol <= 1e-9:
        return 0.0
    raw = target_ann_vol / cur_vol
    return float(np.clip(raw, 0.0, 1.5))  # cap leverage at 1.5x
