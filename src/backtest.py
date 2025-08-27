from __future__ import annotations
import pandas as pd, numpy as np
from .strategy import trend_signal, target_weight

def backtest_daily(closes: dict[str, pd.Series], target_ann_vol: float = 0.10) -> pd.DataFrame:
    """
    Simplified daily backtest across assets with SMA trend filter + vol targeting.
    Assumes equal vol-targeted weights across active assets.
    """
    # Align all series
    df = pd.DataFrame({k: v for k, v in closes.items()}).dropna()
    rets = df.pct_change().dropna()
    index = rets.index

    def ann_vol(x): return x.std(ddof=0) * np.sqrt(252)

    weights = pd.DataFrame(0.0, index=index, columns=df.columns)
    for t in index:
        cols = []
        for col in df.columns:
            s = df[col].loc[:t].tail(220)
            if len(s) < 200: continue
            sig = trend_signal(s, 50, 200)
            if sig <= 0: continue
            rv = ann_vol(s.pct_change().dropna().tail(200))
            w = 0 if rv==0 else min(1.5, target_ann_vol/rv)
            cols.append((col, w))
        if cols:
            total = sum(w for _, w in cols)
            for col, w in cols:
                weights.loc[t, col] = w/total if total>0 else 0.0
    port_rets = (weights.shift().fillna(0) * rets).sum(axis=1)
    equity = (1+port_rets).cumprod()
    return pd.DataFrame({"equity": equity, "returns": port_rets})
