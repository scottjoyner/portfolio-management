"""Print each quality_a strategy's (win%, pf, ret%, sharpe, PASS) on real
BTC + AAPL (and other datasets) so a reviewer can verify quality."""
from __future__ import annotations

import csv
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from trading_system.strategies.quality_a import (  # noqa: E402
    FisherTransformStochStrategy,
    WilliamsPctRStrategy,
    CciShortReversalStrategy,
    SessionOpeningRangeBreakoutStrategy,
    ChaikinVolatilityBreakoutStrategy,
    UpDownVolumeRatioStrategy,
    RelativeVigorIndexStrategy,
    TripleEmaTrendStrategy,
)

ROOT = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(ROOT, "data")
FILES = {
    "BTC-hourly": "btc_real_hourly.csv",
    "BTC-daily": "historical/BTC-USD_daily.csv",
    "AAPL": "historical/AAPL_daily.csv",
    "MSFT": "historical/MSFT_daily.csv",
}
STRATS = [
    FisherTransformStochStrategy,
    WilliamsPctRStrategy,
    CciShortReversalStrategy,
    SessionOpeningRangeBreakoutStrategy,
    ChaikinVolatilityBreakoutStrategy,
    UpDownVolumeRatioStrategy,
    RelativeVigorIndexStrategy,
    TripleEmaTrendStrategy,
]


def load(name):
    path = os.path.join(DATA_DIR, name)
    closes, highs, lows, volumes = [], [], [], []
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            closes.append(float(row["close"]))
            highs.append(float(row.get("high", row["close"])))
            lows.append(float(row.get("low", row["close"])))
            volumes.append(float(row.get("volume", 0.0)))
    return closes, highs, lows, volumes


def market_state(i, closes, highs, lows, volumes):
    return {
        "product_id": "BTC-USD",
        "close": closes[i],
        "closes": closes[: i + 1],
        "highs": highs[: i + 1],
        "lows": lows[: i + 1],
        "volumes": volumes[: i + 1],
        "open": closes[0],
        "score": 0.0,
        "warmup_complete": True,
    }


def verdict(s, closes, highs, lows, volumes):
    w = getattr(s.config, "warmup_period", 0) or 30
    w = min(w, len(closes) - 1)
    tr = wi = 0
    gp = gl = rp = 0.0
    rets = []
    for i in range(w, len(closes) - 1):
        try:
            sig = s.generate_signal(market_state(i, closes, highs, lows, volumes))
        except Exception:
            sig = None
        if not sig or abs(sig.score) <= 0:
            continue
        d = 1.0 if sig.score > 0 else -1.0
        if closes[i] <= 0:
            continue
        pnl = d * (closes[i + 1] - closes[i]) / closes[i]
        rets.append(pnl)
        tr += 1
        if pnl > 0:
            wi += 1
            gp += pnl
        else:
            gl += -pnl
        rp += pnl * 100.0
    wr = wi / tr * 100.0 if tr else 0.0
    pf = gp / gl if gl > 0 else (float("inf") if gp > 0 else 0.0)
    sh = 0.0
    if len(rets) > 1:
        m = sum(rets) / len(rets)
        sd = math.sqrt(sum((r - m) ** 2 for r in rets) / (len(rets) - 1))
        sh = (m / sd) * math.sqrt(len(rets)) if sd > 0 else 0.0
    passed = bool(tr > 0 and wr >= 50.0 and sh > 0.5 and pf > 1.20 and rp > -10.0)
    return tr, wr, pf, rp, sh, passed


def main():
    data = {name: load(f) for name, f in FILES.items()}
    for cls in STRATS:
        s = cls()
        print(f"\n### {cls.__name__}")
        print(f"{'dataset':<12}{'tr':>5}{'win%':>8}{'pf':>10}{'ret%':>10}{'sharpe':>9}{'PASS':>6}")
        n_pass = 0
        for name in FILES:
            tr, wr, pf, rp, sh, ok = verdict(s, *data[name])
            n_pass += 1 if ok else 0
            pf_s = "inf" if pf == float("inf") else f"{pf:9.2f}"
            print(f"{name:<12}{tr:>5}{wr:>8.1f}{pf_s:>10}{rp:>10.2f}{sh:>9.2f}{'Y' if ok else 'N':>6}")
        print("OVERALL:", "PASS (>=2 datasets)" if n_pass >= 2 else "not passing gate")


if __name__ == "__main__":
    main()
