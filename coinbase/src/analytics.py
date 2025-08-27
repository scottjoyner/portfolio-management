from __future__ import annotations
import os, csv, statistics
from typing import Dict, Any, List

STATE_DIR = os.path.join(os.path.dirname(__file__), "..", "state")
os.makedirs(STATE_DIR, exist_ok=True)
TRADES_CSV = os.path.join(STATE_DIR, "trades.csv")

def log_trade(row: Dict[str, Any]) -> None:
    header = ["ts_open","ts_close","product_id","setup","side","entry","stop","target","exit_price","exit_reason","r_multiple","pnl_usd"]
    exists = os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists: w.writeheader()
        w.writerow({k: row.get(k) for k in header})

def rolling_stats(window:int=500) -> Dict[str, Dict[str, float]]:
    if not os.path.exists(TRADES_CSV): return {}
    with open(TRADES_CSV, "r") as f:
        rows = list(csv.DictReader(f))[-window:]
    by_setup: Dict[str, List[float]] = {}
    for r in rows:
        by_setup.setdefault(r["setup"], []).append(float(r.get("r_multiple","0") or 0))
    out = {}
    for setup, arr in by_setup.items():
        n = len(arr); wins=[x for x in arr if x>0]; losses=[x for x in arr if x<=0]
        win_rate = len(wins)/n if n>0 else 0.0
        avg_win = sum(wins)/len(wins) if wins else 0.0
        avg_loss = sum(losses)/len(losses) if losses else 0.0
        mean_R = sum(arr)/n if n>0 else 0.0
        std_R = statistics.pstdev(arr) if n>1 else 0.0
        out[setup] = {"count": float(n), "win_rate": win_rate, "avg_win_R": avg_win, "avg_loss_R": avg_loss, "mean_R": mean_R, "std_R": std_R}
    return out

def kelly_fraction_from_rr(win_rate: float, rr: float) -> float:
    if rr <= 0: return 0.0
    p = max(0.0, min(1.0, win_rate)); f = p - (1.0 - p)/rr
    return max(0.0, min(1.0, f))

def kelly_from_history(stats: Dict[str, float], default_rr: float=2.0) -> float:
    n = stats.get("count", 0.0); p = stats.get("win_rate", 0.0)
    avg_win = stats.get("avg_win_R", default_rr); avg_loss = abs(stats.get("avg_loss_R", -1.0)) or 1.0
    rr = avg_win / avg_loss if avg_loss>0 else default_rr
    return kelly_fraction_from_rr(p, rr)
