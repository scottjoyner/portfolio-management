from __future__ import annotations
import os, csv, time, math, statistics
from typing import Dict, Any, List, Tuple

STATE_DIR = os.path.join(os.path.dirname(__file__), "..", "state")
os.makedirs(STATE_DIR, exist_ok=True)
TRADES_CSV = os.path.join(STATE_DIR, "trades.csv")

def log_trade(row: Dict[str, Any]) -> None:
    """
    Append a closed-trade record.
    Required fields: ts_open, ts_close, product_id, setup, side, entry, stop, target, exit_price, exit_reason, r_multiple, pnl_usd
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    header = ["ts_open","ts_close","product_id","setup","side","entry","stop","target","exit_price","exit_reason","r_multiple","pnl_usd"]
    exists = os.path.exists(TRADES_CSV)
    with open(TRADES_CSV, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        w.writerow({k: row.get(k) for k in header})

def rolling_stats(window:int=500) -> Dict[str, Dict[str, float]]:
    """
    Return per-setup rolling stats: count, win_rate, avg_win_R, avg_loss_R, mean_R, std_R.
    """
    if not os.path.exists(TRADES_CSV):
        return {}
    rows: List[Dict[str,str]] = []
    with open(TRADES_CSV, "r") as f:
        r = csv.DictReader(f)
        rows = list(r)[-window:]
    by_setup: Dict[str, List[float]] = {}
    for row in rows:
        setup = row["setup"]
        r_mult = float(row.get("r_multiple", "0") or 0)
        by_setup.setdefault(setup, []).append(r_mult)
    out: Dict[str, Dict[str, float]] = {}
    for setup, arr in by_setup.items():
        n = len(arr)
        wins = [x for x in arr if x > 0]
        losses = [x for x in arr if x <= 0]
        win_rate = len(wins)/n if n>0 else 0.0
        avg_win = sum(wins)/len(wins) if wins else 0.0
        avg_loss = sum(losses)/len(losses) if losses else 0.0
        mean_R = sum(arr)/n if n>0 else 0.0
        std_R = statistics.pstdev(arr) if n>1 else 0.0
        out[setup] = {"count": float(n), "win_rate": win_rate, "avg_win_R": avg_win, "avg_loss_R": avg_loss, "mean_R": mean_R, "std_R": std_R}
    return out

def kelly_fraction_from_rr(win_rate: float, rr: float) -> float:
    """
    Kelly for asymmetric payoff with win probability p and win:loss ratio RR (reward:risk with risk=1).
    f* = p - (1-p)/RR . Clamp to [0, 1].
    """
    if rr <= 0: 
        return 0.0
    p = max(0.0, min(1.0, win_rate))
    f = p - (1.0 - p)/rr
    return max(0.0, min(1.0, f))

def kelly_from_history(stats: Dict[str, float], default_rr: float=2.0) -> float:
    """
    Estimate Kelly from historical win_rate and average positive/negative R. If not enough data, fallback to default_rr.
    """
    n = stats.get("count", 0.0)
    p = stats.get("win_rate", 0.0)
    avg_win = stats.get("avg_win_R", default_rr)
    avg_loss = abs(stats.get("avg_loss_R", -1.0)) or 1.0
    rr = avg_win / avg_loss if avg_loss>0 else default_rr
    return kelly_fraction_from_rr(p, rr)
