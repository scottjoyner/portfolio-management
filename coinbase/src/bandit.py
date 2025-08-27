from __future__ import annotations
import os, csv, math, random
from typing import Dict, Any, List, Tuple

from .analytics import TRADES_CSV

def _load_rows(window:int=2000) -> List[dict]:
    if not os.path.exists(TRADES_CSV):
        return []
    with open(TRADES_CSV, "r") as f:
        rows = list(csv.DictReader(f))
    return rows[-window:]

def ucb1_scores(now:int, arms: List[str], value_key:str="r_multiple", c:float=1.0) -> Dict[str, float]:
    """
    Treat each setup as an arm; reward is realized R per trade (clipped to [-3, +5]).
    """
    rows = _load_rows()
    by = {a: [] for a in arms}
    for r in rows:
        a = r.get("setup")
        if a in by:
            val = float(r.get(value_key, "0") or 0.0)
            val = max(-3.0, min(5.0, val))
            by[a].append(val)
    n_total = sum(len(v) for v in by.values()) + 1e-9
    scores = {}
    for a in arms:
        n = len(by[a])
        mean = sum(by[a])/n if n>0 else 0.0
        bonus = c * math.sqrt(math.log(max(2.0, n_total)) / max(1.0, n))
        scores[a] = mean + bonus
    return scores

def thompson_scores(arms: List[str]) -> Dict[str, float]:
    """
    Beta-Bernoulli Thompson sampling using win/loss only (R>0 as success).
    """
    rows = _load_rows()
    by = {a: {"w":1.0, "l":1.0} for a in arms}  # priors
    for r in rows:
        a = r.get("setup")
        if a in by:
            win = float(r.get("r_multiple","0") or 0) > 0
            if win: by[a]["w"] += 1.0
            else: by[a]["l"] += 1.0
    scores = {}
    for a, wl in by.items():
        # sample from Beta(w, l)
        w, l = wl["w"], wl["l"]
        # simple gamma sampler for Beta via two gammas
        import random
        x = random.gammavariate(w, 1.0)
        y = random.gammavariate(l, 1.0)
        scores[a] = x / (x + y)
    return scores
