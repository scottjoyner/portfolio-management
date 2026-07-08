from __future__ import annotations
import os, csv, math, random
from typing import List, Dict
from .analytics import TRADES_CSV

def _rows(window:int=2000):
    if not os.path.exists(TRADES_CSV): return []
    with open(TRADES_CSV, "r") as f:
        return list(csv.DictReader(f))[-window:]

def ucb1_scores(now:int, arms: List[str], value_key:str="r_multiple", c:float=1.0) -> Dict[str, float]:
    rows = _rows()
    by = {a: [] for a in arms}
    for r in rows:
        a = r.get("setup"); 
        if a in by:
            v = float(r.get(value_key,"0") or 0); v = max(-3.0, min(5.0, v)); by[a].append(v)
    n_total = sum(len(v) for v in by.values()) + 1e-9
    scores = {}
    for a in arms:
        n = len(by[a]); mean = sum(by[a])/n if n>0 else 0.0
        bonus = c * math.sqrt(math.log(max(2.0, n_total)) / max(1.0, n))
        scores[a] = mean + bonus
    return scores

def thompson_scores(arms: List[str]) -> Dict[str, float]:
    rows = _rows()
    by = {a: {"w":1.0, "l":1.0} for a in arms}
    for r in rows:
        a = r.get("setup")
        if a in by:
            win = float(r.get("r_multiple","0") or 0) > 0
            if win: by[a]["w"] += 1.0
            else: by[a]["l"] += 1.0
    scores = {}
    for a, wl in by.items():
        x = random.gammavariate(wl["w"], 1.0); y = random.gammavariate(wl["l"], 1.0)
        scores[a] = x/(x+y)
    return scores
