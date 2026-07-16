#!/usr/bin/env python3
"""
hermes_meta.py — META-LAYER: learn from the strategy engine's own track record.

The bot cannot analyze its own _records to bias entries. The Hermes agent can:
we load data/live_performance.json, aggregate the bot's REAL per-asset P&L and
win-rate, and expose an "edge score" the loop uses as a confirmation filter.

Key finding (2026-07-16): the bot makes money ONLY on BTC-USD (one backtest-
scale win) and marginally ETH-USD. EVERY other asset in _records is net-negative,
many with 0% win-rate across multiple trades. So the meta-filter says:
  - prioritize BTC/ETH longs in TREND_UP (bot's only proven winners)
  - penalize/avoid alts the bot is bleeding on (don't fight a proven loser)
  - require stronger own-signal conviction on negative-edge alts

Edge score per asset:
  edge = sum of total_pnl over NON-disabled strategies  (already aggregated)
  + confidence flag if total trades >= MIN_TRADES
Returns a dict {asset: {edge, trades, win_rate, disabled, confidence}}.
"""
from __future__ import annotations
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
PERF = ROOT / "data" / "live_performance.json"
MIN_TRADES = 2  # need >=2 trades to trust the sign of edge


def load_bot_edge() -> dict:
    """Aggregate the bot's _records into a per-asset edge score."""
    if not PERF.exists():
        return {}
    try:
        d = json.loads(PERF.read_text() or "{}")
    except Exception:
        return {}
    recs = d.get("_records", {})
    agg = defaultdict(lambda: {"pnl": 0.0, "trades": 0, "wins": 0,
                              "losses": 0, "disabled": 0, "n_strat": 0})
    for k, v in recs.items():
        prod = v.get("product_id", "?")
        a = agg[prod]
        a["pnl"] += float(v.get("total_pnl", 0))
        a["trades"] += int(v.get("trades", 0))
        a["wins"] += int(v.get("wins", 0))
        a["losses"] += int(v.get("losses", 0))
        a["disabled"] += 1 if v.get("disabled") else 0
        a["n_strat"] += 1
    out = {}
    for prod, a in agg.items():
        wr = (a["wins"] / (a["wins"] + a["losses"]) * 100.0
              if (a["wins"] + a["losses"]) else 0.0)
        out[prod] = {
            "edge": round(a["pnl"], 4),
            "trades": a["trades"],
            "win_rate": round(wr, 2),
            "disabled_strategies": a["disabled"],
            "n_strategies": a["n_strat"],
            "confidence": a["trades"] >= MIN_TRADES,
        }
    return out


def asset_edge(asset: str, cache: dict | None = None) -> dict:
    """Edge for a single asset. Returns {edge, trades, win_rate, confidence, verdict}."""
    if cache is None:
        cache = load_bot_edge()
    e = cache.get(asset)
    if not e:
        return {"edge": 0.0, "trades": 0, "win_rate": 0.0,
                "confidence": False, "verdict": "unknown"}
    if e["edge"] > 0 and e["confidence"]:
        verdict = "bot_wins_here"
    elif e["edge"] < 0 and e["confidence"]:
        verdict = "bot_bleeds_here"
    else:
        verdict = "neutral"
    return {"edge": e["edge"], "trades": e["trades"],
            "win_rate": e["win_rate"], "confidence": e["confidence"],
            "verdict": verdict}


def best_assets(n: int = 5) -> list:
    """Top-N assets by bot edge (the bot's proven winners)."""
    cache = load_bot_edge()
    ranked = sorted(cache.items(), key=lambda x: -x[1]["edge"])
    return [(p, v["edge"], v["win_rate"], v["trades"]) for p, v in ranked[:n]]


# Map the bot's 20 strategies -> the indicator family the agent can confirm with
# its own TechnicalIndicatorSet (Phase 9). Used for strategy mimicry (Phase 8):
# if the bot PROVABLY wins on asset X with a mean-reversion strat, and the
# agent's Bollinger/z-score says X is mean-reverting, that's a confirmed double.
STRAT_TYPE = {
    "vwap_revert": "mean_revert", "kalman_mr": "mean_revert",
    "boll_break": "breakout", "donchian": "breakout",
    "snr_idx": "mean_revert", "sup_res": "mean_revert",
    "chaikin_mf": "flow", "cvd_flow": "flow", "obv_div": "flow",
    "vol_prof": "vol_profile", "range_exp_idx": "vol_profile",
    "williams_r": "momentum", "mfi": "momentum", "cmo": "momentum",
    "de_marker": "momentum", "ad_div": "momentum", "vrsi": "momentum",
    "multi_rsi": "momentum", "force_idx": "momentum", "ema_cross": "trend",
}


def best_bot_setups(min_trades: int = 2, min_pnl: float = 0.0) -> list:
    """The bot's proven (strategy, asset) winners. The agent steals this alpha:
    trigger entries when its own indicator confirms the SAME setup type."""
    if not PERF.exists():
        return []
    try:
        d = json.loads(PERF.read_text() or "{}")
    except Exception:
        return []
    recs = d.get("_records", {})
    out = []
    for k, v in recs.items():
        strat, prod = k.split("/", 1) if "/" in k else (k, "?")
        trades = int(v.get("trades", 0))
        pnl = float(v.get("total_pnl", 0))
        wins = int(v.get("wins", 0))
        losses = int(v.get("losses", 0))
        if trades < min_trades or pnl < min_pnl:
            continue
        wr = (wins / (wins + losses) * 100.0) if (wins + losses) else 0.0
        out.append({
            "strategy": strat, "asset": prod,
            "type": STRAT_TYPE.get(strat, "other"),
            "pnl": round(pnl, 4), "trades": trades,
            "win_rate": round(wr, 2),
        })
    return sorted(out, key=lambda x: -x["pnl"])


def bot_confirms(asset: str, setup_type: str, cache: list | None = None) -> dict:
    """Does the bot PROVABLY win on `asset` with a strategy of `setup_type`?
    Returns {confirmed, best_pnl, n, examples} so the agent can boost conviction
    on confirmed setups (the bot's edge, gated by the agent's risk layers)."""
    if cache is None:
        cache = best_bot_setups()
    hits = [s for s in cache if s["asset"] == asset and s["type"] == setup_type]
    if not hits:
        return {"confirmed": False, "best_pnl": 0.0, "n": 0, "examples": []}
    return {
        "confirmed": True,
        "best_pnl": max(s["pnl"] for s in hits),
        "n": len(hits),
        "examples": [(s["strategy"], s["pnl"], s["win_rate"]) for s in hits[:3]],
    }
