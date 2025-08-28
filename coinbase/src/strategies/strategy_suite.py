# src/strategies/strategy_suite.py
from __future__ import annotations
import os, math, time, statistics
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import pandas as pd
from dotenv import load_dotenv

# Project imports
try:
    from ..cb_client import CBClient
    from ..data import fetch_candles_df, compute_atr, rsi, rolling_high, rolling_low
except Exception:
    # allow `python src/strategies/strategy_suite.py` for quick tests
    from src.cb_client import CBClient
    from src.data import fetch_candles_df, compute_atr, rsi, rolling_high, rolling_low

# Neo4j (optional; only needed for strategies 8 & 10)
try:
    from neo4j import GraphDatabase
except Exception:
    GraphDatabase = None  # graceful fallback if driver not installed

load_dotenv(override=False)

# ---------------------------- Context ----------------------------
@dataclass
class StrategyContext:
    cb: CBClient
    products: List[str]
    cash_ccy: str = os.getenv("CASH", "USD")
    granularity: str = os.getenv("BAR_GRANULARITY", "ONE_HOUR")
    lookback_days: int = int(os.getenv("LOOKBACK_DAYS", "240"))
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "please_change_me")
    neo4j_database: str = os.getenv("NEO4J_DATABASE", "neo4j")

    def neo_session(self):
        if GraphDatabase is None:
            return None
        drv = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
        return drv.session(database=self.neo4j_database)

# ---------------------------- Utils ----------------------------
def _ma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).mean()

def _bb(series: pd.Series, n: int = 20, k: float = 2.0):
    m = _ma(series, n)
    s = series.rolling(n).std(ddof=0)
    return m, m + k*s, m - k*s

def _zscore(series: pd.Series, n: int = 60) -> pd.Series:
    m = series.rolling(n).mean()
    s = series.rolling(n).std(ddof=0).replace(0, float("nan"))
    return (series - m) / s

def _rr(entry: float, stop: float, target: float, side: str):
    if side == "buy":
        risk = max(1e-9, entry - stop); reward = max(0.0, target - entry)
    else:
        risk = max(1e-9, stop - entry); reward = max(0.0, entry - target)
    return (reward / risk) if risk > 0 else 0.0

def _signal(name: str, product_id: str, side: str, entry: float, stop: float, target: float,
            atr: float, confidence: float, meta: Optional[dict] = None):
    sig = {
        "name": name,
        "type": "entry",
        "product_id": product_id,
        "side": side,  # "buy" / "sell"
        "entry": float(entry),
        "stop": float(stop),
        "target": float(target),
        "atr": float(atr),
        "rr": float(_rr(entry, stop, target, side)),
        "confidence": float(max(0.0, min(1.0, confidence))),
    }
    if meta:
        sig.update(meta)
    return sig

# ---------------------------- Strategies ----------------------------
# 1) BTC Halving Cycle Macro
def strat_btc_halving_cycle(ctx: StrategyContext) -> List[Dict[str, Any]]:
    """Long-bias BTC strategy using halving phase + trend filter."""
    if "BTC-USD" not in ctx.products:
        return []
    import datetime as dt
    now = dt.datetime.now(dt.timezone.utc)
    last_halving = dt.datetime(2024, 4, 20, tzinfo=dt.timezone.utc)
    months = (now.year - last_halving.year) * 12 + (now.month - last_halving.month)

    df = fetch_candles_df(ctx.cb, "BTC-USD", ctx.lookback_days, ctx.granularity)
    if len(df) < 220:
        return []
    cl = df["close"]
    atr = compute_atr(df).iloc[-1]
    sma200 = _ma(cl, 200).iloc[-1]
    price = float(cl.iloc[-1])

    trend_ok = price > sma200
    if months <= 12:
        bias, stop_k, tgt_k, conf = "aggressive_bull", 2.0, 4.0, 0.9
    elif months <= 30:
        bias, stop_k, tgt_k, conf = "neutral_trend", 2.5, 3.0, 0.7
    else:
        bias, stop_k, tgt_k, conf = "defensive", 3.0, 2.0, 0.5
    if not trend_ok and bias != "aggressive_bull":
        return []

    entry, stop, target = price, price - stop_k*atr, price + tgt_k*atr
    return [_signal("btc_halving_cycle", "BTC-USD", "buy", entry, stop, target, atr, conf,
                    {"bias": bias, "months_since_halving": months})]

# 2) 20/50/100-day MA crossover
def strat_triple_ma(ctx: StrategyContext) -> List[Dict[str, Any]]:
    out = []
    enable_shorts = os.getenv("ENABLE_SHORTS", "false").lower() == "true"
    for p in ctx.products:
        df = fetch_candles_df(ctx.cb, p, ctx.lookback_days, ctx.granularity)
        if len(df) < 110:
            continue
        cl = df["close"]
        m20, m50, m100 = _ma(cl, 20), _ma(cl, 50), _ma(cl, 100)
        atr = compute_atr(df).iloc[-1]; price = float(cl.iloc[-1])

        bull = (m20.iloc[-2] <= m50.iloc[-2] and m20.iloc[-1] > m50.iloc[-1]) and price > m100.iloc[-1]
        bear = (m20.iloc[-2] >= m50.iloc[-2] and m20.iloc[-1] < m50.iloc[-1]) and price < m100.iloc[-1]

        if bull:
            out.append(_signal("triple_ma_bull", p, "buy", price, price - 2.0*atr, price + 3.0*atr, atr, 0.65))
        elif bear and enable_shorts:
            out.append(_signal("triple_ma_bear", p, "sell", price, price + 2.0*atr, price - 3.0*atr, atr, 0.6))
    return out

# 3) Donchian breakout
def strat_donchian_breakout(ctx: StrategyContext, lookback: int = 20) -> List[Dict[str, Any]]:
    out = []
    for p in ctx.products:
        df = fetch_candles_df(ctx.cb, p, ctx.lookback_days, ctx.granularity)
        if len(df) < max(lookback, 50):
            continue
        atr = compute_atr(df).iloc[-1]
        entry = float(df["close"].iloc[-1])
        breakout = float(rolling_high(df, lookback).iloc[-2])
        if entry > breakout:
            out.append(_signal("donchian_breakout", p, "buy", entry, entry - 2.0*atr, entry + 3.0*atr, atr, 0.6))
    return out

# 4) Trend + RSI pullback
def strat_trend_rsi_pullback(ctx: StrategyContext) -> List[Dict[str, Any]]:
    out = []
    for p in ctx.products:
        df = fetch_candles_df(ctx.cb, p, ctx.lookback_days, ctx.granularity)
        if len(df) < 220:
            continue
        cl = df["close"]; sma50 = _ma(cl, 50).iloc[-1]; sma200 = _ma(cl, 200).iloc[-1]
        if not (cl.iloc[-1] > sma50 and sma50 > sma200):
            continue
        r_now = rsi(cl).iloc[-1]
        if r_now >= 35:
            continue
        atr = compute_atr(df).iloc[-1]; entry = float(cl.iloc[-1])
        target = float(df["high"].rolling(20).max().iloc[-2])
        if target <= entry:
            target = entry + 2.5*atr
        out.append(_signal("trend_rsi_pullback", p, "buy", entry, entry - 2.0*atr, target, atr, 0.6))
    return out

# 5) Bollinger mean reversion
def strat_bollinger_revert(ctx: StrategyContext) -> List[Dict[str, Any]]:
    out = []
    enable_shorts = os.getenv("ENABLE_SHORTS", "false").lower() == "true"
    for p in ctx.products:
        df = fetch_candles_df(ctx.cb, p, ctx.lookback_days, ctx.granularity)
        if len(df) < 40:
            continue
        cl = df["close"]; m, up, lo = _bb(cl, 20, 2.0); atr_v = compute_atr(df).iloc[-1]; px = float(cl.iloc[-1])
        if px < float(lo.iloc[-1]):  # long to mid
            out.append(_signal("boll_revert_long", p, "buy", px, px - 1.5*atr_v, float(m.iloc[-1]), atr_v, 0.55))
        elif px > float(up.iloc[-1]) and enable_shorts:
            out.append(_signal("boll_revert_short", p, "sell", px, px + 1.5*atr_v, float(m.iloc[-1]), atr_v, 0.55))
    return out

# 6) Aggressive momentum bot
def strat_aggressive_momo(ctx: StrategyContext) -> List[Dict[str, Any]]:
    """Top-30D momentum; 20-day breakout; tight stops; dynamic trail hints."""
    scans = []
    for p in ctx.products:
        df = fetch_candles_df(ctx.cb, p, max(90, ctx.lookback_days // 2), ctx.granularity)
        if len(df) < 60:
            continue
        cl = df["close"]
        ret30 = float(cl.iloc[-1] / cl.iloc[-30] - 1.0) if len(cl) >= 31 else 0.0
        scans.append((ret30, p, df))
    scans.sort(reverse=True)
    out = []
    for _, p, df in scans[:min(5, len(scans))]:
        atr_v = compute_atr(df).iloc[-1]; px = float(df["close"].iloc[-1]); brk = float(rolling_high(df, 20).iloc[-2])
        if px <= brk:
            continue
        out.append(_signal("aggressive_momo", p, "buy", px, px - 1.5*atr_v, px + 4.0*atr_v, atr_v, 0.7,
                           {"dynamic_trail_atr_mult": 1.0, "breakeven_after_R": 0.5}))
    return out

# 7) BTC–ETH ratio pair (no USD)
def strat_pair_btc_eth(ctx: StrategyContext) -> List[Dict[str, Any]]:
    if "BTC-USD" not in ctx.products or "ETH-USD" not in ctx.products:
        return []
    df_b = fetch_candles_df(ctx.cb, "BTC-USD", max(120, ctx.lookback_days // 2), ctx.granularity)
    df_e = fetch_candles_df(ctx.cb, "ETH-USD", max(120, ctx.lookback_days // 2), ctx.granularity)
    if len(df_b) < 60 or len(df_e) < 60:
        return []
    df = pd.DataFrame({"btc": df_b["close"], "eth": df_e["close"]}).dropna()
    ratio = (df["btc"] / df["eth"]).rename("ratio"); z = _zscore(ratio, 60).iloc[-1]
    zc = max(-2.5, min(2.5, float(z)))
    tilt = 0.2 * (zc / 2.5)  # ±20% tilt at 2.5σ
    w_btc = max(0.1, min(0.9, 0.5 + tilt)); w_eth = 1.0 - w_btc
    return [{
        "name": "pair_btc_eth_ratio",
        "type": "pair_rebalance",
        "pair": ["BTC-USD", "ETH-USD"],
        "target_pair_weights": {"BTC-USD": round(w_btc, 4), "ETH-USD": round(w_eth, 4)},
        "zscore": float(zc),
        "confidence": 0.6,
    }]

# 8) Category rotation (Neo4j)
def strat_category_rotation(ctx: StrategyContext) -> List[Dict[str, Any]]:
    if GraphDatabase is None: return []
    sess = ctx.neo_session()
    if sess is None: return []
    syms = [p.split("-")[0].upper() for p in ctx.products]
    q = """
    MATCH (a:Asset)
    WHERE toUpper(a.symbol) IN $syms
    RETURN toUpper(a.symbol) AS sym, coalesce(a.categories, []) AS cats,
           a.market_cap_rank AS rank, coalesce(a.volume_24h,0) AS vol
    """
    rows = [r.data() for r in sess.run(q, syms=syms)]
    sess.close()
    cats: Dict[str, List[dict]] = {}
    for r in rows:
        for c in r["cats"]:
            cats.setdefault(c, []).append(r)
    picks = []
    for c, lst in cats.items():
        lst2 = [x for x in lst if (x["rank"] or 9999) <= 300 and x["vol"] >= 5_000_000]
        if lst2:
            picks.append(sorted(lst2, key=lambda x: (x["rank"] or 9999, -x["vol"]))[0])
    out = []
    for r in picks[:6]:
        p = f"{r['sym']}-USD"
        df = fetch_candles_df(ctx.cb, p, ctx.lookback_days, ctx.granularity)
        if len(df) < 50: continue
        atr_v = compute_atr(df).iloc[-1]; px = float(df["close"].iloc[-1])
        out.append(_signal("category_rotation", p, "buy", px, px - 2.0*atr_v, px + 3.0*atr_v, atr_v, 0.58))
    return out

# 9) Vol-targeted risk parity rebalance
def strat_risk_parity(ctx: StrategyContext) -> List[Dict[str, Any]]:
    vols = []
    for p in ctx.products:
        df = fetch_candles_df(ctx.cb, p, max(60, ctx.lookback_days // 4), ctx.granularity)
        if len(df) < 40: continue
        cl = df["close"].pct_change().dropna()
        vol = cl.rolling(30).std().iloc[-1] if len(cl) >= 31 else cl.std()
        if pd.isna(vol) or vol == 0: continue
        vols.append((p, float(vol)))
    if not vols: return []
    inv = [(p, 1.0 / v) for p, v in vols]; s = sum(w for _, w in inv) or 1.0
    raw = {p: w / s for p, w in inv}
    cap = float(os.getenv("RP_MAX_WEIGHT", "0.25"))
    capped = {p: min(cap, w) for p, w in raw.items()}
    s2 = sum(capped.values()) or 1.0
    wts = {p: w / s2 for p, w in capped.items()}
    return [{
        "name": "risk_parity_rebalance",
        "type": "rebalance",
        "target_weights": {k: round(v, 4) for k, v in wts.items()},
        "lookback": "30D",
        "confidence": 0.6,
    }]

# 10) Developer-activity tilt (Neo4j)
def strat_dev_activity_tilt(ctx: StrategyContext) -> List[Dict[str, Any]]:
    if GraphDatabase is None: return []
    sess = ctx.neo_session()
    if sess is None: return []
    syms = [p.split("-")[0].upper() for p in ctx.products]
    q = """
    MATCH (a:Asset)-[r:HAS_LINK {kind:'github_repo'}]->(l:Link)
    WHERE toUpper(a.symbol) IN $syms
    WITH a, count(l) AS gh_repos, a.market_cap_rank AS rank, coalesce(a.volume_24h,0) AS vol
    RETURN toUpper(a.symbol) AS sym, gh_repos, rank, vol
    """
    rows = [r.data() for r in sess.run(q, syms=syms)]
    sess.close()
    if not rows: return []
    df = pd.DataFrame(rows)
    if df.empty: return []
    df["gh_repos"] = df["gh_repos"].fillna(0); df["vol"] = df["vol"].fillna(0.0)
    z = lambda s: (s - s.mean()) / (s.std(ddof=0) if s.std(ddof=0) != 0 else 1.0)
    df["z_repos"] = z(df["gh_repos"]); df["z_liq"] = z(df["vol"])
    df["score"] = 0.6*df["z_repos"] + 0.4*df["z_liq"] - 0.1*(df["rank"].fillna(9999)/1000.0)
    df = df.sort_values("score", ascending=False).head(5)
    out = []
    for _, r in df.iterrows():
        p = f"{r['sym']}-USD"
        dfp = fetch_candles_df(ctx.cb, p, ctx.lookback_days, ctx.granularity)
        if len(dfp) < 40: continue
        atr_v = compute_atr(dfp).iloc[-1]; px = float(dfp["close"].iloc[-1])
        out.append(_signal("dev_activity_tilt", p, "buy", px, px - 2.0*atr_v, px + 3.5*atr_v, atr_v, 0.6,
                           {"score": float(r["score"])}))
    return out

# ---------------------------- Registry & Runner ----------------------------
STRATEGIES = [
    strat_btc_halving_cycle,
    strat_triple_ma,
    strat_donchian_breakout,
    strat_trend_rsi_pullback,
    strat_bollinger_revert,
    strat_aggressive_momo,
    strat_pair_btc_eth,
    strat_category_rotation,
    strat_risk_parity,
    strat_dev_activity_tilt,
]

def generate_signals(ctx: StrategyContext) -> List[Dict[str, Any]]:
    """Run all strategies and collect signals."""
    signals: List[Dict[str, Any]] = []
    for fn in STRATEGIES:
        try:
            sigs = fn(ctx) or []
            signals.extend(sigs)
        except Exception as e:
            signals.append({"name": fn.__name__, "type": "error", "error": str(e)})
    return signals

# quick CLI test: python -m src.strategies.strategy_suite  (if path allows)
if __name__ == "__main__":
    load_dotenv(override=False)
    cb = CBClient()
    products = [p.strip() for p in os.getenv("PRODUCTS","BTC-USD,ETH-USD,SOL-USD").split(",") if p.strip()]
    ctx = StrategyContext(cb=cb, products=products)
    from pprint import pprint
    pprint(generate_signals(ctx))
