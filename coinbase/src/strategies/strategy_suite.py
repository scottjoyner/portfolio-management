# src/strategies/portfolio_suite.py
from __future__ import annotations
import os, json, math, time, pathlib, argparse
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Project imports
try:
    from ..cb_client import CBClient
    from ..data import fetch_candles_df
except Exception:
    from src.cb_client import CBClient
    from src.data import fetch_candles_df

# Neo4j (optional for category-aware overlay)
try:
    from neo4j import GraphDatabase
except Exception:
    GraphDatabase = None

load_dotenv(override=False)

# ---------------------------- Config / Context ----------------------------
@dataclass
class PMContext:
    cb: CBClient
    products: List[str]
    granularity: str = os.getenv("BAR_GRANULARITY", "ONE_HOUR")
    lookback_days: int = int(os.getenv("LOOKBACK_DAYS", "240"))
    # Neo4j
    neo4j_uri: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user: str = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password: str = os.getenv("NEO4J_PASSWORD", "please_change_me")
    neo4j_database: str = os.getenv("NEO4J_DATABASE", "neo4j")
    # Constraints / knobs
    max_weight: float = float(os.getenv("MAX_WEIGHT", "0.25"))
    min_weight: float = float(os.getenv("MIN_WEIGHT", "0.0"))
    max_turnover: float = float(os.getenv("MAX_TURNOVER", "0.35"))  # L1 turnover cap per rebalance
    stables: Tuple[str, ...] = ("USDC-USD", "USDT-USD", "DAI-USD")

    def neo_session(self):
        if GraphDatabase is None:
            return None
        drv = GraphDatabase.driver(self.neo4j_uri, auth=(self.neo4j_user, self.neo4j_password))
        return drv.session(database=self.neo4j_database)

STATE_DIR = pathlib.Path("state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
LAST_WEIGHTS_FP = STATE_DIR / "pm_last_weights.json"

# ---------------------------- Data utils ----------------------------
def get_prices_matrix(ctx: PMContext) -> pd.DataFrame:
    """Fetch and align close prices for ctx.products."""
    frames = {}
    for p in ctx.products:
        df = fetch_candles_df(ctx.cb, p, lookback_days=ctx.lookback_days, granularity=ctx.granularity)
        if df is None or df.empty or "close" not in df:
            continue
        frames[p] = df["close"].rename(p)
    if not frames:
        return pd.DataFrame()
    prices = pd.concat(frames.values(), axis=1).dropna(how="any")
    return prices

def returns_from_prices(prices: pd.DataFrame, mode: str = "log") -> pd.DataFrame:
    if mode == "log":
        rets = np.log(prices / prices.shift(1))
    else:
        rets = prices.pct_change()
    return rets.dropna(how="any")

def ewma_cov(rets: pd.DataFrame, lam: float = 0.94) -> pd.DataFrame:
    """EWMA covariance (RiskMetrics style)."""
    arr = rets.values
    n, k = arr.shape
    S = np.cov(arr, rowvar=False) * 0  # init zeros
    w = 1.0
    denom = 0.0
    for t in range(n):
        x = arr[t:t+1, :]
        S = lam * S + (1 - lam) * (x.T @ x)
        denom = lam * denom + (1 - lam)
    S = S / max(denom, 1e-12)
    return pd.DataFrame(S, index=rets.columns, columns=rets.columns)

def vol_series(rets: pd.DataFrame, window: int = 30) -> pd.Series:
    return rets.rolling(window).std().iloc[-1]

# ---------------------------- Portfolio solvers ----------------------------
def _project_simplex_pos(w: np.ndarray) -> np.ndarray:
    """Project weights to simplex (non-negative, sum to 1)."""
    w = np.array(w, dtype=float)
    w[w < 0] = 0.0
    s = w.sum()
    if s <= 0:
        n = len(w)
        return np.ones(n) / n
    return w / s

def min_variance_weights(cov: pd.DataFrame) -> pd.Series:
    """Long-only heuristic: w ∝ Σ^{-1} 1, clip negatives, renorm."""
    Sigma = cov.values
    n = Sigma.shape[0]
    try:
        inv = np.linalg.pinv(Sigma)
    except Exception:
        inv = np.eye(n)
    ones = np.ones(n)
    w = inv @ ones
    w = _project_simplex_pos(w)
    return pd.Series(w, index=cov.index)

def max_diversification_weights(cov: pd.DataFrame) -> pd.Series:
    """
    Heuristic for Maximum Diversification:
    w ∝ Σ^{-1} σ (Choueifaty & Coignard), then clip + renorm.
    """
    Sigma = cov.values
    n = Sigma.shape[0]
    sigma = np.sqrt(np.diag(Sigma))
    try:
        inv = np.linalg.pinv(Sigma)
    except Exception:
        inv = np.eye(n)
    w = inv @ sigma
    w = _project_simplex_pos(w)
    return pd.Series(w, index=cov.index)

def risk_parity_weights(cov: pd.DataFrame, iters: int = 200, tol: float = 1e-7) -> pd.Series:
    """
    Equal Risk Contribution via cyclic scaling (long-only).
    """
    Sigma = cov.values
    n = Sigma.shape[0]
    w = np.ones(n) / n
    target = 1.0 / n
    for _ in range(iters):
        # Marginal risk contribution: m = Σw
        m = Sigma @ w
        # Risk contributions: rc_i = w_i * m_i / (portfolio_vol)
        port_var = float(w @ m)
        if port_var <= 0: break
        rc = (w * m) / max(math.sqrt(port_var), 1e-12)
        # Scale weights towards equal RC
        scale = target / (rc + 1e-12)
        # small step to ensure stability
        step = 0.5
        w = _project_simplex_pos(w * (1 - step) + w * scale * step)
        if np.max(np.abs(rc - rc.mean())) < tol:
            break
    return pd.Series(w, index=cov.index)

def momentum_quality_scores(prices: pd.DataFrame) -> pd.Series:
    """
    Combine 120D (or max available) and 30D momentum; add liquidity proxy from recent turnover.
    """
    # momentum
    horizonL = min(120, max(30, prices.shape[0] - 1))
    retL = (prices.iloc[-1] / prices.iloc[-horizonL] - 1.0)
    retS = (prices.iloc[-1] / prices.iloc[-30] - 1.0) if prices.shape[0] >= 31 else 0.0
    retS = retS if isinstance(retS, pd.Series) else pd.Series(0.0, index=prices.columns)
    # recent vol as (negative) quality penalty (prefer smoother)
    rets = returns_from_prices(prices, mode="log")
    vol30 = rets.rolling(30).std().iloc[-1].replace(0, np.nan).fillna(rets.std())
    # liquidity proxy via |returns| sum (crude)
    liq = rets.tail(30).abs().sum()
    # standardize
    def z(s: pd.Series) -> pd.Series:
        s = s.fillna(s.median())
        mu, sd = float(s.mean()), float(s.std(ddof=0) or 1.0)
        return (s - mu) / sd
    score = 0.6 * z(retL) + 0.3 * z(retS) + 0.2 * z(liq) - 0.2 * z(vol30)
    return score

def cap_and_normalize(w: pd.Series, max_w: float, min_w: float = 0.0) -> pd.Series:
    w2 = w.clip(lower=min_w)
    if max_w is not None:
        w2 = w2.clip(upper=max_w)
    s = float(w2.sum())
    if s <= 0:
        return pd.Series(np.ones(len(w2)) / len(w2), index=w.index)
    return w2 / s

def blend_models(models: Dict[str, pd.Series], weights: Dict[str, float]) -> pd.Series:
    # Align indexes
    idx = next(iter(models.values())).index
    for s in models.values():
        idx = idx.union(s.index)
    combo = pd.Series(0.0, index=idx)
    for name, wts in models.items():
        combo = combo.add(wts.reindex(idx).fillna(0.0) * weights.get(name, 0.0), fill_value=0.0)
    # renorm
    return combo / combo.sum() if combo.sum() > 0 else combo

# ---------------------------- Regime & Category overlays ----------------------------
def btc_regime(prices: pd.DataFrame, btc_pid: str = "BTC-USD") -> Dict[str, float]:
    """Return dict with risk_on in [0,1] based on BTC 200D SMA and drawdown."""
    if btc_pid not in prices.columns or prices.shape[0] < 220:
        return {"risk_on": 0.5, "trend": False, "dd": 0.0}
    px = prices[btc_pid]
    sma200 = px.rolling(200).mean()
    trend = float(px.iloc[-1] > sma200.iloc[-1])
    ath = float(px.cummax().iloc[-1])
    dd = 0.0 if ath == 0 else float(px.iloc[-1] / ath - 1.0)
    # Map to risk_on
    risk_on = 0.7 if trend else 0.3
    if dd < -0.3:
        risk_on *= 0.6
    elif dd > -0.1 and trend:
        risk_on = min(1.0, risk_on + 0.2)
    return {"risk_on": float(risk_on), "trend": bool(trend), "dd": float(dd)}

def equalize_by_category(ctx: PMContext, base_weights: pd.Series) -> pd.Series:
    """If Neo4j available, equal-weight across present categories, then distribute within category by base_weights."""
    sess = ctx.neo_session()
    if sess is None:
        return base_weights
    syms = [p.split("-")[0].upper() for p in base_weights.index]
    q = """
    MATCH (a:Asset)
    WHERE toUpper(a.symbol) IN $syms
    RETURN toUpper(a.symbol) AS sym, coalesce(a.categories, []) AS cats
    """
    rows = [r.data() for r in sess.run(q, syms=syms)]
    sess.close()
    sym2cats = {r["sym"]: r["cats"] for r in rows}
    # choose one main category per asset (first non-empty)
    asset2cat = {}
    for pid in base_weights.index:
        sym = pid.split("-")[0].upper()
        cats = [c for c in sym2cats.get(sym, []) if c]
        asset2cat[pid] = (cats[0] if cats else "Uncategorized")
    # Category sums using base weights
    df = pd.DataFrame({"w": base_weights})
    df["cat"] = df.index.map(asset2cat.get)
    cat_w = df.groupby("cat")["w"].sum()
    # Equalize categories present
    k = len(cat_w)
    if k <= 1:
        return base_weights
    cat_target = pd.Series(1.0 / k, index=cat_w.index)
    # Distribute within category proportional to base_weights
    out = pd.Series(0.0, index=base_weights.index)
    for cat, target in cat_target.items():
        sub = df[df["cat"] == cat]["w"]
        s = float(sub.sum())
        if s <= 0:
            # equal share within this category
            n = len(sub)
            out[sub.index] = target / n
        else:
            out[sub.index] = target * (sub / s)
    return out

def apply_defensive_overlay(w: pd.Series, regime: Dict[str, float], stables: Tuple[str, ...]) -> pd.Series:
    """Shift portion to stables when risk_on < 1.0."""
    risk_on = regime.get("risk_on", 0.5)
    if risk_on >= 0.99:
        return w
    # compute stable bucket presence
    idx = w.index
    stables_present = [s for s in stables if s in idx]
    if not stables_present:
        return w  # nothing to shift to
    # portion to stables = (1 - risk_on)
    shift = (1.0 - risk_on)
    # scale risk assets
    risk_mask = ~w.index.isin(stables_present)
    risk_sum = float(w[risk_mask].sum())
    if risk_sum > 0:
        w.loc[risk_mask] *= (1.0 - shift) / risk_sum
    # distribute shift equally across stables present
    per = shift / len(stables_present)
    for s in stables_present:
        w.loc[s] = per
    return w

# ---------------------------- Turnover control ----------------------------
def turnover_shrink(new_w: pd.Series, ctx: PMContext) -> pd.Series:
    """
    Limit L1 turnover vs last weights stored on disk.
    """
    last = {}
    if LAST_WEIGHTS_FP.exists():
        try:
            last = json.loads(LAST_WEIGHTS_FP.read_text())
        except Exception:
            last = {}
    last_w = pd.Series({k: float(v) for k, v in last.items()})
    # align
    idx = new_w.index.union(last_w.index)
    nw = new_w.reindex(idx).fillna(0.0)
    lw = last_w.reindex(idx).fillna(0.0)
    # actual turnover
    tau = float(np.abs(nw - lw).sum())
    if tau <= ctx.max_turnover or lw.sum() == 0.0:
        return new_w  # within limits or first run
    # shrink towards lw until turnover cap is hit
    alpha = 1.0
    for _ in range(30):
        alpha *= 0.8
        candidate = (alpha * nw + (1 - alpha) * lw)
        cand_tau = float(np.abs(candidate - lw).sum())
        if cand_tau <= ctx.max_turnover:
            out = candidate.reindex(new_w.index).fillna(0.0)
            return out / out.sum()
    # fallback
    out = (0.5 * nw + 0.5 * lw).reindex(new_w.index).fillna(0.0)
    return out / out.sum()

def persist_weights(w: pd.Series):
    d = {k: float(v) for k, v in w.items()}
    LAST_WEIGHTS_FP.write_text(json.dumps(d, indent=2, sort_keys=True))

# ---------------------------- Model: BTC halving macro tilt ----------------------------
def btc_halving_tilt(prices: pd.DataFrame, btc_pid: str = "BTC-USD") -> float:
    """
    Return [0.5..1.2] multiplier for BTC weight based on halving phase.
    2024-04-20 as last halving. Early expansion: >1.0 tilt; late-cycle: ~1.0.
    """
    import datetime as dt
    phase_mult = 1.0
    try:
        last_halving = dt.datetime(2024, 4, 20, tzinfo=dt.timezone.utc)
        now = dt.datetime.now(dt.timezone.utc)
        months = (now.year - last_halving.year) * 12 + (now.month - last_halving.month)
        if months <= 12: phase_mult = 1.15
        elif months <= 30: phase_mult = 1.05
        else: phase_mult = 0.95
    except Exception:
        pass
    return float(phase_mult)

# ---------------------------- Main portfolio constructor ----------------------------
def build_portfolio(ctx: PMContext) -> Dict[str, Any]:
    prices = get_prices_matrix(ctx)
    if prices.empty or prices.shape[1] < 2:
        return {"name":"pm_portfolio","type":"rebalance","target_weights":{},"note":"insufficient data"}

    # Compute returns, covariance
    rets = returns_from_prices(prices, mode="log")
    cov = ewma_cov(rets, lam=0.94)

    # Base models
    w_rp   = risk_parity_weights(cov)
    w_minv = min_variance_weights(cov)
    w_maxd = max_diversification_weights(cov)

    # Momentum / quality tilt
    score = momentum_quality_scores(prices).reindex(w_rp.index).fillna(0.0)
    w_momo = (score.clip(lower=0) + 1e-9)  # non-negative
    w_momo = w_momo / w_momo.sum()

    # Blend models (you can tune these)
    base_models = {
        "risk_parity": w_rp,
        "min_variance": w_minv,
        "max_diversification": w_maxd,
        "momentum_quality": w_momo,
    }
    base_weights = {"risk_parity": 0.40, "min_variance": 0.20, "max_diversification": 0.20, "momentum_quality": 0.20}
    w = blend_models(base_models, base_weights)

    # Halving tilt for BTC, if present
    if "BTC-USD" in w.index:
        mult = btc_halving_tilt(prices, "BTC-USD")
        w.loc["BTC-USD"] *= mult
        w = w / w.sum()

    # Cap/min constraints
    w = cap_and_normalize(w, max_w=ctx.max_weight, min_w=ctx.min_weight)

    # Category equalization overlay via Neo4j (optional)
    try:
        w = equalize_by_category(ctx, w)
    except Exception:
        pass

    # Defensive overlay (shift to stables when risk_off)
    regime = btc_regime(prices, "BTC-USD")
    w = apply_defensive_overlay(w, regime, ctx.stables)

    # Re-apply caps after overlays
    w = cap_and_normalize(w, max_w=ctx.max_weight, min_w=ctx.min_weight)

    # Turnover control vs last run
    w_tc = turnover_shrink(w, ctx)

    # Persist
    persist_weights(w_tc)

    out = {
        "name": "pm_portfolio_v2",
        "type": "rebalance",
        "target_weights": {k: round(float(v), 6) for k, v in w_tc.items()},
        "models": {k: round(float(v), 3) for k, v in base_weights.items()},
        "notes": {
            "regime": regime,
            "constraints": {"max_weight": ctx.max_weight, "min_weight": ctx.min_weight, "max_turnover": ctx.max_turnover},
            "halving_tilt_applied": "BTC-USD" in w.index
        }
    }
    return out

# ---------------------------- CLI ----------------------------
def _parse_args():
    ap = argparse.ArgumentParser(description="Portfolio management strategy suite")
    ap.add_argument("--products", type=str, default=os.getenv("PRODUCTS","BTC-USD,ETH-USD,SOL-USD"),
                    help="Comma-separated product IDs")
    ap.add_argument("--granularity", type=str, default=os.getenv("BAR_GRANULARITY","ONE_HOUR"))
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("LOOKBACK_DAYS","240")))
    ap.add_argument("--max-weight", type=float, default=float(os.getenv("MAX_WEIGHT","0.25")))
    ap.add_argument("--max-turnover", type=float, default=float(os.getenv("MAX_TURNOVER","0.35")))
    return ap.parse_args()

def main():
    args = _parse_args()
    cb = CBClient()
    products = [p.strip() for p in args.products.split(",") if p.strip()]
    ctx = PMContext(cb=cb, products=products, granularity=args.granularity,
                    lookback_days=args.lookback_days, max_weight=args.max_weight, max_turnover=args.max_turnover)
    sig = build_portfolio(ctx)
    print(json.dumps(sig, indent=2))

if __name__ == "__main__":
    main()
