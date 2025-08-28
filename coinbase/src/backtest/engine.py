# src/backtest/engine.py
from __future__ import annotations
import os, math, json, dataclasses, enum
from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

# Project imports
try:
    from ..cb_client import CBClient
    from ..data import fetch_candles_df, compute_atr, rolling_high, rolling_low, rsi
except Exception:
    from src.cb_client import CBClient
    from src.data import fetch_candles_df, compute_atr, rolling_high, rolling_low, rsi

load_dotenv(override=False)

STATE_DIR = Path("state"); STATE_DIR.mkdir(exist_ok=True, parents=True)

# -------------------- Config & Enums --------------------
class FillWhen(str, enum.Enum):
    CLOSE = "close"      # fill at bar close (same bar)
    NEXT_OPEN = "next"   # fill at next bar's open

@dataclass
class ExecModel:
    fee_bps: float = float(os.getenv("TAKER_FEE_BPS", "8.0"))
    slippage_bps: float = float(os.getenv("SLIPPAGE_BPS", "1.5"))
    fill_when: FillWhen = FillWhen.NEXT_OPEN

@dataclass
class RiskModel:
    initial_cash: float = 10_000.0
    risk_per_trade: float = 0.01           # fraction of equity at risk per trade
    max_leverage: float = 1.0              # portfolio-level gross cap
    max_positions: int = 12
    min_notional: float = 25.0

@dataclass
class BTConfig:
    products: List[str]
    granularity: str = os.getenv("BAR_GRANULARITY", "ONE_HOUR")
    lookback_days: int = 240
    start: Optional[str] = None            # ISO date (UTC) optional cut
    end: Optional[str] = None
    exec_model: ExecModel = dataclasses.field(default_factory=ExecModel)
    risk_model: RiskModel = dataclasses.field(default_factory=RiskModel)
    # trailing/management
    enable_trailing: bool = True
    trail_atr_mult: float = 1.0
    breakeven_after_R: float = 1.0         # move stop to BE after +1R

# -------------------- Data Portal --------------------
class DataPortal:
    """Preloads aligned OHLCV for products and exposes rolling windows."""
    def __init__(self, cb: CBClient, cfg: BTConfig):
        self.cb = cb
        self.cfg = cfg
        self.dfs = self._load()

    def _load(self) -> Dict[str, pd.DataFrame]:
        dfs: Dict[str, pd.DataFrame] = {}
        for p in self.cfg.products:
            df = fetch_candles_df(self.cb, p, self.cfg.lookback_days, self.cfg.granularity)
            if df is None or df.empty:
                continue
            # normalize columns
            df = df.copy()
            df = df[["open","high","low","close","volume"]]
            dfs[p] = df
        if not dfs:
            raise RuntimeError("No price series loaded.")
        # align on intersection to avoid lookahead; then slice date range
        idx = None
        for d in dfs.values():
            idx = d.index if idx is None else idx.intersection(d.index)
        for k in list(dfs.keys()):
            dfs[k] = dfs[k].reindex(idx).dropna()
        if self.cfg.start:
            idx = idx[idx >= pd.to_datetime(self.cfg.start, utc=True)]
        if self.cfg.end:
            idx = idx[idx <= pd.to_datetime(self.cfg.end, utc=True)]
        for k in list(dfs.keys()):
            dfs[k] = dfs[k].reindex(idx).dropna()
        return dfs

    def time_index(self) -> pd.DatetimeIndex:
        # All dfs share same index now
        return next(iter(self.dfs.values())).index

    def window(self, product: str, t_idx: int) -> pd.DataFrame:
        df = self.dfs[product]
        return df.iloc[: t_idx + 1]

# -------------------- Strategy Adapters --------------------
# Each adapter must implement .on_bar(t_idx) and return a list of signals (entry/stop/target)
# Signal schema:
#  {"name": "...","type":"entry","product_id":"BTC-USD","side":"buy|sell",
#   "entry":float,"stop":float,"target":float,
#   "atr":float,"rr":float,"confidence":0..1, ...}

class BaseAdapter:
    def __init__(self, portal: DataPortal, cfg: BTConfig, name: str):
        self.portal = portal
        self.cfg = cfg
        self.name = name

    def on_bar(self, t_idx: int) -> List[Dict[str,Any]]:
        return []

# --- Example adapters covering your earlier strategies (vectorized-ish) ---

class TripleMAAdapter(BaseAdapter):
    """20/50/100 MA crossovers; long on 20>50 and price>100."""
    def __init__(self, portal, cfg):
        super().__init__(portal, cfg, "triple_ma")

    def on_bar(self, t_idx):
        out=[]
        for p in self.portal.cfg.products:
            w = self.portal.window(p, t_idx)
            if len(w) < 110: continue
            cl = w["close"]
            m20 = cl.rolling(20).mean()
            m50 = cl.rolling(50).mean()
            m100= cl.rolling(100).mean()
            if pd.isna(m20.iloc[-1]) or pd.isna(m50.iloc[-1]) or pd.isna(m100.iloc[-1]): continue
            atr = compute_atr(w).iloc[-1]
            price = float(cl.iloc[-1])
            bull = (m20.iloc[-2] <= m50.iloc[-2] and m20.iloc[-1] > m50.iloc[-1]) and (price > m100.iloc[-1])
            if bull:
                stop = price - 2.0*atr; target = price + 3.0*atr
                out.append(self._sig(p, "buy", price, stop, target, atr, 0.6))
        return out

    def _sig(self, p, side, entry, stop, target, atr, conf):
        return {"name": self.name, "type":"entry","product_id":p,"side":side,
                "entry":float(entry),"stop":float(stop),"target":float(target),
                "atr":float(atr),"rr":float(max(0.0,(target-entry)/(entry-stop))) if side=="buy" else 0.0,
                "confidence":float(conf)}

class DonchianAdapter(BaseAdapter):
    def __init__(self, portal, cfg, lookback: int = 20):
        super().__init__(portal, cfg, "donchian_breakout")
        self.lookback = lookback

    def on_bar(self, t_idx):
        out=[]
        for p in self.portal.cfg.products:
            w = self.portal.window(p, t_idx)
            if len(w) < max(self.lookback,50): continue
            atr = compute_atr(w).iloc[-1]
            entry = float(w["close"].iloc[-1])
            breakout = float(rolling_high(w, self.lookback).iloc[-2])
            if entry > breakout:
                stop = entry - 2.0*atr; target = entry + 3.0*atr
                out.append({"name": self.name,"type":"entry","product_id":p,"side":"buy",
                            "entry":entry,"stop":stop,"target":target,"atr":float(atr),
                            "rr": float((target-entry)/max(1e-9, entry-stop)),"confidence":0.6})
        return out

class AggressiveMomoAdapter(BaseAdapter):
    """Top momentum (30 bars), 20-bar breakout, tight stop."""
    def __init__(self, portal, cfg, topk:int=4):
        super().__init__(portal, cfg, "aggressive_momo")
        self.topk = topk

    def on_bar(self, t_idx):
        scans=[]
        for p in self.portal.cfg.products:
            w = self.portal.window(p, t_idx)
            if len(w) < 60: continue
            cl = w["close"]
            ret30 = float(cl.iloc[-1]/cl.iloc[-30]-1.0) if len(cl)>=31 else 0.0
            scans.append((ret30, p, w))
        scans.sort(reverse=True)
        out=[]
        for _, p, w in scans[:min(self.topk, len(scans))]:
            atr = compute_atr(w).iloc[-1]
            px = float(w["close"].iloc[-1])
            brk = float(rolling_high(w,20).iloc[-2])
            if px <= brk: continue
            out.append({"name": self.name,"type":"entry","product_id":p,"side":"buy",
                        "entry":px,"stop":px-1.5*atr,"target":px+4.0*atr,"atr":float(atr),
                        "rr": float(4.0/1.5),"confidence":0.7,
                        "breakeven_after_R":0.5,"trail_atr_mult":1.0})
        return out

# -------------------- Execution & Portfolio --------------------
@dataclass
class Position:
    product: str
    side: str                 # "long" only in these adapters
    qty: float
    entry_px: float
    stop_px: float
    target_px: float
    atr: float
    name: str
    opened_ts: pd.Timestamp

class Portfolio:
    def __init__(self, risk: RiskModel):
        self.cash = risk.initial_cash
        self.equity = risk.initial_cash
        self.positions: Dict[str, Position] = {}  # by product
        self.gross = 0.0
        self.turnover = 0.0

    def valuation(self, prices: Dict[str,float]) -> float:
        eq = self.cash
        for pos in self.positions.values():
            px = prices.get(pos.product, pos.entry_px)
            eq += pos.qty * px
        self.equity = eq
        return eq

# -------------------- Metrics --------------------
def _drawdown_curve(series: pd.Series) -> Tuple[pd.Series, float, float]:
    roll_max = series.cummax()
    dd = series/roll_max - 1.0
    max_dd = float(dd.min())
    calmar = (series.iloc[-1]/series.iloc[0]-1.0)/abs(max_dd) if max_dd < 0 else np.inf
    return dd, max_dd, calmar

def compute_metrics(equity_curve: pd.Series, daily_curve: pd.Series) -> Dict[str,float]:
    # annualization assumes 365d for daily
    ret = equity_curve.iloc[-1]/equity_curve.iloc[0]-1.0
    years = max(1e-9, (equity_curve.index[-1]-equity_curve.index[0]).days/365.25)
    cagr = (equity_curve.iloc[-1]/equity_curve.iloc[0])**(1/years)-1 if years>0 else 0.0
    dd, max_dd, calmar = _drawdown_curve(equity_curve)
    d_rets = daily_curve.pct_change().dropna()
    ann = math.sqrt(365.0)
    mu, sd = float(d_rets.mean()), float(d_rets.std(ddof=0))
    sharpe = (mu*365.0)/(sd*ann) if sd>0 else np.nan  # same as sqrt(365)*mu/sd
    downside = float(d_rets[d_rets<0].std(ddof=0))
    sortino = (mu*365.0)/(downside*ann) if downside>0 else np.nan
    return {
        "CAGR": round(cagr,4),
        "TotalReturn": round(ret,4),
        "MaxDrawdown": round(max_dd,4),
        "Calmar": round(calmar,3) if np.isfinite(calmar) else np.nan,
        "Sharpe": round(sharpe,3),
        "Sortino": round(sortino,3),
    }

# -------------------- Backtest Engine --------------------
class BacktestEngine:
    def __init__(self, cb: CBClient, cfg: BTConfig, adapters: List[BaseAdapter]):
        self.cb = cb
        self.cfg = cfg
        self.portal = DataPortal(cb, cfg)
        self.adapters = adapters
        self.port = Portfolio(cfg.risk_model)
        self.trades: List[Dict[str,Any]] = []
        self.eq_curve = pd.Series(dtype=float)
        self.eq_daily = pd.Series(dtype=float)

    # --- helpers ---
    def _price_at(self, product: str, t_idx: int, field: str="close") -> float:
        w = self.portal.window(product, t_idx)
        if self.cfg.exec_model.fill_when == FillWhen.NEXT_OPEN and field=="entry":
            if t_idx+1 < len(self.portal.time_index()):
                return float(self.portal.window(product, t_idx+1)["open"].iloc[-1])
        return float(w[field].iloc[-1])

    def _apply_fees_slippage(self, px: float, side: str) -> float:
        # simple bps on price + slippage
        bps = (self.cfg.exec_model.fee_bps + self.cfg.exec_model.slippage_bps)/10_000.0
        return px * (1 + bps) if side=="buy" else px * (1 - bps)

    def _size_position(self, equity: float, entry: float, stop: float) -> float:
        risk_dollar = equity * self.cfg.risk_model.risk_per_trade
        per_unit_risk = max(1e-9, entry - stop)
        units = risk_dollar / per_unit_risk
        return max(0.0, units)

    def _update_trailing(self, pos: Position, px: float):
        if not self.cfg.enable_trailing: return pos
        # move stop up if price advanced; ATR trail
        trail = self.cfg.trail_atr_mult * pos.atr
        new_stop = max(pos.stop_px, px - trail)
        # optional breakeven
        r_gain = (px - pos.entry_px) / max(1e-9, pos.entry_px - pos.stop_px)
        if r_gain >= self.cfg.breakeven_after_R:
            new_stop = max(new_stop, pos.entry_px)
        pos.stop_px = new_stop
        return pos

    # --- core loop ---
    def run(self) -> Dict[str,Any]:
        idx = self.portal.time_index()
        daily_marks: Dict[pd.Timestamp, float] = {}
        for t_idx, ts in enumerate(idx):
            # 1) strategy signals at this bar
            signals: List[Dict[str,Any]] = []
            for ad in self.adapters:
                try:
                    signals.extend(ad.on_bar(t_idx) or [])
                except Exception as e:
                    signals.append({"type":"error","name":ad.name,"error":str(e)})

            # 2) close/target/stop checks for open positions
            prices_now = {p: float(self.portal.window(p, t_idx)["close"].iloc[-1]) for p in self.cfg.products}
            to_close = []
            for p, pos in self.port.positions.items():
                px = prices_now[p]
                # trailing
                self._update_trailing(pos, px)
                # exits
                if px <= pos.stop_px:
                    to_close.append((p, pos.stop_px, "stop"))
                elif px >= pos.target_px:
                    to_close.append((p, pos.target_px, "target"))
            for p, fill_px, reason in to_close:
                pos = self.port.positions.pop(p, None)
                if pos is None: continue
                px_eff = self._apply_fees_slippage(fill_px, "sell")
                notional = pos.qty * px_eff
                self.port.cash += notional
                self.port.turnover += abs(notional)
                self.trades.append({
                    "ts": ts.isoformat(), "product": p, "side": "EXIT", "reason": reason,
                    "qty": -pos.qty, "price": px_eff, "name": pos.name
                })

            # 3) new entries
            for s in signals:
                if s.get("type") != "entry": continue
                p = s["product_id"]; side = s["side"]
                if side != "buy": continue  # long-only in this engine
                if p in self.port.positions: continue
                if len(self.port.positions) >= self.cfg.risk_model.max_positions: continue
                # execution price
                fill_px = self._price_at(p, t_idx, field="entry")
                fill_px = self._apply_fees_slippage(fill_px, "buy")
                # sizing
                eq_now = self.port.valuation(prices_now)
                qty = self._size_position(eq_now, fill_px, s["stop"])
                notional = qty * fill_px
                if notional < self.cfg.risk_model.min_notional or qty <= 0: continue
                if notional > self.port.cash:  # cash cap (no leverage unless enabled later)
                    continue
                # place
                self.port.cash -= notional
                self.port.turnover += abs(notional)
                self.port.positions[p] = Position(
                    product=p, side="long", qty=qty, entry_px=fill_px, stop_px=float(s["stop"]),
                    target_px=float(s["target"]), atr=float(s.get("atr",0.0)), name=s["name"], opened_ts=ts
                )
                self.trades.append({
                    "ts": ts.isoformat(), "product": p, "side": "BUY", "qty": qty,
                    "price": fill_px, "name": s["name"]
                })

            # 4) mark equity
            eq = self.port.valuation(prices_now)
            self.eq_curve.loc[ts] = eq
            if ts.normalize() not in daily_marks:
                daily_marks[ts.normalize()] = eq
            else:
                daily_marks[ts.normalize()] = eq

        # close all positions at last close
        last_ts = idx[-1]
        prices_now = {p: float(self.portal.window(p, len(idx)-1)["close"].iloc[-1]) for p in self.cfg.products}
        for p, pos in list(self.port.positions.items()):
            px_eff = self._apply_fees_slippage(prices_now[p], "sell")
            notional = pos.qty * px_eff
            self.port.cash += notional
            self.port.turnover += abs(notional)
            self.trades.append({
                "ts": last_ts.isoformat(), "product": p, "side":"EXIT", "reason":"final",
                "qty": -pos.qty, "price": px_eff, "name": pos.name
            })
            self.port.positions.pop(p, None)
        eq = self.port.valuation(prices_now)
        self.eq_curve.loc[last_ts] = eq
        daily_series = pd.Series(daily_marks).sort_index()
        self.eq_daily = daily_series

        # compute metrics & benchmarks
        bench = self._benchmarks(self.portal, self.cfg, self.eq_curve.index, self.port.cash)
        metrics = compute_metrics(self.eq_curve, self.eq_daily)
        result = {
            "metrics": metrics,
            "benchmarks": bench["metrics"],
        }
        # write csvs
        trades_df = pd.DataFrame(self.trades)
        trades_fp = STATE_DIR/"bt_trades.csv"; trades_df.to_csv(trades_fp, index=False)
        equity_fp = STATE_DIR/"bt_equity.csv"; self.eq_curve.rename("equity").to_csv(equity_fp)
        daily_fp = STATE_DIR/"bt_daily.csv"; self.eq_daily.rename("equity").to_csv(daily_fp)
        result["files"] = {"trades": str(trades_fp), "equity": str(equity_fp), "daily": str(daily_fp)}
        return result

    # --- simple benchmarks ---
    def _benchmarks(self, portal: DataPortal, cfg: BTConfig, idx: pd.DatetimeIndex, cash0: float):
        # BTC HODL
        if "BTC-USD" in cfg.products:
            px = portal.dfs["BTC-USD"]["close"]
        else:
            px = next(iter(portal.dfs.values()))["close"]
        px = px.reindex(idx).dropna()
        units = cash0 / float(px.iloc[0])
        eq = units * px
        met = compute_metrics(eq, eq.resample("1D").last().dropna())
        # Equal-weight HODL
        eqw = None
        for p, df in portal.dfs.items():
            s = (cash0/len(cfg.products)) * df["close"] / float(df["close"].iloc[0])
            eqw = s if eqw is None else eqw.add(s, fill_value=0.0)
        met2 = compute_metrics(eqw.reindex(idx).dropna(), eqw.resample("1D").last().dropna())
        return {"metrics":{"HODL_BTC":met, "HODL_EW":met2}}
