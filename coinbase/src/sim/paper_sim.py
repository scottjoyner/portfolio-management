from __future__ import annotations
import math, time
from dataclasses import dataclass
from typing import Dict, List, Optional
import pandas as pd

from ..data import fetch_candles_df, compute_atr
from ..alpha.alpha import (
    donchian_breakout_setup, trend_rsi_pullback_setup,
    donchian_breakdown_setup, trend_rsi_rip_setup
)
from ..tcost import effective_fill_price
from ..analytics import log_trade

@dataclass
class SimConfig:
    initial_cash: float = 10_000.0
    risk_per_trade: float = 0.01
    min_rr: float = 2.0
    stop_k: float = 2.0
    target_k: float = 3.0
    taker_fee_bps: float = 8.0
    slippage_bps: float = 0.0
    impact_coeff: float = 1.5

@dataclass
class Position:
    side: str            # 'long' | 'short'
    base: float          # base size
    entry: float         # entry price
    stop: float
    target: float
    atr: float
    setup: str
    open_ts: int

class PaperBroker:
    def __init__(self, cfg: SimConfig):
        self.cfg = cfg
        self.cash = cfg.initial_cash
        self.positions: Dict[str, Position] = {}
        self.equity_history: List[Dict] = []

    def mark_to_market(self, t: pd.Timestamp, prices: Dict[str, float]) -> float:
        equity = self.cash
        for pid, pos in self.positions.items():
            px = prices.get(pid, pos.entry)
            if pos.side == "long":
                equity += pos.base * px
            else:
                # short: liability decreases when price falls
                equity += pos.base * (2*pos.entry - px)  # simplistic: assume borrow at entry
        self.equity_history.append({"t": t, "equity": equity, "cash": self.cash})
        return equity

    def place(self, pid: str, side: str, size: float, entry: float, stop: float, target: float, atr: float, t: pd.Timestamp, bid: float, ask: float):
        notional = size * entry
        fill = effective_fill_price("buy" if side=="long" else "sell", entry, bid, ask, notional,
                                    self.cfg.taker_fee_bps, self.cfg.slippage_bps, self.cfg.impact_coeff)
        cost = fill * size
        if side == "long":
            if self.cash < cost: 
                return False
            self.cash -= cost
        else:
            # short: receive proceeds now
            self.cash += cost
        self.positions[pid] = Position(side=side, base=size, entry=fill, stop=stop, target=target, atr=atr, setup="sim", open_ts=int(t.timestamp()))
        return True

    def try_exit(self, pid: str, px: float, t: pd.Timestamp, bid: float, ask: float):
        if pid not in self.positions: 
            return
        pos = self.positions[pid]
        exit_reason = None
        if pos.side == "long":
            if px <= pos.stop: exit_reason = "stop"
            elif px >= pos.target: exit_reason = "target"
        else:
            if px >= pos.stop: exit_reason = "stop"
            elif px <= pos.target: exit_reason = "target"
        if exit_reason:
            notional = pos.base * px
            fill = effective_fill_price("sell" if pos.side=="long" else "buy", px, bid, ask, notional,
                                        self.cfg.taker_fee_bps, self.cfg.slippage_bps, self.cfg.impact_coeff)
            if pos.side == "long":
                self.cash += fill * pos.base
                r = (fill - pos.entry) / max(1e-9, pos.entry - pos.stop)
                pnl = (fill - pos.entry) * pos.base
            else:
                self.cash -= fill * pos.base  # buy to cover
                r = (pos.entry - fill) / max(1e-9, pos.stop - pos.entry)
                pnl = (pos.entry - fill) * pos.base
            log_trade({
                "ts_open": pos.open_ts, "ts_close": int(t.timestamp()), "product_id": pid, "setup": pos.setup, "side": pos.side,
                "entry": pos.entry, "stop": pos.stop, "target": pos.target, "exit_price": fill, "exit_reason": exit_reason,
                "r_multiple": r, "pnl_usd": pnl
            })
            del self.positions[pid]

def simulate(cb, products: List[str], start_days: int, end_now: bool, granularity: str, cfg: SimConfig) -> pd.DataFrame:
    """
    Pull candles, simulate bracket entries at bar close using RR filter, and manage exits at next bars.
    """
    all_dfs: Dict[str, pd.DataFrame] = {}
    for p in products:
        df = fetch_candles_df(cb, p, lookback_days=start_days, granularity=granularity)
        all_dfs[p] = df

    index = all_dfs[products[0]].index
    broker = PaperBroker(cfg)

    for t in index:
        # Build prices & bids/asks using close +/- half-spread proxy
        prices, bids, asks = {}, {}, {}
        for p, df in all_dfs.items():
            if t not in df.index: continue
            row = df.loc[t]
            mid = float(row["close"])
            spr = max(1e-6, float(row["high"] - row["low"])) / max(1e-6, float(row["close"])) * 10000.0  # bps proxy
            bid = mid * (1 - spr/2/10000.0); ask = mid * (1 + spr/2/10000.0)
            prices[p], bids[p], asks[p] = mid, bid, ask

        # Manage exits on current bar mid
        for p in list(broker.positions.keys()):
            if p in prices:
                broker.try_exit(p, prices[p], t, bids[p], asks[p])

        # If flat, consider new entries at bar close
        for p, df in all_dfs.items():
            if p in broker.positions: 
                continue
            if t not in df.index: 
                continue
            window = df.loc[:t].tail(240)  # have enough context
            if len(window) < 220: 
                continue
            # Build setups
            cands = []
            s1 = donchian_breakout_setup(window, cfg.stop_k, cfg.target_k)
            if s1: cands.append(s1 | {"name": "donchian_breakout"})
            s2 = trend_rsi_pullback_setup(window, cfg.stop_k)
            if s2: cands.append(s2 | {"name": "trend_rsi_pullback"})
            s3 = donchian_breakdown_setup(window, cfg.stop_k, cfg.target_k)
            if s3: cands.append(s3 | {"name": "donchian_breakdown"})
            s4 = trend_rsi_rip_setup(window, cfg.stop_k)
            if s4: cands.append(s4 | {"name": "trend_rsi_rip"})
            if not cands: 
                continue
            best = max(cands, key=lambda x: x["rr"])
            if best["rr"] < cfg.min_rr:
                continue
            # Size
            if best["side"] == "buy":
                rpu = best["entry"] - best["stop"]
            else:
                rpu = best["stop"] - best["entry"]
            risk_budget = cfg.risk_per_trade * broker.mark_to_market(t, prices)
            size = max(0.0, risk_budget / max(1e-9, rpu))
            broker.place(p, "long" if best["side"]=="buy" else "short", size, best["entry"], best["stop"], best["target"], best.get("atr", 0.0), t, bids[p], asks[p])
        # Mark-to-market at bar end
        broker.mark_to_market(t, prices)

    import pandas as pd
    eq = pd.DataFrame(broker.equity_history).set_index("t")
    return eq
