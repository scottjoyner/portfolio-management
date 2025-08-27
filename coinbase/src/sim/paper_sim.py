from __future__ import annotations
import pandas as pd
from dataclasses import dataclass
from typing import Dict, List
from ..data import fetch_candles_df
from ..alpha.alpha import donchian_breakout_setup, trend_rsi_pullback_setup, donchian_breakdown_setup, trend_rsi_rip_setup
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

class PaperBroker:
    def __init__(self, cfg: SimConfig):
        self.cfg = cfg
        self.cash = cfg.initial_cash
        self.positions: Dict[str, dict] = {}
        self.equity_history: List[Dict] = []

    def mark_to_market(self, t, prices: Dict[str, float]) -> float:
        equity = self.cash
        for pid, pos in self.positions.items():
            px = prices.get(pid, pos['entry'])
            equity += pos['base'] * (px if pos['side']=='long' else (2*pos['entry']-px))
        self.equity_history.append({"t": t, "equity": equity, "cash": self.cash})
        return equity

    def place(self, pid, side, size, entry, stop, target, t, bid, ask):
        notional = size * entry
        fill = effective_fill_price("buy" if side=="long" else "sell", entry, bid, ask, notional, self.cfg.taker_fee_bps, self.cfg.slippage_bps, self.cfg.impact_coeff)
        cost = fill * size
        if side == "long":
            if self.cash < cost: return False
            self.cash -= cost
        else:
            self.cash += cost
        self.positions[pid] = {"side":side, "base":size, "entry":fill, "stop":stop, "target":target, "open_ts":int(pd.Timestamp(t).timestamp())}
        return True

    def try_exit(self, pid, px, t, bid, ask):
        if pid not in self.positions: return
        pos = self.positions[pid]; reason=None
        if pos["side"]=="long":
            if px<=pos["stop"]: reason="stop"
            elif px>=pos["target"]: reason="target"
        else:
            if px>=pos["stop"]: reason="stop"
            elif px<=pos["target"]: reason="target"
        if reason:
            notional = pos["base"] * px
            fill = effective_fill_price("sell" if pos["side"]=="long" else "buy", px, bid, ask, notional, self.cfg.taker_fee_bps, self.cfg.slippage_bps, self.cfg.impact_coeff)
            if pos["side"]=="long":
                self.cash += fill * pos["base"]; r=(fill-pos["entry"])/max(1e-9,pos["entry"]-pos["stop"]); pnl=(fill-pos["entry"])*pos["base"]
            else:
                self.cash -= fill * pos["base"]; r=(pos["entry"]-fill)/max(1e-9,pos["stop"]-pos["entry"]); pnl=(pos["entry"]-fill)*pos["base"]
            log_trade({"ts_open": pos["open_ts"], "ts_close": int(pd.Timestamp(t).timestamp()), "product_id": pid, "setup": "sim", "side": pos["side"],
                       "entry": pos["entry"], "stop": pos["stop"], "target": pos["target"], "exit_price": fill, "exit_reason": reason, "r_multiple": r, "pnl_usd": pnl})
            del self.positions[pid]

def simulate(cb, products: List[str], start_days: int, granularity: str, cfg: SimConfig) -> pd.DataFrame:
    all_dfs = {p: fetch_candles_df(cb, p, lookback_days=start_days, granularity=granularity) for p in products}
    index = next(iter(all_dfs.values())).index
    broker = PaperBroker(cfg)
    for t in index:
        prices, bids, asks = {}, {}, {}
        for p, df in all_dfs.items():
            if t not in df.index: continue
            row = df.loc[t]; mid = float(row["close"]); spr = max(1e-6,float(row["high"]-row["low"])) / max(1e-6,mid) * 10000.0
            prices[p]=mid; bids[p]=mid*(1-spr/2/10000.0); asks[p]=mid*(1+spr/2/10000.0)
        for p in list(broker.positions.keys()):
            if p in prices: broker.try_exit(p, prices[p], t, bids[p], asks[p])
        for p, df in all_dfs.items():
            if p in broker.positions or t not in df.index: continue
            window = df.loc[:t].tail(240)
            if len(window) < 220: continue
            cands = []
            for fn in (donchian_breakout_setup, trend_rsi_pullback_setup, donchian_breakdown_setup, trend_rsi_rip_setup):
                s = fn(window) if fn in (donchian_breakout_setup, donchian_breakdown_setup) else fn(window, cfg.stop_k)
                if s: cands.append(s)
            if not cands: continue
            best = max(cands, key=lambda x: x["rr"])
            if best["rr"] < cfg.min_rr: continue
            rpu = (best["entry"]-best["stop"]) if best["side"]=="buy" else (best["stop"]-best["entry"])
            risk_budget = cfg.risk_per_trade * broker.mark_to_market(t, prices)
            size = max(0.0, risk_budget / max(1e-9, rpu))
            broker.place(p, "long" if best["side"]=="buy" else "short", size, best["entry"], best["stop"], best["target"], t, bids[p], asks[p])
        broker.mark_to_market(t, prices)
    import pandas as pd
    return pd.DataFrame(broker.equity_history).set_index("t")
