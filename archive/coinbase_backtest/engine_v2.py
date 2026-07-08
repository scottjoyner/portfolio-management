from __future__ import annotations
import math
import json
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from enum import Enum

import numpy as np
import pandas as pd

try:
    from ..cb_client import CBClient
    from ..data import fetch_candles_df, compute_atr
except Exception:
    from src.cb_client import CBClient
    from src.data import fetch_candles_df, compute_atr

from ..protocols import (
    Direction, InstrumentType, Bar, BacktestPosition, BacktestFill,
    BracketSetup, BaseStrategy, Opportunity, FillModel,
)
from ..fill_model import AdaptiveFillModel


class FillTiming(Enum):
    CLOSE = "close"
    NEXT_OPEN = "next_open"


@dataclass
class PerpConfig:
    funding_rate_bps: float = 0.01
    funding_interval_hours: int = 8
    initial_margin_pct: float = 0.05
    maintenance_margin_pct: float = 0.025


@dataclass
class BacktestConfig:
    initial_cash: float = 10_000.0
    risk_per_trade: float = 0.01
    max_positions: int = 12
    min_notional: float = 25.0
    enable_short: bool = True
    enable_futures: bool = True
    max_leverage: float = 3.0
    fee_bps: float = 8.0
    fill_timing: FillTiming = FillTiming.NEXT_OPEN
    enable_trailing: bool = True
    trail_atr_mult: float = 1.5
    breakeven_after_r: float = 1.0
    perp_config: PerpConfig = field(default_factory=PerpConfig)


@dataclass
class BacktestPositionState:
    product: str
    direction: Direction
    qty: float
    entry_px: float
    stop_px: float
    target_px: float
    atr: float
    strategy: str
    instrument: InstrumentType
    leverage: float
    opened_ts: pd.Timestamp
    entry_fees: float = 0.0
    cum_funding: float = 0.0


class DataPortalV2:
    def __init__(self, cb: CBClient, products: List[str],
                 granularity: str = "ONE_HOUR", lookback_days: int = 240,
                 start: Optional[str] = None, end: Optional[str] = None):
        self.products = products
        self.granularity = granularity
        self.lookback_days = lookback_days
        self.dfs = self._load(cb, start, end)

    def _load(self, cb: CBClient, start: Optional[str],
              end: Optional[str]) -> Dict[str, pd.DataFrame]:
        dfs: Dict[str, pd.DataFrame] = {}
        for p in self.products:
            df = fetch_candles_df(cb, p, self.lookback_days, self.granularity)
            if df is None or df.empty:
                continue
            df = df[["open", "high", "low", "close", "volume"]].copy()
            dfs[p] = df
        if not dfs:
            raise RuntimeError("No price series loaded")
        idx = None
        for d in dfs.values():
            idx = d.index if idx is None else idx.intersection(d.index)
        for k in list(dfs.keys()):
            dfs[k] = dfs[k].reindex(idx).dropna()
        if start:
            idx = idx[idx >= pd.to_datetime(start, utc=True)]
        if end:
            idx = idx[idx <= pd.to_datetime(end, utc=True)]
        for k in list(dfs.keys()):
            dfs[k] = dfs[k].reindex(idx).dropna()
        return dfs

    def time_index(self) -> pd.DatetimeIndex:
        return next(iter(self.dfs.values())).index

    def at(self, product: str, t_idx: int) -> Bar:
        row = self.dfs[product].iloc[t_idx]
        return Bar(
            timestamp=row.name.timestamp() if hasattr(row.name, 'timestamp') else float(t_idx),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        )

    def price(self, product: str, t_idx: int, field: str = "close") -> float:
        return float(self.dfs[product].iloc[t_idx][field])

    def window(self, product: str, t_idx: int) -> pd.DataFrame:
        return self.dfs[product].iloc[: t_idx + 1]

    def history_bars(self, product: str, t_idx: int, lookback: int) -> List[Bar]:
        df = self.dfs[product].iloc[max(0, t_idx - lookback + 1): t_idx + 1]
        return [
            Bar(
                timestamp=row.name.timestamp() if hasattr(row.name, 'timestamp') else float(i),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
            for i, (_, row) in enumerate(df.iterrows())
        ]

    def bid_ask(self, product: str, t_idx: int) -> Tuple[float, float]:
        bar = self.at(product, t_idx)
        spread = (bar.high - bar.low) / max(bar.close, 1e-9) * 10000
        bid = bar.close * (1 - spread / 2 / 10000)
        ask = bar.close * (1 + spread / 2 / 10000)
        return bid, ask


class EnhancedBacktestEngine:
    def __init__(self, portal: DataPortalV2, cfg: BacktestConfig):
        self.portal = portal
        self.cfg = cfg
        self.cash = cfg.initial_cash
        self.positions: Dict[str, BacktestPositionState] = {}
        self.trades: List[Dict[str, Any]] = []
        self.eq_curve = pd.Series(dtype=float)
        self.fill_model: FillModel = AdaptiveFillModel(fee_bps=cfg.fee_bps)

    def _apply_fill(self, direction: Direction, price: float, size: float,
                    bid: float, ask: float, volume: float) -> BacktestFill:
        return self.fill_model.fill(direction, price, size, bid, ask, volume)

    def _size_position(self, equity: float, entry: float, stop: float,
                       leverage: float = 1.0) -> float:
        risk_dollar = equity * self.cfg.risk_per_trade * leverage
        per_unit_risk = max(1e-9, abs(entry - stop))
        return risk_dollar / per_unit_risk

    def _update_trailing(self, pos: BacktestPositionState, px: float):
        if not self.cfg.enable_trailing or pos.atr <= 0:
            return pos
        trail = self.cfg.trail_atr_mult * pos.atr
        r_gain = abs(px - pos.entry_px) / max(1e-9, abs(pos.entry_px - pos.stop_px))
        if pos.direction == Direction.LONG:
            new_stop = max(pos.stop_px, px - trail)
            if r_gain >= self.cfg.breakeven_after_r:
                new_stop = max(new_stop, pos.entry_px)
        else:
            new_stop = min(pos.stop_px, px + trail)
            if r_gain >= self.cfg.breakeven_after_r:
                new_stop = min(new_stop, pos.entry_px)
        pos.stop_px = new_stop
        return pos

    def _check_liquidation(self, pos: BacktestPositionState, px: float) -> Optional[str]:
        if pos.instrument == InstrumentType.SPOT:
            return None
        pnl = (px - pos.entry_px) * pos.qty
        if pos.direction == Direction.SHORT:
            pnl = (pos.entry_px - px) * pos.qty
        equity_value = pos.qty * pos.entry_px / pos.leverage + pnl
        maintenance = pos.qty * px * self.cfg.perp_config.maintenance_margin_pct
        if equity_value <= maintenance:
            return "liquidation"
        return None

    def _apply_funding(self, pos: BacktestPositionState, bars_elapsed: int):
        if pos.instrument == InstrumentType.SPOT:
            return
        hours_per_bar = 1
        funding_interval_bars = self.cfg.perp_config.funding_interval_hours / max(hours_per_bar, 1)
        intervals = bars_elapsed // max(int(funding_interval_bars), 1)
        if intervals > 0:
            funding_payment = pos.qty * pos.entry_px * (self.cfg.perp_config.funding_rate_bps / 10000) * intervals
            pos.cum_funding += funding_payment

    def run(self, strategies: List[BaseStrategy],
            warmup: int = 100) -> Dict[str, Any]:
        idx = self.portal.time_index()
        daily_marks: Dict[pd.Timestamp, float] = {}

        for t_idx, ts in enumerate(idx):
            if t_idx < warmup:
                continue

            prices_now: Dict[str, float] = {}
            for p in self.portal.products:
                try:
                    prices_now[p] = self.portal.price(p, t_idx)
                except Exception:
                    continue

            # Check exits and manage brackets
            to_close: List[Tuple[str, float, str]] = []
            for p, pos in list(self.positions.items()):
                px = prices_now.get(p)
                if px is None:
                    continue
                self._update_trailing(pos, px)
                liq = self._check_liquidation(pos, px)
                if liq:
                    to_close.append((p, px, liq))
                    continue
                if pos.direction == Direction.LONG:
                    if px <= pos.stop_px:
                        to_close.append((p, pos.stop_px, "stop"))
                    elif px >= pos.target_px:
                        to_close.append((p, pos.target_px, "target"))
                else:
                    if px >= pos.stop_px:
                        to_close.append((p, pos.stop_px, "stop"))
                    elif px <= pos.target_px:
                        to_close.append((p, pos.target_px, "target"))

            for p, fill_px, reason in to_close:
                pos = self.positions.pop(p, None)
                if pos is None:
                    continue
                bid, ask = self.portal.bid_ask(p, t_idx)
                vol = self.portal.at(p, t_idx).volume
                if pos.direction == Direction.LONG:
                    fill = self._apply_fill(Direction.SHORT, fill_px, pos.qty, bid, ask, vol)
                else:
                    fill = self._apply_fill(Direction.LONG, fill_px, pos.qty, bid, ask, vol)
                notional = pos.qty * fill.price
                if pos.direction == Direction.LONG:
                    self.cash += notional - fill.fees
                    pnl = (fill.price - pos.entry_px) * pos.qty
                else:
                    self.cash -= notional + fill.fees
                    pnl = (pos.entry_px - fill.price) * pos.qty
                self.cash -= pos.cum_funding
                r = pnl / max(1e-9, abs(pos.entry_px - pos.stop_px) * pos.qty)
                self.trades.append({
                    "ts": ts.isoformat(),
                    "product": p,
                    "direction": pos.direction.value,
                    "qty": -pos.qty,
                    "entry": pos.entry_px,
                    "exit": fill.price,
                    "reason": reason,
                    "pnl": pnl,
                    "r_multiple": r,
                    "fees": fill.fees,
                    "funding": pos.cum_funding,
                    "strategy": pos.strategy,
                    "instrument": pos.instrument.value,
                    "leverage": pos.leverage,
                })

            # Generate signals from strategies
            for strategy in strategies:
                try:
                    for p in self.portal.products:
                        if p not in prices_now:
                            continue
                        bar = self.portal.at(p, t_idx)
                        history = self.portal.history_bars(p, t_idx, 200)
                        setup = strategy.on_bar(bar, history)
                        if setup is None:
                            continue
                        if p in self.positions:
                            continue
                        if setup.direction == Direction.SHORT and not self.cfg.enable_short:
                            continue
                        if setup.instrument_type != InstrumentType.SPOT and not self.cfg.enable_futures:
                            continue

                        bid, ask = self.portal.bid_ask(p, t_idx)
                        fill = self._apply_fill(
                            setup.direction, setup.entry_price,
                            self._size_position(
                                self._equity(prices_now),
                                setup.entry_price,
                                setup.stop_price,
                                setup.leverage,
                            ),
                            bid, ask, bar.volume,
                        )
                        if fill.size <= 0:
                            continue
                        notional = fill.size * fill.price
                        if notional < self.cfg.min_notional:
                            continue
                        if setup.direction == Direction.LONG:
                            if notional > self.cash:
                                continue
                            self.cash -= notional + fill.fees
                        else:
                            self.cash += notional - fill.fees
                        self.positions[p] = BacktestPositionState(
                            product=p,
                            direction=setup.direction,
                            qty=fill.size,
                            entry_px=fill.price,
                            stop_px=setup.stop_price,
                            target_px=setup.target_price,
                            atr=setup.atr,
                            strategy=setup.strategy_name,
                            instrument=setup.instrument_type,
                            leverage=setup.leverage,
                            opened_ts=ts,
                            entry_fees=fill.fees,
                        )
                        self.trades.append({
                            "ts": ts.isoformat(),
                            "product": p,
                            "direction": setup.direction.value,
                            "qty": fill.size,
                            "entry": fill.price,
                            "stop": setup.stop_price,
                            "target": setup.target_price,
                            "reason": f"entry_{setup.strategy_name}",
                            "pnl": 0.0,
                            "fees": fill.fees,
                            "strategy": setup.strategy_name,
                            "instrument": setup.instrument_type.value,
                            "leverage": setup.leverage,
                        })
                except Exception:
                    continue

            # Mark equity
            eq = self._equity(prices_now)
            self.eq_curve.loc[ts] = eq
            daily_marks[ts.normalize()] = eq

        # Final liquidation
        if len(idx) > 0:
            last_ts = idx[-1]
            prices_now = {p: self.portal.price(p, len(idx) - 1) for p in self.portal.products}
            for p, pos in list(self.positions.items()):
                px = prices_now.get(p, pos.entry_px)
                bid, ask = self.portal.bid_ask(p, len(idx) - 1)
                vol = self.portal.at(p, len(idx) - 1).volume
                if pos.direction == Direction.LONG:
                    fill = self._apply_fill(Direction.SHORT, px, pos.qty, bid, ask, vol)
                    self.cash += pos.qty * fill.price - fill.fees
                    pnl = (fill.price - pos.entry_px) * pos.qty
                else:
                    fill = self._apply_fill(Direction.LONG, px, pos.qty, bid, ask, vol)
                    self.cash -= pos.qty * fill.price + fill.fees
                    pnl = (pos.entry_px - fill.price) * pos.qty
                self.trades.append({
                    "ts": last_ts.isoformat(),
                    "product": p,
                    "direction": pos.direction.value,
                    "qty": -pos.qty,
                    "entry": pos.entry_px,
                    "exit": fill.price,
                    "reason": "final",
                    "pnl": pnl,
                    "fees": fill.fees,
                    "funding": pos.cum_funding,
                    "strategy": pos.strategy,
                    "instrument": pos.instrument.value,
                    "leverage": pos.leverage,
                })
                self.positions.pop(p, None)
            eq = self._equity(prices_now)
            self.eq_curve.loc[last_ts] = eq

        daily_series = pd.Series(daily_marks).sort_index()
        metrics = self._compute_metrics(self.eq_curve, daily_series)
        return {
            "metrics": metrics,
            "equity_curve": self.eq_curve,
            "trades": pd.DataFrame(self.trades),
        }

    def _equity(self, prices: Dict[str, float]) -> float:
        eq = self.cash
        for pos in self.positions.values():
            px = prices.get(pos.product, pos.entry_px)
            unrealized = (px - pos.entry_px) * pos.qty
            if pos.direction == Direction.SHORT:
                unrealized = (pos.entry_px - px) * pos.qty
            eq += pos.qty * px / pos.leverage + unrealized - pos.cum_funding
        return eq

    def _compute_metrics(self, eq_curve: pd.Series,
                         daily_curve: pd.Series) -> Dict[str, float]:
        if len(eq_curve) < 2:
            return {"CAGR": 0, "TotalReturn": 0, "MaxDrawdown": 0, "Sharpe": 0, "Sortino": 0}
        ret = eq_curve.iloc[-1] / eq_curve.iloc[0] - 1.0
        years = max(1e-9, (eq_curve.index[-1] - eq_curve.index[0]).days / 365.25)
        cagr = (eq_curve.iloc[-1] / eq_curve.iloc[0]) ** (1 / years) - 1 if years > 0 else 0.0
        dd_series = eq_curve / eq_curve.cummax() - 1.0
        max_dd = float(dd_series.min())
        d_rets = daily_curve.pct_change().dropna()
        vol = float(d_rets.std(ddof=0))
        mu = float(d_rets.mean())
        sharpe = (mu * 365) / (vol * math.sqrt(365)) if vol > 0 else 0.0
        downside = float(d_rets[d_rets < 0].std(ddof=0))
        sortino = (mu * 365) / (downside * math.sqrt(365)) if downside > 0 else 0.0
        calmar = cagr / abs(max_dd) if max_dd < 0 else 0.0
        return {
            "CAGR": round(cagr, 4),
            "TotalReturn": round(ret, 4),
            "MaxDrawdown": round(max_dd, 4),
            "Calmar": round(calmar, 3) if calmar else 0.0,
            "Sharpe": round(sharpe, 3),
            "Sortino": round(sortino, 3),
        }
