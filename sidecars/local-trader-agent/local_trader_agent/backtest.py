from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd

from .indicators import crossed_above, rsi_wilder
from .schemas import BacktestConfig, BacktestSummary, Trade


def _fmt_time(value) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _ann(interval: str) -> float:
    interval = interval.lower()
    if interval.endswith("wk") or interval.endswith("w"):
        return 52.0
    if interval.endswith("mo"):
        return 12.0
    if interval.endswith("h"):
        return 252.0 * 6.5
    if interval.endswith("m"):
        return 252.0 * 6.5 * 60.0
    return 252.0


def max_drawdown_pct(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    return float(((equity / equity.cummax()) - 1.0).min() * 100.0)


def enrich_with_signals(df: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    out = df.copy()
    out["RSI"] = rsi_wilder(out["Close"], cfg.rsi_period)
    out["BuySignal"] = crossed_above(out["RSI"], cfg.buy_rsi_cross).fillna(False)
    return out


def _fee(notional: float, cfg: BacktestConfig) -> float:
    return cfg.commission_per_trade + cfg.commission_pct * notional


def _entry_price(price: float, cfg: BacktestConfig) -> float:
    return price * (1.0 + (cfg.slippage_bps + cfg.spread_bps / 2.0) / 10_000.0)


def _exit_price(price: float, cfg: BacktestConfig) -> float:
    return price * (1.0 - (cfg.slippage_bps + cfg.spread_bps / 2.0) / 10_000.0)


def _make_summary(data: pd.DataFrame, trades: list[Trade], cfg: BacktestConfig, final_equity: float) -> BacktestSummary:
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl < 0]
    gross_profit = sum(t.pnl for t in wins)
    gross_loss = sum(t.pnl for t in losses)
    returns = data["Equity"].pct_change().replace([np.inf, -np.inf], np.nan).dropna()
    annual = _ann(cfg.interval)
    vol = float(returns.std(ddof=0) * math.sqrt(annual) * 100.0) if not returns.empty else 0.0
    sharpe = float((returns.mean() / returns.std(ddof=0)) * math.sqrt(annual)) if len(returns) > 1 and returns.std(ddof=0) else 0.0
    downside = returns[returns < 0]
    sortino = float((returns.mean() / downside.std(ddof=0)) * math.sqrt(annual)) if len(downside) > 1 and downside.std(ddof=0) else 0.0
    days = max((pd.to_datetime(data.index[-1]) - pd.to_datetime(data.index[0])).days, 1)
    years = days / 365.25
    cagr = ((final_equity / cfg.initial_cash) ** (1.0 / years) - 1.0) * 100.0 if final_equity > 0 else -100.0
    mdd = max_drawdown_pct(data["Equity"])
    profit_factor = math.inf if gross_loss == 0 and gross_profit > 0 else (gross_profit / abs(gross_loss) if gross_loss else 0.0)
    max_wins = max_losses = cur_wins = cur_losses = 0
    for t in trades:
        if t.pnl > 0:
            cur_wins, cur_losses = cur_wins + 1, 0
        elif t.pnl < 0:
            cur_losses, cur_wins = cur_losses + 1, 0
        else:
            cur_wins = cur_losses = 0
        max_wins, max_losses = max(max_wins, cur_wins), max(max_losses, cur_losses)
    return BacktestSummary(
        ticker=cfg.ticker,
        start=_fmt_time(data.index[0]),
        end=_fmt_time(data.index[-1]),
        initial_cash=round(cfg.initial_cash, 2),
        final_equity=round(final_equity, 2),
        total_return_pct=round(((final_equity / cfg.initial_cash) - 1.0) * 100.0, 4),
        buy_hold_return_pct=round(((float(data.iloc[-1]["Close"]) / float(data.iloc[0]["Close"])) - 1.0) * 100.0, 4),
        num_trades=len(trades),
        win_rate_pct=round((len(wins) / len(trades) * 100.0) if trades else 0.0, 4),
        gross_profit=round(gross_profit, 4),
        gross_loss=round(gross_loss, 4),
        profit_factor=round(profit_factor, 4) if math.isfinite(profit_factor) else math.inf,
        max_drawdown_pct=round(mdd, 4),
        average_trade_return_pct=round(float(np.mean([t.return_pct for t in trades])) if trades else 0.0, 4),
        cagr_pct=round(cagr, 4),
        volatility_ann_pct=round(vol, 4),
        sharpe=round(sharpe, 4),
        sortino=round(sortino, 4),
        calmar=round((cagr / abs(mdd)) if mdd < 0 else 0.0, 4),
        exposure_pct=round(float(data["InPosition"].mean() * 100.0), 4),
        average_bars_held=round(float(np.mean([t.bars_held for t in trades])) if trades else 0.0, 4),
        max_consecutive_wins=max_wins,
        max_consecutive_losses=max_losses,
        total_fees=round(sum(t.total_fees for t in trades), 4),
    )


def run_backtest_from_prices(df: pd.DataFrame, cfg: BacktestConfig) -> tuple[pd.DataFrame, list[Trade], BacktestSummary]:
    if not 0 < cfg.position_size_pct <= 1:
        raise ValueError("position_size_pct must be in (0, 1]")
    if cfg.execution_mode not in {"signal_close", "next_open", "next_close"}:
        raise ValueError("execution_mode must be signal_close, next_open, or next_close")
    if cfg.same_bar_policy not in {"stop_first", "take_profit_first", "close"}:
        raise ValueError("same_bar_policy must be stop_first, take_profit_first, or close")
    data = enrich_with_signals(df, cfg)
    if len(data) < cfg.rsi_period + 2:
        raise ValueError("Not enough rows for RSI/backtest")

    cash = float(cfg.initial_cash)
    shares = entry = entry_cost = entry_fee = 0.0
    entry_time = None
    entry_i = -1
    entry_low = math.inf
    entry_high = 0.0
    pending = False
    trades: list[Trade] = []
    equity: list[float] = []
    in_pos: list[bool] = []

    def enter(price: float, ts, i: int):
        nonlocal cash, shares, entry, entry_cost, entry_fee, entry_time, entry_i, entry_low, entry_high
        fill = _entry_price(price, cfg)
        deploy = cash * cfg.position_size_pct
        notional = max((deploy - cfg.commission_per_trade) / (1.0 + cfg.commission_pct), 0.0)
        fee = _fee(notional, cfg)
        if fill <= 0 or notional <= 0 or notional + fee > cash + 1e-9:
            return
        shares = notional / fill
        cash -= notional + fee
        entry, entry_cost, entry_fee = fill, notional + fee, fee
        entry_time, entry_i = ts, i
        entry_low = entry_high = fill

    def exit_position(price: float, reason: str, ts, i: int):
        nonlocal cash, shares, entry, entry_cost, entry_fee, entry_time, entry_i, entry_low, entry_high
        fill = _exit_price(price, cfg)
        gross = shares * fill
        fee = _fee(gross, cfg)
        net = gross - fee
        pnl = net - entry_cost
        cash += net
        trades.append(Trade(
            ticker=cfg.ticker,
            entry_time=_fmt_time(entry_time),
            exit_time=_fmt_time(ts),
            entry_price=round(entry, 6),
            exit_price=round(fill, 6),
            shares=round(shares, 8),
            pnl=round(pnl, 6),
            return_pct=round(((net / entry_cost) - 1.0) * 100.0 if entry_cost else 0.0, 6),
            exit_reason=reason,
            bars_held=i - entry_i,
            entry_fee=round(entry_fee, 6),
            exit_fee=round(fee, 6),
            total_fees=round(entry_fee + fee, 6),
            mae_pct=round(((entry_low / entry) - 1.0) * 100.0, 6),
            mfe_pct=round(((entry_high / entry) - 1.0) * 100.0, 6),
        ))
        shares = entry = entry_cost = entry_fee = 0.0
        entry_time, entry_i, entry_low, entry_high = None, -1, math.inf, 0.0

    for i, (ts, row) in enumerate(data.iterrows()):
        open_, close, high, low = float(row["Open"]), float(row["Close"]), float(row["High"]), float(row["Low"])
        exited_this_bar = False
        if shares == 0 and pending and cfg.execution_mode == "next_open":
            enter(open_, ts, i)
            pending = False
        if shares > 0:
            entry_low, entry_high = min(entry_low, low), max(entry_high, high)
            target, stop = entry * (1.0 + cfg.take_profit_pct), entry * (1.0 - cfg.stop_loss_pct)
            hit_target, hit_stop = high >= target, low <= stop
            if hit_target and hit_stop:
                if cfg.same_bar_policy == "stop_first":
                    exit_position(stop, "stop_loss_same_bar", ts, i)
                elif cfg.same_bar_policy == "take_profit_first":
                    exit_position(target, "take_profit_same_bar", ts, i)
                else:
                    exit_position(close, "same_bar_close", ts, i)
                exited_this_bar = True
            elif hit_stop:
                exit_position(stop, "stop_loss", ts, i)
                exited_this_bar = True
            elif hit_target:
                exit_position(target, "take_profit", ts, i)
                exited_this_bar = True
        if shares == 0 and pending and cfg.execution_mode == "next_close" and not exited_this_bar:
            enter(close, ts, i)
            pending = False
        if shares == 0 and bool(row["BuySignal"]) and not exited_this_bar:
            if cfg.execution_mode == "signal_close":
                enter(close, ts, i)
            else:
                pending = True
        equity.append(cash + shares * close)
        in_pos.append(shares > 0)

    if shares > 0:
        exit_position(float(data.iloc[-1]["Close"]), "final_close", data.index[-1], len(data) - 1)
        equity[-1] = cash
        in_pos[-1] = False
    data["Equity"] = equity
    data["InPosition"] = in_pos
    return data, trades, _make_summary(data, trades, cfg, cash)


def trades_to_frame(trades: Iterable[Trade]) -> pd.DataFrame:
    return pd.DataFrame([t.to_dict() for t in trades])
