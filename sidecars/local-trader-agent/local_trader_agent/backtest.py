from __future__ import annotations

import math
from dataclasses import replace
from typing import Iterable

import numpy as np
import pandas as pd

from .indicators import crossed_above, rsi_wilder
from .schemas import BacktestConfig, BacktestSummary, Trade


def _fmt_time(value) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def max_drawdown_pct(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    running_max = equity.cummax()
    dd = (equity / running_max) - 1.0
    return float(dd.min() * 100.0)


def enrich_with_signals(df: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    out = df.copy()
    out["RSI"] = rsi_wilder(out["Close"], cfg.rsi_period)
    out["BuySignal"] = crossed_above(out["RSI"], cfg.buy_rsi_cross).fillna(False)
    return out


def run_backtest_from_prices(df: pd.DataFrame, cfg: BacktestConfig) -> tuple[pd.DataFrame, list[Trade], BacktestSummary]:
    """Run a strict no-overlap long-only RSI strategy.

    Entry: close of the signal bar.
    Exit: take profit or stop loss using each later bar's high/low.
    Same-bar ambiguity: controlled by cfg.same_bar_policy.
    Position sizing: cfg.position_size_pct of current cash/equity at entry.
    """
    if not 0 < cfg.position_size_pct <= 1:
        raise ValueError("position_size_pct must be in (0, 1]")
    if cfg.take_profit_pct <= 0 or cfg.stop_loss_pct <= 0:
        raise ValueError("take_profit_pct and stop_loss_pct must be positive")
    if cfg.same_bar_policy not in {"stop_first", "take_profit_first", "close"}:
        raise ValueError("same_bar_policy must be stop_first, take_profit_first, or close")

    data = enrich_with_signals(df, cfg)
    if len(data) < cfg.rsi_period + 2:
        raise ValueError("Not enough rows for RSI/backtest")

    cash = float(cfg.initial_cash)
    shares = 0.0
    entry_price = 0.0
    entry_time = None
    entry_i = -1
    trades: list[Trade] = []
    equity_values: list[float] = []
    position_flags: list[bool] = []

    for i, (ts, row) in enumerate(data.iterrows()):
        close = float(row["Close"])
        high = float(row["High"])
        low = float(row["Low"])

        if shares > 0:
            target = entry_price * (1.0 + cfg.take_profit_pct)
            stop = entry_price * (1.0 - cfg.stop_loss_pct)
            hit_target = high >= target
            hit_stop = low <= stop
            exit_price = None
            exit_reason = None

            if hit_target and hit_stop:
                if cfg.same_bar_policy == "stop_first":
                    exit_price, exit_reason = stop, "stop_loss_same_bar"
                elif cfg.same_bar_policy == "take_profit_first":
                    exit_price, exit_reason = target, "take_profit_same_bar"
                else:
                    exit_price, exit_reason = close, "same_bar_close"
            elif hit_stop:
                exit_price, exit_reason = stop, "stop_loss"
            elif hit_target:
                exit_price, exit_reason = target, "take_profit"

            if exit_price is not None:
                proceeds = shares * exit_price
                pnl = (exit_price - entry_price) * shares
                cash += proceeds
                trade_return = (exit_price / entry_price) - 1.0
                trades.append(
                    Trade(
                        ticker=cfg.ticker,
                        entry_time=_fmt_time(entry_time),
                        exit_time=_fmt_time(ts),
                        entry_price=round(entry_price, 6),
                        exit_price=round(float(exit_price), 6),
                        shares=round(float(shares), 8),
                        pnl=round(float(pnl), 6),
                        return_pct=round(float(trade_return * 100.0), 6),
                        exit_reason=exit_reason,
                        bars_held=i - entry_i,
                    )
                )
                shares = 0.0
                entry_price = 0.0
                entry_time = None
                entry_i = -1

        # Strict no-overlap: only enter after exit processing. If we exited this bar,
        # we do not re-enter on the same bar.
        if shares == 0 and bool(row["BuySignal"]):
            deploy_cash = cash * cfg.position_size_pct
            if deploy_cash > 0:
                entry_price = close
                shares = deploy_cash / entry_price
                cash -= deploy_cash
                entry_time = ts
                entry_i = i

        mark_to_market = cash + shares * close
        equity_values.append(mark_to_market)
        position_flags.append(shares > 0)

    # Close open position at final close for accounting/reporting.
    if shares > 0:
        ts = data.index[-1]
        close = float(data.iloc[-1]["Close"])
        pnl = (close - entry_price) * shares
        cash += shares * close
        trade_return = (close / entry_price) - 1.0
        trades.append(
            Trade(
                ticker=cfg.ticker,
                entry_time=_fmt_time(entry_time),
                exit_time=_fmt_time(ts),
                entry_price=round(entry_price, 6),
                exit_price=round(close, 6),
                shares=round(float(shares), 8),
                pnl=round(float(pnl), 6),
                return_pct=round(float(trade_return * 100.0), 6),
                exit_reason="final_close",
                bars_held=len(data) - 1 - entry_i,
            )
        )
        shares = 0.0
        equity_values[-1] = cash
        position_flags[-1] = False

    data["Equity"] = equity_values
    data["InPosition"] = position_flags

    final_equity = cash
    total_return_pct = ((final_equity / cfg.initial_cash) - 1.0) * 100.0
    buy_hold_return_pct = ((float(data.iloc[-1]["Close"]) / float(data.iloc[0]["Close"])) - 1.0) * 100.0
    wins = [t for t in trades if t.pnl > 0]
    losses = [t for t in trades if t.pnl < 0]
    gross_profit = sum(t.pnl for t in wins)
    gross_loss = sum(t.pnl for t in losses)
    profit_factor = math.inf if gross_loss == 0 and gross_profit > 0 else (gross_profit / abs(gross_loss) if gross_loss else 0.0)
    avg_trade_return = float(np.mean([t.return_pct for t in trades])) if trades else 0.0

    summary = BacktestSummary(
        ticker=cfg.ticker,
        start=_fmt_time(data.index[0]),
        end=_fmt_time(data.index[-1]),
        initial_cash=round(float(cfg.initial_cash), 2),
        final_equity=round(float(final_equity), 2),
        total_return_pct=round(float(total_return_pct), 4),
        buy_hold_return_pct=round(float(buy_hold_return_pct), 4),
        num_trades=len(trades),
        win_rate_pct=round(float((len(wins) / len(trades) * 100.0) if trades else 0.0), 4),
        gross_profit=round(float(gross_profit), 4),
        gross_loss=round(float(gross_loss), 4),
        profit_factor=round(float(profit_factor), 4) if math.isfinite(profit_factor) else math.inf,
        max_drawdown_pct=round(max_drawdown_pct(data["Equity"]), 4),
        average_trade_return_pct=round(avg_trade_return, 4),
    )
    return data, trades, summary


def trades_to_frame(trades: Iterable[Trade]) -> pd.DataFrame:
    return pd.DataFrame([t.to_dict() for t in trades])
