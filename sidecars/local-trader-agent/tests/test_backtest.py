from __future__ import annotations

import pandas as pd

from local_trader_agent.backtest import run_backtest_from_prices
from local_trader_agent.schemas import BacktestConfig


def sample_prices() -> pd.DataFrame:
    # Shape forces an RSI washout then recovery, then a take-profit event.
    close = [100, 98, 96, 94, 92, 90, 89, 88, 90, 92, 94, 96, 98, 100, 102, 104]
    idx = pd.date_range("2024-01-01", periods=len(close), freq="D")
    df = pd.DataFrame(index=idx)
    df["Close"] = close
    df["Open"] = df["Close"].shift(1).fillna(df["Close"])
    df["High"] = df[["Open", "Close"]].max(axis=1) * 1.01
    df["Low"] = df[["Open", "Close"]].min(axis=1) * 0.99
    df["Volume"] = 1000
    return df


def test_backtest_no_overlap_and_summary():
    cfg = BacktestConfig(
        ticker="TEST",
        start="2024-01-01",
        rsi_period=3,
        buy_rsi_cross=30,
        take_profit_pct=0.02,
        stop_loss_pct=0.01,
        initial_cash=10000,
        same_bar_policy="stop_first",
    )
    data, trades, summary = run_backtest_from_prices(sample_prices(), cfg)
    assert "RSI" in data.columns
    assert summary.num_trades >= 1
    assert all(t.bars_held >= 1 for t in trades)
    assert summary.final_equity > 0


def test_stop_first_same_bar_policy():
    idx = pd.date_range("2024-01-01", periods=8, freq="D")
    df = pd.DataFrame(
        {
            "Open": [100, 99, 98, 97, 96, 95, 100, 100],
            "High": [101, 100, 99, 98, 97, 96, 103, 103],
            "Low": [99, 98, 97, 96, 95, 94, 98, 98],
            "Close": [100, 99, 98, 97, 96, 95, 100, 100],
            "Volume": [1000] * 8,
        },
        index=idx,
    )
    cfg = BacktestConfig(
        ticker="TEST",
        start="2024-01-01",
        rsi_period=2,
        buy_rsi_cross=30,
        take_profit_pct=0.02,
        stop_loss_pct=0.01,
        same_bar_policy="stop_first",
    )
    _, trades, _ = run_backtest_from_prices(df, cfg)
    assert trades
