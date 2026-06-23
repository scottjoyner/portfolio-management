from __future__ import annotations

import pandas as pd

from local_trader_agent.backtest import run_backtest_from_prices
from local_trader_agent.schemas import BacktestConfig


def sample_prices() -> pd.DataFrame:
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
        execution_mode="signal_close",
    )
    data, trades, summary = run_backtest_from_prices(sample_prices(), cfg)
    assert "RSI" in data.columns
    assert summary.num_trades >= 1
    assert all(t.bars_held >= 1 for t in trades)
    assert summary.final_equity > 0
    assert hasattr(summary, "sharpe")
    assert hasattr(summary, "exposure_pct")


def test_next_open_entry_occurs_after_signal_bar():
    cfg = BacktestConfig(
        ticker="TEST",
        start="2024-01-01",
        rsi_period=3,
        buy_rsi_cross=30,
        take_profit_pct=0.02,
        stop_loss_pct=0.01,
        execution_mode="next_open",
    )
    data, trades, _ = run_backtest_from_prices(sample_prices(), cfg)
    signal_times = list(data.index[data["BuySignal"]])
    assert trades
    first_entry = pd.Timestamp(trades[0].entry_time)
    assert first_entry > signal_times[0]


def test_fees_reduce_final_equity():
    base_cfg = BacktestConfig(
        ticker="TEST",
        start="2024-01-01",
        rsi_period=3,
        buy_rsi_cross=30,
        take_profit_pct=0.02,
        stop_loss_pct=0.01,
        execution_mode="signal_close",
    )
    fee_cfg = BacktestConfig(**{**base_cfg.to_dict(), "commission_per_trade": 1.0, "slippage_bps": 10, "spread_bps": 4})
    _, _, base_summary = run_backtest_from_prices(sample_prices(), base_cfg)
    _, _, fee_summary = run_backtest_from_prices(sample_prices(), fee_cfg)
    assert fee_summary.total_fees > 0
    assert fee_summary.final_equity < base_summary.final_equity
