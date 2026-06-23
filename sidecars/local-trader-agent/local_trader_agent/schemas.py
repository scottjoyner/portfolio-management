from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Literal, Optional


SameBarPolicy = Literal["stop_first", "take_profit_first", "close"]


@dataclass(frozen=True)
class BacktestConfig:
    ticker: str = "GOOGL"
    start: str = "2020-01-01"
    end: Optional[str] = None
    interval: str = "1d"
    rsi_period: int = 14
    buy_rsi_cross: float = 30.0
    take_profit_pct: float = 0.02
    stop_loss_pct: float = 0.01
    initial_cash: float = 10_000.0
    position_size_pct: float = 1.0
    same_bar_policy: SameBarPolicy = "stop_first"
    output_html: str = "reports/backtest_report.html"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Trade:
    ticker: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    shares: float
    pnl: float
    return_pct: float
    exit_reason: str
    bars_held: int

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class BacktestSummary:
    ticker: str
    start: str
    end: str
    initial_cash: float
    final_equity: float
    total_return_pct: float
    buy_hold_return_pct: float
    num_trades: int
    win_rate_pct: float
    gross_profit: float
    gross_loss: float
    profit_factor: float
    max_drawdown_pct: float
    average_trade_return_pct: float

    def to_dict(self) -> dict:
        return asdict(self)
