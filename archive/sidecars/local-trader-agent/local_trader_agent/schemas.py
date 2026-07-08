from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal, Optional


SameBarPolicy = Literal["stop_first", "take_profit_first", "close"]
ExecutionMode = Literal["signal_close", "next_open", "next_close"]
PriceAdjustment = Literal["raw", "auto_adjusted"]


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
    execution_mode: ExecutionMode = "next_open"
    commission_per_trade: float = 0.0
    commission_pct: float = 0.0
    slippage_bps: float = 0.0
    spread_bps: float = 0.0
    price_adjustment: PriceAdjustment = "raw"
    data_cache: bool = True
    refresh_cache: bool = False
    data_cache_dir: str = "workspace/cache/yfinance"
    report_embed_plotly_js: bool = True
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
    entry_fee: float = 0.0
    exit_fee: float = 0.0
    total_fees: float = 0.0
    mae_pct: float = 0.0
    mfe_pct: float = 0.0

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
    cagr_pct: float
    volatility_ann_pct: float
    sharpe: float
    sortino: float
    calmar: float
    exposure_pct: float
    average_bars_held: float
    max_consecutive_wins: int
    max_consecutive_losses: int
    total_fees: float

    def to_dict(self) -> dict:
        return asdict(self)
