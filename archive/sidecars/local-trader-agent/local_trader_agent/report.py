from __future__ import annotations

import html
import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .schemas import BacktestConfig, BacktestSummary, Trade


def _money(value: float) -> str:
    return f"${value:,.2f}"


def _pct(value: float) -> str:
    return f"{value:,.2f}%"


def _num(value: float) -> str:
    return "∞" if value == float("inf") else f"{value:,.2f}"


def _summary_table(summary: BacktestSummary) -> str:
    rows = [
        ("Ticker", summary.ticker),
        ("Period", f"{summary.start} → {summary.end}"),
        ("Initial Cash", _money(summary.initial_cash)),
        ("Final Equity", _money(summary.final_equity)),
        ("Strategy Return", _pct(summary.total_return_pct)),
        ("Buy & Hold Return", _pct(summary.buy_hold_return_pct)),
        ("CAGR", _pct(summary.cagr_pct)),
        ("Annualized Volatility", _pct(summary.volatility_ann_pct)),
        ("Sharpe", _num(summary.sharpe)),
        ("Sortino", _num(summary.sortino)),
        ("Calmar", _num(summary.calmar)),
        ("Trades", str(summary.num_trades)),
        ("Win Rate", _pct(summary.win_rate_pct)),
        ("Exposure", _pct(summary.exposure_pct)),
        ("Avg Bars Held", _num(summary.average_bars_held)),
        ("Gross Profit", _money(summary.gross_profit)),
        ("Gross Loss", _money(summary.gross_loss)),
        ("Profit Factor", _num(summary.profit_factor)),
        ("Max Drawdown", _pct(summary.max_drawdown_pct)),
        ("Avg Trade Return", _pct(summary.average_trade_return_pct)),
        ("Max Consecutive Wins", str(summary.max_consecutive_wins)),
        ("Max Consecutive Losses", str(summary.max_consecutive_losses)),
        ("Total Fees", _money(summary.total_fees)),
    ]
    return "\n".join(f"<tr><th>{html.escape(k)}</th><td>{html.escape(v)}</td></tr>" for k, v in rows)


def _trades_table(trades: list[Trade]) -> str:
    if not trades:
        return "<p>No trades were triggered by this configuration.</p>"
    return pd.DataFrame([t.to_dict() for t in trades]).to_html(index=False, classes="trades", border=0, escape=True)


def build_figure(data: pd.DataFrame, trades: list[Trade], cfg: BacktestConfig) -> go.Figure:
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.04, row_heights=[0.55, 0.25, 0.20], subplot_titles=(f"{cfg.ticker} Candles + Trades", "RSI", "Equity Curve"))
    fig.add_trace(go.Candlestick(x=data.index, open=data["Open"], high=data["High"], low=data["Low"], close=data["Close"], name="OHLC"), row=1, col=1)
    if trades:
        entries = pd.DataFrame([t.to_dict() for t in trades])
        entries["entry_time"] = pd.to_datetime(entries["entry_time"])
        entries["exit_time"] = pd.to_datetime(entries["exit_time"])
        fig.add_trace(go.Scatter(x=entries["entry_time"], y=entries["entry_price"], mode="markers", marker_symbol="triangle-up", marker_size=12, name="Buy"), row=1, col=1)
        fig.add_trace(go.Scatter(x=entries["exit_time"], y=entries["exit_price"], mode="markers", marker_symbol="triangle-down", marker_size=12, name="Sell"), row=1, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data["RSI"], name="RSI", mode="lines"), row=2, col=1)
    fig.add_hline(y=cfg.buy_rsi_cross, line_dash="dash", annotation_text=f"RSI {cfg.buy_rsi_cross}", row=2, col=1)
    fig.add_trace(go.Scatter(x=data.index, y=data["Equity"], name="Equity", mode="lines"), row=3, col=1)
    fig.update_layout(template="plotly_white", xaxis_rangeslider_visible=False, height=950, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0), margin=dict(l=40, r=30, t=80, b=40))
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="RSI", row=2, col=1, range=[0, 100])
    fig.update_yaxes(title_text="Equity", row=3, col=1)
    return fig


def write_html_report(data: pd.DataFrame, trades: list[Trade], summary: BacktestSummary, cfg: BacktestConfig, output_path: str | Path) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    chart_html = build_figure(data, trades, cfg).to_html(full_html=False, include_plotlyjs=cfg.report_embed_plotly_js)
    config_json = html.escape(json.dumps(cfg.to_dict(), indent=2, default=str))
    doc = f"""<!doctype html>
<html lang=\"en\"><head><meta charset=\"utf-8\" /><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
<title>{html.escape(cfg.ticker)} RSI Backtest Report</title>
<style>body{{font-family:system-ui;margin:0;background:#f7f7f8;color:#111827}}header{{padding:28px 36px;background:#111827;color:white}}main{{padding:24px 36px 48px;max-width:1280px;margin:auto}}.card{{background:white;border-radius:14px;padding:20px;margin:18px 0;box-shadow:0 1px 8px rgba(0,0,0,.08)}}table{{border-collapse:collapse;width:100%}}th,td{{padding:10px 12px;border-bottom:1px solid #e5e7eb;text-align:left}}th{{width:220px;color:#374151}}.trades th{{width:auto}}pre{{background:#f3f4f6;border-radius:8px;padding:14px;overflow-x:auto}}.disclaimer{{color:#6b7280;font-size:.95rem}}</style></head><body>
<header><h1>{html.escape(cfg.ticker)} RSI Strategy Backtest</h1><p>Buy when RSI crosses above {cfg.buy_rsi_cross}; execution={html.escape(cfg.execution_mode)}; sell at +{cfg.take_profit_pct:.2%} take profit or -{cfg.stop_loss_pct:.2%} stop loss; no overlapping positions.</p></header>
<main><section class=\"card\"><h2>Summary</h2><table>{_summary_table(summary)}</table></section><section class=\"card\"><h2>Charts</h2>{chart_html}</section><section class=\"card\"><h2>Trades</h2>{_trades_table(trades)}</section><section class=\"card\"><h2>Configuration</h2><pre>{config_json}</pre></section><section class=\"card disclaimer\"><strong>Disclaimer:</strong> Research/backtesting output only. Historical tests do not guarantee future results.</section></main></body></html>
"""
    output_path.write_text(doc, encoding="utf-8")
    return output_path
