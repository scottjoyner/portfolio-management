from __future__ import annotations

from pathlib import Path

from local_trader_agent.backtest import run_backtest_from_prices
from local_trader_agent.report import write_html_report
from local_trader_agent.schemas import BacktestConfig
from tests.test_backtest import sample_prices


def test_report_is_offline_when_embed_enabled(tmp_path: Path):
    cfg = BacktestConfig(
        ticker="TEST",
        start="2024-01-01",
        rsi_period=3,
        buy_rsi_cross=30,
        execution_mode="signal_close",
        report_embed_plotly_js=True,
    )
    data, trades, summary = run_backtest_from_prices(sample_prices(), cfg)
    path = write_html_report(data, trades, summary, cfg, tmp_path / "report.html")
    html = path.read_text(encoding="utf-8")
    assert '<script src="https://cdn.plot.ly' not in html
    assert "Plotly.newPlot" in html
