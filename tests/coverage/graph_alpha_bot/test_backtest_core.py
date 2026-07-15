from app.backtest.backtest_core import run_backtest


def test_run_backtest(capsys):
    run_backtest()
    out = capsys.readouterr().out
    assert "Backtest scaffold" in out
