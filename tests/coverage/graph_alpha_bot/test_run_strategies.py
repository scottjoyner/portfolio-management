from unittest.mock import MagicMock, patch


def _driver():
    driver = MagicMock()
    sess = MagicMock()
    sess.__enter__.return_value = sess
    sess.__exit__.return_value = False
    driver.session.return_value = sess
    return driver, sess


def test_main_runs_all_strategies(monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["run_strategies", "--symbols", "BTC,ETH,SOL"])
    driver, sess = _driver()
    with patch("app.strategies.base.GraphDatabase.driver", return_value=driver):
        from app.strategies.run_strategies import main

        main()
    out = capsys.readouterr().out
    assert "Total signals" in out
    # 4 strategy lines printed
    assert out.count("signals") >= 4
