import sys
from unittest.mock import MagicMock

import neo4j

import app.screen.screener_eps_growth as m
from app.screen.screener_eps_growth import query_top


def _patch_driver(monkeypatch, rows):
    sess = MagicMock()
    sess.run.return_value = rows
    driver = MagicMock()
    driver.session.return_value.__enter__.return_value = sess
    driver.session.return_value.__exit__.return_value = False
    monkeypatch.setattr(neo4j.GraphDatabase, "driver", lambda *a, **k: driver)


def test_query_top(monkeypatch):
    rows = [
        {"symbol": "AAPL", "name": "Apple", "sector": "Tech", "eps_yoy": 0.5,
         "eps_diluted_fy": 1.0, "eps_fy": 1.0, "eps_diluted_fy_prev": 0.5, "eps_fy_prev": 0.5},
        {"symbol": "MSFT", "name": "Microsoft", "sector": "Tech", "eps_yoy": 0.3,
         "eps_diluted_fy": 2.0, "eps_fy": 2.0, "eps_diluted_fy_prev": 1.5, "eps_fy_prev": 1.5},
    ]
    _patch_driver(monkeypatch, rows)
    df = query_top(5)
    assert len(df) == 2
    assert df.iloc[0]["symbol"] == "AAPL"


def test_main_empty(monkeypatch):
    _patch_driver(monkeypatch, [])
    monkeypatch.setattr(sys, "argv", ["prog", "--top", "1"])
    m.main()


def test_main_csv(tmp_path, monkeypatch):
    rows = [
        {"symbol": "AAPL", "name": "Apple", "sector": "Tech", "eps_yoy": 0.5,
         "eps_diluted_fy": 1.0, "eps_fy": 1.0, "eps_diluted_fy_prev": 0.5, "eps_fy_prev": 0.5},
    ]
    _patch_driver(monkeypatch, rows)
    csvp = tmp_path / "out.csv"
    monkeypatch.setattr(sys, "argv", ["prog", "--top", "1", "--csv", str(csvp)])
    m.main()
    assert csvp.exists()
    assert "AAPL" in csvp.read_text()


def test_main_pretty(monkeypatch):
    rows = [
        {"symbol": "AAPL", "name": "Apple", "sector": "Tech", "eps_yoy": 0.5,
         "eps_diluted_fy": 1.0, "eps_fy": 1.0, "eps_diluted_fy_prev": 0.5, "eps_fy_prev": 0.5},
    ]
    _patch_driver(monkeypatch, rows)
    monkeypatch.setattr(sys, "argv", ["prog", "--top", "1"])
    m.main()
