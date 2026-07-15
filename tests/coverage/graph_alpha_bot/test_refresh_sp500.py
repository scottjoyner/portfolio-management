import pandas as pd
from unittest.mock import MagicMock, patch

import app.data.refresh_sp500 as m
from app.data.refresh_sp500 import fetch_sec_ticker_map, upsert_tickers, parse_wiki_table, main


def test_fetch_sec_ticker_map_list():
    j = [{"ticker": "AAPL", "cik": 320193, "title": "Apple Inc."}]
    resp = MagicMock(); resp.json.return_value = j; resp.raise_for_status.return_value = None
    with patch.object(m.requests, "get", return_value=resp):
        mp = fetch_sec_ticker_map()
    assert mp["AAPL"]["cik"] == 320193


def test_fetch_sec_ticker_map_data_fields():
    j = {"fields": ["ticker", "cik", "title"], "data": [["MSFT", 789, "Microsoft"]]}
    resp = MagicMock(); resp.json.return_value = j; resp.raise_for_status.return_value = None
    with patch.object(m.requests, "get", return_value=resp):
        mp = fetch_sec_ticker_map()
    assert mp["MSFT"]["cik"] == 789


def test_fetch_sec_ticker_map_dict():
    j = {"0": {"ticker": "X", "cik": 1, "name": "X Corp"}}
    resp = MagicMock(); resp.json.return_value = j; resp.raise_for_status.return_value = None
    with patch.object(m.requests, "get", return_value=resp):
        mp = fetch_sec_ticker_map()
    assert mp["X"]["cik"] == 1


def test_fetch_sec_ticker_map_empty_raises():
    resp = MagicMock(); resp.json.return_value = {}; resp.raise_for_status.return_value = None
    with patch.object(m.requests, "get", return_value=resp):
        try:
            fetch_sec_ticker_map()
            assert False
        except RuntimeError:
            pass


def test_upsert_tickers():
    drv = MagicMock(); sess = MagicMock()
    sess.__enter__.return_value = sess; sess.__exit__.return_value = False
    drv.session.return_value = sess
    df = pd.DataFrame([{"symbol": "AAPL", "name": "Apple", "sector": "Tech", "cik": 320193}])
    with patch("app.data.refresh_sp500.GraphDatabase.driver", return_value=drv):
        upsert_tickers(df)


def test_parse_wiki_table(monkeypatch):
    df = pd.DataFrame({"Symbol": ["AAPL", "BRK.B"], "Security": ["Apple", "Berkshire"],
                       "GICS Sector": ["Tech", "Financials"]})
    fake_table = MagicMock(); fake_table.__str__.return_value = "<table></table>"
    fake_soup = MagicMock(); fake_soup.select.return_value = [fake_table]
    monkeypatch.setattr(m, "BeautifulSoup", lambda *a, **k: fake_soup)
    monkeypatch.setattr(m.pd, "read_html", lambda *a, **k: [df])
    out = parse_wiki_table("<html></html>")
    assert "symbol" in out.columns
    assert out.iloc[1]["symbol"] == "BRK-B"  # dot replaced with dash


def test_parse_wiki_table_no_wikitable(monkeypatch):
    fake_soup = MagicMock(); fake_soup.select.return_value = []
    monkeypatch.setattr(m, "BeautifulSoup", lambda *a, **k: fake_soup)
    try:
        parse_wiki_table("<html></html>")
        assert False
    except RuntimeError:
        pass


def test_parse_wiki_table_bad_columns(monkeypatch):
    df = pd.DataFrame({"foo": [1]})
    fake_table = MagicMock(); fake_table.__str__.return_value = "<table></table>"
    fake_soup = MagicMock(); fake_soup.select.return_value = [fake_table]
    monkeypatch.setattr(m, "BeautifulSoup", lambda *a, **k: fake_soup)
    monkeypatch.setattr(m.pd, "read_html", lambda *a, **k: [df])
    try:
        parse_wiki_table("<html></html>")
        assert False
    except RuntimeError:
        pass


def test_main_no_write_fallback(monkeypatch, tmp_path):
    # Wikipedia fetch fails -> fallback CSV path
    def boom(*a, **k):
        raise RuntimeError("blocked")
    monkeypatch.setattr(m, "fetch_html", boom)
    fb = pd.DataFrame({"Symbol": ["AAPL"], "Name": ["Apple"], "Sector": ["Tech"]})
    monkeypatch.setattr(m.pd, "read_csv", lambda *a, **k: fb)
    monkeypatch.setattr(m, "fetch_sec_ticker_map", lambda: {"AAPL": {"cik": 320193, "company": "Apple"}})
    monkeypatch.setattr(m, "upsert_tickers", lambda df: None)
    monkeypatch.setattr(m.os, "makedirs", lambda *a, **k: None)
    monkeypatch.setattr(m.pd.DataFrame, "to_csv", lambda self, *a, **k: None)
    main()


def test_main_write(monkeypatch, tmp_path):
    html = "<html></html>"
    monkeypatch.setattr(m, "fetch_html", lambda *a, **k: html)
    df = pd.DataFrame({"Symbol": ["AAPL"], "Security": ["Apple"], "GICS Sector": ["Tech"]})
    fake_table = MagicMock(); fake_table.__str__.return_value = "<table></table>"
    fake_soup = MagicMock(); fake_soup.select.return_value = [fake_table]
    monkeypatch.setattr(m, "BeautifulSoup", lambda *a, **k: fake_soup)
    monkeypatch.setattr(m.pd, "read_html", lambda *a, **k: [df])
    monkeypatch.setattr(m, "fetch_sec_ticker_map", lambda: {"AAPL": {"cik": 320193, "company": "Apple"}})
    monkeypatch.setattr(m, "upsert_tickers", lambda df: None)
    monkeypatch.setattr(m.os, "makedirs", lambda *a, **k: None)
    monkeypatch.setattr(m.pd.DataFrame, "to_csv", lambda self, *a, **k: None)
    main()
