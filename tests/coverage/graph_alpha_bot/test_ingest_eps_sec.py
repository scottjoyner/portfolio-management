import pandas as pd
from unittest.mock import MagicMock, patch

import app.data.ingest_eps_sec as m
from app.data.ingest_eps_sec import (
    fetch_company_concept,
    extract_annual_eps,
    compute_yoy,
    upsert_eps,
    main,
)


def test_fetch_company_concept_404():
    resp = MagicMock(); resp.status_code = 404
    with patch.object(m.requests, "get", return_value=resp):
        assert fetch_company_concept(123, "us-gaap", "EarningsPerShareDiluted") is None


def test_fetch_company_concept_200():
    resp = MagicMock(); resp.status_code = 200; resp.json.return_value = {"units": {}}
    with patch.object(m.requests, "get", return_value=resp):
        assert fetch_company_concept(123, "us-gaap", "EarningsPerShareDiluted") == {"units": {}}


def test_fetch_company_concept_raise():
    resp = MagicMock(); resp.status_code = 500; resp.raise_for_status.side_effect = RuntimeError("boom")
    with patch.object(m.requests, "get", return_value=resp):
        assert fetch_company_concept(123, "us-gaap", "X") is None


def test_extract_annual_eps_empty():
    assert extract_annual_eps(None).empty
    assert extract_annual_eps({"units": {}}).empty


def test_extract_annual_eps_valid():
    js = {"units": {"USD": [
        {"fy": 2022, "form": "10-K", "val": 1.0},
        {"fy": 2023, "form": "10-K", "val": 2.0},
        {"fy": 2023, "form": "10-K", "val": 2.5},  # duplicate fy -> last kept
    ]}}
    df = extract_annual_eps(js)
    assert len(df) == 2
    assert df.iloc[-1]["val"] == 2.5


def test_compute_yoy_too_few():
    assert compute_yoy(pd.DataFrame({"fy": ["2023"], "form": ["10-K"], "val": [1.0]})) is None


def test_compute_yoy_prev_zero():
    df = pd.DataFrame({"fy": ["2022", "2023"], "form": ["10-K", "10-K"], "val": [0.0, 1.0]})
    assert compute_yoy(df) is None


def test_compute_yoy_prev_negative():
    df = pd.DataFrame({"fy": ["2022", "2023"], "form": ["10-K", "10-K"], "val": [-1.0, 2.0]})
    assert compute_yoy(df) is None


def test_compute_yoy_valid():
    df = pd.DataFrame({"fy": ["2022", "2023"], "form": ["10-K", "10-K"], "val": [2.0, 3.0]})
    comp = compute_yoy(df)
    assert comp[0] == 0.5


def test_upsert_eps():
    drv = MagicMock(); sess = MagicMock()
    sess.__enter__.return_value = sess; sess.__exit__.return_value = False
    drv.session.return_value = sess
    with patch("app.data.ingest_eps_sec.GraphDatabase.driver", return_value=drv):
        upsert_eps("AAPL", 3.0, 2.0, "2023", "2022", 0.5)


def test_main_full(monkeypatch, tmp_path):
    drv = MagicMock(); sess = MagicMock()
    sess.__enter__.return_value = sess; sess.__exit__.return_value = False
    drv.session.return_value = sess
    mapping = tmp_path / "map.csv"
    mapping.write_text("symbol,cik\nAAPL,320193\nBAD,0\n")
    monkeypatch.setattr(m.pd, "read_csv", lambda p: pd.read_csv(mapping))

    concept = {"units": {"USD": [
        {"fy": 2022, "form": "10-K", "val": 2.0},
        {"fy": 2023, "form": "10-K", "val": 3.0},
    ]}}
    monkeypatch.setattr(m, "fetch_company_concept", lambda cik, tx, tag: concept)
    monkeypatch.setattr(m, "upsert_eps", lambda *a, **k: None)
    monkeypatch.setattr(m, "GraphDatabase", MagicMock())
    main()


def test_main_no_annual_facts(monkeypatch, tmp_path):
    drv = MagicMock()
    mapping = tmp_path / "map.csv"
    mapping.write_text("symbol,cik\nAAPL,320193\n")
    monkeypatch.setattr(m.pd, "read_csv", lambda p: pd.read_csv(mapping))
    monkeypatch.setattr(m, "fetch_company_concept", lambda cik, tx, tag: {"units": {}})
    monkeypatch.setattr(m, "GraphDatabase", MagicMock())
    main()


def test_main_insufficient_yoy(monkeypatch, tmp_path):
    drv = MagicMock()
    mapping = tmp_path / "map.csv"
    mapping.write_text("symbol,cik\nAAPL,320193\n")
    monkeypatch.setattr(m.pd, "read_csv", lambda p: pd.read_csv(mapping))
    concept = {"units": {"USD": [{"fy": 2023, "form": "10-K", "val": 1.0}]}}
    monkeypatch.setattr(m, "fetch_company_concept", lambda cik, tx, tag: concept)
    monkeypatch.setattr(m, "GraphDatabase", MagicMock())
    main()
