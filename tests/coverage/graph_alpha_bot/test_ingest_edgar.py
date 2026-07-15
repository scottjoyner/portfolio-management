import requests
from unittest.mock import MagicMock, patch

import app.data.ingest_edgar as m
from app.data.ingest_edgar import (
    company_submissions,
    _normalize_recent,
    upsert_filings,
    load_company,
    main,
)


def test_company_submissions():
    resp = MagicMock(); resp.raise_for_status.return_value = None
    resp.json.return_value = {"filings": {}}
    with patch.object(m.requests, "get", return_value=resp):
        assert company_submissions(320193) == {"filings": {}}


def test_normalize_recent_list_of_dicts():
    recent = [{"form": "10-K", "accessionNumber": "a", "filingDate": "2023"}]
    out = _normalize_recent(recent)
    assert out[0]["form"] == "10-K"


def test_normalize_recent_list_of_dicts_alt_keys():
    recent = [{"filingType": "10-K", "accession_number": "a", "filed": "2023"}]
    out = _normalize_recent(recent)
    assert out[0]["form"] == "10-K"


def test_normalize_recent_list_skip_non_dict():
    out = _normalize_recent(["notadict"])
    assert out == []


def test_normalize_recent_dict_of_arrays():
    recent = {"form": ["10-K"], "accessionNumber": ["a"], "filingDate": ["2023"]}
    out = _normalize_recent(recent)
    assert out[0]["form"] == "10-K"


def test_normalize_recent_dict_alt_keys():
    recent = {"filingType": ["10-K"], "accession_number": ["a"], "filed": ["2023"]}
    out = _normalize_recent(recent)
    assert out[0]["form"] == "10-K"


def test_normalize_recent_unknown():
    assert _normalize_recent(42) == []
    assert _normalize_recent({"form": []}) == []


def test_upsert_filings():
    tx = MagicMock()
    upsert_filings(tx, 320193, [{"form": "10-K", "accessionNumber": "a", "filingDate": "2023"}])
    tx.run.assert_called_once()


def test_load_company_no_recent():
    with patch.object(m, "company_submissions", return_value={}):
        with patch("app.data.ingest_edgar.GraphDatabase") as GD:
            load_company(1)
    GD.driver.assert_not_called()


def test_load_company_empty_rows():
    data = {"filings": {"recent": []}}
    with patch.object(m, "company_submissions", return_value=data):
        with patch("app.data.ingest_edgar.GraphDatabase") as GD:
            load_company(1)
    GD.driver.assert_not_called()


def test_load_company_writes():
    data = {"filings": {"recent": [{"form": "10-K", "accessionNumber": "a", "filingDate": "2023"}]}}
    drv = MagicMock(); sess = MagicMock()
    sess.__enter__.return_value = sess; sess.__exit__.return_value = False
    drv.session.return_value = sess
    with patch.object(m, "company_submissions", return_value=data):
        with patch("app.data.ingest_edgar.GraphDatabase.driver", return_value=drv):
            load_company(1)


def test_main_ok(monkeypatch):
    monkeypatch.setattr(m, "load_company", lambda cik: None)
    monkeypatch.setattr("sys.argv", ["ingest_edgar", "--cik", "320193"])
    main()


def test_main_http_error(monkeypatch):
    def boom(cik):
        raise requests.HTTPError("404")
    monkeypatch.setattr(m, "load_company", boom)
    monkeypatch.setattr("sys.argv", ["ingest_edgar", "--cik", "320193"])
    main()


def test_main_generic_error(monkeypatch):
    def boom(cik):
        raise ValueError("x")
    monkeypatch.setattr(m, "load_company", boom)
    monkeypatch.setattr("sys.argv", ["ingest_edgar", "--cik", "320193"])
    main()
