"""Coverage tests for coinbase/src/yahoo_chart.py"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import coinbase.src.yahoo_chart as yc


def make_session(status_code=200, payload=None):
    session = MagicMock()
    resp = MagicMock()
    resp.status_code = status_code
    if payload is None:
        payload = {
            "chart": {
                "result": [
                    {"indicators": {"quote": [{"close": [1.0, 2.0, None, 3.0]}]}}
                ]
            }
        }
    resp.json.return_value = payload
    session.get.return_value = resp
    return session


@pytest.fixture
def patch_session(monkeypatch):
    session = make_session()
    monkeypatch.setattr(yc, "_session", lambda: session)
    monkeypatch.setattr(yc, "_LAST_FETCH", 0.0)
    return session


def test_fetch_closes_success(patch_session):
    closes = yc.fetch_closes("SPY")
    assert closes == [1.0, 2.0, 3.0]  # None filtered out


def test_fetch_closes_non_200(patch_session):
    patch_session.get.return_value.status_code = 404
    assert yc.fetch_closes("SPY") == []


def test_fetch_closes_no_result(patch_session):
    patch_session.get.return_value.json.return_value = {"chart": {"result": []}}
    assert yc.fetch_closes("SPY") == []


def test_fetch_closes_no_quotes(patch_session):
    patch_session.get.return_value.json.return_value = {
        "chart": {"result": [{"indicators": {"quote": []}}]}}
    assert yc.fetch_closes("SPY") == []


def test_fetch_closes_exception(patch_session):
    # source only catches RequestException/ValueError/KeyError/IndexError
    patch_session.get.side_effect = ValueError("boom")
    assert yc.fetch_closes("SPY") == []


def test_session_lazy_creation(monkeypatch):
    monkeypatch.setattr(yc, "_SESSION", None)
    s = yc._session()
    assert s is not None
    assert yc._session() is s  # cached on second call


def test_fetch_multiple_skips_empty(monkeypatch):
    session = make_session()
    monkeypatch.setattr(yc, "_session", lambda: session)
    monkeypatch.setattr(yc, "_LAST_FETCH", 0.0)
    orig = yc.fetch_closes

    def fake(sym, *a, **k):
        return [] if sym == "EMPTY" else orig(sym, *a, **k)
    monkeypatch.setattr(yc, "fetch_closes", fake)
    out = yc.fetch_multiple(["SPY", "EMPTY"])
    assert "SPY" in out and "EMPTY" not in out


def test_fetch_closes_rate_limit(monkeypatch):
    session = make_session()
    monkeypatch.setattr(yc, "_session", lambda: session)
    monkeypatch.setattr(yc, "_LAST_FETCH", 0.0)
    slept = {"n": 0}

    def fake_sleep(s):
        slept["n"] += 1
    monkeypatch.setattr(yc.time, "sleep", fake_sleep)
    yc.fetch_closes("SPY")
    yc.fetch_closes("SPY")  # second call within interval -> sleeps
    assert slept["n"] >= 1


def test_fetch_multiple(patch_session):
    out = yc.fetch_multiple(["SPY", "QQQ"])
    assert "SPY" in out
    assert out["SPY"] == [1.0, 2.0, 3.0]
