import json
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

import coinbase.src.pair_discovery as pd
from coinbase.src.pair_discovery import (
    get_all_coinbase_pairs,
    top_coinbase_pairs,
    _filter_by_volume,
    _quote_to_usd_rate,
    _STABLE_QUOTES,
)


@contextmanager
def _noop_slot():
    yield


def _fake_response(payload):
    r = MagicMock()
    r.data = json.dumps(payload).encode()
    return r


@pytest.fixture(autouse=True)
def _clear_caches(monkeypatch):
    monkeypatch.setattr(pd, "_QUOTE_USD_CACHE", {})
    monkeypatch.setattr(pd, "_PAIRS_CACHE", {})


@pytest.fixture
def fake_http():
    http = MagicMock()
    return http
    monkeypatch.setattr(pd, "_http", fake_http)
    assert _quote_to_usd_rate("USDC") == 1.0
    assert _quote_to_usd_rate("USD") == 1.0


def test_quote_to_usd_crypto_via_api(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    fake_http.request.return_value = _fake_response({"price": "42000.0"})
    rate = _quote_to_usd_rate("BTC")
    assert rate == pytest.approx(42000.0)
    fake_http.request.assert_called_once()


def test_quote_to_usd_crypto_api_error(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    fake_http.request.side_effect = Exception("boom")
    assert _quote_to_usd_rate("BTC") is None


def test_quote_to_usd_unknown(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    assert _quote_to_usd_rate("XYZ") is None


def test_get_all_coinbase_pairs(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    monkeypatch.setattr(pd, "api_slot", _noop_slot)
    monkeypatch.setattr(pd, "_PAIRS_CACHE", {})
    products = [
        {"id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD", "status": "online"},
        {"id": "ETH-USDC", "base_currency": "ETH", "quote_currency": "USDC", "status": "online"},
        {"id": "DOGE-USD", "base_currency": "DOGE", "quote_currency": "USD", "status": "offline"},
        {"id": "SHIB-BTC", "base_currency": "SHIB", "quote_currency": "BTC", "status": "online"},
    ]
    fake_http.request.return_value = _fake_response(products)
    res = get_all_coinbase_pairs(min_volume_usd=0, quote_currencies=("USD", "USDC", "BTC"))
    ids = {p["id"] for p in res}
    assert ids == {"BTC-USD", "ETH-USDC", "SHIB-BTC"}


def test_get_all_coinbase_pairs_fetch_error(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    monkeypatch.setattr(pd, "api_slot", _noop_slot)
    monkeypatch.setattr(pd, "_PAIRS_CACHE", {})
    fake_http.request.side_effect = Exception("network down")
    assert get_all_coinbase_pairs() == []


def test_get_all_coinbase_pairs_cache(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    monkeypatch.setattr(pd, "api_slot", _noop_slot)
    monkeypatch.setattr(pd, "_PAIRS_CACHE", {})
    products = [{"id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD", "status": "online"}]
    fake_http.request.return_value = _fake_response(products)
    first = get_all_coinbase_pairs(min_volume_usd=0)
    # second call should hit cache, request not called again
    second = get_all_coinbase_pairs(min_volume_usd=0)
    assert first == second
    assert fake_http.request.call_count == 1


def test_filter_by_volume(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    monkeypatch.setattr(pd, "api_slot", _noop_slot)
    prods = [
        {"id": "BTC-USD", "base": "BTC", "quote": "USD"},
        {"id": "ETH-USD", "base": "ETH", "quote": "USD"},
    ]

    def _side_effect(method, url, timeout):
        if "BTC-USD" in url:
            return _fake_response({"price": "100", "volume": "1000"})
        return _fake_response({"price": "50", "volume": "5"})

    fake_http.request.side_effect = _side_effect
    out = _filter_by_volume(prods, min_volume=500.0)
    out_ids = {p["id"] for p in out}
    assert out_ids == {"BTC-USD"}


def test_filter_by_volume_skip_on_error(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    monkeypatch.setattr(pd, "api_slot", _noop_slot)
    prods = [{"id": "BTC-USD", "base": "BTC", "quote": "USD"}]
    fake_http.request.side_effect = Exception("x")
    assert _filter_by_volume(prods, min_volume=0.0) == []


def test_top_coinbase_pairs(fake_http, monkeypatch):
    monkeypatch.setattr(pd, "_http", fake_http)
    monkeypatch.setattr(pd, "api_slot", _noop_slot)
    monkeypatch.setattr(pd, "_PAIRS_CACHE", {})
    products = [
        {"id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD", "status": "online"},
        {"id": "ETH-USD", "base_currency": "ETH", "quote_currency": "USD", "status": "online"},
    ]
    fake_http.request.return_value = _fake_response(products)
    top = top_coinbase_pairs(n=10, min_volume_usd=0)
    assert ("BTC-USD", "BTC") in top
    assert ("ETH-USD", "ETH") in top
