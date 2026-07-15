"""Coverage tests for coinbase/src/pair_discovery.py"""
from __future__ import annotations

from unittest.mock import Mock

import pytest

from coinbase.src import pair_discovery as pd_mod


@pytest.fixture(autouse=True)
def reset_cache():
    pd_mod._PAIRS_CACHE.clear()
    pd_mod._QUOTE_USD_CACHE.clear()
    yield
    pd_mod._PAIRS_CACHE.clear()
    pd_mod._QUOTE_USD_CACHE.clear()


def _http(handler):
    return lambda method, url, timeout=10: Mock(data=handler(url, timeout))


def test_quote_to_usd_rate():
    # stable cached
    assert pd_mod._quote_to_usd_rate("USDC") == 1.0
    assert pd_mod._quote_to_usd_rate("USD") == 1.0
    # unknown
    assert pd_mod._quote_to_usd_rate("XYZ") is None
    # BTC fetch
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 50000}'
        return b"{}"
    pd_mod._http.request = _http(h)
    assert pd_mod._quote_to_usd_rate("BTC") == 50000.0
    # BTC fetch fails
    pd_mod._http.request = lambda method, url, timeout=10: (_ for _ in ()).throw(RuntimeError("x"))
    assert pd_mod._quote_to_usd_rate("ETH") is None


def test_get_all_coinbase_pairs_basic():
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 10, "volume": 100}'
        return b'[{"id":"BTC-USD","base_currency":"BTC","quote_currency":"USD","status":"online","trading_disabled":false},{"id":"X-OFF","base_currency":"X","quote_currency":"USD","status":"offline"}]'
    pd_mod._http.request = _http(h)
    pairs = pd_mod.get_all_coinbase_pairs(min_volume_usd=0)
    assert len(pairs) == 1
    assert pairs[0]["id"] == "BTC-USD"
    # cache hit
    pairs2 = pd_mod.get_all_coinbase_pairs(min_volume_usd=0)
    assert pairs2 == pairs


def test_get_all_coinbase_pairs_volume_filter():
    def h(url, timeout=10):
        if "ticker" in url:
            if "BIG" in url:
                return b'{"price": 100, "volume": 100000}'
            return b'{"price": 1, "volume": 1}'
        return b'[{"id":"BIG-USD","base_currency":"BIG","quote_currency":"USD","status":"online"},{"id":"SMALL-USD","base_currency":"SMALL","quote_currency":"USD","status":"online"}]'
    pd_mod._http.request = _http(h)
    pairs = pd_mod.get_all_coinbase_pairs(min_volume_usd=1000)
    assert [p["id"] for p in pairs] == ["BIG-USD"]


def test_get_all_coinbase_pairs_fetch_error():
    pd_mod._http.request = lambda method, url, timeout=10: (_ for _ in ()).throw(RuntimeError("x"))
    assert pd_mod.get_all_coinbase_pairs() == []


def test_filter_by_volume_error_branch():
    def h(url, timeout=10):
        if "MISSING" in url:
            return b'{"price": 1, "volume": 1}'
        return b'not json'
    pd_mod._http.request = _http(h)
    products = [
        {"id": "OK-USD", "base": "OK", "quote": "USD"},
        {"id": "MISSING-USD", "base": "MISSING", "quote": "USD"},
    ]
    out = pd_mod._filter_by_volume(products, 0)
    ids = [p["id"] for p in out]
    assert "OK-USD" not in ids  # json parse error -> skipped


def test_top_coinbase_pairs():
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 10, "volume": 50}'
        return b'[{"id":"AAA-USD","base_currency":"AAA","quote_currency":"USD","status":"online"},{"id":"BBB-USD","base_currency":"BBB","quote_currency":"USD","status":"online"}]'
    pd_mod._http.request = _http(h)
    top = pd_mod.top_coinbase_pairs(n=1, min_volume_usd=0)
    assert top[0][0] == "AAA-USD"
