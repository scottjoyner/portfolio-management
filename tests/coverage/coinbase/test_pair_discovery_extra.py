"""Extra branch-coverage tests for coinbase/src/pair_discovery.py (target >=90%)."""
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


def test_quote_to_usd_btc_zero_price():
    # BTC ticker returns price 0 -> falls through to `return None` (line 60).
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 0}'
        return b"{}"
    pd_mod._http.request = _http(h)
    assert pd_mod._quote_to_usd_rate("BTC") is None


def test_get_all_coinbase_pairs_trading_disabled():
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 10, "volume": 100}'
        return b'[{"id":"X-USD","base_currency":"X","quote_currency":"USD","status":"online","trading_disabled":true}]'
    pd_mod._http.request = _http(h)
    pairs = pd_mod.get_all_coinbase_pairs(min_volume_usd=0)
    assert pairs == []  # trading_disabled -> skipped (line 98)


def test_get_all_coinbase_pairs_quote_filtered():
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 10, "volume": 100}'
        return b'[{"id":"X-EUR","base_currency":"X","quote_currency":"EUR","status":"online"}]'
    pd_mod._http.request = _http(h)
    # EUR not in default quote_currencies -> skipped at line 100
    pairs = pd_mod.get_all_coinbase_pairs(min_volume_usd=0)
    assert pairs == []


def test_get_all_coinbase_pairs_volume_filter_unknown_quote():
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 10, "volume": 100}'
        return b'[{"id":"X-EUR","base_currency":"X","quote_currency":"EUR","status":"online"}]'
    pd_mod._http.request = _http(h)
    # EUR allowed as quote but has no USD rate -> _filter_by_volume returns None (line 137)
    pairs = pd_mod.get_all_coinbase_pairs(min_volume_usd=1, quote_currencies=("EUR",))
    assert pairs == []


def test_top_coinbase_pairs_empty():
    def h(url, timeout=10):
        if "ticker" in url:
            return b'{"price": 10, "volume": 0}'
        return b'[{"id":"AAA-USD","base_currency":"AAA","quote_currency":"USD","status":"online"}]'
    pd_mod._http.request = _http(h)
    top = pd_mod.top_coinbase_pairs(n=5, min_volume_usd=1_000_000)
    assert top == []
