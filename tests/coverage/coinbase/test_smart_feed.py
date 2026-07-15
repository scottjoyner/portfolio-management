"""Tests for coinbase/src/smart_feed.py"""
from __future__ import annotations

import time
from unittest.mock import patch, MagicMock

import pytest

from coinbase.src import smart_feed
from coinbase.src.smart_feed import (
    SmartFeedRefreshManager,
    CandleCacheEntry,
    ProductFeedState,
    TIER_0, TIER_1, TIER_2, TIER_3,
)


@pytest.fixture
def mgr():
    fetch_calls = []

    def fetch_fn(pid, granularity=3600, limit=100):
        fetch_calls.append((pid, granularity, limit))
        return [(1, 100.0, 110.0, 90.0, 105.0, 10.0)]

    batch_calls = []

    def batch_fn(pids, granularity=3600, limit=100):
        batch_calls.append((list(pids), granularity, limit))
        return {pid: [(1, 100.0, 110.0, 90.0, 105.0, 10.0)] for pid in pids}

    m = SmartFeedRefreshManager(fetch_fn=fetch_fn, batch_fn=batch_fn)
    m._fetch_calls = fetch_calls
    m._batch_calls = batch_calls
    return m


def test_cache_entry_and_product_state():
    e = CandleCacheEntry(ts=1.0, candles=[1, 2])
    assert e.candles == [1, 2]
    s = ProductFeedState("BTC-USD", tier=TIER_1, volume_24h=5.0)
    assert s.tier == TIER_1


def test_set_critical_and_positions(mgr):
    mgr.set_critical(["BTC-USD", "ETH-USD"])
    assert mgr._critical == {"BTC-USD", "ETH-USD"}
    assert mgr._products["BTC-USD"].tier == TIER_0
    mgr.add_position("XRP-USD")
    assert mgr._positions == {"XRP-USD"}
    assert mgr._products["XRP-USD"].tier == TIER_0
    assert mgr._products["XRP-USD"].has_position is True
    # remove
    mgr.remove_position("XRP-USD")
    assert "XRP-USD" not in mgr._positions
    assert mgr._products["XRP-USD"].tier == TIER_3
    # remove critical keeps tier 0
    mgr.remove_position("BTC-USD")
    assert mgr._products["BTC-USD"].tier == TIER_0


def test_set_volume_tiering(mgr):
    mgr.set_volume("BIG", 200_000_000)
    assert mgr._products["BIG"].tier == TIER_1
    mgr.set_volume("MED", 50_000_000)
    assert mgr._products["MED"].tier == TIER_2
    mgr.set_volume("SMALL", 1_000_000)
    assert mgr._products["SMALL"].tier == TIER_3
    # critical not overridden by volume
    mgr.set_critical(["BIG"])
    mgr.set_volume("BIG", 1_000)
    assert mgr._products["BIG"].tier == TIER_0


def test_ensure_product_and_known(mgr):
    s = mgr._ensure_product("NEW")
    assert s.product_id == "NEW"
    assert "NEW" in mgr.known_products()
    assert mgr.products_by_tier(TIER_3)  # NEW is tier 3


def test_product_tier(mgr):
    mgr.set_critical(["BTC-USD"])
    assert mgr._product_tier("BTC-USD") == TIER_0
    assert mgr._product_tier("UNKNOWN") == TIER_3


def test_cache_get_set_stale(mgr):
    mgr._cache_set("BTC-USD", 3600, 100, [1, 2, 3])
    got = mgr._cache_get("BTC-USD", 3600, 100)
    assert got == [1, 2, 3]
    # stale
    stale, age = mgr._cache_get_stale("BTC-USD", 3600, 100, max_stale_s=10)
    assert stale == [1, 2, 3]
    stale2, _ = mgr._cache_get_stale("NOPE", 3600, 100)
    assert stale2 is None


def test_get_candles_cache_hit(mgr):
    mgr.get_candles("BTC-USD", force=True)  # populate
    assert len(mgr._fetch_calls) == 1
    # now cached -> no new fetch
    mgr.get_candles("BTC-USD")
    assert len(mgr._fetch_calls) == 1


def test_get_candles_force(mgr):
    mgr.get_candles("BTC-USD")
    before = len(mgr._fetch_calls)
    mgr.get_candles("BTC-USD", force=True)
    assert len(mgr._fetch_calls) == before + 1


def test_get_candles_failure_empty(mgr):
    mgr._fetch_fn = lambda *a, **k: []
    res = mgr.get_candles("BTC-USD", force=True)
    assert res == []
    assert mgr._fetch_failures >= 1


def test_get_candles_failure_stale(mgr):
    mgr.get_candles("BTC-USD", force=True)  # cache populated
    mgr._fetch_fn = lambda *a, **k: []
    res = mgr.get_candles("BTC-USD", force=True)
    # falls back to stale cache
    assert res == [(1, 100.0, 110.0, 90.0, 105.0, 10.0)]
    assert mgr._stale_served >= 1


def test_get_candles_failure_stale_disabled(mgr):
    mgr.get_candles("BTC-USD", force=True)
    mgr._fetch_fn = lambda *a, **k: []
    res = mgr.get_candles("BTC-USD", force=True, allow_stale=False)
    assert res == []
    assert mgr._fetch_failures >= 1


def test_get_candles_no_fetch_fn(mgr):
    mgr._fetch_fn = None
    # fallback also returns [] here (no network) -> empty
    res = mgr.get_candles("BTC-USD", force=True)
    assert isinstance(res, list)


def test_get_candles_batch(mgr):
    res = mgr.get_candles_batch(["BTC-USD", "ETH-USD"], force=True)
    assert "BTC-USD" in res and "ETH-USD" in res
    assert len(mgr._batch_calls) == 1
    # cached second time
    res2 = mgr.get_candles_batch(["BTC-USD", "ETH-USD"])
    assert "BTC-USD" in res2
    assert len(mgr._batch_calls) == 1


def test_get_candles_batch_failure_stale(mgr):
    mgr.get_candles_batch(["BTC-USD"], force=True)
    mgr._batch_fn = lambda pids, **k: {}
    mgr._fetch_fn = lambda *a, **k: []  # fallback also empty
    res = mgr.get_candles_batch(["BTC-USD"], force=True)
    assert res["BTC-USD"] == [(1, 100.0, 110.0, 90.0, 105.0, 10.0)]


def test_get_candles_batch_no_batch_fn(mgr):
    mgr._batch_fn = None
    res = mgr.get_candles_batch(["BTC-USD"], force=True)
    assert "BTC-USD" in res


def test_invalidate(mgr):
    mgr._cache_set("BTC-USD", 3600, 100, [1])
    mgr._cache_set("ETH-USD", 3600, 100, [2])
    mgr.invalidate("BTC-USD")
    assert mgr._cache_get("BTC-USD", 3600, 100) is None
    assert mgr._cache_get("ETH-USD", 3600, 100) == [2]
    mgr.invalidate()
    assert mgr._cache_get("ETH-USD", 3600, 100) is None


def test_stats(mgr):
    mgr.set_critical(["BTC-USD"])
    s = mgr.stats()
    assert s["products"] >= 1
    assert s["tiers"][TIER_0] >= 1
    assert "stale_served" in s
    assert "running" in s


def test_refresh_critical_now(mgr):
    mgr.set_critical(["BTC-USD", "ETH-USD"])
    res = mgr.refresh_critical_now()
    assert "BTC-USD" in res


def test_refresh_critical_now_callback(mgr):
    mgr.set_critical(["BTC-USD"])
    captured = {}

    def cb(fresh):
        captured["fresh"] = fresh

    mgr.on_critical_refresh = cb
    mgr.refresh_critical_now()
    assert "fresh" in captured


def test_refresh_critical_now_empty(mgr):
    res = mgr.refresh_critical_now()
    assert res == {}


def test_refresh_all_active(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr.set_volume("ETH-USD", 200_000_000)  # tier 1
    res = mgr.refresh_all_active()
    assert "BTC-USD" in res


def test_refresh_all_active_empty(mgr):
    res = mgr.refresh_all_active()
    assert res == {}


def test_start_stop_running(mgr):
    mgr.start()
    assert mgr.running
    mgr.stop()
    assert not mgr.running


def test_start_idempotent(mgr):
    mgr.start()
    t = mgr._thread
    mgr.start()
    assert mgr._thread is t
    mgr.stop()


def test_wait_ready(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr.start()
    try:
        mgr.wait_ready(timeout=3.0)
    finally:
        mgr.stop()


def test_wait_ready_timeout(mgr):
    mgr.start()
    try:
        # no critical products -> timeout warning but returns
        mgr.wait_ready(timeout=0.3)
    finally:
        mgr.stop()


def test_tick_and_loop(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr.set_volume("ETH-USD", 200_000_000)  # tier 1
    mgr.set_volume("XRP-USD", 50_000_000)  # tier 2
    # make last_fetch stale so they get picked up
    for pid in ("ETH-USD", "XRP-USD"):
        mgr._products[pid].last_fetch = 0.0
    mgr._tick()
    # background loop runs without error
    mgr._loop_count = 0
    mgr._shutdown.clear()
    mgr._tick()
    mgr._loop()


def test_fetch_fallback_success():
    mgr = SmartFeedRefreshManager()  # no fetch fn
    fake_resp = MagicMock()
    fake_resp.status = 200
    fake_resp.data = b"[[1, 90, 110, 100, 105, 10]]"
    fake_http = MagicMock()
    fake_http.request.return_value = fake_resp
    with patch.object(smart_feed.urllib3, "PoolManager", return_value=fake_http):
        res = mgr._fetch_fallback("BTC-USD", 3600, 100)
    assert res == [[1, 90, 110, 100, 105, 10]]


def test_fetch_fallback_failure():
    mgr = SmartFeedRefreshManager()
    fake_http = MagicMock()
    fake_http.request.side_effect = Exception("boom")
    with patch.object(smart_feed.urllib3, "PoolManager", return_value=fake_http):
        res = mgr._fetch_fallback("BTC-USD", 3600, 100)
    assert res == []


def test_get_candles_fallback_used(mgr):
    mgr._fetch_fn = None
    mgr._batch_fn = None
    with patch.object(smart_feed.urllib3, "PoolManager") as pm:
        fake_resp = MagicMock()
        fake_resp.status = 200
        fake_resp.data = b"[[1, 90, 110, 100, 105, 10]]"
        fake_http = MagicMock()
        fake_http.request.return_value = fake_resp
        pm.return_value = fake_http
        res = mgr.get_candles("BTC-USD", force=True)
    assert res == [[1, 90, 110, 100, 105, 10]]
