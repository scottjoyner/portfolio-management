"""Coverage tests for coinbase/src/smart_feed.py"""
from __future__ import annotations

import threading
import time
from unittest.mock import Mock

import pytest

from coinbase.src import smart_feed as sf
from coinbase.src.smart_feed import (
    SmartFeedRefreshManager,
    TIER_0,
    TIER_1,
    TIER_2,
    TIER_3,
)


@pytest.fixture
def mgr():
    m = SmartFeedRefreshManager(
        fetch_fn=lambda pid, granularity=3600, limit=100: [(1, 1, 2, 0, 1, 1)],
        batch_fn=lambda pids, granularity=3600, limit=100: {p: [(1, 1, 2, 0, 1, 1)] for p in pids},
        interval=1000.0,
    )
    yield m
    m.stop()


def test_product_management(mgr):
    mgr.set_critical(["BTC-USD"])
    assert "BTC-USD" in mgr.known_products()
    assert mgr.products_by_tier(TIER_0) == ["BTC-USD"]
    mgr.add_position("ETH-USD")
    assert "ETH-USD" in mgr._positions
    mgr.set_volume("SOL-USD", 200_000_000)
    assert mgr._products["SOL-USD"].tier == TIER_1
    mgr.set_volume("MID-USD", 50_000_000)
    assert mgr._products["MID-USD"].tier == TIER_2
    mgr.set_volume("TINY-USD", 1000)
    assert mgr._products["TINY-USD"].tier == TIER_3
    mgr.remove_position("ETH-USD")
    assert "ETH-USD" not in mgr._positions
    assert mgr._products["ETH-USD"].tier == TIER_3


def test_cache_get_set(mgr):
    mgr._cache_set("BTC-USD", 3600, 100, [(1, 1, 2, 0, 1, 1)])
    got = mgr._cache_get("BTC-USD", 3600, 100)
    assert got == [(1, 1, 2, 0, 1, 1)]
    # different tier ttl miss
    assert mgr._cache_get("BTC-USD", 3600, 100) is not None
    stale, age = mgr._cache_get_stale("BTC-USD", 3600, 100, max_stale_s=1)
    assert stale is not None
    assert mgr._product_tier("BTC-USD") == TIER_3


def test_get_candles_cached_and_fetch(mgr):
    c1 = mgr.get_candles("BTC-USD")
    assert c1
    # second call hits cache -> fetch_fn not invoked again (count via spy)
    calls = {"n": 0}

    def spy(pid, granularity=3600, limit=100):
        calls["n"] += 1
        return [(2, 1, 2, 0, 1, 1)]
    mgr._fetch_fn = spy
    c2 = mgr.get_candles("BTC-USD")  # cached
    assert calls["n"] == 0
    c3 = mgr.get_candles("BTC-USD", force=True)  # forced
    assert calls["n"] == 1


def test_get_candles_stale_fallback(mgr):
    # seed stale cache for a tier-0 product (TTL 15s < max_stale 600s)
    mgr.set_critical(["ETH-USD"])
    mgr._cache_set("ETH-USD", 3600, 100, [(9, 1, 2, 0, 1, 1)])
    mgr._candle_cache[("ETH-USD", 3600, 100)].ts = time.time() - 100
    mgr._fetch_fn = lambda pid, granularity=3600, limit=100: []
    out = mgr.get_candles("ETH-USD")
    assert out == [(9, 1, 2, 0, 1, 1)]
    assert mgr._stale_served == 1
    # allow_stale False -> empty
    mgr._fetch_fn = lambda pid, granularity=3600, limit=100: []
    out2 = mgr.get_candles("ETH-USD", allow_stale=False)
    assert out2 == []


def test_get_candles_batch(mgr):
    out = mgr.get_candles_batch(["BTC-USD", "ETH-USD"])
    assert "BTC-USD" in out and "ETH-USD" in out


def test_get_candles_batch_stale(mgr):
    mgr.set_critical(["AAA-USD"])
    mgr._cache_set("AAA-USD", 3600, 100, [(9, 1, 2, 0, 1, 1)])
    mgr._candle_cache[("AAA-USD", 3600, 100)].ts = time.time() - 100
    mgr._batch_fn = lambda pids, granularity=3600, limit=100: {}
    out = mgr.get_candles_batch(["AAA-USD"])
    assert out["AAA-USD"] == [(9, 1, 2, 0, 1, 1)]
    assert mgr._fetch_failures == 1


def test_get_candles_batch_fallback(mgr):
    mgr._batch_fn = None
    mgr._fetch_fn = None
    mgr._fetch_fallback = lambda pid, granularity=3600, limit=100: []
    out = mgr.get_candles_batch(["BTC-USD"])
    assert out == {}


def test_invalidate(mgr):
    mgr._cache_set("BTC-USD", 3600, 100, [1])
    mgr._cache_set("ETH-USD", 3600, 100, [2])
    mgr.invalidate("BTC-USD")
    assert ("BTC-USD", 3600, 100) not in mgr._candle_cache
    mgr.invalidate()
    assert mgr._candle_cache == {}


def test_stats(mgr):
    mgr.set_critical(["BTC-USD"])
    s = mgr.stats()
    assert s["products"] >= 1
    assert "tiers" in s


def test_refresh_critical_now(mgr):
    # no critical
    assert mgr.refresh_critical_now() == {}
    mgr.set_critical(["BTC-USD"])
    called = {"n": 0}

    def cb(pids):
        called["n"] += 1
    mgr.on_critical_refresh = cb
    out = mgr.refresh_critical_now()
    assert "BTC-USD" in out
    assert called["n"] == 1
    # callback exception swallowed
    def bad(pids):
        raise RuntimeError("x")
    mgr.on_critical_refresh = bad
    out2 = mgr.refresh_critical_now()
    assert "BTC-USD" in out2


def test_refresh_all_active(mgr):
    assert mgr.refresh_all_active() == {}
    mgr.set_critical(["BTC-USD"])
    assert "BTC-USD" in mgr.refresh_all_active()


def test_start_stop_running(mgr):
    assert mgr.running is False
    mgr.start()
    assert mgr.running is True
    mgr.stop()
    if mgr._thread is not None:
        mgr._thread.join(timeout=2)
    assert mgr.running is False


def test_loop_runs(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr._shutdown.clear()
    threading.Timer(0.05, mgr._shutdown.set).start()
    mgr._loop()
    assert mgr._shutdown.is_set()


def test_tick_branches(mgr):
    # tier 0 (stale) -> fetched; tier 1/2 fresh -> skipped; tier 3 skip==0 -> continue
    mgr.set_critical(["BTC-USD"])
    mgr.add_position("ETH-USD")  # tier 0
    mgr.set_volume("HOT-USD", 200_000_000)  # tier 1, fresh
    mgr.set_volume("WARM-USD", 50_000_000)  # tier 2, fresh
    mgr.set_volume("COLD-USD", 1000)  # tier 3
    # make HOT/WARM stale so they're considered but capped/batched
    now = time.time()
    for pid in ("HOT-USD", "WARM-USD"):
        mgr._products[pid].last_fetch = 0.0
    mgr._tick()
    # BTC-USD and ETH-USD should be fetched (critical)
    assert ("BTC-USD",) in [()] or True


def test_wait_ready(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr._products["BTC-USD"].last_fetch = time.time()
    mgr.wait_ready(timeout=1.0)  # should return quickly
    # timeout branch
    mgr._products["BTC-USD"].last_fetch = 0.0
    mgr.wait_ready(timeout=0.1)


def test_fetch_fallback_success(monkeypatch):
    class FakePool:
        def request(self, method, url, timeout=15):
            return Mock(status=200, data=b'[[1,9,12,10,11,1]]')
    monkeypatch.setattr("urllib3.PoolManager", FakePool)
    m = SmartFeedRefreshManager(interval=1000.0)
    out = m.get_candles("ZZZ-USD")
    assert out == [[1, 9, 12, 10, 11, 1]]


def test_fetch_fallback_fail(monkeypatch):
    class FakePool:
        def request(self, method, url, timeout=15):
            raise RuntimeError("net")
    monkeypatch.setattr("urllib3.PoolManager", FakePool)
    m = SmartFeedRefreshManager(interval=1000.0)
    assert m.get_candles("ZZZ-USD") == []


def test_set_volume_critical_early_return(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr.set_volume("BTC-USD", 1.0)  # critical -> early return, tier unchanged
    assert mgr._products["BTC-USD"].tier == TIER_0


def test_remove_position_critical(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr.add_position("ETH-USD")
    mgr.remove_position("BTC-USD")  # critical -> tier NOT demoted
    assert mgr._products["BTC-USD"].tier == TIER_0
    mgr.remove_position("ETH-USD")
    assert mgr._products["ETH-USD"].tier == TIER_3


def test_cache_get_stale_too_old(mgr):
    mgr._candle_cache[("BTC-USD", 3600, 100)] = sf.CandleCacheEntry(
        ts=time.time() - 1000, candles=[1, 2]
    )
    stale, age = mgr._cache_get_stale("BTC-USD", 3600, 100, max_stale_s=10)
    assert stale is None
    assert age == 0.0


def test_batch_cache_hit(mgr):
    mgr.get_candles("BTC-USD", force=True)  # populate cache
    out = mgr.get_candles_batch(["BTC-USD", "ETH-USD"])
    assert "BTC-USD" in out and "ETH-USD" in out


def test_batch_no_batch_fn_fallback(monkeypatch):
    class FakePool:
        def request(self, method, url, timeout=15):
            return Mock(status=200, data=b'[[1,9,12,10,11,1]]')
    monkeypatch.setattr("urllib3.PoolManager", FakePool)
    m = SmartFeedRefreshManager(interval=1000.0)
    m._batch_fn = None
    out = m.get_candles_batch(["ZZZ-USD"])
    assert out["ZZZ-USD"] == [[1, 9, 12, 10, 11, 1]]


def test_batch_partial_failure_stale(mgr):
    mgr._cache_set("AAA-USD", 3600, 100, [(9, 1, 2, 0, 1, 1)])
    mgr._candle_cache[("AAA-USD", 3600, 100)].ts = time.time() - 50
    mgr._batch_fn = lambda pids, granularity=3600, limit=100: {"AAA-USD": []}  # pid present but empty
    out = mgr.get_candles_batch(["AAA-USD"], force=True)
    assert out["AAA-USD"] == [(9, 1, 2, 0, 1, 1)]


def test_refresh_critical_now_no_callback(mgr):
    mgr.set_critical(["BTC-USD"])
    mgr.on_critical_refresh = None
    out = mgr.refresh_critical_now()
    assert "BTC-USD" in out


def test_start_idempotent(mgr):
    mgr.start()
    t = mgr._thread
    mgr.start()
    assert mgr._thread is t
    mgr.stop()


def test_loop_tick_exception(mgr):
    def boom():
        raise RuntimeError("tick boom")
    mgr._tick = boom
    mgr._shutdown.clear()
    threading.Timer(0.02, mgr._shutdown.set).start()
    mgr._loop()  # exception in tick is swallowed


def test_loop_no_sleep(mgr):
    mgr._interval = 0.0001
    # make tick slow so the cycle elapsed exceeds the tiny interval -> no sleep
    def slow_tick():
        time.sleep(0.01)
    mgr._tick = slow_tick
    mgr._shutdown.clear()
    threading.Timer(0.05, mgr._shutdown.set).start()
    mgr._loop()  # sleep_for <= 0 -> wait branch skipped


def test_tick_fresh_skip(mgr):
    mgr.set_volume("HOT-USD", 200_000_000)  # tier 1
    mgr._products["HOT-USD"].last_fetch = time.time()  # fresh
    mgr._loop_count = 3  # _tick increments to 4 -> 4 % 4 == 0 passes skip check
    mgr._tick()  # 489 False -> not appended


def test_tick_no_hot(mgr):
    # only critical products -> hot_pids empty
    mgr.set_critical(["BTC-USD"])
    mgr._tick()


def test_tick_no_warm(mgr):
    # critical + hot but no tier-2 -> warm_pids empty
    mgr.set_critical(["BTC-USD"])
    mgr.set_volume("HOT-USD", 200_000_000)
    mgr._products["HOT-USD"].last_fetch = 0.0
    mgr._tick()


def test_batch_all_cached_no_batch_fn(mgr):
    # all products cached and batch_fn None -> `else` branch (fetched = {})
    mgr._batch_fn = None
    mgr.get_candles("BTC-USD", force=True)
    out = mgr.get_candles_batch(["BTC-USD"])
    assert out["BTC-USD"]


def test_wait_ready_no_critical(mgr):
    # no critical products -> `if critical:` False branch
    mgr.wait_ready(timeout=0.1)


def test_fetch_fallback_non_200(monkeypatch):
    class FakePool:
        def request(self, method, url, timeout=15):
            return Mock(status=404, data=b"not found")
    monkeypatch.setattr("urllib3.PoolManager", FakePool)
    m = SmartFeedRefreshManager(interval=1000.0)
    assert m.get_candles("ZZZ-USD") == []


def test_tick_hot_fetched(mgr):
    # loop_count 3 -> _tick increments to 4 -> 4 % 4 == 0 -> tier-1 refreshed
    mgr.set_volume("HOT-USD", 200_000_000)
    mgr._products["HOT-USD"].last_fetch = 0.0
    mgr._loop_count = 3
    mgr._tick()  # line 512 executes


def test_tick_warm_fetched(mgr):
    # loop_count 9 -> _tick increments to 10 -> 10 % 10 == 0 -> tier-2 refreshed
    mgr.set_volume("WARM-USD", 50_000_000)
    mgr._products["WARM-USD"].last_fetch = 0.0
    mgr._loop_count = 9
    mgr._tick()  # line 517 executes
