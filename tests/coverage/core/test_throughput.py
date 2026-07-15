"""Tests for trading_system.core.throughput."""

import time
from unittest.mock import MagicMock, patch

import pytest

from trading_system.core.throughput import (
    CandleSet,
    IndicatorCache,
    RingBuffer,
    StrategyRunner,
)


# ---------------------------------------------------------------------------
# RingBuffer
# ---------------------------------------------------------------------------

def test_ringbuffer_init_and_len():
    rb = RingBuffer(maxlen=3)
    assert len(rb) == 0
    assert rb._maxlen == 3


def test_ringbuffer_append_grows_then_caps():
    rb = RingBuffer(maxlen=3)
    rb.append(1.0)
    rb.append(2.0)
    assert len(rb) == 2
    rb.append(3.0)
    rb.append(4.0)
    # size capped at maxlen
    assert len(rb) == 3
    assert rb.to_list() == [2.0, 3.0, 4.0]


def test_ringbuffer_getitem_positive():
    rb = RingBuffer(maxlen=5)
    for v in [10.0, 20.0, 30.0]:
        rb.append(v)
    assert rb[0] == 10.0
    assert rb[2] == 30.0


def test_ringbuffer_getitem_negative():
    rb = RingBuffer(maxlen=5)
    for v in [10.0, 20.0, 30.0]:
        rb.append(v)
    assert rb[-1] == 30.0
    assert rb[-3] == 10.0


def test_ringbuffer_getitem_slice():
    rb = RingBuffer(maxlen=5)
    for v in [1.0, 2.0, 3.0, 4.0]:
        rb.append(v)
    assert rb[0:3] == [1.0, 2.0, 3.0]
    assert rb[::2] == [1.0, 3.0]


def test_ringbuffer_getitem_out_of_range_positive():
    rb = RingBuffer(maxlen=5)
    rb.append(1.0)
    with pytest.raises(IndexError):
        _ = rb[5]


def test_ringbuffer_getitem_out_of_range_negative():
    rb = RingBuffer(maxlen=5)
    rb.append(1.0)
    with pytest.raises(IndexError):
        _ = rb[-5]


def test_ringbuffer_getitem_bad_type():
    rb = RingBuffer(maxlen=5)
    with pytest.raises(TypeError):
        _ = rb["x"]


def test_ringbuffer_setitem_valid():
    rb = RingBuffer(maxlen=5)
    for v in [1.0, 2.0, 3.0]:
        rb.append(v)
    rb[1] = 99.0
    assert rb[1] == 99.0


def test_ringbuffer_setitem_negative():
    rb = RingBuffer(maxlen=5)
    for v in [1.0, 2.0, 3.0]:
        rb.append(v)
    rb[-1] = 42.0
    assert rb[2] == 42.0


def test_ringbuffer_setitem_out_of_range():
    rb = RingBuffer(maxlen=5)
    rb.append(1.0)
    with pytest.raises(IndexError):
        rb[3] = 5.0


def test_ringbuffer_delitem_raises():
    rb = RingBuffer(maxlen=5)
    with pytest.raises(NotImplementedError):
        del rb[0]


def test_ringbuffer_insert_raises():
    rb = RingBuffer(maxlen=5)
    with pytest.raises(NotImplementedError):
        rb.insert(0, 1.0)


def test_ringbuffer_repr():
    rb = RingBuffer(maxlen=5)
    rb.append(1.0)
    assert "RingBuffer" in repr(rb)


# ---------------------------------------------------------------------------
# IndicatorCache
# ---------------------------------------------------------------------------

def test_indicator_cache_key():
    c = IndicatorCache()
    assert c._key("BTC", "ema", 12) == "BTC:ema:(12,)"
    assert c._key("BTC", "ema") == "BTC:ema:()"


def test_indicator_cache_set_get():
    c = IndicatorCache()
    c.set("BTC", "ema", 12.5, 12)
    assert c.get("BTC", "ema", 12) == 12.5


def test_indicator_cache_get_missing():
    c = IndicatorCache()
    assert c.get("BTC", "ema", 12) is None


def test_indicator_cache_get_expired():
    c = IndicatorCache(ttl_secs=10.0)
    c.set("BTC", "ema", 12.5, 12)
    # simulate time passing beyond ttl
    with patch("trading_system.core.throughput.time.monotonic", return_value=1000.0):
        # set timestamp far in the past relative to get call
        c._timestamps[c._key("BTC", "ema", 12)] = 0.0
        assert c.get("BTC", "ema", 12) is None
    # entry removed
    assert c.size == 0


def test_indicator_cache_get_many():
    c = IndicatorCache()
    c.set("BTC", "ema", 12.5, 12)
    c.set("BTC", "ema", 13.5, 13)
    res = c.get_many("BTC", "ema", (12,), (13,), (99,))
    assert res == [12.5, 13.5, None]


def test_indicator_cache_lru_move_to_end():
    c = IndicatorCache()
    c.set("A", "x", 1.0)
    c.set("B", "x", 2.0)
    # access A -> moved to end; B remains oldest
    c.get("A", "x")
    assert list(c._store.keys())[-1] == "A:x:()"


def test_indicator_cache_eviction():
    c = IndicatorCache(max_entries=2)
    c.set("A", "x", 1.0)
    c.set("B", "x", 2.0)
    c.set("C", "x", 3.0)  # triggers eviction of oldest (A)
    assert c.size == 2
    assert c.get("A", "x") is None
    assert c.get("B", "x") == 2.0
    assert c.get("C", "x") == 3.0
    # timestamp for evicted key removed
    assert "A:x:()" not in c._timestamps


def test_indicator_cache_invalidate_all():
    c = IndicatorCache()
    c.set("A", "x", 1.0)
    c.set("B", "y", 2.0)
    n = c.invalidate()
    assert n == 2
    assert c.size == 0


def test_indicator_cache_invalidate_by_product():
    c = IndicatorCache()
    c.set("A", "x", 1.0)
    c.set("B", "x", 2.0)
    c.set("A", "y", 3.0)
    n = c.invalidate(product_id="A")
    assert n == 2
    assert c.get("B", "x") == 2.0


def test_indicator_cache_invalidate_by_indicator():
    c = IndicatorCache()
    c.set("A", "x", 1.0)
    c.set("B", "x", 2.0)
    c.set("A", "y", 3.0)
    n = c.invalidate(indicator="x")
    assert n == 2
    assert c.get("A", "y") == 3.0


def test_indicator_cache_invalidate_no_match():
    c = IndicatorCache()
    c.set("A", "x", 1.0)
    n = c.invalidate(product_id="Z")
    assert n == 0


def test_indicator_cache_size_property():
    c = IndicatorCache()
    c.set("A", "x", 1.0)
    assert c.size == 1


# ---------------------------------------------------------------------------
# StrategyRunner
# ---------------------------------------------------------------------------

def test_strategy_runner_default_cache():
    r = StrategyRunner()
    assert isinstance(r.cache, IndicatorCache)


def test_strategy_runner_provided_cache():
    cache = IndicatorCache()
    r = StrategyRunner(indicator_cache=cache)
    assert r.cache is cache


def test_strategy_runner_get_or_create_new():
    r = StrategyRunner()
    inst = r.get_or_create("s1", MagicMock, return_value=MagicMock())
    assert r._instances["s1"] is inst


def test_strategy_runner_get_or_create_cached():
    r = StrategyRunner()
    cls = MagicMock()
    inst = r.get_or_create("s1", cls)
    inst2 = r.get_or_create("s1", cls)
    assert inst is inst2
    # class not re-instantiated
    cls.assert_called_once()


def test_strategy_runner_reset_one():
    r = StrategyRunner()
    r.get_or_create("s1", MagicMock)
    r.get_or_create("s2", MagicMock)
    r.reset("s1")
    assert "s1" not in r._instances
    assert "s2" in r._instances


def test_strategy_runner_reset_all():
    r = StrategyRunner()
    r.get_or_create("s1", MagicMock)
    r.get_or_create("s2", MagicMock)
    r.reset()
    assert r._instances == {}


def test_strategy_runner_cache_property():
    r = StrategyRunner()
    assert isinstance(r.cache, IndicatorCache)


# ---------------------------------------------------------------------------
# CandleSet
# ---------------------------------------------------------------------------

def test_candleset_empty():
    cs = CandleSet("BTC")
    assert cs.product_id == "BTC"
    assert len(cs) == 0
    assert cs.current_price == 0.0


def test_candleset_with_data():
    cs = CandleSet(
        "BTC",
        closes=[1.0, 2.0, 3.0],
        volumes=[10.0, 20.0, 30.0],
        highs=[1.5, 2.5, 3.5],
        lows=[0.5, 1.5, 2.5],
    )
    assert len(cs) == 3
    assert cs.current_price == 3.0
    assert cs.closes.to_list() == [1.0, 2.0, 3.0]
    assert cs.volumes.to_list() == [10.0, 20.0, 30.0]
    assert cs.highs.to_list() == [1.5, 2.5, 3.5]
    assert cs.lows.to_list() == [0.5, 1.5, 2.5]


def test_candleset_append_bar():
    cs = CandleSet("BTC")
    cs.append_bar(1.0, 10.0, 1.5, 0.5)
    cs.append_bar(2.0, 20.0, 2.5, 1.5)
    assert len(cs) == 2
    assert cs.current_price == 2.0
    assert cs.closes.to_list() == [1.0, 2.0]


def test_candleset_len():
    cs = CandleSet("BTC", closes=[1.0, 2.0])
    assert len(cs) == 2
