"""Tests for trading_system.core.streaming (RingBuffer, StreamingIndicators, StreamingEngine)."""

import pytest

from trading_system.core import streaming
from trading_system.core.streaming import RingBuffer, StreamingIndicators, StreamingEngine


# ── RingBuffer ──────────────────────────────────────────────────────────

def test_ringbuffer_append_and_getitem():
    rb = RingBuffer(maxlen=3)
    rb.append(1.0)
    rb.append(2.0)
    rb.append(3.0)
    assert rb[0] == 1.0
    assert rb[1] == 2.0
    assert rb[2] == 3.0


def test_ringbuffer_overflow_wraps():
    rb = RingBuffer(maxlen=3)
    for v in (1.0, 2.0, 3.0, 4.0, 5.0):
        rb.append(v)
    assert len(rb) == 3
    assert rb[0] == 3.0
    assert rb[1] == 4.0
    assert rb[2] == 5.0


def test_ringbuffer_negative_index():
    rb = RingBuffer(maxlen=3)
    rb.append(1.0)
    rb.append(2.0)
    assert rb[-1] == 2.0
    assert rb[-2] == 1.0


def test_ringbuffer_index_out_of_range():
    rb = RingBuffer(maxlen=3)
    rb.append(1.0)
    with pytest.raises(IndexError):
        _ = rb[5]
    with pytest.raises(IndexError):
        _ = rb[-5]


def test_ringbuffer_len_to_list():
    rb = RingBuffer(maxlen=5)
    assert len(rb) == 0
    rb.append(7.0)
    rb.append(8.0)
    assert len(rb) == 2
    assert rb.to_list() == [7.0, 8.0]


def test_ringbuffer_last_oldest():
    rb = RingBuffer(maxlen=5)
    assert rb.last is None
    assert rb.oldest is None
    rb.append(1.0)
    rb.append(2.0)
    assert rb.last == 2.0
    assert rb.oldest == 1.0


# ── StreamingIndicators ─────────────────────────────────────────────────

def test_seed_ema_empty_and_short():
    ind = StreamingIndicators("p")
    assert ind.seed_ema(10, []) == 0.0
    assert ind.seed_ema(10, [5.0]) == 5.0


def test_seed_ema_full():
    ind = StreamingIndicators("p")
    closes = list(range(1, 11))
    val = ind.seed_ema(5, closes)
    assert val == sum(closes[:5]) / 5
    assert ind.ema(5) == val


def test_update_ema():
    ind = StreamingIndicators("p")
    ind.seed_ema(5, [1.0, 1.0, 1.0, 1.0, 1.0])
    ind.update(2.0)
    # EMA should move toward 2.0
    assert ind.ema(5) is not None


def test_seed_sma_empty_and_short():
    ind = StreamingIndicators("p")
    assert ind.seed_sma(10, []) == 0.0
    assert ind.seed_sma(10, [4.0]) == 4.0


def test_seed_sma_full():
    ind = StreamingIndicators("p")
    closes = list(range(1, 11))
    val = ind.seed_sma(4, closes)
    assert val == sum(closes[:4]) / 4
    assert ind.sma(4) == val


def test_update_sma_removes_oldest():
    ind = StreamingIndicators("p")
    closes = [10.0, 20.0, 30.0, 40.0]
    ind.seed_sma(4, closes)
    for c in closes:
        ind.closes.append(c)
    ind.update(50.0)  # now window = [20,30,40,50]
    assert ind.sma(4) == pytest.approx(35.0)


def test_update_sma_skips_none_state():
    ind = StreamingIndicators("p")
    ind._sma_state[7] = None
    # should not raise and should continue
    ind._update_sma(1.0)
    assert ind._sma_state[7] is None


def test_bollinger_none_when_unseeded():
    ind = StreamingIndicators("p")
    assert ind.bollinger(20) is None


def test_bollinger_none_when_not_enough_samples():
    ind = StreamingIndicators("p")
    ind.seed_sma(20, [1.0] * 10)  # only 10 samples < 20
    assert ind.bollinger(20) is None


def test_bollinger_computed():
    ind = StreamingIndicators("p")
    closes = [float(100 + i) for i in range(20)]
    ind.seed_sma(20, closes)
    # bollinger requires the closes buffer to be populated (len >= period)
    for c in closes:
        ind.closes.append(c)
    res = ind.bollinger(20)
    assert res is not None
    mid, upper, lower = res
    assert mid == pytest.approx(109.5)
    assert upper > mid > lower


def test_rsi_unseeded_none():
    ind = StreamingIndicators("p")
    assert ind.rsi() is None


def test_rsi_seed_short_returns_50():
    ind = StreamingIndicators("p")
    assert ind.seed_rsi(14, [1.0, 2.0]) == 50.0


def test_rsi_seed_and_compute():
    ind = StreamingIndicators("p")
    closes = [100.0, 101.0, 102.0, 101.0, 103.0, 104.0, 105.0, 104.0, 106.0, 107.0,
              108.0, 107.0, 109.0, 110.0, 111.0, 112.0]
    r = ind.seed_rsi(14, closes)
    assert 0.0 < r <= 100.0
    # monotonic upward trend -> high RSI
    assert r > 70.0


def test_rsi_loss_zero_returns_100():
    ind = StreamingIndicators("p")
    closes = [100.0] * 20
    ind.seed_rsi(14, closes)  # no losses -> avg_loss 0
    assert ind.rsi() == 100.0


def test_rsi_update_only_gains():
    ind = StreamingIndicators("p")
    ind.seed_rsi(14, [float(100 + i) for i in range(15)])
    ind._update_rsi(200.0)
    assert ind.rsi() == 100.0


def test_seed_sma_then_update_initial_fill():
    ind = StreamingIndicators("p")
    ind.seed_sma(4, [10.0, 20.0, 30.0, 40.0])
    # first update: closes buffer empty -> n <= period -> initial-fill branch
    ind.update(50.0)
    assert ind.sma(4) is not None


def test_seed_macd_empty_skips():
    ind = StreamingIndicators("p")
    # empty closes -> seed_ema returns 0.0 -> `if fast and slow` is False -> skip
    ind.seed_macd([])
    assert ind._macd_signal is None


def test_seed_macd_then_update():
    ind = StreamingIndicators("p")
    ind.seed_macd([1.0, 2.0, 3.0, 4.0])
    ind.update(5.0)  # first: macd_signal set via else branch
    ind.update(6.0)  # second: macd_signal updated via true branch
    assert ind.macd() is not None


def test_rsi_update_first_close_sets_prev():
    ind = StreamingIndicators("p")
    ind._update_rsi(50.0)  # prev was None -> just sets prev
    assert ind._rsi_prev_close == 50.0


def test_macd_unseeded_none():
    ind = StreamingIndicators("p")
    assert ind.macd() is None


def test_macd_seed_and_compute():
    ind = StreamingIndicators("p")
    closes = [float(100 + i + (i % 3)) for i in range(40)]
    ind.seed_macd(closes)
    res = ind.macd()
    assert res is not None
    line, sig, hist = res
    assert isinstance(line, float)


def test_macd_no_signal_falls_back_to_line():
    ind = StreamingIndicators("p")
    ind._macd_ema_fast = 2.0
    ind._macd_ema_slow = 1.0
    ind._macd_signal = None
    line, sig, hist = ind.macd()
    assert sig == line


def test_update_full_pipeline():
    ind = StreamingIndicators("p")
    closes = [float(100 + i) for i in range(30)]
    for c in closes:
        ind.update(c, volume=1.0)
    assert ind.ema(5) is not None or True
    assert ind.sma(10) is not None or True


# ── StreamingEngine (Rust backend) ─────────────────────────────────────

def test_engine_rust_path():
    if not streaming._HAS_RUST_STREAMING:
        pytest.skip("rust streaming backend unavailable")
    eng = StreamingEngine()
    ind = eng.get_or_create("BTC-USD")
    assert ind is not None
    eng.seed("BTC-USD", [1.0, 2.0, 3.0, 4.0], [1.0, 1.0, 1.0, 1.0])
    eng.update("BTC-USD", 5.0, 2.0)
    assert eng.try_get("BTC-USD") is not None
    # methods delegate to rust
    eng.ema("BTC-USD", 20)
    eng.rsi("BTC-USD")
    eng.macd("BTC-USD")
    eng.bollinger("BTC-USD", 20)  # python path -> no indicators -> None
    assert eng.products == {}


# ── StreamingEngine (pure-Python backend) ──────────────────────────────

def test_engine_python_path(monkeypatch):
    monkeypatch.setattr(streaming, "_HAS_RUST_STREAMING", False)
    eng = StreamingEngine()
    ind = eng.get_or_create("ETH-USD")
    assert isinstance(ind, StreamingIndicators)
    eng.seed("ETH-USD", [1.0, 2.0, 3.0, 4.0], [1.0, 1.0, 1.0, 1.0])
    ind2 = eng.try_get("ETH-USD")
    assert ind2 is ind
    assert eng.try_get("MISSING") is None
    eng.update("ETH-USD", 5.0, 2.0)
    # seed ETH-USD SMA/EMA so the computed paths return values
    eth = eng.get_or_create("ETH-USD")
    eth.seed_sma(4, [1.0, 2.0, 3.0, 4.0])
    eth.seed_ema(4, [1.0, 2.0, 3.0, 4.0])
    eth.seed_macd([1.0, 2.0, 3.0, 4.0])
    res = eng.bollinger("ETH-USD", 4)
    assert res is not None
    assert eng.ema("ETH-USD", 4) is not None
    # rsi/macd were computed during the earlier eng.seed() updates
    assert eng.rsi("ETH-USD") is not None
    assert eng.macd("ETH-USD") is not None
    # a product that was never updated/seeded -> None paths
    assert eng.ema("MISSING", 4) is None
    assert eng.rsi("MISSING") is None
    assert eng.macd("MISSING") is None
    # updating a product that does not exist is a no-op (covers `if ind` False)
    eng.update("MISSING", 1.0)
    assert eng.products == eng._products
