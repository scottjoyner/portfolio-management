"""Tests for trading_system.core.signal_aggregator (UnifiedSignal + SignalAggregator)."""

import logging
import sys
from concurrent.futures import Future
from typing import Any, Dict, List, Tuple

import pytest

from trading_system.core import signal_aggregator as sa
from trading_system.core.signal_aggregator import UnifiedSignal, SignalAggregator


# ── Fake Rust backend & verdiscts ──────────────────────────────────────

class FakeVerdict:
    def __init__(self, passed=True, total_trades=10, win_rate=0.6,
                 sharpe_ratio=1.0, profit_factor=2.0):
        self.passed = passed
        self.total_trades = total_trades
        self.win_rate = win_rate
        self.sharpe_ratio = sharpe_ratio
        self.profit_factor = profit_factor


class FakeRust:
    def __init__(self, signals=None, sma_value=None, adx_raise=False):
        # signals: list of (name, action, conf)
        self._signals = signals if signals is not None else [
            ("ema_cross", "BUY", 0.8), ("rsi_revert", "SELL", 0.6)
        ]
        self._sma_value = sma_value
        self._adx_raise = adx_raise

    def evaluate_all_opens_py(self, closes, opens, volumes, highs, lows):
        return [(n, a, c, "r") for n, a, c in self._signals]

    def sma_py(self, closes, period):
        if self._sma_value is not None:
            return self._sma_value
        data = closes[-period:] if len(closes) >= period else closes
        return sum(data) / len(data) if data else 0.0

    def run_strategy_py(self, name, closes, volumes, highs, lows):
        if self._adx_raise and name == "adx":
            raise RuntimeError("adx boom")
        return ("BUY", 0.5, "r")


def patch_backend(monkeypatch, rust=None, bt_rust=None):
    """Force the module globals to controlled fakes."""
    monkeypatch.setattr(sa, "_HAS_RUST", True)
    monkeypatch.setattr(sa, "_rust_core", rust if rust is not None else FakeRust())
    monkeypatch.setattr(sa, "_batch_backtest_rust", bt_rust)


def make_closes(n, base=100.0, step=1.0):
    return [base + i * step for i in range(n)]


# ── UnifiedSignal ──────────────────────────────────────────────────────

def test_unified_signal_priority():
    s = UnifiedSignal(
        product_id="BTC-USD", base="BTC", price=100.0, unified_score=0.5,
        consensus_score=0.3, backtest_quality=0.5, trend_score=0.1,
        conviction=0.5, active_buys=2, active_sells=1, total_signals=3,
    )
    # 0.5 * (0.5 + 0.5*0.5) * (0.5 + 0.5*0.5) = 0.5 * 0.75 * 0.75
    assert s.priority == pytest.approx(0.28125)


def test_unified_signal_direction():
    buy = UnifiedSignal("BTC-USD", "BTC", 1, 0.5, 0, 0, 0, 1, 0, 0, 0)
    sell = UnifiedSignal("BTC-USD", "BTC", 1, -0.5, 0, 0, 0, 0, 1, 0, 0)
    hold = UnifiedSignal("BTC-USD", "BTC", 1, 0.1, 0, 0, 0, 0, 0, 0, 0)
    assert buy.direction == "BUY"
    assert sell.direction == "SELL"
    assert hold.direction == "HOLD"


def test_unified_signal_short_report():
    s = UnifiedSignal(
        product_id="BTC-USD", base="BTC", price=100.0, unified_score=0.5,
        consensus_score=0.3, backtest_quality=0.5, trend_score=0.1,
        conviction=0.5, active_buys=2, active_sells=1, total_signals=3,
        top_strategies=["a", "b", "c", "d"],
    )
    rep = s.short_report()
    assert "BTC-USD" in rep
    assert "a,b,c" in rep


# ── scan_universe: no rust ─────────────────────────────────────────────

def test_scan_universe_no_rust(monkeypatch, caplog):
    monkeypatch.setattr(sa, "_ensure_rust", lambda: None)  # prevent real import
    monkeypatch.setattr(sa, "_HAS_RUST", False)
    monkeypatch.setattr(sa, "_rust_core", None)
    agg = SignalAggregator()
    with caplog.at_level(logging.ERROR):
        res = agg.scan_universe([("BTC-USD", "BTC")], {"BTC-USD": [1.0]}, {}, {}, {})
    assert res == []
    assert "Rust core not available" in caplog.text


# ── scan_universe: full rust path ──────────────────────────────────────

def test_scan_universe_full(monkeypatch):
    rust = FakeRust([("ema_cross", "BUY", 0.9), ("rsi_revert", "SELL", 0.1)])

    def fake_bt(bt_list, warmup=30):
        out = {}
        for name, base, closes, volumes, highs, lows in bt_list:
            out[f"{name}/{base}"] = FakeVerdict()
        return out

    patch_backend(monkeypatch, rust=rust, bt_rust=fake_bt)

    closes = make_closes(250)
    products = [
        ("BTC-USD", "BTC"),
        ("BTC-USD", "BTC"),  # duplicate, should be skipped
        ("ETH-USD", "ETH"),
        ("SHORT-USD", "SHORT"),  # too few candles -> skipped
    ]
    cdict = {
        "BTC-USD": closes,
        "ETH-USD": make_closes(250, base=50.0),
        "SHORT-USD": [1.0, 2.0],
    }
    agg = SignalAggregator()
    res = agg.scan_universe(products, cdict, cdict, cdict, cdict, min_candles=60)
    # BTC + ETH produce signals; duplicate & short skipped
    assert len(res) == 2
    assert all(isinstance(r, UnifiedSignal) for r in res)
    # sorted by priority descending
    assert res[0].priority >= res[1].priority


def test_scan_universe_future_exception(monkeypatch, caplog):
    patch_backend(monkeypatch, rust=FakeRust())

    class FakeExecutor:
        def submit(self, fn, *a, **k):
            f = Future()
            f.set_exception(RuntimeError("boom"))
            return f

    agg = SignalAggregator()
    agg._executor = FakeExecutor()
    closes = make_closes(100)
    with caplog.at_level(logging.DEBUG):
        res = agg.scan_universe([("BTC-USD", "BTC")], {"BTC-USD": closes},
                                {"BTC-USD": closes}, {"BTC-USD": closes},
                                {"BTC-USD": closes})
    assert res == []
    assert any("Failed for" in r.message for r in caplog.records)


# ── _evaluate_one ──────────────────────────────────────────────────────

def test_evaluate_one_ok(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust([("ema_cross", "BUY", 0.8)]))
    agg = SignalAggregator()
    us = agg._evaluate_one("BTC-USD", "BTC", make_closes(100),
                           make_closes(100), make_closes(100), make_closes(100))
    assert isinstance(us, UnifiedSignal)
    assert us.active_buys == 1


def test_evaluate_one_empty_signals(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(signals=[]))
    agg = SignalAggregator()
    assert agg._evaluate_one("BTC-USD", "BTC", make_closes(100),
                             make_closes(100), make_closes(100), make_closes(100)) is None


def test_evaluate_one_rust_raises(monkeypatch, caplog):
    class RaisingRust(FakeRust):
        def evaluate_all_opens_py(self, *a):
            raise RuntimeError("nope")

    patch_backend(monkeypatch, rust=RaisingRust())
    agg = SignalAggregator()
    with caplog.at_level(logging.DEBUG):
        assert agg._evaluate_one("BTC-USD", "BTC", make_closes(100),
                                 make_closes(100), make_closes(100), make_closes(100)) is None


def test_evaluate_one_too_short(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust([("ema_cross", "BUY", 0.8)]))
    agg = SignalAggregator()
    # closes length 1 -> hits `if len(closes) < 2` guard
    assert agg._evaluate_one("BTC-USD", "BTC", [1.0], [1.0], [1.0], [1.0]) is None


def test_evaluate_one_backtest_raises(monkeypatch, caplog):
    patch_backend(monkeypatch, rust=FakeRust(), bt_rust=lambda *a, **k: (_ for _ in ()).throw(RuntimeError("bt")))

    agg = SignalAggregator()
    with caplog.at_level(logging.DEBUG):
        assert agg._evaluate_one("BTC-USD", "BTC", make_closes(100),
                                 make_closes(100), make_closes(100), make_closes(100)) is None


# ── _backtest_one ──────────────────────────────────────────────────────

def test_backtest_one_no_rust(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(), bt_rust=None)
    agg = SignalAggregator()
    assert agg._backtest_one("BTC-USD", "BTC", make_closes(100), [], [], [], []) == {}


def test_backtest_one_no_signals(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(), bt_rust=lambda *a, **k: {})
    agg = SignalAggregator()
    assert agg._backtest_one("BTC-USD", "BTC", make_closes(100), [], [], [], []) == {}


def test_backtest_one_with_signals(monkeypatch):
    def fake_bt(bt_list, warmup=30):
        out = {}
        for name, base, *_ in bt_list:
            out[f"{name}/{base}"] = FakeVerdict()
        return out

    patch_backend(monkeypatch, rust=FakeRust(), bt_rust=fake_bt)
    agg = SignalAggregator()
    sigs = [("ema_cross", 0.8), ("rsi_revert", 0.6)]
    bt_map = agg._backtest_one("BTC-USD", "BTC", make_closes(100), [], [], [], sigs)
    assert bt_map["ema_cross"] is not None
    assert bt_map["rsi_revert"] is not None
    # cache reuse: second call still invokes backend but cached verdicts are returned
    calls = {"n": 0}

    def fake_bt2(bt_list, warmup=30):
        calls["n"] += 1
        return {}

    monkeypatch.setattr(sa, "_batch_backtest_rust", fake_bt2)
    bt_map2 = agg._backtest_one("BTC-USD", "BTC", make_closes(100), [], [], [], sigs)
    assert calls["n"] == 1  # backend still called
    # cached verdicts are returned (not overwritten by the empty result)
    assert bt_map2["ema_cross"] is not None


def test_backtest_one_cache_hit(monkeypatch):
    # First call populates the cache for "ema_cross/BTC".
    def fake_bt(bt_list, warmup=30):
        out = {}
        for name, base, *_ in bt_list:
            out[f"{name}/{base}"] = FakeVerdict()
        return out

    patch_backend(monkeypatch, rust=FakeRust(), bt_rust=fake_bt)
    agg = SignalAggregator()
    sigs = [("ema_cross", 0.8)]
    agg._backtest_one("BTC-USD", "BTC", make_closes(100), [], [], [], sigs)

    # Second call: backend returns the SAME key which is already cached ->
    # the `if ck not in self._bt_cache` False branch is exercised.
    calls = {"n": 0}

    def fake_bt2(bt_list, warmup=30):
        calls["n"] += 1
        return {"ema_cross/BTC": FakeVerdict()}

    monkeypatch.setattr(sa, "_batch_backtest_rust", fake_bt2)
    bt_map = agg._backtest_one("BTC-USD", "BTC", make_closes(100), [], [], [], sigs)
    assert calls["n"] == 1
    assert bt_map["ema_cross"] is not None


# ── _compute_trend ─────────────────────────────────────────────────────

def test_compute_trend_full(monkeypatch):
    # sma_200 == 0 forces the `if sma_200 != 0.0` False branches
    patch_backend(monkeypatch, rust=FakeRust(sma_value=0.0))
    agg = SignalAggregator()
    t = agg._compute_trend(make_closes(250), make_closes(250, 1.0), make_closes(250), make_closes(250))
    assert -1.0 <= t <= 1.0


def test_compute_trend_full_with_adx(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(sma_value=100.0))
    agg = SignalAggregator()
    t = agg._compute_trend(make_closes(250), make_closes(250, 1.0), make_closes(250), make_closes(250))
    assert -1.0 <= t <= 1.0


def test_compute_trend_adx_raises(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(sma_value=100.0, adx_raise=True))
    agg = SignalAggregator()
    t = agg._compute_trend(make_closes(250), make_closes(250, 1.0), make_closes(250), make_closes(250))
    assert -1.0 <= t <= 1.0


def test_compute_trend_short_range(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(sma_value=100.0))
    agg = SignalAggregator()
    # 100 bars -> short trend path
    t = agg._compute_trend(make_closes(100), make_closes(100, 1.0), make_closes(100), make_closes(100))
    assert -1.0 <= t <= 1.0


def test_compute_trend_under_50(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(sma_value=100.0))
    agg = SignalAggregator()
    assert agg._compute_trend(make_closes(30), make_closes(30), make_closes(30), make_closes(30)) == 0.0


def test_compute_trend_short_explicit(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(sma_value=100.0))
    agg = SignalAggregator()
    # n >= 50 uses sma_50
    assert -1.0 <= agg._compute_trend_short(make_closes(100), 100) <= 1.0
    # n < 50 falls back to sma_20
    assert -1.0 <= agg._compute_trend_short(make_closes(20), 20) <= 1.0


# ── _backtest_quality ──────────────────────────────────────────────────

def test_backtest_quality_empty():
    agg = SignalAggregator()
    assert agg._backtest_quality({}, []) == 0.0


def test_backtest_quality_various():
    agg = SignalAggregator()
    bt_map = {
        "good": FakeVerdict(passed=True, total_trades=10, win_rate=0.8, sharpe_ratio=1.0, profit_factor=2.0),
        "fail": FakeVerdict(passed=False, total_trades=10),
        "few_trades": FakeVerdict(passed=True, total_trades=2),
        "none": None,
    }
    sigs = [("good", 1.0), ("fail", 1.0), ("few_trades", 1.0), ("none", 1.0)]
    q = agg._backtest_quality(bt_map, sigs)
    assert 0.0 < q <= 1.0


# ── _compute_unified ───────────────────────────────────────────────────

def test_compute_unified_bullish():
    agg = SignalAggregator()
    buys = [("ema_cross", 0.8), ("rsi_revert", 0.6)]
    sells = [("macd", 0.2)]
    bt_map = {"ema_cross": FakeVerdict(), "rsi_revert": FakeVerdict(), "macd": FakeVerdict()}
    us = agg._compute_unified("BTC-USD", "BTC", 100.0, buys, sells, bt_map, 0.2)
    assert us.unified_score > 0
    assert us.conviction == pytest.approx(2 / 3)
    assert "ema_cross" in us.top_strategies


def test_compute_unified_bearish():
    agg = SignalAggregator()
    buys = [("ema_cross", 0.2)]
    sells = [("macd", 0.8), ("rsi_revert", 0.6)]
    bt_map = {"ema_cross": FakeVerdict(), "macd": FakeVerdict(), "rsi_revert": FakeVerdict()}
    us = agg._compute_unified("BTC-USD", "BTC", 100.0, buys, sells, bt_map, -0.2)
    assert us.unified_score < 0
    assert us.conviction == pytest.approx(2 / 3)


def test_compute_unified_neutral():
    agg = SignalAggregator()
    buys = [("ema_cross", 0.5)]
    sells = [("macd", 0.5)]
    bt_map = {}
    us = agg._compute_unified("BTC-USD", "BTC", 100.0, buys, sells, bt_map, 0.0)
    assert us.conviction == 0.0
    # no verdicts -> top strategies get default q=0.1
    assert us.top_strategies == ["ema_cross", "macd"]


def test_compute_unified_top_strategies_ranked():
    agg = SignalAggregator()
    buys = [("weak", 0.1), ("strong", 0.9)]
    bt_map = {"weak": FakeVerdict(win_rate=0.9, sharpe_ratio=2.0),
              "strong": FakeVerdict(win_rate=0.5, sharpe_ratio=0.5)}
    us = agg._compute_unified("BTC-USD", "BTC", 100.0, buys, [], bt_map, 0.0)
    # strong has higher conf -> ranked first
    assert us.top_strategies[0] == "strong"


# ── top_n / print_report ───────────────────────────────────────────────

def test_top_n():
    agg = SignalAggregator()
    sigs = [UnifiedSignal(f"P{i}", "P", 1.0, 0.1 * i, 0, 0, 0, 0, 0, 0, 0) for i in range(5)]
    assert len(agg.top_n(sigs, 3)) == 3
    assert len(agg.top_n(sigs, 10)) == 5


def test_print_report_empty(capsys):
    SignalAggregator.print_report([])
    assert "No signals found." in capsys.readouterr().out


def test_print_report_nonempty(capsys):
    sigs = [UnifiedSignal("BTC-USD", "BTC", 100.0, 0.5, 0.3, 0.5, 0.1, 0.5, 1, 0, 1,
                          top_strategies=["a", "b"])]
    SignalAggregator.print_report(sigs, n=5)
    out = capsys.readouterr().out
    assert "BTC-USD" in out


def test_scan_universe_none_result(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust())
    agg = SignalAggregator()
    # Force a None result so the `if us is not None` False branch is exercised
    agg._evaluate_one = lambda *a, **k: None
    closes = make_closes(100)
    res = agg.scan_universe(
        [("BTC-USD", "BTC")], {"BTC-USD": closes}, {"BTC-USD": closes},
        {"BTC-USD": closes}, {"BTC-USD": closes}, min_candles=60,
    )
    assert res == []


def test_compute_trend_adx_none(monkeypatch):
    class AdxNoneRust(FakeRust):
        def run_strategy_py(self, name, closes, volumes, highs, lows):
            if name == "adx":
                return None
            return ("BUY", 0.5, "r")

    patch_backend(monkeypatch, rust=AdxNoneRust(sma_value=100.0))
    agg = SignalAggregator()
    t = agg._compute_trend(make_closes(250), make_closes(250, 1.0), make_closes(250), make_closes(250))
    assert -1.0 <= t <= 1.0


def test_compute_trend_zero_volumes(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(sma_value=100.0))
    agg = SignalAggregator()
    zeros = [0.0] * 250
    t = agg._compute_trend(make_closes(250), zeros, make_closes(250), make_closes(250))
    assert -1.0 <= t <= 1.0


def test_compute_trend_short_zero_sma(monkeypatch):
    patch_backend(monkeypatch, rust=FakeRust(sma_value=0.0))
    agg = SignalAggregator()
    assert agg._compute_trend_short(make_closes(100), 100) == 0.0
    assert agg._compute_trend_short(make_closes(20), 20) == 0.0


def test_compute_unified_low_trades(monkeypatch):
    agg = SignalAggregator()
    buys = [("weak", 0.5)]
    bt_map = {"weak": FakeVerdict(passed=True, total_trades=2, win_rate=0.9, sharpe_ratio=1.0)}
    us = agg._compute_unified("BTC-USD", "BTC", 100.0, buys, [], bt_map, 0.0)
    # low-trade verdict -> q stays at default 0.1, still ranked
    assert us.top_strategies == ["weak"]


def test_ensure_rust_rust_import_failure(monkeypatch):
    monkeypatch.setattr(sa, "_rust_core", None)
    monkeypatch.setattr(sa, "_HAS_RUST", False)
    monkeypatch.setattr(sa, "_batch_backtest_rust", None)
    monkeypatch.setitem(sys.modules, "rust_core", None)
    sa._ensure_rust()
    assert sa._HAS_RUST is False
    assert sa._rust_core is None


def test_ensure_rust_batch_import_failure(monkeypatch):
    monkeypatch.setattr(sa, "_rust_core", None)
    monkeypatch.setattr(sa, "_HAS_RUST", False)
    monkeypatch.setattr(sa, "_batch_backtest_rust", None)
    monkeypatch.setitem(sys.modules, "strategy_engine", None)
    sa._ensure_rust()
    # rust_core import succeeds, but strategy_engine import fails -> fallback
    assert sa._HAS_RUST is True
    assert sa._batch_backtest_rust is None
