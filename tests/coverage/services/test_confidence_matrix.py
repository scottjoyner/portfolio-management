"""Coverage tests for confidence_matrix (Rust + pure-Python aggregation paths)."""

from unittest.mock import MagicMock

import pytest

import confidence_matrix as cm
from confidence_matrix import ConfidenceMatrix, AggregatedSignal
from strategy_engine import Signal as StrategySignal


def _sig(strategy, action, confidence, reason=""):
    return StrategySignal(action=action, price=1.0, confidence=confidence,
                          reason=reason, strategy=strategy)


def test_aggregate_empty():
    m = ConfidenceMatrix()
    assert m.aggregate([]) == []


def test_aggregate_rust_path():
    # default: rust available
    assert cm._HAS_RUST_CONFIDENCE is True
    m = ConfidenceMatrix()
    sigs = [
        _sig("ema_cross", "BUY", 0.8, "trend up"),
        _sig("rsi_revert", "BUY", 0.6, "momentum"),
        _sig("boll_break", "SELL", 0.7, "vol"),
    ]
    out = m.aggregate(sigs, asset_class="growth", currency="BTC-USD")
    assert isinstance(out, list)
    assert all(isinstance(o, AggregatedSignal) for o in out)
    # BUY should aggregate two groups
    buy = [o for o in out if o.direction == "BUY"]
    assert buy and buy[0].agreeing_groups >= 2


def test_aggregate_rust_with_bt_cache():
    m = ConfidenceMatrix(bt_cache={"ema_cross/BTC-USD": {"win_rate": 0.6, "sharpe_ratio": 1.0}})
    sigs = [_sig("ema_cross", "BUY", 0.8), _sig("ema_cross", "BUY", 0.5)]
    out = m.aggregate(sigs, currency="BTC-USD")
    assert out


def test_aggregate_rust_bt_cache_non_dict_and_cached_none():
    m = ConfidenceMatrix(bt_cache={"ema_cross/BTC-USD": "notadict", "rsi_revert/BTC-USD": None})
    sigs = [_sig("ema_cross", "BUY", 0.8), _sig("rsi_revert", "BUY", 0.6),
            _sig("boll_break", "BUY", 0.7)]
    out = m.aggregate(sigs, currency="BTC-USD")
    assert out


def test_aggregate_rust_bt_cache_dict_zero_perf():
    # cached dict present but win_rate/sharpe == 0 -> falls to default weight
    m = ConfidenceMatrix(bt_cache={"ema_cross/BTC-USD": {"win_rate": 0, "sharpe_ratio": 0}})
    sigs = [_sig("ema_cross", "BUY", 0.8), _sig("boll_break", "BUY", 0.7)]
    out = m.aggregate(sigs, currency="BTC-USD")
    assert out


def test_aggregate_py_path_forced():
    cm._HAS_RUST_CONFIDENCE = False
    try:
        m = ConfidenceMatrix()
        sigs = [
            _sig("ema_cross", "BUY", 0.8, "trend"),
            _sig("rsi_revert", "BUY", 0.6, "momentum"),
            _sig("boll_break", "BUY", 0.7, "vol"),
            _sig("vol_mom", "BUY", 0.5, "volume"),
            _sig("candle_pat", "BUY", 0.5, "pattern"),
            _sig("kalshi", "BUY", 0.5, "pm"),
        ]
        out = m.aggregate(sigs, asset_class="speculative", currency="SOL-USD")
        assert out and out[0].direction == "BUY"
        assert out[0].agreeing_groups >= 5
        # diversity bonus for >=5 unique
        assert out[0].confidence >= 0.0
    finally:
        cm._HAS_RUST_CONFIDENCE = True


def test_aggregate_py_single_group_and_zero_groups():
    cm._HAS_RUST_CONFIDENCE = False
    try:
        m = ConfidenceMatrix()
        # only one group -> agreeing>=2 false, agreeing==0 false (it's 1)
        out = m.aggregate([_sig("ema_cross", "BUY", 0.8)], asset_class="growth")
        assert out[0].agreeing_groups == 1
        # unknown strategy -> no group -> agreeing==0
        out2 = m.aggregate([_sig("nonexistent_strat", "BUY", 0.8)], asset_class="growth")
        assert out2 and out2[0].agreeing_groups == 0
        # SELL direction path
        out3 = m.aggregate([_sig("ema_cross", "SELL", 0.8)], asset_class="growth")
        assert out3[0].direction == "SELL"
    finally:
        cm._HAS_RUST_CONFIDENCE = True


def test_strategy_weight_and_class_boost():
    m = ConfidenceMatrix(bt_cache={"ema_cross/BTC-USD": {"win_rate": 0.7, "sharpe_ratio": 2.0}})
    w = m._strategy_weight("ema_cross", "BTC-USD")
    assert 0.0 <= w <= 1.0
    # default path (unknown strategy not in defaults)
    assert m._strategy_weight("unknown_strat_xyz", "BTC-USD") == 0.5
    # cached dict but wr/sh=0 -> falls back to default weights table (unknown strat -> 0.5)
    m2 = ConfidenceMatrix(bt_cache={"unknown_strat_xyz/BTC-USD": {"win_rate": 0, "sharpe_ratio": 0}})
    assert m2._strategy_weight("unknown_strat_xyz", "BTC-USD") == 0.5
    boost = m._class_boost("ema_cross", "speculative")
    assert boost == cm.CLASS_BOOST["speculative"]["trend"]
    # unknown class -> growth default
    assert m._class_boost("ema_cross", "weird") == cm.CLASS_BOOST["growth"]["trend"]
    # unknown strategy -> momentum default group
    assert m._class_boost("zzz", "growth") == cm.CLASS_BOOST["growth"]["momentum"]


def test_format_aggregated():
    sig = AggregatedSignal(
        asset="BTC-USD", direction="BUY", confidence=0.9, raw_confidence=0.7,
        agreeing_groups=2, total_groups=10, strategy_count=2,
        strategies=["ema_cross", "rsi_revert"], best_reason="trend up",
        asset_class="growth",
    )
    assert "BUY" in cm.format_aggregated(sig)
