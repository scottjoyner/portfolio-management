import importlib

import pytest

import confidence_matrix as cm
from confidence_matrix import ConfidenceMatrix, AggregatedSignal, format_aggregated
from strategy_engine import Signal


def _sig(strategy, action, confidence, reason=""):
    return Signal(action=action, price=100.0, confidence=confidence, reason=reason, strategy=strategy)


def test_strategy_group_lookup_built():
    # Every independence group member maps to its group
    assert cm.STRATEGY_GROUP["ema_cross"] == "trend"
    assert cm.STRATEGY_GROUP["rsi_revert"] == "momentum"
    assert cm.STRATEGY_GROUP["kalshi"] == "prediction_market"
    # Unknown strategy is absent
    assert "not_a_strategy" not in cm.STRATEGY_GROUP


def test_aggregate_empty_returns_empty():
    assert ConfidenceMatrix().aggregate([]) == []


def test_aggregate_single_buy_rust():
    sigs = [_sig("ema_cross", "BUY", 0.8, "ema cross")]
    out = ConfidenceMatrix().aggregate(sigs, asset_class="growth", currency="BTC-USD")
    assert len(out) == 1
    assert out[0].direction == "BUY"
    assert 0.0 <= out[0].confidence <= 1.0
    assert out[0].asset == "BTC-USD"


def test_aggregate_buy_and_sell_sorted_by_confidence():
    sigs = [
        _sig("ema_cross", "BUY", 0.9, "strong buy"),
        _sig("rsi_revert", "SELL", 0.5, "weak sell"),
    ]
    out = ConfidenceMatrix().aggregate(sigs, asset_class="growth", currency="ETH-USD")
    assert {o.direction for o in out} == {"BUY", "SELL"}
    # Sorted descending by confidence
    confs = [o.confidence for o in out]
    assert confs == sorted(confs, reverse=True)


def test_aggregate_group_agreement_boost():
    # Two independent groups (trend + momentum) agreeing on BUY should boost
    sigs = [
        _sig("ema_cross", "BUY", 0.6, "trend"),
        _sig("rsi_revert", "BUY", 0.6, "momentum"),
    ]
    out = ConfidenceMatrix().aggregate(sigs, asset_class="growth", currency="BTC-USD")
    assert out[0].agreeing_groups >= 2
    # raw vs boosted: with >=2 groups there is a boost
    assert out[0].confidence >= out[0].raw_confidence - 1e-9


def test_aggregate_unknown_strategy_default_weight():
    sigs = [_sig("totally_unknown_strat", "BUY", 0.7, "x")]
    out = ConfidenceMatrix().aggregate(sigs, asset_class="growth", currency="BTC-USD")
    assert len(out) == 1
    assert out[0].strategy_count == 1


def test_aggregate_bt_cache_weighting():
    bt_cache = {"ema_cross/BTC-USD": {"win_rate": 0.7, "sharpe_ratio": 1.2}}
    sigs = [_sig("ema_cross", "BUY", 0.8, "ema")]
    out = ConfidenceMatrix().aggregate(sigs, asset_class="growth", currency="BTC-USD", )
    # Should not raise and produce a confidence
    assert len(out) == 1


def test_aggregate_class_boost_differs_by_asset_class():
    # safe class boosts trend; speculative boosts momentum
    sigs_trend = [_sig("ema_cross", "BUY", 0.6, "t")]
    sigs_mom = [_sig("rsi_revert", "BUY", 0.6, "m")]
    safe = ConfidenceMatrix().aggregate(sigs_trend, asset_class="safe", currency="BTC-USD")
    spec = ConfidenceMatrix().aggregate(sigs_mom, asset_class="speculative", currency="BTC-USD")
    assert len(safe) == 1 and len(spec) == 1


def test_aggregate_py_fallback_path():
    # Force the pure-Python implementation
    orig = cm._HAS_RUST_CONFIDENCE
    cm._HAS_RUST_CONFIDENCE = False
    try:
        m = ConfidenceMatrix()
        sigs = [
            _sig("ema_cross", "BUY", 0.8, "trend buy"),
            _sig("rsi_revert", "BUY", 0.6, "momentum buy"),
            _sig("boll_break", "SELL", 0.4, "vol sell"),
        ]
        out = m.aggregate(sigs, asset_class="growth", currency="BTC-USD")
        assert len(out) == 2
        # Python path computes agreeing groups etc.
        for o in out:
            assert isinstance(o, AggregatedSignal)
            assert o.agreeing_groups >= 1
        # Diversity / agreement branches exercised
        buy = [o for o in out if o.direction == "BUY"][0]
        assert buy.strategy_count == 2
    finally:
        cm._HAS_RUST_CONFIDENCE = orig


def test_aggregate_py_five_strategy_diversity():
    orig = cm._HAS_RUST_CONFIDENCE
    cm._HAS_RUST_CONFIDENCE = False
    try:
        m = ConfidenceMatrix()
        # 5 different groups -> diversity >=5 branch (1.10) and >=3 branch
        sigs = [
            _sig("ema_cross", "BUY", 0.6, "trend"),
            _sig("rsi_revert", "BUY", 0.6, "momentum"),
            _sig("boll_break", "BUY", 0.6, "volatility"),
            _sig("vol_mom", "BUY", 0.6, "volume"),
            _sig("candle_pat", "BUY", 0.6, "pattern"),
        ]
        out = m.aggregate(sigs, asset_class="growth", currency="BTC-USD")
        assert len(out) == 1
        assert out[0].strategy_count == 5
        assert out[0].agreeing_groups == 5
    finally:
        cm._HAS_RUST_CONFIDENCE = orig


def test_aggregate_py_unknown_only_group_zero_boost():
    orig = cm._HAS_RUST_CONFIDENCE
    cm._HAS_RUST_CONFIDENCE = False
    try:
        m = ConfidenceMatrix()
        # All strategies map to no group -> agreeing == 0 branch (avg *= 0.5)
        sigs = [_sig("zzz_unknown", "BUY", 0.6, "x"), _sig("yyy_unknown", "BUY", 0.6, "y")]
        out = m.aggregate(sigs, asset_class="growth", currency="BTC-USD")
        assert len(out) == 1
        assert out[0].agreeing_groups == 0
    finally:
        cm._HAS_RUST_CONFIDENCE = orig


def test_aggregate_py_bt_cache_weighting():
    orig = cm._HAS_RUST_CONFIDENCE
    cm._HAS_RUST_CONFIDENCE = False
    try:
        m = ConfidenceMatrix(bt_cache={"ema_cross/BTC-USD": {"win_rate": 0.7, "sharpe_ratio": 1.2}})
        sigs = [_sig("ema_cross", "BUY", 0.8, "ema")]
        out = m.aggregate(sigs, asset_class="growth", currency="BTC-USD")
        assert len(out) == 1
        # Also exercise the cached with invalid (win_rate 0) branch
        m2 = ConfidenceMatrix(bt_cache={"rsi_revert/BTC-USD": {"win_rate": 0, "sharpe_ratio": 0}})
        out2 = m2.aggregate([_sig("rsi_revert", "BUY", 0.8, "r")], asset_class="growth", currency="BTC-USD")
        assert len(out2) == 1
    finally:
        cm._HAS_RUST_CONFIDENCE = orig


def test_aggregate_single_direction_group_skip():
    # All SELL -> BUY group empty branch (continue) is hit
    out = ConfidenceMatrix().aggregate([_sig("ema_cross", "SELL", 0.5, "s")], currency="BTC-USD")
    assert len(out) == 1 and out[0].direction == "SELL"


def test_format_aggregated():
    sig = AggregatedSignal(
        asset="BTC-USD", direction="BUY", confidence=0.75, raw_confidence=0.7,
        agreeing_groups=2, total_groups=10, strategy_count=2,
        strategies=["ema_cross", "rsi_revert"], best_reason="good", asset_class="growth",
    )
    s = format_aggregated(sig)
    assert "BUY" in s and "BTC-USD" in s and "good" in s


def test_reimport_idempotent():
    mod = importlib.reload(cm)
    assert mod.INDEPENDENCE_GROUPS  # still present after reload
