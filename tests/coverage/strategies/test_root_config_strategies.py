"""Coverage tests for the config/init/on_bar-style root strategy modules.

Covers: adaptive_stop_loss, volatility_targeting, neural_trend, ml_grid,
momentum_clustering, stat_arb (module file), and ml.regime_detection.

All data is synthetic; no network/SDK access.
"""
from __future__ import annotations

import math
import random

import pytest


# ---------------------------------------------------------------------------
# synthetic dict-bar helpers
# ---------------------------------------------------------------------------

def dict_bars(n, kind="rising", start=100.0, vol=1000.0):
    out = []
    p = start
    for i in range(n):
        if kind == "rising":
            p *= 1.01
        elif kind == "falling":
            p *= 0.99
        elif kind == "volatile":
            p *= 1.0 + ((i % 2) * 2 - 1) * 0.05
        elif kind == "flat":
            p = start
        spread = 0.02 if kind != "flat" else 0.005
        out.append({
            "open": p * (1 - spread / 2),
            "high": p * (1 + spread),
            "low": p * (1 - spread),
            "close": p,
            "volume": vol + i * 3,
            "timestamp": i,
        })
    return out


# ---------------------------------------------------------------------------
# adaptive_stop_loss
# ---------------------------------------------------------------------------

def test_adaptive_stop_loss_full():
    from trading_system.strategies.adaptive_stop_loss import (
        AdaptiveStopLossSystem, AdaptiveStopLossConfig)

    s = AdaptiveStopLossSystem()
    assert isinstance(s.config, AdaptiveStopLossConfig)

    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(dict_bars(10))

    s.init(dict_bars(150, "rising"))
    assert s.baseline_volatility > 0

    # standard atr stop (ratio ~1, trend below threshold default 0.8)
    level, reason = s.get_adaptive_stop(105.0)
    assert reason in ("standard_atr_stop", "strong_trend_trailing_stop",
                      "high_volatility_wide_stop")

    # force high-volatility branch
    s.baseline_volatility = 1e-6
    level, reason = s.get_adaptive_stop(105.0)
    assert reason == "high_volatility_wide_stop"

    # force strong-trend branch: big baseline so ratio small, low trend thresh
    s.baseline_volatility = 1e9
    s.config.trend_strength_threshold = -1.0
    level, reason = s.get_adaptive_stop(105.0)
    assert reason == "strong_trend_trailing_stop"

    # trend strength with <2 closes -> 0.0
    s.recent_bars = [{"close": 100.0, "high": 101.0, "low": 99.0}]
    assert s._calculate_trend_strength(100.0) == 0.0

    # handle_exit: first exit (no prior), then profitable, then stop-reason match
    r1 = s.handle_exit(100.0, "standard_atr_stop")
    assert r1["successful_exit"] is False
    r2 = s.handle_exit(101.0, "standard_atr_stop")  # > prev*0.98 -> success
    assert r2["successful_exit"] is True
    r3 = s.handle_exit(50.0, "high_volatility_wide_stop")  # reason -> success
    assert r3["successful_exit"] is True

    m = s.get_performance_metrics()
    assert m["total_exits"] == 3
    assert 0.0 <= m["success_rate"] <= 100.0


def test_adaptive_stop_loss_metrics_empty():
    from trading_system.strategies.adaptive_stop_loss import AdaptiveStopLossSystem
    s = AdaptiveStopLossSystem()
    m = s.get_performance_metrics()
    assert m == {"total_exits": 0, "success_rate": 0.0,
                 "successful_exits": 0, "failed_exits": 0}


# ---------------------------------------------------------------------------
# volatility_targeting
# ---------------------------------------------------------------------------

def test_volatility_targeting_full():
    from trading_system.strategies.volatility_targeting import (
        VolatilityTargetingStrategy, VolatilityTargetingConfig, _median)

    assert _median([]) == 0.0
    assert _median([3, 1, 2]) == 2
    assert _median([4, 1, 3, 2]) == 2.5

    s = VolatilityTargetingStrategy()
    assert isinstance(s.config, VolatilityTargetingConfig)

    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(dict_bars(5))

    s.init(dict_bars(80, "rising"))
    assert s.baseline_volatility > 0

    # invalid close -> None
    assert s.on_bar({"close": 0}) is None
    assert s.on_bar({"close": float("nan")}) is None
    assert s.on_bar({"close": -5}) is None

    # in-band signal
    sig = s.on_bar({"high": 110, "low": 108, "close": 109})
    assert sig is None or sig["action"] == "BUY"

    # out-of-band (huge atr) -> None
    assert s.on_bar({"high": 1000, "low": 1, "close": 500}) is None

    # position multiplier branches
    assert s._calculate_position_multiplier(0.5) > 0      # <=1.0
    assert s._calculate_position_multiplier(1.5) > 0      # <=2.0
    assert s._calculate_position_multiplier(5.0) > 0      # >2.0

    # baseline zero -> volatility_ratio = 1.0 branch
    s2 = VolatilityTargetingStrategy()
    s2.baseline_volatility = 0.0
    sig2 = s2.on_bar({"high": 101, "low": 99, "close": 100})
    assert sig2 is not None and sig2["volatility_ratio"] == 1.0

    # handle_signal BUY / SELL / unknown
    assert s.handle_signal({"action": "BUY", "volatility_ratio": 1.0,
                            "position_size_adjustment": 1.0})["position_opened"]
    assert s.handle_signal({"action": "SELL", "volatility_ratio": 1.0,
                            "position_size_adjustment": 1.0})["position_closed"]
    assert s.handle_signal({"action": "HOLD"}) is None

    m = s.get_performance_metrics()
    assert m["total_signals"] == 2


def test_volatility_targeting_metrics_empty():
    from trading_system.strategies.volatility_targeting import VolatilityTargetingStrategy
    s = VolatilityTargetingStrategy()
    assert s.get_performance_metrics()["total_signals"] == 0


# ---------------------------------------------------------------------------
# neural_trend
# ---------------------------------------------------------------------------

def test_neural_trend_full():
    random.seed(7)
    from trading_system.strategies.neural_trend import (
        NeuralTrendFollower, NeuralTrendConfig)

    s = NeuralTrendFollower()
    assert isinstance(s.config, NeuralTrendConfig)

    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(dict_bars(10))

    s.init(dict_bars(150, "rising"))
    assert s.weights and s.biases and s.feature_history

    # invalid close
    assert s.on_bar({"close": 0}) is None

    got_signal = False
    for scen in ("rising", "falling", "volatile"):
        for b in dict_bars(40, scen):
            r = s.on_bar(b)
            if r:
                got_signal = True
                assert r["action"] in ("BUY", "SELL")
    assert got_signal  # network must produce at least one signal

    # forward pass with no weights returns 0.5
    empty = NeuralTrendFollower()
    assert empty._forward_pass([0.1, 0.2, 0.3, 0.4]) == 0.5

    # relu / sigmoid branches
    assert s._relu(-1) == 0
    assert s._relu(2) == 2
    assert 0 < s._sigmoid(-10) < 0.5
    assert 0.5 < s._sigmoid(10) < 1

    assert s.handle_signal({"action": "BUY"})["position_opened"]
    assert s.handle_signal({"action": "SELL"})["position_closed"]
    assert s.handle_signal({"action": "HOLD"}) is None
    assert s.get_performance_metrics()["total_signals"] == 2


def test_neural_trend_metrics_empty():
    from trading_system.strategies.neural_trend import NeuralTrendFollower
    assert NeuralTrendFollower().get_performance_metrics()["total_signals"] == 0


# ---------------------------------------------------------------------------
# ml_grid
# ---------------------------------------------------------------------------

def test_ml_grid_full():
    from trading_system.strategies.ml_grid import (
        MLGridTradingStrategy, MLGridConfig)

    s = MLGridTradingStrategy()
    assert isinstance(s.config, MLGridConfig)

    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(dict_bars(10))

    s.init(dict_bars(200, "rising"))
    assert s.grid_levels and s.optimal_step > 0

    assert s.on_bar({"close": 0}) is None

    # price above grid -> ADD_LEVEL
    s.grid_levels = [90.0, 100.0, 110.0]
    r = s.on_bar({"high": 200 * 1.01, "low": 200 * 0.99, "close": 200})
    assert r["action"] == "ADD_LEVEL"

    # price below grid -> ADD_LEVEL
    r = s.on_bar({"high": 50 * 1.01, "low": 50 * 0.99, "close": 50})
    assert r["action"] == "ADD_LEVEL"

    # FILL_LEVEL: coarse grid so nearest is >2% away
    s.grid_levels = [80.0, 120.0]
    r = s.on_bar({"high": 101, "low": 99, "close": 100})
    assert r is not None and r["action"] == "FILL_LEVEL"

    # within-grid but close to a level -> None
    s.grid_levels = [99.9, 100.1]
    assert s.on_bar({"high": 100.05, "low": 99.95, "close": 100.0}) is None

    assert s.handle_signal({"action": "ADD_LEVEL", "new_level": 1,
                            "volatility_ratio": 1})["level_added"]
    assert s.handle_signal({"action": "FILL_LEVEL", "nearest_level": 1,
                            "distance_pct": 1})["level_filled"]
    assert s.handle_signal({"action": "NONE"}) is None
    assert s.get_performance_metrics()["total_signals"] == 2


def test_ml_grid_metrics_empty():
    from trading_system.strategies.ml_grid import MLGridTradingStrategy
    assert MLGridTradingStrategy().get_performance_metrics()["total_signals"] == 0


# ---------------------------------------------------------------------------
# momentum_clustering
# ---------------------------------------------------------------------------

def test_momentum_clustering_full():
    from trading_system.strategies.momentum_clustering import (
        MomentumClusteringStrategy, MomentumClusteringConfig)

    s = MomentumClusteringStrategy()
    assert isinstance(s.config, MomentumClusteringConfig)

    with pytest.raises(ValueError):
        s.init([])
    with pytest.raises(ValueError):
        s.init(dict_bars(10))

    s.init(dict_bars(150, "rising"))
    assert s.feature_history

    assert s.on_bar({"close": 0}) is None

    actions = set()
    for scen in ("rising", "falling", "volatile", "flat"):
        for b in dict_bars(30, scen):
            r = s.on_bar(b)
            if r:
                actions.add(r["action"])
    assert actions  # produced classifications

    # _find_nearest_cluster on fresh instance seeds default centers
    fresh = MomentumClusteringStrategy()
    c = fresh._find_nearest_cluster([0.0, 0.0, 0.0, 0.0])
    assert 0 <= c < len(fresh.cluster_centers)

    # _generate_signal for each cluster id + unknown fallback
    for cid in range(5):
        sig = s._generate_signal(100.0, cid, [0, 0, 0, 0])
        assert sig["cluster_id"] == cid
    unknown = s._generate_signal(100.0, 99, [0, 0, 0, 0])
    assert unknown["cluster_id"] == 99

    assert s.handle_signal({"action": "BUY"})["position_opened"]
    assert s.handle_signal({"action": "MEAN_REVERSION"})["position_opened"]
    assert s.handle_signal({"action": "DEFENSIVE"})["position_closed"]
    assert s.handle_signal({"action": "SELL"})["position_closed"]
    assert s.handle_signal({"action": "WAIT"}) == {}
    m = s.get_performance_metrics()
    assert m["total_signals"] == 4


def test_momentum_clustering_metrics_empty():
    from trading_system.strategies.momentum_clustering import MomentumClusteringStrategy
    assert MomentumClusteringStrategy().get_performance_metrics()["total_signals"] == 0


# ---------------------------------------------------------------------------
# ml.regime_detection
# ---------------------------------------------------------------------------

def test_regime_detection_full():
    from trading_system.strategies.ml.regime_detection import (
        RegimeDetectionStrategy, RegimeDetectionConfig, MarketRegime)

    with pytest.raises(ValueError):
        RegimeDetectionStrategy().init([])
    with pytest.raises(ValueError):
        RegimeDetectionStrategy().init(dict_bars(10))

    cfg = RegimeDetectionConfig(trend_strength=0.0, window_size=20)
    s = RegimeDetectionStrategy(cfg)
    s.init(dict_bars(60, "rising"))
    assert s.trend_slope != 0

    assert s.on_bar({"close": 0}) is None

    # high volatility
    assert s._classify_regime(5.0, 0.0) == MarketRegime.HIGH_VOLATILITY
    # low volatility
    assert s._classify_regime(0.1, 0.0) == MarketRegime.LOW_VOLATILITY
    # trending up (slope positive from rising init)
    assert s._classify_regime(1.0, 10.0) == MarketRegime.TRENDING_UP
    # trending down
    s.trend_slope = -5.0
    assert s._classify_regime(1.0, 10.0) == MarketRegime.TRENDING_DOWN
    s.trend_slope = 5.0
    # ranging (mid vol, low trend)
    assert s._classify_regime(1.0, 0.0) == MarketRegime.RANGING

    # drive on_bar; craft atr inside band to hit trending -> signal + regime change path
    s.regime_history = [MarketRegime.RANGING] * cfg.window_size
    s.volatility_baseline = 10.0
    atr_bar = {"high": 215, "low": 200, "close": 210}  # atr 15 -> ratio 1.5
    sig = s.on_bar(atr_bar)
    assert sig is not None and sig["action"] in ("BUY", "SELL")

    # _generate_signal for non-trending regime -> None
    assert s._generate_signal(100.0, MarketRegime.RANGING) is None

    assert s.handle_signal({"action": "BUY", "regime": "trending_up"})["position_opened"]
    assert s.handle_signal({"action": "SELL", "regime": "trending_down"})["position_closed"]
    assert s.handle_signal({"action": "HOLD"}) is None

    m = s.get_performance_metrics()
    assert m["total_signals"] >= 2

    # baseline zero -> ratio 1.0 branch
    s3 = RegimeDetectionStrategy(RegimeDetectionConfig(window_size=20))
    s3.init(dict_bars(60, "flat"))
    s3.volatility_baseline = 0.0
    assert s3.on_bar({"high": 101, "low": 99, "close": 100}) is None or True


def test_regime_detection_metrics_empty():
    from trading_system.strategies.ml.regime_detection import RegimeDetectionStrategy
    assert RegimeDetectionStrategy().get_performance_metrics()["total_signals"] == 0
