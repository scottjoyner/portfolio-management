import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from strategies.adaptive_exec.session_decay_vwap import SessionDecayVwapStrategy
from strategies.adaptive_exec.orderflow_turbulence import OrderFlowTurbulenceStrategy
from strategies.adaptive_exec.inventory_skew import InventorySkewSignalStrategy
from trading_system.strategies.base.interfaces import StrategySignal


def test_metadata_mode_flags():
    for strat in (
        SessionDecayVwapStrategy(),
        OrderFlowTurbulenceStrategy(),
        InventorySkewSignalStrategy(),
    ):
        meta = strat.metadata()
        assert meta["strategy_type"] == "adaptive_exec"
        assert meta["live_supported"] is True
        assert meta["paper_mode"] is True
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert strat.supports_mode("live")
        assert strat.supports_mode("backtest")
    ids = {
        SessionDecayVwapStrategy().strategy_id,
        OrderFlowTurbulenceStrategy().strategy_id,
        InventorySkewSignalStrategy().strategy_id,
    }
    assert ids == {
        "SessionDecayVWAPDeviation",
        "OrderFlowTurbulenceAutocorr",
        "MarketMakerInventorySkew",
    }


def test_session_decay_vwap_emits_signal():
    s = SessionDecayVwapStrategy(window=48)
    # Flat history, then a sharp spike well above the decayed VWAP -> SELL.
    closes = [100.0] * 47 + [110.0]
    highs = [c + 0.5 for c in closes]
    lows = [c - 0.5 for c in closes]
    volumes = [1000.0] * 48
    sig = s.generate_signal(
        {
            "product_id": "BTC-USD",
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
            "warmup_complete": True,
        }
    )
    assert isinstance(sig, StrategySignal)
    assert sig.score < 0  # price rich -> mean-reversion SELL
    assert "decayed_vwap" in sig.features


def test_orderflow_turbulence_emits_on_persistent_flow():
    s = OrderFlowTurbulenceStrategy(window=30)
    # Clustered directional flow (long run of ups) -> high signed-volume
    # autocorrelation + net positive flow -> continuation BUY.
    deltas = [1.0] * 22 + [-1.0] * 8
    closes = [100.0]
    for d in deltas:
        closes.append(closes[-1] + d)
    volumes = [1000.0] * 30
    sig = s.generate_signal(
        {
            "product_id": "ETH-USD",
            "closes": closes,
            "volumes": volumes,
            "warmup_complete": True,
        }
    )
    assert isinstance(sig, StrategySignal)
    assert sig.score > 0
    assert sig.features["autocorr"] >= 0.1


def test_inventory_skew_long_inventory_sells():
    s = InventorySkewSignalStrategy(window=30, gamma=5.0)
    # Volatile-ish oscillating closes to create realised variance.
    closes = [100.0 + (2.0 if i % 2 == 0 else -2.0) for i in range(30)]
    sig = s.generate_signal(
        {
            "product_id": "BTC-USD",
            "closes": closes,
            "inventory": 0.9,  # long inventory
            "warmup_complete": True,
        }
    )
    assert isinstance(sig, StrategySignal)
    assert sig.score < 0  # long inventory -> expect downward unwind -> SELL


def test_returns_none_before_warmup():
    s = SessionDecayVwapStrategy(window=48)
    sig = s.generate_signal(
        {
            "product_id": "BTC-USD",
            "closes": [100.0] * 10,
            "highs": [100.5] * 10,
            "lows": [99.5] * 10,
            "volumes": [1000.0] * 10,
            "warmup_complete": True,
        }
    )
    assert sig is None


def test_cooldown_blocks_immediate_resignal():
    s = OrderFlowTurbulenceStrategy(window=30)
    deltas = [1.0] * 22 + [-1.0] * 8
    closes = [100.0]
    for d in deltas:
        closes.append(closes[-1] + d)
    state = {
        "product_id": "BTC-USD",
        "closes": closes,
        "volumes": [1000.0] * 30,
        "warmup_complete": True,
    }
    first = s.generate_signal(state)
    assert first is not None
    second = s.generate_signal(state)
    assert second is None
