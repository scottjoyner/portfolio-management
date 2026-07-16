import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from strategies.microstructure.trade_flow_imbalance import TradeFlowImbalanceStrategy
from strategies.microstructure.spread_compression import SpreadCompressionStrategy
from strategies.microstructure.cvd_exhaustion import CvdExhaustionStrategy


def test_metadata_mode_flags():
    for strat in (
        TradeFlowImbalanceStrategy(),
        SpreadCompressionStrategy(),
        CvdExhaustionStrategy(),
    ):
        meta = strat.metadata()
        assert meta["strategy_type"] == "microstructure"
        assert meta["live_supported"] is True
        assert meta["paper_mode"] is True
        assert meta["backtest_supported"] is True
        assert meta["replay_supported"] is True
        assert strat.supports_mode("live")
        assert strat.supports_mode("backtest")


def test_generate_signal_emits_on_order_flow():
    s = TradeFlowImbalanceStrategy()
    sig = s.generate_signal(
        {
            "product_id": "BTC-USD",
            "buy_volume": 900.0,
            "sell_volume": 100.0,
            "warmup_complete": True,
        }
    )
    assert sig is not None
    assert sig.strategy_id == "TradeFlowImbalanceStrategy"
    assert sig.score > 0.3
    assert sig.product_id == "BTC-USD"
    assert "imbalance" in sig.features


def test_spread_compression_signal():
    s = SpreadCompressionStrategy()
    sig = s.generate_signal(
        {
            "product_id": "ETH-USD",
            "spread_bps": 1.0,
            "baseline_spread_bps": 10.0,
            "book_pressure": 0.8,
            "warmup_complete": True,
        }
    )
    assert sig is not None
    assert sig.score > 0.3


def test_cvd_exhaustion_signal():
    s = CvdExhaustionStrategy()
    sig = s.generate_signal(
        {
            "product_id": "BTC-USD",
            "cumulative_delta": 800.0,
            "delta_scale": 1000.0,
            "warmup_complete": True,
        }
    )
    assert sig is not None
    # Exhaustion fades the extreme: positive CVD -> negative (SELL) score.
    assert sig.score < 0


def test_cooldown_blocks_immediate_resignal():
    s = TradeFlowImbalanceStrategy()
    state = {
        "product_id": "BTC-USD",
        "buy_volume": 900.0,
        "sell_volume": 100.0,
        "warmup_complete": True,
    }
    first = s.generate_signal(state)
    assert first is not None
    # Immediate second call must be blocked by cooldown.
    second = s.generate_signal(state)
    assert second is None
