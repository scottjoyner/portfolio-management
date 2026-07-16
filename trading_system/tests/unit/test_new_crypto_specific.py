"""Tests for the new crypto-specific microstructure strategies."""
from __future__ import annotations

import time

import pytest

from strategies.crypto.funding_contrarian_proxy import FundingRateContrarianProxyStrategy
from strategies.crypto.liquidation_cascade_proxy import LiquidationCascadeProxyStrategy
from strategies.crypto.ob_imbalance_extreme import OBImbalanceExtremeReversionStrategy


def _closes(n: int, start: float = 100.0, step: float = 0.0) -> list[float]:
    return [start + step * i for i in range(n)]


def _flat(n: int, v: float) -> list[float]:
    return [v] * n


def test_instantiate_and_metadata_flags():
    for cls in (
        FundingRateContrarianProxyStrategy,
        LiquidationCascadeProxyStrategy,
        OBImbalanceExtremeReversionStrategy,
    ):
        s = cls()
        meta = s.metadata()
        assert meta["strategy_id"] == s.strategy_id
        assert meta["strategy_type"] == "crypto_microstructure"
        assert meta["paper_mode"] is True
        assert meta["replay_supported"] is True
        assert meta["backtest_supported"] is True
        # none are live-supported (no live crypto feed here)
        assert meta["live_supported"] is False


def test_funding_proxy_returns_signal_when_crowded():
    s = FundingRateContrarianProxyStrategy()
    closes = _closes(21, start=100.0, step=2.0)  # big sustained up move
    volumes = _flat(18, 1.0) + [4.0, 4.0, 4.0]  # recent window spiked
    state = {
        "product_id": "BTC-USD",
        "closes": closes,
        "volumes": volumes,
        "warmup_complete": True,
    }
    sig = s.generate_signal(state)
    assert sig is not None
    assert sig.strategy_id == "FundingProxyContrarianStrategy"
    # Crowded long -> contrarian fade (negative score)
    assert sig.score < 0


def test_funding_proxy_none_when_fields_missing():
    s = FundingRateContrarianProxyStrategy()
    # Missing closes/volumes entirely
    assert s.generate_signal({"product_id": "BTC-USD"}) is None


def test_liq_cascade_returns_signal_on_exhaustion():
    s = LiquidationCascadeProxyStrategy()
    # 15 baseline bars dropping, single spike bar (long lower wick, high vol), 2 exhaustion bars
    closes = [40.0 - i for i in range(15)] + [25.0, 24.5, 24.8]
    highs = [c + 0.5 for c in closes]
    lows = [c - 2.0 for c in closes]  # long lower wicks, especially on spike bar
    volumes = _flat(15, 1.0) + [6.0] + _flat(2, 0.5)  # spike then exhaust
    state = {
        "product_id": "BTC-USD",
        "closes": closes,
        "highs": highs,
        "lows": lows,
        "volumes": volumes,
        "warmup_complete": True,
    }
    sig = s.generate_signal(state)
    assert sig is not None
    assert sig.strategy_id == "LiqCascadeProxyStrategy"
    assert sig.score > 0  # exhaustion of down cascade -> long


def test_liq_cascade_none_when_fields_missing():
    s = LiquidationCascadeProxyStrategy()
    assert s.generate_signal({"product_id": "BTC-USD"}) is None


def test_ob_imbalance_book_path():
    s = OBImbalanceExtremeReversionStrategy()
    state = {
        "product_id": "BTC-USD",
        "best_bid": 99.0,
        "best_ask": 101.0,
        "mid_price": 98.0,  # strongly skewed toward bid -> fade long
        "closes": [100.0],
        "highs": [101.0],
        "lows": [99.0],
        "warmup_complete": True,
    }
    sig = s.generate_signal(state)
    assert sig is not None
    assert sig.strategy_id == "OBImbalanceExtremeStrategy"
    assert sig.score > 0


def test_ob_imbalance_range_fallback():
    s = OBImbalanceExtremeReversionStrategy()
    # close at top of range -> fade short
    state = {
        "product_id": "BTC-USD",
        "closes": [100.0],
        "highs": [100.0],
        "lows": [95.0],
        "warmup_complete": True,
    }
    sig = s.generate_signal(state)
    assert sig is not None
    assert sig.score < 0


def test_ob_imbalance_none_when_no_data():
    s = OBImbalanceExtremeReversionStrategy()
    assert s.generate_signal({"product_id": "BTC-USD"}) is None


def test_cooldown_blocks_resignal():
    s = FundingRateContrarianProxyStrategy()
    s.config.cooldown_seconds = 5.0
    closes = _closes(21, start=100.0, step=2.0)
    volumes = _flat(18, 1.0) + [4.0, 4.0, 4.0]
    state = {
        "product_id": "BTC-USD",
        "closes": closes,
        "volumes": volumes,
        "warmup_complete": True,
    }
    first = s.generate_signal(state)
    assert first is not None
    # immediate repeat within cooldown must be blocked
    second = s.generate_signal(state)
    assert second is None
    # after cooldown elapses, signal returns
    time.sleep(5.1)
    third = s.generate_signal(state)
    assert third is not None
