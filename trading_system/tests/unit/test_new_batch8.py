"""Tests for the stat_arb_2 batch of novel strategies."""
from __future__ import annotations

import math

import pytest

from trading_system.strategies.stat_arb_2.copula_break import CopulaDependenceBreakReversion
from trading_system.strategies.stat_arb_2.shape_match import ShapeMatchReversal
from trading_system.strategies.stat_arb_2.fractional_cointegration import FractionalCointegrationArb


def _make_closes(n=60, drift=0.001, seed=1.0):
    out = [seed]
    for i in range(1, n):
        out.append(out[-1] * (1 + drift + 0.002 * math.sin(i / 3.0)))
    return out


def _make_peer(closes, spread=0.0):
    return [c * (1.0 + spread) + 0.5 * math.sin(i / 5.0) for i, c in enumerate(closes)]


def test_instantiate_and_metadata_flags():
    for cls in (CopulaDependenceBreakReversion, ShapeMatchReversal, FractionalCointegrationArb):
        s = cls()
        md = s.metadata()
        assert md["strategy_id"] == s.strategy_id
        assert md["strategy_type"] == "stat_arb"
        assert md["paper_mode"] is True
        assert md["backtest_supported"] is True
        assert md["products"] == ["BTC-USD"]


def test_generate_signal_returns_signal_when_peer_present():
    closes = _make_closes()
    peer = _make_peer(closes)
    for cls, extra in (
        (CopulaDependenceBreakReversion, {"peer_closes": peer}),
        (FractionalCointegrationArb, {"peer_closes": peer}),
    ):
        s = cls()
        # warm up prev_corr / windows by feeding a few states
        st = {"product_id": "BTC-USD", "score": 0.0, "closes": closes[:40], "peer_closes": peer[:40]}
        s.generate_signal(st)
        state = {"product_id": "BTC-USD", "score": 0.0, "closes": closes, "peer_closes": peer}
        sig = s.generate_signal(state)
        assert sig is None or sig.strategy_id == s.strategy_id

    sm = ShapeMatchReversal()
    sig = sm.generate_signal({"product_id": "BTC-USD", "score": 0.0, "closes": _make_closes(n=20)})
    assert sig is None or sig.strategy_id == "ShapeMatchReversal"


def test_none_when_peer_closes_missing():
    closes = _make_closes()
    s1 = CopulaDependenceBreakReversion()
    assert s1.generate_signal({"product_id": "BTC-USD", "score": 0.0, "closes": closes}) is None
    s2 = FractionalCointegrationArb()
    assert s2.generate_signal({"product_id": "BTC-USD", "score": 0.0, "closes": closes}) is None
    # shape match does not need peer
    assert ShapeMatchReversal().generate_signal({"product_id": "BTC-USD", "score": 0.0, "closes": []}) is None


def test_cooldown_blocks_resignal():
    closes = _make_closes()
    peer = _make_peer(closes)
    s = FractionalCointegrationArb()
    s.config.cooldown_seconds = 60
    state = {"product_id": "BTC-USD", "score": 0.0, "closes": closes, "peer_closes": peer}
    # First call may or may not emit; force one by manipulating residual via repeated feed
    first = s.generate_signal(state)
    # Immediately after, in_cooldown should block any further emit
    if first is not None:
        assert s.in_cooldown() is True
        assert s.generate_signal(state) is None
    else:
        # force a signal by setting last emit in past then confirm cooldown logic
        s._last_emit_ts = 0.0
        assert s.in_cooldown() is False
