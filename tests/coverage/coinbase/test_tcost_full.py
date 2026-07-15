"""Coverage tests for coinbase/src/tcost.py"""
from __future__ import annotations

import math

import pytest

from coinbase.src import tcost


def test_bps():
    assert tcost._bps(10000) == 1.0
    assert tcost._bps(0) == 0.0


def test_estimate_spread_python(monkeypatch):
    monkeypatch.setattr(tcost, "_HAS_RUST_TCOST", False)
    assert tcost.estimate_spread_bps(100.0, 102.0) == pytest.approx(20000.0 * 2.0 / 202.0)
    with pytest.raises(ValueError):
        tcost.estimate_spread_bps(0.0, 0.0)
    with pytest.raises(ValueError):
        tcost.estimate_spread_bps(102.0, 100.0)


def test_impact_python():
    assert tcost.impact_bps(0.0, 1.5) == 0.0
    assert tcost.impact_bps(10000.0, 1.5) == pytest.approx(1.5 * math.sqrt(1.0))


def test_effective_fill_python(monkeypatch):
    monkeypatch.setattr(tcost, "_HAS_RUST_TCOST", False)
    with pytest.raises(ValueError):
        tcost.effective_fill_price("buy", 0.0, 99.0, 101.0, 1000.0)
    buy = tcost.effective_fill_price("buy", 100.0, 99.0, 101.0, 1000.0)
    sell = tcost.effective_fill_price("sell", 100.0, 99.0, 101.0, 1000.0)
    assert buy > 100.0
    assert sell < 100.0


def test_estimate_spread_rust(monkeypatch):
    monkeypatch.setattr(tcost, "_HAS_RUST_TCOST", True)
    monkeypatch.setattr(tcost, "_rust_estimate_spread_bps", lambda b, a: 12.0)
    assert tcost.estimate_spread_bps(1.0, 2.0) == 12.0


def test_impact_rust(monkeypatch):
    monkeypatch.setattr(tcost, "_HAS_RUST_TCOST", True)
    monkeypatch.setattr(tcost, "_rust_impact_bps", lambda n, c: 5.0)
    assert tcost.impact_bps(100.0, 1.0) == 5.0


def test_effective_fill_rust(monkeypatch):
    monkeypatch.setattr(tcost, "_HAS_RUST_TCOST", True)
    monkeypatch.setattr(tcost, "_rust_effective_fill_price", lambda *a: 123.0)
    assert tcost.effective_fill_price("buy", 1.0, 1.0, 1.0, 1.0) == 123.0
