"""Focused coverage for portfolio_optimizer helpers + bug #46 side guard.

Targets previously-untested module-level helpers and confidence/allocation
branches (per the coverage-gate remediation plan for portfolio_optimizer.py).
"""
from __future__ import annotations

from unittest import mock

import pytest

import portfolio_optimizer as P


# ----------------------------------------------------------- hermes meta-layer
def test_hermes_confirmation_neutral_no_edge():
    assert P.hermes_confirmation("BTC-USD") == {
        "confirmed": True, "mult": 1.0, "verdict": "no_hermes_data"}


def test_hermes_confirmation_bad_edge():
    assert P.hermes_confirmation("BTC-USD", "not-a-dict")["confirmed"] is True
    # unknown verdict or missing verdict both fall back to the neutral no-op.
    assert P.hermes_confirmation("BTC-USD", {"verdict": "unknown"})["verdict"] == "no_hermes_data"
    assert P.hermes_confirmation("BTC-USD", {"verdict": None})["verdict"] == "no_hermes_data"


def test_hermes_confirmation_branches():
    assert P.hermes_confirmation("BTC-USD", {"verdict": "x", "confidence": 0.0})["mult"] == 0.9
    r = P.hermes_confirmation("BTC-USD", {"verdict": "bot_wins_here", "confidence": 0.8})
    assert r["confirmed"] is True and r["mult"] == 1.15
    r = P.hermes_confirmation("BTC-USD", {"verdict": "bot_bleeds_here", "confidence": 0.8})
    assert r["confirmed"] is False and r["mult"] == 0.6
    assert P.hermes_confirmation("BTC-USD", {"verdict": "other", "confidence": 0.5})["mult"] == 1.0


def test_hermes_rank_boost_empty_and_no_fn():
    assert P.hermes_rank_boost([]) == []
    cands = [{"product_id": "BTC-USD", "priority": 0.5}]
    assert P.hermes_rank_boost(cands, edge_fn=None) == cands


def test_hermes_rank_boost_applies():
    cands = [
        {"product_id": "BTC-USD", "priority": 1.0},
        {"product_id": "ETH-USD", "priority": 1.0},
        {"product_id": "DOGE-USD", "priority": 1.0},
    ]

    def edge_fn(pid):
        return {
            "BTC-USD": {"verdict": "bot_wins_here", "confidence": 0.9},
            "ETH-USD": {"verdict": "bot_bleeds_here", "confidence": 0.9},
            "DOGE-USD": {"verdict": "no_data", "confidence": 0.0},
        }.get(pid)

    out = P.hermes_rank_boost(cands, edge_fn=edge_fn)
    pids = [c["product_id"] for c in out]
    # ETH dropped (not confirmed); BTC boosted.
    assert "ETH-USD" not in pids
    assert "BTC-USD" in pids and "DOGE-USD" in pids
    btc = next(c for c in out if c["product_id"] == "BTC-USD")
    assert btc["priority"] == 1.15


def test_hermes_rank_boost_missing_pid_and_error():
    cands = [{"currency": "SOL-USD", "priority": 2.0}, {"product_id": "BTC-USD", "priority": 1.0}]
    # edge_fn raises -> candidate kept (defensive except path)
    out = P.hermes_rank_boost(cands, edge_fn=mock.MagicMock(side_effect=RuntimeError("x")))
    assert len(out) == 2


# ----------------------------------------------------------- adx regime helper
def test_compute_adx_runs():
    highs = [i + 1.0 for i in range(30)]
    lows = [max(0.1, i - 1.0) for i in range(30)]
    closes = [float(i) for i in range(30)]
    val = P._compute_adx(highs, lows, closes, period=14)
    assert isinstance(val, float)


# ----------------------------------------------------------- bug #46 side guard
def test_validate_opportunity_side_guard():
    from coinbase.src.config import validate_opportunity_side
    # Valid sides normalize fine.
    assert validate_opportunity_side("BUY") == "BUY"
    assert validate_opportunity_side("sell") == "SELL"
    # Bug #46: "PAIR" must never pass the safety boundary.
    with pytest.raises(ValueError):
        validate_opportunity_side("PAIR")
    with pytest.raises(ValueError):
        validate_opportunity_side(None)
