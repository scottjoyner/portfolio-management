from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO))

import portfolio_optimizer as po


def test_hermes_confirmation_noop_without_data():
    c = po.hermes_confirmation("BTC-USD", None)
    assert c["confirmed"] is True
    assert c["mult"] == 1.0
    assert c["verdict"] == "no_hermes_data"


def test_hermes_confirmation_unknown_asset_noop():
    c = po.hermes_confirmation("NOPE-USD", {"verdict": "unknown",
                                            "confidence": False})
    assert c["confirmed"] is True
    assert c["mult"] == 1.0


def test_hermes_confirmation_winner_boost():
    c = po.hermes_confirmation("BTC-USD", {"verdict": "bot_wins_here",
                                           "confidence": True, "edge": 5.0})
    assert c["confirmed"] is True
    assert c["mult"] == 1.15


def test_hermes_confirmation_loser_blocks():
    c = po.hermes_confirmation("ALT-USD", {"verdict": "bot_bleeds_here",
                                           "confidence": True, "edge": -3.0})
    assert c["confirmed"] is False
    assert c["mult"] == 0.6


def test_hermes_confirmation_unconfident_dampened():
    c = po.hermes_confirmation("MID-USD", {"verdict": "neutral",
                                           "confidence": False, "edge": 0.0})
    assert c["confirmed"] is True
    assert c["mult"] == 0.9


def test_hermes_confirmation_corrupt_data_noop():
    assert po.hermes_confirmation("X", {"weird": 1})["mult"] == 1.0


def test_hermes_rank_boost_empty():
    assert po.hermes_rank_boost([], edge_fn=lambda p: None) == []


def test_hermes_rank_boost_no_fn_passthrough():
    cands = [{"product_id": "BTC-USD", "priority": 1.0}]
    assert po.hermes_rank_boost(cands) == cands


def test_hermes_rank_boost_filters_and_sorts():
    cands = [
        {"product_id": "BTC-USD", "priority": 1.0},
        {"product_id": "ALT-USD", "priority": 5.0},
        {"product_id": "ETH-USD", "priority": 2.0},
    ]

    def edge_fn(pid):
        return {"verdict": "bot_wins_here", "confidence": True,
                "edge": 1.0} if pid == "BTC-USD" else \
               {"verdict": "bot_bleeds_here", "confidence": True,
                "edge": -1.0} if pid == "ALT-USD" else \
               {"verdict": "neutral", "confidence": False}

    out = po.hermes_rank_boost(cands, edge_fn=edge_fn)
    pids = [c["product_id"] for c in out]
    assert "ALT-USD" not in pids
    assert set(pids) == {"BTC-USD", "ETH-USD"}
    for c in out:
        if c["product_id"] == "BTC-USD":
            assert c["priority"] == pytest.approx(1.15)


def test_hermes_rank_boost_exception_safe():
    cands = [{"product_id": "BTC-USD", "priority": 1.0}]

    def edge_fn(pid):
        raise RuntimeError("boom")

    out = po.hermes_rank_boost(cands, edge_fn=edge_fn)
    assert len(out) == 1
    assert out[0]["product_id"] == "BTC-USD"
