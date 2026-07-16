from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO))

import scripts.hermes_agent_trader as trader
import scripts.hermes_meta as meta


def _ledger_tmp(tmp_path: Path, trades):
    led = {"positions": {}, "trades": trades, "realized_pnl": 0.0,
           "created_at": "2026-01-01T00:00:00+00:00"}
    p = tmp_path / "hermes_agent_ledger.json"
    p.write_text(json.dumps(led))
    return p


def test_load_empty_ledger(tmp_path, monkeypatch):
    p = tmp_path / "hermes_agent_ledger.json"
    monkeypatch.setattr(trader, "LEDGER", p)
    led = trader.load_ledger()
    assert led["trades"] == []
    assert led["positions"] == {}
    assert "created_at" in led


def test_save_then_load_roundtrip(tmp_path, monkeypatch):
    p = tmp_path / "hermes_agent_ledger.json"
    monkeypatch.setattr(trader, "LEDGER", p)
    led = {"positions": {"BTC-USD": {"base": 0.5, "cost_basis": 10.0, "entries": 1}},
           "trades": [{"product_id": "BTC-USD", "side": "BUY", "realized_pnl": 1.0}],
           "realized_pnl": 1.0, "created_at": "x"}
    trader.save_ledger(led)
    assert p.exists()
    back = trader.load_ledger()
    assert back["positions"] == led["positions"]
    assert back["trades"] == led["trades"]
    assert back["realized_pnl"] == 1.0


def test_recorded_trade_persists(tmp_path, monkeypatch):
    p = tmp_path / "hermes_agent_ledger.json"
    monkeypatch.setattr(trader, "LEDGER", p)
    before = trader.load_ledger()
    assert before["trades"] == []
    before["trades"].append({"product_id": "ETH-USD", "side": "BUY", "realized_pnl": 0.0})
    trader.save_ledger(before)
    after = trader.load_ledger()
    assert len(after["trades"]) == 1
    assert after["trades"][0]["product_id"] == "ETH-USD"


def test_recent_stats_empty():
    assert trader.recent_stats({"trades": []}, n=5)["n"] == 0


def test_recent_stats_window(tmp_path, monkeypatch):
    p = _ledger_tmp(tmp_path, [
        {"product_id": "BTC-USD", "side": "BUY", "realized_pnl": 1.0},
        {"product_id": "BTC-USD", "side": "SELL", "realized_pnl": -0.5},
        {"product_id": "BTC-USD", "side": "SELL", "realized_pnl": 2.0},
    ])
    monkeypatch.setattr(trader, "LEDGER", p)
    s = trader.recent_stats(n=10)
    assert s["n"] == 3
    assert s["wins"] == 2
    assert s["losses"] == 1
    assert abs(s["pnl"] - 2.5) < 1e-6
    assert 0 <= s["win_rate"] <= 100


def test_drawdown_circuit_insufficient_samples():
    led = {"trades": [{"realized_pnl": -1.0}]}
    c = trader.drawdown_circuit(led, n=10)
    assert c["open"] is True


def test_drawdown_circuit_low_winrate_opens():
    led = {"trades": [{"realized_pnl": -1.0} for _ in range(5)]}
    c = trader.drawdown_circuit(led, n=5)
    assert c["open"] is False
    assert "win_rate" in c["reason"]


def test_negative_pnl_trade_is_loss():
    led = {"trades": [{"realized_pnl": -2.0}, {"realized_pnl": 3.0}]}
    s = trader.recent_stats(led)
    assert s["wins"] == 1
    assert s["losses"] == 1


def test_load_bot_edge_real_file():
    edge = meta.load_bot_edge()
    assert isinstance(edge, dict)
    for v in edge.values():
        assert {"edge", "trades", "win_rate", "confidence"} <= set(v)


def test_asset_edge_unknown():
    e = meta.asset_edge("NONEXISTENT-USD", cache={})
    assert e["verdict"] == "unknown"
    assert e["confidence"] is False


def test_asset_edge_winner_and_loser(tmp_path, monkeypatch):
    perf = tmp_path / "live_performance.json"
    perf.write_text(json.dumps({"_records": {
        "ema_cross/BTC-USD": {"product_id": "BTC-USD", "trades": 5, "wins": 4,
                              "losses": 1, "total_pnl": 12.0, "disabled": False},
        "ema_cross/ALT-USD": {"product_id": "ALT-USD", "trades": 5, "wins": 1,
                              "losses": 4, "total_pnl": -8.0, "disabled": False},
    }}))
    monkeypatch.setattr(meta, "PERF", perf)
    win = meta.asset_edge("BTC-USD")
    assert win["verdict"] == "bot_wins_here"
    assert win["confidence"] is True
    lose = meta.asset_edge("ALT-USD")
    assert lose["verdict"] == "bot_bleeds_here"


def test_asset_edge_unknown_with_corrupt_perf(tmp_path, monkeypatch):
    perf = tmp_path / "live_performance.json"
    perf.write_text("{not valid json")
    monkeypatch.setattr(meta, "PERF", perf)
    assert meta.load_bot_edge() == {}
