from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO))

import scripts.hermes_agent_trader as trader
import scripts.hermes_expectancy as exp


def _ledger_tmp(tmp_path: Path, trades):
    led = {"positions": {}, "trades": trades, "realized_pnl": 0.0,
           "created_at": "2026-01-01T00:00:00+00:00"}
    p = tmp_path / "hermes_agent_ledger.json"
    p.write_text(json.dumps(led))
    return p


def test_expectancy_table_empty(tmp_path, monkeypatch):
    p = _ledger_tmp(tmp_path, [])
    monkeypatch.setattr(trader, "LEDGER", p)
    assert exp.expectancy_table() == {}


def test_expectancy_table_cells(tmp_path, monkeypatch):
    p = _ledger_tmp(tmp_path, [
        {"product_id": "BTC-USD", "side": "BUY", "regime": "TREND_UP",
         "realized_pnl": 5.0},
        {"product_id": "BTC-USD", "side": "BUY", "regime": "TREND_UP",
         "realized_pnl": -1.0},
        {"product_id": "BTC-USD", "side": "BUY", "regime": "TREND_UP",
         "realized_pnl": 3.0},
        {"product_id": "BTC-USD", "side": "SELL", "regime": "TREND_UP",
         "realized_pnl": -2.0},
    ])
    monkeypatch.setattr(trader, "LEDGER", p)
    tbl = exp.expectancy_table()
    key = "TREND_UP|BTC-USD|BUY"
    assert key in tbl
    cell = tbl[key]
    assert cell["n"] == 3
    assert cell["win_rate"] == pytest.approx(2 / 3 * 100, abs=0.1)
    assert cell["expectancy"] == pytest.approx(7 / 3, abs=1e-3)
    assert cell["pnl"] == pytest.approx(7.0, abs=1e-6)
    assert cell["trusted"] is True


def test_expectancy_table_skip_open_trades(tmp_path, monkeypatch):
    p = _ledger_tmp(tmp_path, [
        {"product_id": "ETH-USD", "side": "BUY", "regime": "RANGE",
         "realized_pnl": 4.0},
        {"product_id": "ETH-USD", "side": "BUY", "regime": "RANGE"},
    ])
    monkeypatch.setattr(trader, "LEDGER", p)
    tbl = exp.expectancy_table()
    assert "RANGE|ETH-USD|BUY" in tbl
    assert tbl["RANGE|ETH-USD|BUY"]["n"] == 1
    assert tbl["RANGE|ETH-USD|BUY"]["trusted"] is False


def test_universe_tilt_boost_drop_keep(tmp_path, monkeypatch):
    p = _ledger_tmp(tmp_path, [
        {"product_id": "BTC-USD", "realized_pnl": 2.0},
        {"product_id": "BTC-USD", "realized_pnl": 3.0},
        {"product_id": "BTC-USD", "realized_pnl": 1.0},
        {"product_id": "ALT-USD", "realized_pnl": -2.0},
        {"product_id": "ALT-USD", "realized_pnl": -1.0},
        {"product_id": "ALT-USD", "realized_pnl": -3.0},
        {"product_id": "THIN-USD", "realized_pnl": 1.0},
    ])
    monkeypatch.setattr(trader, "LEDGER", p)
    tilt = exp.universe_tilt()
    assert tilt["BTC-USD"]["tilt"] == "boost"
    assert tilt["ALT-USD"]["tilt"] == "drop"
    assert tilt["THIN-USD"]["tilt"] == "keep"
    assert tilt["BTC-USD"]["agent_pnl"] == pytest.approx(6.0, abs=1e-6)
    assert tilt["ALT-USD"]["agent_wr"] == pytest.approx(0.0, abs=1e-6)


def test_live_ready_blocked_on_thin_sample(tmp_path, monkeypatch):
    p = _ledger_tmp(tmp_path, [
        {"product_id": "BTC-USD", "regime": "TREND_UP", "realized_pnl": 1.0},
    ])
    monkeypatch.setattr(trader, "LEDGER", p)
    res = exp.live_ready()
    assert res["ready"] is False
    assert res["n_closed"] == 1
    assert any("closed paper trades" in r for r in res["reasons"])


def test_live_ready_promotes_with_positive_regimes(tmp_path, monkeypatch):
    trades = []
    for i in range(4):
        trades.append({"product_id": "BTC-USD", "regime": "TREND_UP",
                       "realized_pnl": 2.0})
    for i in range(4):
        trades.append({"product_id": "ETH-USD", "regime": "RANGE",
                       "realized_pnl": 1.5})
    for i in range(6):
        trades.append({"product_id": "SOL-USD", "regime": "VOL",
                       "realized_pnl": -0.1})
    p = _ledger_tmp(tmp_path, trades)
    monkeypatch.setattr(trader, "LEDGER", p)

    def _good_circuit(led=None, n=10):
        return {"open": True, "reason": "ok"}
    monkeypatch.setattr(trader, "drawdown_circuit", _good_circuit)
    res = exp.live_ready(min_regimes=2, min_closed=12)
    assert res["ready"] is True
    assert len(res["regimes_positive"]) >= 2


def test_live_ready_circuit_tripped_blocks(tmp_path, monkeypatch):
    trades = [{"product_id": "BTC-USD", "regime": "TREND_UP", "realized_pnl": 2.0}
              for _ in range(14)]
    p = _ledger_tmp(tmp_path, trades)
    monkeypatch.setattr(trader, "LEDGER", p)

    def _bad_circuit(led=None, n=10):
        return {"open": False, "reason": "win_rate 0.0"}
    monkeypatch.setattr(exp, "drawdown_circuit", _bad_circuit)
    res = exp.live_ready(min_regimes=2, min_closed=12)
    assert res["ready"] is False
    assert any("circuit" in r for r in res["reasons"])
