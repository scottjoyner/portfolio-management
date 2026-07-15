"""Additional coverage for EventTraderV4 persistence / health / watchdog methods."""
from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

from coinbase.src.run_trader_v4 import EventTraderV4, PaperPosition  # noqa: E402


def _make_trader(**kw):
    kw.setdefault("dry_run", True)
    mode = kw.pop("mode", "paper")
    return EventTraderV4(mode=mode, products=["BTC-USD", "ETH-USD", "SOL-USD"], **kw)


def test_save_paper_state_paper(tmp_path, monkeypatch):
    t = _make_trader()
    monkeypatch.setattr(t, "_paper_state_path", tmp_path / "paper_state.json")
    t._save_paper_state()
    assert (tmp_path / "paper_state.json").exists()


def test_save_paper_state_non_paper(monkeypatch):
    t = _make_trader(mode="live")
    saved = {}
    monkeypatch.setattr(t, "_save_core_holdings_state", lambda: saved.setdefault("called", True))
    t._save_paper_state()
    assert saved.get("called") is True


def test_load_paper_state_missing(tmp_path, monkeypatch):
    t = _make_trader()
    monkeypatch.setattr(t, "_paper_state_path", tmp_path / "nope.json")
    t._load_paper_state()
    assert t.paper_positions == {}


def test_load_paper_state_valid(tmp_path, monkeypatch):
    t = _make_trader()
    pos = PaperPosition(
        product_id="BTC-USD", side="LONG", qty=1.0, entry_price=100.0,
        entry_ts=time.time(), strategy="ema_cross", confidence=0.6,
        win_rate=0.6, sharpe=1.0,
    )
    t.paper_positions = {"BTC-USD": pos}
    monkeypatch.setattr(t, "_paper_state_path", tmp_path / "paper_state.json")
    t._save_paper_state()
    t.paper_positions = {}
    t._load_paper_state()
    assert "BTC-USD" in t.paper_positions


def test_paper_refresh_health(tmp_path, monkeypatch):
    t = _make_trader()
    monkeypatch.setattr(t, "_last_price", {"BTC-USD": 100.0})
    t._news_sentiment = None
    t._macro_risk = None
    t._paper_refresh_health()
    assert "paper" in t.health_status
    assert "pulses" in t.health_status


def test_watchdog_loop_one_pass(tmp_path, monkeypatch):
    t = _make_trader()
    t._last_ticker_ts = None
    t._last_eval_ts = None
    t._last_scan_ts = None
    t._last_minute_scan_ts = None
    t._last_full_scan_ts = None
    t._shutdown = False
    calls = {"n": 0}

    def _sleep(seconds):
        calls["n"] += 1
        if calls["n"] >= 2:
            t._shutdown = True

    with patch("time.sleep", _sleep):
        t._watchdog_loop()
    assert t._shutdown is True
    assert "alerts" in t.health_status
