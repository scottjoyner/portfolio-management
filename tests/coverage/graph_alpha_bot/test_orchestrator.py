import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, mock_open, patch

from app.orchestrator import PipelineOrchestrator, ComponentState, PipelineConfig, setup_logging


def _recent_kg():
    now = datetime.utcnow().isoformat()
    return {
        "articles": [
            {"published_at": now, "tickers": ["BTC"], "sentiment_score": 0.9},
            {"published_at": now, "tickers": ["BTC"], "sentiment_score": 0.9},
            {"published_at": now, "tickers": ["BTC"], "sentiment_score": 0.9},
        ]
    }


def test_config_defaults():
    c = PipelineConfig()
    assert c.news_interval_minutes == 5
    assert c.max_position_size_pct == 0.10
    assert c.news_symbols


def test_init_component_states():
    o = PipelineOrchestrator()
    assert o.component_states["news_ingestion"]["state"] == ComponentState.IDLE
    assert o.running is False


def test_run_signal_generation_no_kg(monkeypatch):
    o = PipelineOrchestrator()
    # kg does not exist -> no signals but loop runs
    monkeypatch.setattr("app.orchestrator.Path", __import__("pathlib").Path)
    sigs = o.run_signal_generation()
    assert sigs == []


def test_run_signal_generation_with_kg(monkeypatch):
    o = PipelineOrchestrator()

    class FakePath:
        def __init__(self, *a, **k):
            pass

        def exists(self):
            return True

    monkeypatch.setattr("app.orchestrator.Path", FakePath)
    m = mock_open(read_data=json.dumps(_recent_kg()))
    monkeypatch.setattr("builtins.open", m)
    sigs = o.run_signal_generation()
    assert len(sigs) == 1
    assert sigs[0]["symbol"] == "BTC-USD"
    assert sigs[0]["direction"] == "LONG"
    assert sigs[0]["confidence"] > 0.5


def test_execute_signal_filled():
    o = PipelineOrchestrator()
    o.circuit_open = False
    res = o.execute_signal({"symbol": "BTC-USD"})
    assert res["status"] == "filled"
    assert o.positions["BTC-USD"]["side"] == "BUY"


def test_execute_signal_blocked_recent():
    o = PipelineOrchestrator()
    o.circuit_open = True
    o.circuit_opened_at = datetime.utcnow()
    res = o.execute_signal({"symbol": "BTC-USD"})
    assert res["status"] == "blocked"


def test_execute_signal_blocked_recovers():
    o = PipelineOrchestrator()
    o.circuit_open = True
    o.circuit_opened_at = datetime.utcnow() - timedelta(seconds=1000)
    res = o.execute_signal({"symbol": "BTC-USD"})
    assert res["status"] == "filled"
    assert o.circuit_open is False


def test_execute_signal_no_price():
    o = PipelineOrchestrator()
    o.circuit_open = False
    res = o.execute_signal({"symbol": "NOPE-USD"})
    assert res["status"] == "failed"
    assert o.circuit_open is True


def test_setup_logging():
    # Should not raise; returns None
    assert setup_logging("/tmp/gab_test_log.log") is None
