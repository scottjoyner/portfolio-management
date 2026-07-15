import json
import signal as signal_module
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import app.orchestrator as om
from app.orchestrator import PipelineOrchestrator, setup_logging


def test_setup_logging(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    setup_logging()
    setup_logging(str(tmp_path / "pipe.log"))


def _make_kg(tmp_path, articles):
    d = tmp_path / "graph-alpha-bot" / "app" / "data"
    d.mkdir(parents=True)
    (d / "knowledge_graph.json").write_text(json.dumps({"articles": articles}))


def _recent_article(tickers, sentiment):
    return {
        "tickers": tickers,
        "sentiment_score": sentiment,
        "published_at": datetime.utcnow().isoformat() + "Z",
    }


def test_run_signal_generation_produces(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [
        _recent_article(["BTC-USD"], 0.9),
        _recent_article(["BTC-USD"], 0.8),
    ]
    _make_kg(tmp_path, arts)
    o = PipelineOrchestrator()
    sigs = o.run_signal_generation()
    assert len(sigs) == 1
    assert sigs[0]["symbol"] == "BTC-USD"


def test_run_signal_generation_cooldown(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [_recent_article(["BTC-USD"], 0.9), _recent_article(["BTC-USD"], 0.8)]
    _make_kg(tmp_path, arts)
    o = PipelineOrchestrator()
    o.run_signal_generation()  # first generates + sets last_signal_times
    sigs2 = o.run_signal_generation()  # cooldown blocks
    assert sigs2 == []


def test_run_signal_generation_cooldown_expired(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [_recent_article(["BTC-USD"], 0.9), _recent_article(["BTC-USD"], 0.8)]
    _make_kg(tmp_path, arts)
    o = PipelineOrchestrator()
    o.run_signal_generation()
    o.last_signal_times["BTC-USD"] = datetime.utcnow() - timedelta(seconds=1000)
    sigs = o.run_signal_generation()  # cooldown elapsed -> regenerate
    assert len(sigs) == 1


def test_run_signal_generation_no_kg(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # no knowledge graph file
    o = PipelineOrchestrator()
    assert o.run_signal_generation() == []


def test_run_signal_generation_kg_exception(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    d = tmp_path / "graph-alpha-bot" / "app" / "data"
    d.mkdir(parents=True)
    (d / "knowledge_graph.json").write_text("{bad json")
    o = PipelineOrchestrator()
    assert o.run_signal_generation() == []


def test_run_signal_generation_save_error(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    arts = [_recent_article(["BTC-USD"], 0.9), _recent_article(["BTC-USD"], 0.8)]
    _make_kg(tmp_path, arts)
    o = PipelineOrchestrator()
    o.signal_cache_file = "/no/such/dir/cache.json"
    # should not raise; save failure is caught
    assert len(o.run_signal_generation()) == 1


def test_execute_signal_buy():
    o = PipelineOrchestrator()
    sig = {"symbol": "BTC-USD", "direction": "LONG", "confidence": 0.5,
           "sentiment_score": 0.5, "news_count": 2, "signal_reason": "r"}
    res = o.execute_signal(sig)
    assert res["status"] == "filled"
    assert o.portfolio["BTC"] > 0.5


def test_execute_signal_circuit_open_blocked():
    o = PipelineOrchestrator()
    o.circuit_open = True
    o.circuit_opened_at = datetime.utcnow()
    res = o.execute_signal({"symbol": "BTC-USD"})
    assert res["status"] == "blocked"


def test_execute_signal_circuit_open_expired():
    o = PipelineOrchestrator()
    o.circuit_open = True
    o.circuit_opened_at = datetime.utcnow() - timedelta(seconds=1000)
    sig = {"symbol": "BTC-USD", "direction": "LONG"}
    res = o.execute_signal(sig)
    assert res["status"] == "filled"
    assert o.circuit_open is False


def test_execute_signal_no_price():
    o = PipelineOrchestrator()
    res = o.execute_signal({"symbol": "NOPE-USD"})
    assert res["status"] == "failed"
    assert o.circuit_open is True


class _StopLoop(Exception):
    pass


def test_main_no_run():
    # running defaults to False -> loop body never executes
    om.main()


def test_main_runs_one_cycle(monkeypatch):
    orig_init = om.PipelineOrchestrator.__init__

    def patched_init(self):
        orig_init(self)
        self.running = True

    monkeypatch.setattr(om.PipelineOrchestrator, "__init__", patched_init)
    monkeypatch.setattr(om.PipelineOrchestrator, "run_signal_generation",
                        lambda self: [{"symbol": "BTC-USD", "direction": "LONG", "confidence": 0.5,
                                       "sentiment_score": 0.5, "news_count": 2, "signal_reason": "r",
                                       "timestamp": "t"}])
    monkeypatch.setattr(om.PipelineOrchestrator, "execute_signal",
                        lambda self, sig: {"status": "filled"})

    def fake_sleep(s):
        raise _StopLoop()

    monkeypatch.setattr(time, "sleep", fake_sleep)
    try:
        om.main()
    except _StopLoop:
        pass


def test_main_runs_error_path(monkeypatch):
    orig_init = om.PipelineOrchestrator.__init__

    def patched_init(self):
        orig_init(self)
        self.running = True

    monkeypatch.setattr(om.PipelineOrchestrator, "__init__", patched_init)
    monkeypatch.setattr(om.PipelineOrchestrator, "run_signal_generation",
                        lambda self: (_ for _ in ()).throw(RuntimeError("boom")))

    def fake_sleep(s):
        raise _StopLoop()

    monkeypatch.setattr(time, "sleep", fake_sleep)
    try:
        om.main()
    except _StopLoop:
        pass


def test_main_runs_one_cycle_no_signals(monkeypatch):
    orig_init = om.PipelineOrchestrator.__init__

    def patched_init(self):
        orig_init(self)
        self.running = True

    monkeypatch.setattr(om.PipelineOrchestrator, "__init__", patched_init)
    monkeypatch.setattr(om.PipelineOrchestrator, "run_signal_generation", lambda self: [])

    def fake_sleep(s):
        raise _StopLoop()

    monkeypatch.setattr(time, "sleep", fake_sleep)
    try:
        om.main()
    except _StopLoop:
        pass
