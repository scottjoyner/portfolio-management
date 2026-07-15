import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import app.pipeline_launcher as m
from app.pipeline_launcher import (
    setup_logging,
    PipelineConfig,
    SignalGenerator,
    TradingExecutor,
    PipelineOrchestrator,
)


def _kg(btc_count=0, btc_sent=0.9, eth_count=0, eth_sent=0.1):
    arts = []
    for i in range(btc_count):
        arts.append({"tickers": ["btc"], "sentiment_score": btc_sent})
    for i in range(eth_count):
        arts.append({"tickers": ["eth"], "sentiment_score": eth_sent})
    return {"articles": arts}


def test_setup_logging_default(tmp_path):
    f = tmp_path / "p.log"
    setup_logging(str(f))
    assert f.exists()


def test_setup_logging_none(tmp_path, monkeypatch):
    monkeypatch.setattr("os.path.expanduser", lambda p: str(tmp_path / "home"))
    setup_logging(None)
    assert (tmp_path / "home").exists()


def test_signal_generator_no_kg(tmp_path):
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = tmp_path / "missing.json"
    sg.signal_cache_file = str(tmp_path / ".cache.json")
    assert sg.generate_signals() == []


def test_signal_generator_with_kg(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(__import__("json").dumps(_kg(btc_count=3, btc_sent=0.9)))
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = kg
    sg.signal_cache_file = str(tmp_path / ".cache.json")
    sigs = sg.generate_signals()
    assert len(sigs) == 1
    assert sigs[0]["symbol"] == "BTC-USD"
    assert sigs[0]["direction"] == "LONG"


def test_signal_generator_low_sentiment_no_signal(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(__import__("json").dumps(_kg(eth_count=3, eth_sent=0.05)))
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = kg
    sg.signal_cache_file = str(tmp_path / ".cache.json")
    assert sg.generate_signals() == []


def test_signal_generator_cooldown(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(__import__("json").dumps(_kg(btc_count=3, btc_sent=0.9)))
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = kg
    sg.signal_cache_file = str(tmp_path / ".cache.json")
    first = sg.generate_signals()
    assert len(first) == 1
    # immediate second call -> within cooldown -> no new signal
    second = sg.generate_signals()
    assert second == []


def test_trading_executor_get_price():
    te = TradingExecutor()
    assert te.get_price("BTC-USD") == 68500.0
    assert te.get_price("DOGE-USD") is None


def test_trading_executor_execute_filled():
    te = TradingExecutor()
    order = te.execute_signal({"symbol": "BTC-USD"})
    assert order["status"] == "filled"
    assert te.portfolio["BTC"] > 0.5


def test_trading_executor_execute_no_price():
    te = TradingExecutor()
    order = te.execute_signal({"symbol": "DOGE-USD"})
    assert order["status"] == "failed"
    assert te.circuit_open is True


def test_trading_executor_circuit_open_blocks():
    te = TradingExecutor()
    te.circuit_open = True
    te.circuit_opened_at = datetime.now(timezone.utc)
    order = te.execute_signal({"symbol": "BTC-USD"})
    assert order["status"] == "blocked"


def test_trading_executor_circuit_open_expired_resets():
    te = TradingExecutor()
    te.circuit_open = True
    te.circuit_opened_at = datetime.now(timezone.utc) - timedelta(seconds=1000)
    order = te.execute_signal({"symbol": "BTC-USD"})
    assert order["status"] == "filled"
    assert te.circuit_open is False


def test_signal_generator_cache_save_error(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(json.dumps(_kg(btc_count=3, btc_sent=0.9)))
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = kg
    sg.signal_cache_file = str(tmp_path / "nodir" / ".cache.json")
    # Saving fails (no such dir) but signals are still returned
    sigs = sg.generate_signals()
    assert len(sigs) == 1


def test_signal_generator_cooldown_elapsed(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(json.dumps(_kg(btc_count=3, btc_sent=0.9)))
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = kg
    sg.signal_cache_file = str(tmp_path / ".c.json")
    first = sg.generate_signals()
    assert len(first) == 1
    # Backdate the last signal time so cooldown has elapsed -> new signal generated
    sg.last_signal_times["BTC-USD"] = datetime.now(timezone.utc) - timedelta(seconds=1000)
    second = sg.generate_signals()
    assert len(second) == 1


def test_signal_generator_no_cache_file(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(json.dumps(_kg(btc_count=3, btc_sent=0.9)))
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = kg
    sg.signal_cache_file = str(tmp_path / ".absent.json")  # does not exist
    sigs = sg.generate_signals()
    assert len(sigs) == 1


def test_signal_generator_cache_load_error(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(json.dumps(_kg(btc_count=3, btc_sent=0.9)))
    cache = tmp_path / ".badcache.json"
    cache.write_text("{not valid json")
    sg = SignalGenerator(PipelineConfig())
    sg.kg_file = kg
    sg.signal_cache_file = str(cache)
    sigs = sg.generate_signals()  # load fails -> graceful -> still returns signals
    assert len(sigs) == 1


def test_main_loop_and_shutdown(monkeypatch):
    import signal as _signal

    state = {"n": 0}
    handlers = {}

    class FakeOrch:
        running = True

        def run_cycle(self):
            state["n"] += 1
            if state["n"] == 1:
                return {"signals_generated": 2, "orders_executed": 2}
            self.running = False
            return {"signals_generated": 0, "orders_executed": 0}

    monkeypatch.setattr(m, "PipelineOrchestrator", FakeOrch)
    monkeypatch.setattr(m.time, "sleep", lambda s: None)
    monkeypatch.setattr(_signal, "signal", lambda sig, h: handlers.update({sig: h}))
    m.main()
    # invoke the registered shutdown handler
    handlers[_signal.SIGINT](None, None)
    assert state["n"] == 2


def test_main_no_loop(monkeypatch):
    # running is False by default -> while loop body is skipped
    m.main()


def test_orchestrator_run_cycle(tmp_path):
    kg = tmp_path / "knowledge_graph.json"
    kg.write_text(json.dumps(_kg(btc_count=3, btc_sent=0.9)))
    orch = PipelineOrchestrator()
    orch.signal_generator.kg_file = kg
    orch.signal_generator.signal_cache_file = str(tmp_path / ".cache.json")
    result = orch.run_cycle()
    assert result["signals_generated"] == 1
    assert result["orders_executed"] == 1
