import json
import signal as signal_module
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import parallel_pipeline_runner as m


# ---------------------------------------------------------------------------
# logging / config / enums
# ---------------------------------------------------------------------------

def test_setup_logging_default(tmp_path, monkeypatch):
    monkeypatch.setattr("os.path.expanduser", lambda p: str(tmp_path / "home"))
    log = m.setup_logging()
    assert log is not None


def test_setup_logging_explicit(tmp_path):
    log = m.setup_logging(str(tmp_path / "p.log"))
    assert log is not None
    assert (tmp_path / "p.log").exists()


def test_pipeline_config_defaults():
    c = m.PipelineConfig()
    assert c.max_workers == 4
    assert c.processing_interval_seconds == 300
    assert c.symbols


def test_pipeline_state_enum():
    assert m.PipelineState.RUNNING.value == "running"
    assert m.PipelineState.ERROR.value == "error"


def test_health_status_record_error():
    h = m.HealthStatus(name="x")
    h.record_error("boom")
    assert h.error_count == 1 and h.message == "boom"


# ---------------------------------------------------------------------------
# ComponentManager
# ---------------------------------------------------------------------------

def test_component_manager():
    cm = m.ComponentManager(m.PipelineConfig())
    assert cm.state == m.PipelineState.STOPPED
    cm.register_component("news")
    assert "news" in cm.health_checks
    assert cm.get_health_status()["news"].name == "news"
    cm.increment_error_count("news")
    assert cm.metrics["errors_total"] == 1
    assert cm.health_checks["news"].error_count == 1
    # unknown source: only increments global metric
    cm.increment_error_count("ghost")
    assert cm.metrics["errors_total"] == 2
    assert "ghost" not in cm.health_checks


# ---------------------------------------------------------------------------
# NewsIngestionPipeline
# ---------------------------------------------------------------------------

def _npipe():
    return m.NewsIngestionPipeline(m.PipelineConfig(), m.ComponentManager(m.PipelineConfig()))


def test_news_cache_load_exists(tmp_path):
    cache = tmp_path / ".news_cache.json"
    cache.write_text(json.dumps({"processed_ids": ["a", "b"]}))
    p = _npipe()
    p.cache_file = str(cache)
    loaded = p._load_cache()
    assert loaded == {"a", "b"}


def test_news_cache_load_missing():
    p = _npipe()
    p.cache_file = "/nonexistent/path/cache.json"
    assert p._load_cache() == set()


def test_news_cache_load_error(tmp_path):
    cache = tmp_path / ".news_cache.json"
    cache.write_text("{bad json")
    p = _npipe()
    p.cache_file = str(cache)
    assert p._load_cache() == set()


def test_news_cache_save_error():
    p = _npipe()
    p.cache_file = str("/no/such/dir/cache.json")
    p._save_cache({"x"})
    # should not raise


def test_fetch_articles_success():
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "<rss><channel><item><title>t</title><link>l</link></item></channel></rss>"
    p = _npipe()
    with patch("requests.get", return_value=resp):
        arts = p.fetch_articles()
    assert len(arts) == 3  # three configured feeds, one article each


def test_fetch_articles_error_increments():
    p = _npipe()
    with patch("requests.get", side_effect=RuntimeError("down")):
        arts = p.fetch_articles()
    assert arts == []
    assert p.cm.metrics["errors_total"] == len(m.PipelineConfig().news_sources)


def test_parse_rss_valid():
    xml = "<rss><channel><item><title>t</title><link>l</link></item></channel></rss>"
    arts = _npipe()._parse_rss(xml, "src")
    assert len(arts) == 1
    assert arts[0]["title"] == "t"


def test_parse_rss_missing_title_link():
    xml = "<rss><channel><item><link>l</link></channel></rss>"
    arts = _npipe()._parse_rss(xml, "src")
    assert arts == []


def test_parse_rss_malformed():
    arts = _npipe()._parse_rss("<<not xml", "src")
    assert arts == []


def test_parse_rss_valid_pubdate():
    xml = (
        '<rss><channel>'
        '<item><title>t</title><link>l</link>'
        '<pubDate>Mon, 01 Jan 2024 00:00:00 GMT</pubDate>'
        '</item></channel></rss>'
    )
    arts = _npipe()._parse_rss(xml, "src")
    assert len(arts) == 1
    assert arts[0]["published_at"] != ""


def test_news_run(tmp_path):
    resp = MagicMock()
    resp.status_code = 200
    resp.text = "<rss><channel><item><title>t</title><link>l</link></item></channel></rss>"
    p = _npipe()
    p.cache_file = str(tmp_path / "c.json")
    with patch("requests.get", return_value=resp):
        n = p.run()
    assert n == 3
    assert p.cache_file  # cache saved


# ---------------------------------------------------------------------------
# SignalGenerator
# ---------------------------------------------------------------------------

def test_signal_generator_both_branches():
    cm = m.ComponentManager(m.PipelineConfig())
    sg = m.SignalGenerator(m.PipelineConfig(), cm)

    def fake_hash(x):
        return 900 if x == "A-USD" else 600  # 0.4 -> signal ; 0.1 -> none

    with patch("builtins.hash", side_effect=fake_hash):
        sg.config.symbols = ["A-USD", "B-USD"]
        sigs = sg.generate_signals()
    assert len(sigs) == 1
    assert sigs[0]["action"] == "buy"


def test_signal_generator_no_signals():
    cm = m.ComponentManager(m.PipelineConfig())
    sg = m.SignalGenerator(m.PipelineConfig(), cm)
    with patch("builtins.hash", return_value=600):
        sg.config.symbols = ["B-USD"]
        assert sg.generate_signals() == []


# ---------------------------------------------------------------------------
# PipelineRunner
# ---------------------------------------------------------------------------

def _runner():
    return m.PipelineRunner(config_path=None)


def test_runner_start():
    r = _runner()
    r.start()
    assert r.state == m.PipelineState.RUNNING
    assert "news_ingestion" in r.cm.health_checks


def test_runner_handle_shutdown():
    r = _runner()
    r.start()
    r._handle_shutdown(signal_module.SIGTERM, None)
    assert r.shutdown_event.is_set()
    assert r.state == m.PipelineState.PAUSING


def test_runner_run_cycle_running():
    r = _runner()
    r.start()
    metrics = r.run_cycle()
    assert "articles_fetched" in metrics
    assert metrics["duration_seconds"] >= 0


def test_runner_run_cycle_not_running():
    r = _runner()
    r.start()
    r.state = m.PipelineState.PAUSED
    with patch.object(r.news_pipeline, "fetch_articles", side_effect=AssertionError("should not be called")):
        metrics = r.run_cycle()
    assert metrics["articles_fetched"] == 0


def test_runner_run_cycle_exception_and_recovery():
    r = _runner()
    r.start()
    with patch.object(r.news_pipeline, "fetch_articles", side_effect=[RuntimeError("boom"), None]):
        r.run_cycle()
    assert r.state == m.PipelineState.RECOVERING
    # Next cycle succeeds; run_cycle keeps the RECOVERING state (run() recovers it)
    metrics = r.run_cycle()
    assert r.state == m.PipelineState.RECOVERING
    assert metrics["errors"] == 0


def test_runner_run_cycle_exception_when_error_state():
    # When state is already ERROR, the RECOVERING transition is skipped
    r = _runner()
    r.start()
    r.state = m.PipelineState.ERROR
    with patch.object(r.signal_generator, "generate_signals", side_effect=RuntimeError("boom")):
        metrics = r.run_cycle()
    assert r.state == m.PipelineState.ERROR
    assert metrics["errors"] == 1


def test_runner_run_loop_with_recovery():
    r = _runner()
    r.start()

    calls = {"n": 0}

    def fake_run_cycle():
        calls["n"] += 1
        if calls["n"] == 1:
            # simulate a cycle whose run_cycle catches its own error
            r.state = m.PipelineState.RECOVERING
            return {"articles_fetched": 0, "signals_generated": 0, "errors": 1, "duration_seconds": 0.0}
        if calls["n"] >= 3:
            r.shutdown_event.set()
        return {"articles_fetched": 0, "signals_generated": 0, "errors": 0, "duration_seconds": 0.0}

    with patch.object(r, "run_cycle", side_effect=fake_run_cycle):
        with patch.object(time, "sleep", lambda s: None):
            r.run()
    assert calls["n"] == 3
    assert r.state == m.PipelineState.RUNNING  # recovered (covers 382-383)


def test_runner_run_loop(tmp_path):
    r = _runner()
    r.start()

    calls = {"n": 0}

    def fake_run_cycle():
        calls["n"] += 1
        if calls["n"] >= 2:
            r.shutdown_event.set()
        return {"articles_fetched": 1, "signals_generated": 1, "errors": 0, "duration_seconds": 0.0}

    with patch.object(r, "run_cycle", side_effect=fake_run_cycle):
        with patch.object(time, "sleep", lambda s: None):
            r.run()
    assert calls["n"] == 2


def test_runner_start_exception(monkeypatch):
    r = _runner()
    monkeypatch.setattr(signal_module, "signal", MagicMock(side_effect=RuntimeError("no signals")))
    r.start()
    assert r.state == m.PipelineState.ERROR


def test_runner_with_config_file(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("foo: bar")
    r = m.PipelineRunner(config_path=str(cfg))
    assert r.config is not None


def test_runner_shutdown():
    r = _runner()
    r.start()
    r.shutdown()
    assert r.shutdown_event.is_set()


def test_runner_run_loop_exception():
    r = _runner()
    r.start()

    def fake_run_cycle():
        raise RuntimeError("fatal")

    with patch.object(r, "run_cycle", side_effect=fake_run_cycle):
        with patch.object(time, "sleep", lambda s: r.shutdown_event.set()):
            r.run()
    assert r.state == m.PipelineState.ERROR


# ---------------------------------------------------------------------------
# main()
# ---------------------------------------------------------------------------

def test_main_test_mode(monkeypatch, capsys):
    args = MagicMock()
    args.config = None
    args.test_mode = True

    fake_runner = MagicMock()
    fake_runner.run_cycle.return_value = {"articles_fetched": 0, "signals_generated": 0, "errors": 0, "duration_seconds": 0.1}

    with patch("argparse.ArgumentParser") as AP:
        AP.return_value.parse_args.return_value = args
        with patch("parallel_pipeline_runner.PipelineRunner", return_value=fake_runner):
            m.main()
    fake_runner.run_cycle.assert_called_once()
    assert "duration_seconds" in capsys.readouterr().out


def test_main_run_mode(monkeypatch):
    args = MagicMock()
    args.config = None
    args.test_mode = False

    fake_runner = MagicMock()
    fake_runner.run.side_effect = KeyboardInterrupt

    with patch("argparse.ArgumentParser") as AP:
        AP.return_value.parse_args.return_value = args
        with patch("parallel_pipeline_runner.PipelineRunner", return_value=fake_runner):
            m.main()
    fake_runner.run.assert_called_once()
    fake_runner.shutdown.assert_called_once()


def test_main_run_mode_exception(monkeypatch):
    args = MagicMock()
    args.config = None
    args.test_mode = False

    fake_runner = MagicMock()
    fake_runner.run.side_effect = RuntimeError("fatal")

    with patch("argparse.ArgumentParser") as AP:
        AP.return_value.parse_args.return_value = args
        with patch("parallel_pipeline_runner.PipelineRunner", return_value=fake_runner):
            m.main()
    fake_runner.shutdown.assert_called_once()
