import json
import os
import signal as signal_module
import subprocess
from unittest.mock import MagicMock, patch

import pipeline_launcher as m


def test_setup_logging_default(monkeypatch, tmp_path):
    monkeypatch.setattr("os.path.expanduser", lambda p: str(tmp_path / "home"))
    log = m.setup_logging()
    assert log is not None


def test_setup_logging_explicit(tmp_path):
    log = m.setup_logging(str(tmp_path / "launcher.log"))
    assert log is not None
    assert (tmp_path / "launcher.log").exists()


def test_service_manager_configs():
    sm = m.ServiceManager()
    assert "news_ingestion" in sm.service_configs
    assert "signal_generator" in sm.service_configs


def test_start_service_success(tmp_path):
    sm = m.ServiceManager()
    proc = MagicMock()
    proc.pid = 1234
    with patch.object(subprocess, "Popen", return_value=proc), \
         patch("builtins.open", MagicMock()):
        sm.start_service("news_ingestion")
    assert sm.services["news_ingestion"] is proc


def test_start_service_unknown():
    sm = m.ServiceManager()
    with patch.object(subprocess, "Popen", return_value=MagicMock()):
        sm.start_service("does_not_exist")
    assert "does_not_exist" not in sm.services


def test_start_service_exception():
    sm = m.ServiceManager()
    with patch.object(subprocess, "Popen", side_effect=RuntimeError("boom")):
        sm.start_service("news_ingestion")
    assert "news_ingestion" not in sm.services


def test_run_service_cycle_success():
    sm = m.ServiceManager()
    cp = subprocess.CompletedProcess([], 0, stdout=json.dumps({"ok": True}), stderr="")
    with patch.object(subprocess, "run", return_value=cp):
        out = sm.run_service_cycle("news_ingestion")
    assert out == {"ok": True}


def test_run_service_cycle_failure():
    sm = m.ServiceManager()
    cp = subprocess.CompletedProcess([], 1, stdout="", stderr="err")
    with patch.object(subprocess, "run", return_value=cp):
        assert sm.run_service_cycle("news_ingestion") is None


def test_run_service_cycle_timeout():
    sm = m.ServiceManager()
    with patch.object(subprocess, "run", side_effect=subprocess.TimeoutExpired("x", 1)):
        assert sm.run_service_cycle("news_ingestion") is None


def test_run_service_cycle_exception():
    sm = m.ServiceManager()
    with patch.object(subprocess, "run", side_effect=RuntimeError("boom")):
        assert sm.run_service_cycle("news_ingestion") is None


def test_handle_shutdown():
    sm = m.ServiceManager()
    proc = MagicMock()
    proc.pid = 1
    sm.services["news_ingestion"] = proc
    with patch("os.kill", MagicMock()):
        sm.handle_shutdown(signal_module.SIGTERM, None)
    assert sm.shutdown_event.is_set()
    proc.terminate.assert_called_once()


def test_main(monkeypatch):
    handlers = {}

    def fake_run_service_cycle(name):
        return {"ok": True, "name": name}

    fake_manager = MagicMock()
    fake_manager.service_configs = {"news_ingestion": {"interval": 300}, "signal_generator": {"interval": 60}}
    fake_manager.run_service_cycle.side_effect = fake_run_service_cycle
    fake_manager.shutdown_event.is_set.side_effect = [False, True]
    good_proc = MagicMock()
    bad_proc = MagicMock()
    bad_proc.terminate.side_effect = RuntimeError("already gone")
    fake_manager.services = {
        "news_ingestion": good_proc,
        "signal_generator": bad_proc,
        "ghost": None,
    }

    with patch("pipeline_launcher.ServiceManager", return_value=fake_manager):
        with patch.object(signal_module, "signal", lambda sig, h: handlers.update({sig: h})):
            with patch.object(m.time, "sleep", lambda s: None):
                m.main()
    assert handlers  # signal handlers registered
    assert fake_manager.run_service_cycle.call_count == len(fake_manager.service_configs)
    assert good_proc.terminate.called


def test_main_keyboard_interrupt(monkeypatch):
    handlers = {}

    def fake_run_service_cycle(name):
        return {"ok": True, "name": name}

    fake_manager = MagicMock()
    fake_manager.service_configs = {"news_ingestion": {"interval": 300}}
    fake_manager.run_service_cycle.side_effect = fake_run_service_cycle
    fake_manager.services = {}
    fake_manager.shutdown_event.is_set.return_value = False

    with patch("pipeline_launcher.ServiceManager", return_value=fake_manager):
        with patch.object(signal_module, "signal", lambda sig, h: handlers.update({sig: h})):
            with patch.object(m.time, "sleep", side_effect=KeyboardInterrupt):
                m.main()
    assert handlers
