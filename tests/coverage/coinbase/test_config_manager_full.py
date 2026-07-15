"""Coverage tests for coinbase/src/config_manager.py"""
from __future__ import annotations

import os
import threading
import time
from types import SimpleNamespace

import pytest

import coinbase.src.config_manager as cm


def _make_manager(tmp_path, yaml_text=None):
    path = tmp_path / "app.yaml"
    if yaml_text is not None:
        path.write_text(yaml_text)
    return cm.ConfigManager(config_path=str(path))


def test_default_config(tmp_path):
    mgr = _make_manager(tmp_path)
    cfg = mgr.get()
    assert cfg.mode == "paper"
    assert cfg.feature_flags.enable_paper_trading is True
    mgr.stop_watcher()


def test_merge_overrides(tmp_path):
    yaml_text = """
mode: live
scan:
  minute_scan_interval: 120
  minute_scan_top_n: 200
risk:
  max_portfolio_drawdown_pct: 25.0
  max_single_asset_pct: 20.0
feature_flags:
  enable_live_trading: true
unknown_key: 1
"""
    mgr = _make_manager(tmp_path, yaml_text)
    cfg = mgr.get()
    assert cfg.mode == "live"
    assert cfg.scan.minute_scan_interval == 120
    assert cfg.scan.minute_scan_top_n == 200
    assert cfg.risk.max_portfolio_drawdown_pct == 25.0
    assert cfg.risk.max_single_asset_pct == 20.0
    assert cfg.feature_flags.enable_live_trading is True
    mgr.stop_watcher()


def test_env_overrides(tmp_path, monkeypatch):
    for k in ("TRADING_MODE", "ENABLE_LIVE_TRADING", "MINUTE_SCAN_INTERVAL",
              "MAX_PORTFOLIO_DRAWDOWN", "PAPER_MIN_CONFIDENCE", "HEALTH_PORT",
              "LOG_LEVEL", "REQUIRE_APPROVAL", "DRY_RUN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TRADING_MODE", "approval")
    monkeypatch.setenv("ENABLE_LIVE_TRADING", "true")
    monkeypatch.setenv("MINUTE_SCAN_INTERVAL", "90")
    monkeypatch.setenv("MAX_PORTFOLIO_DRAWDOWN", "40")
    monkeypatch.setenv("PAPER_MIN_CONFIDENCE", "0.77")
    monkeypatch.setenv("HEALTH_PORT", "7777")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("REQUIRE_APPROVAL", "false")
    monkeypatch.setenv("DRY_RUN", "false")
    mgr = _make_manager(tmp_path)
    cfg = mgr.get()
    assert cfg.mode == "approval"
    assert cfg.feature_flags.enable_live_trading is True
    assert cfg.scan.minute_scan_interval == 90
    assert cfg.risk.max_portfolio_drawdown_pct == 40.0
    assert cfg.thresholds.paper_min_confidence == 0.77
    assert cfg.health_port == 7777
    assert cfg.log_level == "DEBUG"
    assert cfg.live.require_approval is False
    assert cfg.live.dry_run is False
    mgr.stop_watcher()


def test_env_override_error_branch(tmp_path, monkeypatch):
    monkeypatch.setenv("MINUTE_SCAN_INTERVAL", "notanint")
    mgr = _make_manager(tmp_path)
    # falls back to default without raising
    assert mgr.get().scan.minute_scan_interval == 60
    mgr.stop_watcher()


def test_feature_get_set(tmp_path):
    mgr = _make_manager(tmp_path)
    assert mgr.get_feature("enable_paper_trading") is True
    mgr.set_feature("enable_live_trading", True)
    assert mgr.get_feature("enable_live_trading") is True
    assert mgr.get_feature("nonexistent") is False
    mgr.stop_watcher()


def test_callbacks_and_reload(tmp_path):
    yaml_text = "mode: paper\n"
    mgr = _make_manager(tmp_path, yaml_text)
    calls = []
    mgr.register_callback(lambda old, new: calls.append((old, new)))
    # modify file and trigger reload
    (tmp_path / "app.yaml").write_text("mode: live\n")
    mgr._last_modified = 0.0
    mgr._load()
    assert len(calls) == 1
    assert mgr.get().mode == "live"
    # callback that raises is swallowed
    mgr.register_callback(lambda old, new: (_ for _ in ()).throw(RuntimeError("x")))
    mgr._load()
    mgr.stop_watcher()


def test_save_and_to_dict(tmp_path):
    mgr = _make_manager(tmp_path)
    mgr.get().mode = "live"
    mgr.save()
    assert (tmp_path / "app.yaml").exists()
    d = mgr._config_to_dict(mgr.get())
    assert "mode" in d and "scan" in d
    mgr.stop_watcher()


def test_watcher_start_idempotent_and_stop(tmp_path):
    mgr = _make_manager(tmp_path)
    mgr.start_watcher()  # already started -> returns
    mgr.stop_watcher()
    assert mgr._watching is False


def test_watch_loop_reloads(tmp_path, monkeypatch):
    mgr = _make_manager(tmp_path, "mode: paper\n")

    def fake_sleep(s):
        mgr._watching = False  # exit after one iteration

    monkeypatch.setattr(time, "sleep", fake_sleep)
    # make file appear changed
    mgr._last_modified = 0.0
    (tmp_path / "app.yaml").write_text("mode: live\n")
    t = threading.Thread(target=mgr._watch_loop, args=(5.0,))
    t.start()
    # Poll for the reload rather than relying on a fixed join timeout
    for _ in range(50):
        if mgr.get().mode == "live":
            break
        time.sleep(0.05)
    assert mgr.get().mode == "live"
    t.join(timeout=2)


def test_empty_yaml_uses_defaults(tmp_path):
    # yaml.safe_load("") -> None -> `if data:` branch not taken
    path = tmp_path / "app.yaml"
    path.write_text("")
    mgr = cm.ConfigManager(config_path=str(path))
    assert mgr.get().mode == "paper"
    mgr.stop_watcher()


def test_set_feature_unknown(tmp_path):
    mgr = _make_manager(tmp_path)
    # unknown feature flag name -> no-op, branches to early return
    mgr.set_feature("does_not_exist", True)
    assert mgr.get_feature("does_not_exist") is False
    mgr.stop_watcher()


def test_stop_watcher_without_thread(tmp_path):
    mgr = _make_manager(tmp_path)
    mgr._watcher_thread = None
    mgr.stop_watcher()  # `if self._watcher_thread:` False branch
    assert mgr._watching is False


def test_watch_loop_exception(tmp_path, monkeypatch):
    mgr = _make_manager(tmp_path, "mode: paper\n")

    def boom(*a, **k):
        raise OSError("stat failed")

    monkeypatch.setattr(type(mgr.config_path), "stat", boom)
    monkeypatch.setattr(type(mgr.config_path), "exists", lambda self: True)

    def fake_sleep(s):
        mgr._watching = False  # exit after exception handled

    monkeypatch.setattr(time, "sleep", fake_sleep)
    mgr._watch_loop(0.01)  # exception is caught internally
    assert mgr._watching is False


def test_merge_dataclass_nested_and_dict(tmp_path):
    from dataclasses import dataclass, field

    @dataclass
    class Inner:
        x: int = 1

    @dataclass
    class Outer:
        inner: Inner = field(default_factory=Inner)
        mapping: dict = field(default_factory=dict)

    mgr = _make_manager(tmp_path)
    base = Outer()
    overrides = {"inner": {"x": 5}, "mapping": {"a": 1}}
    merged = mgr._merge_dataclass(base, overrides)
    assert merged.inner.x == 5
    assert merged.mapping == {"a": 1}
    mgr.stop_watcher()


def test_global_getters_reuse(tmp_path, monkeypatch):
    # reset global singleton so get_config_manager creates it
    monkeypatch.setattr(cm, "_CONFIG_MANAGER", None)
    mgr = cm.get_config_manager()  # `if _CONFIG_MANAGER is None:` True branch (creates)
    assert cm.get_config_manager() is mgr  # returns existing instance
    cfg1 = cm.get_config()
    cfg2 = cm.get_config()  # `if _CONFIG_MANAGER is None:` False branch
    assert cfg1 is cfg2
    mgr.stop_watcher()


def test_global_getters(monkeypatch):
    # reset global singleton
    monkeypatch.setattr(cm, "_CONFIG_MANAGER", None)
    cfg = cm.get_config()
    assert cfg.mode in ("paper", "live", "approval")
    mgr = cm.get_config_manager()
    assert isinstance(mgr, cm.ConfigManager)
    # second call returns same
    assert cm.get_config_manager() is mgr
    assert cm.is_feature_enabled("enable_paper_trading") in (True, False)
    mgr.stop_watcher()
