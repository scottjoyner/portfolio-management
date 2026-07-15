"""Tests for coinbase/src/config_manager.py"""
from __future__ import annotations

import os
import threading
import time

import pytest
import yaml

from coinbase.src.config_manager import (
    ConfigManager,
    FeatureFlags,
    ScanConfig,
    RiskConfig,
    ThresholdConfig,
    LiveConfig,
    AppConfig,
    get_config,
    get_config_manager,
    is_feature_enabled,
)


def test_dataclass_defaults():
    ff = FeatureFlags()
    assert ff.enable_paper_trading is True
    sc = ScanConfig()
    assert sc.minute_scan_interval == 60
    rc = RiskConfig()
    assert rc.max_leverage == 1.5
    tc = ThresholdConfig()
    assert tc.paper_min_confidence == 0.55
    lc = LiveConfig()
    assert lc.require_approval is True
    ac = AppConfig()
    assert ac.mode == "paper"


def _write_yaml(tmp_path, data):
    p = tmp_path / "app.yaml"
    p.write_text(yaml.dump(data))
    return str(p)


def test_config_manager_load_defaults(tmp_path):
    cm = ConfigManager(config_path=tmp_path / "missing.yaml")
    cfg = cm.get()
    assert cfg.mode == "paper"
    cm.stop_watcher()


def test_config_manager_load_yaml(tmp_path):
    path = _write_yaml(tmp_path, {
        "mode": "live",
        "feature_flags": {"enable_live_trading": True},
        "risk": {"max_leverage": 3.0},
        "scan": {"minute_scan_interval": 99},
    })
    cm = ConfigManager(config_path=path)
    cfg = cm.get()
    assert cfg.mode == "live"
    assert cfg.feature_flags.enable_live_trading is True
    assert cfg.risk.max_leverage == 3.0
    assert cfg.scan.minute_scan_interval == 99
    cm.stop_watcher()


def test_config_manager_unknown_key(tmp_path):
    path = _write_yaml(tmp_path, {"not_a_key": 1})
    cm = ConfigManager(config_path=path)
    cfg = cm.get()
    assert not hasattr(cfg, "not_a_key")
    cm.stop_watcher()


def test_config_manager_env_overrides(tmp_path, monkeypatch):
    for k in ["TRADING_MODE", "ENABLE_LIVE_TRADING", "MINUTE_SCAN_INTERVAL",
              "MAX_LEVERAGE", "PAPER_MIN_CONFIDENCE", "REQUIRE_APPROVAL",
              "HEALTH_PORT", "LOG_LEVEL", "RISK_PER_TRADE", "ENVIRONMENT"]:
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("TRADING_MODE", "approval")
    monkeypatch.setenv("ENABLE_LIVE_TRADING", "true")
    monkeypatch.setenv("MINUTE_SCAN_INTERVAL", "123")
    monkeypatch.setenv("MAX_LEVERAGE", "2.5")
    monkeypatch.setenv("PAPER_MIN_CONFIDENCE", "0.77")
    monkeypatch.setenv("REQUIRE_APPROVAL", "false")
    monkeypatch.setenv("HEALTH_PORT", "9999")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("RISK_PER_TRADE", "2.0")
    monkeypatch.setenv("ENVIRONMENT", "prod")
    cm = ConfigManager(config_path=tmp_path / "missing.yaml")
    cfg = cm.get()
    assert cfg.mode == "approval"
    assert cfg.feature_flags.enable_live_trading is True
    assert cfg.scan.minute_scan_interval == 123
    assert cfg.risk.max_leverage == 2.5
    assert cfg.thresholds.paper_min_confidence == 0.77
    assert cfg.live.require_approval is False
    assert cfg.health_port == 9999
    assert cfg.log_level == "DEBUG"
    assert cfg.risk.risk_per_trade_pct == 2.0
    assert cfg.environment == "prod"
    cm.stop_watcher()


def test_config_manager_env_override_bad(tmp_path, monkeypatch):
    monkeypatch.setenv("HEALTH_PORT", "notanint")
    cm = ConfigManager(config_path=tmp_path / "missing.yaml")
    # Should not crash; just warn
    cm.stop_watcher()


def test_feature_get_set(tmp_path):
    cm = ConfigManager(config_path=tmp_path / "missing.yaml")
    assert cm.get_feature("enable_paper_trading") is True
    cm.set_feature("enable_paper_trading", False)
    assert cm.get_feature("enable_paper_trading") is False
    cm.stop_watcher()


def test_register_callback(tmp_path):
    cm = ConfigManager(config_path=tmp_path / "missing.yaml")
    calls = []
    cm.register_callback(lambda old, new: calls.append((old, new)))
    cm._load()
    assert calls
    cm.stop_watcher()


def test_save_and_reload(tmp_path):
    path = tmp_path / "app.yaml"
    cm = ConfigManager(config_path=path)
    cm.set_feature("enable_shorts", False)
    cm.save()
    assert path.exists()
    cm2 = ConfigManager(config_path=path)
    assert cm2.get_feature("enable_shorts") is False
    cm2.stop_watcher()


def test_watcher_reload(tmp_path):
    path = _write_yaml(tmp_path, {"mode": "paper"})
    cm = ConfigManager(config_path=path)
    assert cm.get().mode == "paper"
    # Rewrite file
    _write_yaml(tmp_path, {"mode": "live"})
    # force mtime change and trigger
    time.sleep(0.01)
    cm._load()
    assert cm.get().mode == "live"
    cm.stop_watcher()


def test_watcher_loop_runs(tmp_path):
    path = _write_yaml(tmp_path, {"mode": "paper"})
    cm = ConfigManager(config_path=path)
    cm.start_watcher(interval=0.05)
    started = cm._watching
    time.sleep(0.2)
    cm.stop_watcher()
    assert started


def test_global_get_config_and_manager(tmp_path, monkeypatch):
    # Avoid using singleton across tests; reset module globals via fresh ConfigManager
    cm = ConfigManager(config_path=tmp_path / "missing.yaml")
    # get_config() creates its own singleton; just ensure it doesn't crash
    try:
        c = get_config()
        assert c is not None
    finally:
        cm.stop_watcher()
        # stop singleton watcher too
        try:
            get_config_manager().stop_watcher()
        except Exception:
            pass


def test_is_feature_enabled(tmp_path):
    # is_feature_enabled uses global manager; ensure it returns a bool
    try:
        val = is_feature_enabled("enable_paper_trading")
        assert isinstance(val, bool)
    finally:
        try:
            get_config_manager().stop_watcher()
        except Exception:
            pass
