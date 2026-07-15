"""Coverage tests for coinbase/src/config.py"""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from coinbase.src import config


def test_env_helpers(monkeypatch):
    monkeypatch.setenv("X", "true")
    assert config._env_bool("X", False) is True
    monkeypatch.setenv("X", "no")
    assert config._env_bool("X", True) is False
    monkeypatch.delenv("X", raising=False)
    assert config._env_bool("X", True) is True

    monkeypatch.setenv("F", "1.5")
    assert config._env_float("F", 0) == 1.5
    monkeypatch.setenv("F", "bad")
    assert config._env_float("F", 2) == 2
    monkeypatch.delenv("F", raising=False)

    monkeypatch.setenv("I", "3")
    assert config._env_int("I", 0) == 3
    monkeypatch.setenv("I", "bad")
    assert config._env_int("I", 2) == 2
    monkeypatch.delenv("I", raising=False)


def test_trading_config_from_env(monkeypatch):
    env = {
        "TRADER_MODE": "live",
        "COINBASE_DRY_RUN": "false",
        "KILL_SWITCH": "false",
        "LIVE_TRADING_ENABLED": "true",
        "REQUIRE_APPROVALS": "false",
        "PRODUCTS": "BTC-USD,ETH-USD",
        "RISK_PER_TRADE_PCT": "0.02",
        "MAX_NOTIONAL_PER_TRADE_USD": "200",
        "TRADER_MAX_NOTIONAL_PER_TICK": "1000",
        "MAX_POSITIONS": "10",
        "MAX_POSITION_PCT": "0.2",
        "MAX_DAILY_LOSS_PCT": "5",
        "MAX_DRAWDOWN_PCT": "20",
        "MAX_CONSECUTIVE_LOSSES": "7",
        "MIN_RISK_REWARD": "2.0",
        "MIN_EDGE_BPS": "20",
        "PAPER_MIN_CONFIDENCE": "0.6",
        "PAPER_MIN_WIN_RATE": "0.7",
        "PAPER_MIN_SHARPE": "1.0",
        "KELLY_FRACTION": "0.3",
        "BRACKET_STOP_ATR_MULT": "3.0",
        "BRACKET_TARGET_ATR_MULT": "5.0",
        "BREAKEVEN_R_MULTIPLE": "2.0",
        "COINBASE_API_KEY": "k",
        "COINBASE_API_SECRET": "s",
        "COINBASE_CLI_PATH": "coinbase",
        "COINBASE_CLI_ENV": "sandbox",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    cfg = config.TradingConfig.from_env()
    assert cfg.mode == "live"
    assert cfg.dry_run is False
    assert cfg.kill_switch is False
    assert cfg.max_notional_per_trade_usd == 200.0
    assert cfg.max_daily_loss_pct == 0.05
    assert cfg.max_drawdown_pct == 0.20
    assert cfg.products == "BTC-USD,ETH-USD"
    assert cfg.regime_atr_stop_mult["trending"] == 2.0


def test_trading_config_invalid_mode(monkeypatch):
    monkeypatch.setenv("TRADER_MODE", "bogus")
    with pytest.raises(ValueError):
        config.TradingConfig.from_env()


def test_live_safety_validator_issues(monkeypatch):
    monkeypatch.setenv("TRADER_MODE", "approval")
    monkeypatch.setenv("COINBASE_API_KEY", "")
    monkeypatch.setenv("COINBASE_API_SECRET", "")
    # Make CLI appear missing
    import shutil

    monkeypatch.setattr(shutil, "which", lambda c: None)
    cfg = config.TradingConfig.from_env()
    cfg.kill_switch = True
    cfg.live_trading_enabled = False
    issues = config.LiveSafetyValidator.check(cfg)
    assert any("KILL_SWITCH" in i for i in issues)
    assert any("LIVE_TRADING_ENABLED" in i for i in issues)
    assert any("API_KEY" in i for i in issues)
    assert any("CLI" in i for i in issues)

    # out-of-range numeric issues
    cfg.kill_switch = False
    cfg.live_trading_enabled = True
    cfg.max_notional_per_trade_usd = 0
    cfg.risk_per_trade_pct = 0.9
    cfg.max_daily_loss_pct = 0.9
    issues2 = config.LiveSafetyValidator.check(cfg)
    assert any("MAX_NOTIONAL" in i for i in issues2)
    assert any("RISK_PER_TRADE" in i for i in issues2)
    assert any("MAX_DAILY_LOSS" in i for i in issues2)


def test_live_safety_validator_cli_found(monkeypatch):
    monkeypatch.setenv("TRADER_MODE", "approval")
    monkeypatch.setenv("COINBASE_API_KEY", "k")
    monkeypatch.setenv("COINBASE_API_SECRET", "s")
    import shutil

    monkeypatch.setattr(shutil, "which", lambda c: "/usr/bin/coinbase")
    cfg = config.TradingConfig.from_env()
    cfg.kill_switch = False
    cfg.live_trading_enabled = True
    cfg.mode = "live"
    cfg.dry_run = False
    issues = config.LiveSafetyValidator.check(cfg)
    # No cli-found / key issues; only valid config
    assert not any("CLI" in i for i in issues)
    assert not any("API_KEY" in i for i in issues)


def test_assert_safe(monkeypatch):
    cfg = config.TradingConfig.from_env()
    cfg.kill_switch = False
    cfg.live_trading_enabled = True
    config.LiveSafetyValidator.assert_safe(cfg)  # no issues -> returns


def test_assert_safe_raises(monkeypatch):
    cfg = config.TradingConfig.from_env()
    cfg.kill_switch = True
    with pytest.raises(RuntimeError):
        config.LiveSafetyValidator.assert_safe(cfg)


def test_kill_switch(tmp_path, monkeypatch):
    kp = tmp_path / "kill_switch"
    monkeypatch.setattr(config.KillSwitch, "KILL_PATH", kp)
    monkeypatch.setenv("KILL_SWITCH", "true")
    assert config.KillSwitch.is_active() is True

    monkeypatch.setenv("KILL_SWITCH", "false")
    assert config.KillSwitch.is_active() is False
    config.KillSwitch.engage()
    assert kp.exists()
    assert config.KillSwitch.is_active() is True
    config.KillSwitch.disengage()
    assert not kp.exists()
    assert config.KillSwitch.is_active() is False


def test_dataclass_fallback_path(monkeypatch):
    """Cover the non-pydantic TradingConfig branch by hiding pydantic."""
    import importlib
    import sys

    monkeypatch.setitem(sys.modules, "pydantic", None)
    try:
        importlib.reload(config)
        cfg = config.TradingConfig.from_env()
        assert cfg.mode == "paper"
        assert cfg.max_positions == 30
    finally:
        monkeypatch.delitem(sys.modules, "pydantic", raising=False)
        importlib.reload(config)
