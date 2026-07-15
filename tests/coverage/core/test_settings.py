"""Tests for trading_system.core.config.settings."""

import os
from unittest.mock import patch

import pytest

from trading_system.core.config import settings as settings_mod
from trading_system.core.config.settings import (
    Settings,
    TradingMode,
    _parse_bool_env,
)


def test_enum_values():
    assert TradingMode.SIMULATION.value == "SIMULATION"
    assert TradingMode.PAPER.value == "PAPER"
    assert TradingMode.LIVE_AUTO.value == "LIVE_AUTO"
    assert TradingMode.CANARY.value == "CANARY"


def test_parse_bool_none_returns_default():
    with patch.dict(os.environ, {}, clear=True):
        assert _parse_bool_env("NOPE", True) is True
        assert _parse_bool_env("NOPE", False) is False


def test_parse_bool_true_values():
    for v in ["1", "true", "TRUE", "True", "yes", "Y", "on", "  On "]:
        with patch.dict(os.environ, {"X": v}, clear=True):
            assert _parse_bool_env("X", False) is True


def test_parse_bool_false_values():
    for v in ["0", "false", "NO", "n", "off", "  OFF "]:
        with patch.dict(os.environ, {"X": v}, clear=True):
            assert _parse_bool_env("X", True) is False


def test_parse_bool_invalid_raises():
    with patch.dict(os.environ, {"X": "maybe"}, clear=True):
        with pytest.raises(ValueError):
            _parse_bool_env("X", True)


def test_default_settings():
    s = Settings()
    assert s.app_env == "dev"
    assert s.trading_mode is TradingMode.PAPER
    assert s.require_approvals is True
    assert s.live_trading_enabled is False
    assert s.canary_rollout_pct == 0.0


def test_from_env_defaults():
    with patch.dict(os.environ, {}, clear=True):
        s = Settings.from_env()
    assert s.app_env == "dev"
    assert s.trading_mode is TradingMode.PAPER
    assert s.live_trading_enabled is False
    assert s.require_approvals is True
    assert s.queue_model == "simple"


def test_from_env_overrides():
    env = {
        "APP_ENV": "prod",
        "TRADING_MODE": "SHADOW",
        "COINBASE_API_KEY": "k",
        "COINBASE_API_SECRET": "s",
        "COINBASE_PASSPHRASE": "p",
        "COINBASE_PORTFOLIO_IDS": "pid",
        "DATABASE_URL": "postgresql://x",
        "REDIS_URL": "redis://x",
        "REQUIRE_APPROVALS": "false",
        "LIVE_TRADING_ENABLED": "true",
        "LOW_LATENCY_MODE": "yes",
        "GPU_ENABLED": "1",
        "QUEUE_MODEL": "Priority",
    }
    with patch.dict(os.environ, env, clear=True):
        s = Settings.from_env()
    assert s.app_env == "prod"
    assert s.trading_mode is TradingMode.SHADOW
    assert s.coinbase_api_key == "k"
    assert s.require_approvals is False
    assert s.live_trading_enabled is True
    assert s.low_latency_mode is True
    assert s.gpu_enabled is True
    assert s.queue_model == "priority"
    assert s.canary_rollout_pct == 0.0


def test_from_env_canary():
    env = {
        "TRADING_MODE": "CANARY",
        "LIVE_TRADING_ENABLED": "true",
        "CANARY_ROLLOUT_PCT": "12.5",
    }
    with patch.dict(os.environ, env, clear=True):
        s = Settings.from_env()
    assert s.trading_mode is TradingMode.CANARY
    assert s.live_trading_enabled is True
    assert s.canary_rollout_pct == 12.5


def test_from_env_canary_pct_invalid():
    env = {
        "TRADING_MODE": "CANARY",
        "LIVE_TRADING_ENABLED": "true",
        "CANARY_ROLLOUT_PCT": "0",
    }
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(ValueError):
            Settings.from_env()


def test_from_env_queue_model_invalid():
    with patch.dict(os.environ, {"QUEUE_MODEL": "weird"}, clear=True):
        with pytest.raises(ValueError):
            Settings.from_env()


def test_queue_model_validator_lowercases():
    s = Settings(queue_model="PRIORITY")
    assert s.queue_model == "priority"


def test_queue_model_validator_invalid():
    with pytest.raises(ValueError):
        Settings(queue_model="nope")


def test_safety_live_mode_without_live_trading():
    with pytest.raises(ValueError):
        Settings(trading_mode=TradingMode.LIVE_APPROVAL_REQUIRED, live_trading_enabled=False)
    with pytest.raises(ValueError):
        Settings(trading_mode=TradingMode.LIVE_SEMI_AUTO, live_trading_enabled=False)
    with pytest.raises(ValueError):
        Settings(trading_mode=TradingMode.LIVE_AUTO, live_trading_enabled=False)


def test_safety_live_auto_requires_approvals():
    with pytest.raises(ValueError):
        Settings(
            trading_mode=TradingMode.LIVE_AUTO,
            live_trading_enabled=True,
            require_approvals=False,
        )


def test_safety_live_auto_valid():
    s = Settings(
        trading_mode=TradingMode.LIVE_AUTO,
        live_trading_enabled=True,
        require_approvals=True,
    )
    assert s.trading_mode is TradingMode.LIVE_AUTO


def test_safety_canary_requires_positive_pct():
    with pytest.raises(ValueError):
        Settings(trading_mode=TradingMode.CANARY, live_trading_enabled=True, canary_rollout_pct=0)
    with pytest.raises(ValueError):
        Settings(trading_mode=TradingMode.CANARY, live_trading_enabled=True, canary_rollout_pct=-1)


def test_safety_canary_valid():
    s = Settings(trading_mode=TradingMode.CANARY, live_trading_enabled=True, canary_rollout_pct=5)
    assert s.canary_rollout_pct == 5


def test_safety_pct_set_without_canary_raises():
    with pytest.raises(ValueError):
        Settings(trading_mode=TradingMode.PAPER, canary_rollout_pct=5)


def test_safety_paper_with_zero_pct_ok():
    s = Settings(trading_mode=TradingMode.PAPER, canary_rollout_pct=0)
    assert s.trading_mode is TradingMode.PAPER
