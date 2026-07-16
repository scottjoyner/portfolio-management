import os
import pytest

from coinbase.src import config
from coinbase.src.config import (
    TradingConfig,
    LiveSafetyValidator,
    KillSwitch,
    is_kill_switch_active,
    validate_opportunity_side,
    TRUE_VALUES,
    FALSE_VALUES,
    _env_bool,
    _env_float,
    _env_int,
)


def test_true_false_values():
    assert "true" in TRUE_VALUES
    assert "no" in FALSE_VALUES


@pytest.mark.parametrize("raw,expected", [
    (None, False),
    ("", False),
    ("true", True),
    ("YES", True),
    ("0", False),
    ("off", False),
    ("bogus", False),
])
def test_env_bool(raw, expected):
    if raw is None:
        os.environ.pop("TEST_BOOL", None)
    else:
        os.environ["TEST_BOOL"] = raw
    assert _env_bool("TEST_BOOL", False) == expected
    os.environ.pop("TEST_BOOL", None)


def test_env_float_and_int_fallbacks():
    os.environ.pop("TEST_FLOAT", None)
    os.environ.pop("TEST_INT", None)
    assert _env_float("TEST_FLOAT", 1.5) == 1.5
    assert _env_int("TEST_INT", 7) == 7
    os.environ["TEST_FLOAT"] = "notanumber"
    os.environ["TEST_INT"] = "notanumber"
    assert _env_float("TEST_FLOAT", 1.0) == 1.0
    assert _env_int("TEST_INT", 1) == 1
    os.environ["TEST_FLOAT"] = "3.5"
    os.environ["TEST_INT"] = "9"
    assert _env_float("TEST_FLOAT", 1.0) == 3.5
    assert _env_int("TEST_INT", 1) == 9
    os.environ.pop("TEST_FLOAT", None)
    os.environ.pop("TEST_INT", None)


def test_trading_config_defaults():
    cfg = TradingConfig()
    assert cfg.mode == "paper"
    assert cfg.dry_run is True
    assert cfg.kill_switch is True
    assert cfg.min_confidence == 0.40
    assert cfg.regime_atr_stop_mult["high_volatility"] == 3.0
    assert cfg.regime_atr_target_mult["trending_bullish"] == 4.5


def test_trading_config_from_env(monkeypatch):
    monkeypatch.setenv("TRADER_MODE", "live")
    monkeypatch.setenv("KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("PAPER_MIN_CONFIDENCE", "0.66")
    monkeypatch.setenv("MAX_NOTIONAL_PER_TRADE_USD", "250")
    cfg = TradingConfig.from_env()
    assert cfg.mode == "live"
    assert cfg.kill_switch is False
    assert cfg.live_trading_enabled is True
    assert cfg.min_confidence == 0.66
    assert cfg.max_notional_per_trade_usd == 250.0


def test_from_env_invalid_mode(monkeypatch):
    monkeypatch.setenv("TRADER_MODE", "bogus")
    with pytest.raises(ValueError):
        TradingConfig.from_env()


def test_live_safety_validator_paper_ok():
    cfg = TradingConfig()
    issues = LiveSafetyValidator.check(cfg)
    assert isinstance(issues, list)


def test_live_safety_validator_live_problems(monkeypatch):
    monkeypatch.setenv("KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("TRADER_MODE", "live")
    monkeypatch.setenv("COINBASE_DRY_RUN", "true")
    monkeypatch.setenv("MAX_NOTIONAL_PER_TRADE_USD", "100")
    monkeypatch.setenv("RISK_PER_TRADE_PCT", "0.01")
    monkeypatch.setenv("MAX_DAILY_LOSS_PCT", "3")
    cfg = TradingConfig.from_env()
    issues = LiveSafetyValidator.check(cfg)
    joined = "\n".join(issues)
    assert "preview-only" in joined or "COINBASE_DRY_RUN" in joined


def test_live_safety_validator_param_errors(monkeypatch):
    monkeypatch.setenv("KILL_SWITCH", "false")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("TRADER_MODE", "approval")
    monkeypatch.setenv("COINBASE_API_KEY", "")
    monkeypatch.setenv("COINBASE_API_SECRET", "")
    monkeypatch.setenv("MAX_NOTIONAL_PER_TRADE_USD", "0")
    monkeypatch.setenv("RISK_PER_TRADE_PCT", "0.9")
    monkeypatch.setenv("MAX_DAILY_LOSS_PCT", "90")
    cfg = TradingConfig.from_env()
    issues = LiveSafetyValidator.check(cfg)
    joined = "\n".join(issues)
    assert "MAX_NOTIONAL_PER_TRADE_USD" in joined
    assert "RISK_PER_TRADE_PCT" in joined
    assert "MAX_DAILY_LOSS_PCT" in joined


def test_assert_safe_raises(monkeypatch):
    cfg = TradingConfig()
    with pytest.raises(RuntimeError):
        LiveSafetyValidator.assert_safe(cfg)


def test_kill_switch_env_active(monkeypatch, tmp_path):
    monkeypatch.setenv("KILL_SWITCH", "true")
    assert KillSwitch.is_active() is True


def test_kill_switch_file(monkeypatch, tmp_path):
    monkeypatch.setenv("KILL_SWITCH", "false")
    p = tmp_path / "ks"
    monkeypatch.setenv("TRADER_KILL_SWITCH_PATH", str(p))
    assert KillSwitch.is_active() is False
    p.touch()
    assert KillSwitch.is_active() is True
    p.unlink()
    assert KillSwitch.is_active() is False
    assert p.exists() is False


def test_is_kill_switch_active_canonical_env(monkeypatch, tmp_path):
    # Canonical env var is the single source of truth for all execution paths.
    monkeypatch.setenv("KILL_SWITCH", "false")
    p = tmp_path / "ks"
    monkeypatch.setenv("TRADER_KILL_SWITCH_PATH", str(p))
    # Env false + no file -> inactive
    assert is_kill_switch_active() is False
    # Env true overrides everything (file-based divergence no longer matters)
    monkeypatch.setenv("KILL_SWITCH", "true")
    assert is_kill_switch_active() is True
    # KillSwitch.is_active() must agree with the shared helper
    assert KillSwitch.is_active() == is_kill_switch_active()


def test_is_kill_switch_active_file_fallback(monkeypatch, tmp_path):
    monkeypatch.setenv("KILL_SWITCH", "false")
    p = tmp_path / "ks"
    monkeypatch.setenv("TRADER_KILL_SWITCH_PATH", str(p))
    assert is_kill_switch_active() is False
    p.touch()
    assert is_kill_switch_active() is True
    os.remove(str(p))
    assert is_kill_switch_active() is False


def test_validate_opportunity_side_ok():
    assert validate_opportunity_side("BUY") == "BUY"
    assert validate_opportunity_side("sell") == "SELL"
    assert validate_opportunity_side(" BUY ") == "BUY"


def test_validate_opportunity_side_rejects_pair():
    # Bug #46: side="PAIR" must never reach live order placement.
    import pytest
    with pytest.raises(ValueError):
        validate_opportunity_side("PAIR")
    with pytest.raises(ValueError):
        validate_opportunity_side("pair")
    with pytest.raises(ValueError):
        validate_opportunity_side(None)
    with pytest.raises(ValueError):
        validate_opportunity_side("LONG")

