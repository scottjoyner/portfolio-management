import pytest

from core.config.settings import Settings, TradingMode


def test_from_env_parses_boolean_variants(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOW_LATENCY_MODE", "YES")
    monkeypatch.setenv("GPU_ENABLED", "0")
    monkeypatch.setenv("REQUIRE_APPROVALS", "on")

    settings = Settings.from_env()

    assert settings.low_latency_mode is True
    assert settings.gpu_enabled is False
    assert settings.require_approvals is True


def test_from_env_rejects_invalid_boolean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GPU_ENABLED", "sometimes")

    with pytest.raises(ValueError, match="Invalid boolean"):
        Settings.from_env()


def test_live_mode_requires_toggle() -> None:
    with pytest.raises(ValueError, match="Live mode requested"):
        Settings(trading_mode=TradingMode.LIVE_SEMI_AUTO, live_trading_enabled=False)


def test_canary_mode_requires_rollout() -> None:
    with pytest.raises(ValueError, match="CANARY mode requires"):
        Settings(trading_mode=TradingMode.CANARY, canary_rollout_pct=0)


def test_canary_rollout_restricted_to_canary_mode() -> None:
    with pytest.raises(ValueError, match="only be set"):
        Settings(trading_mode=TradingMode.PAPER, canary_rollout_pct=5)


def test_queue_model_is_normalized() -> None:
    settings = Settings(queue_model="PRO_RATA")
    assert settings.queue_model == "pro_rata"


def test_live_auto_requires_approvals() -> None:
    with pytest.raises(ValueError, match="LIVE_AUTO requires"):
        Settings(trading_mode=TradingMode.LIVE_AUTO, live_trading_enabled=True, require_approvals=False)
