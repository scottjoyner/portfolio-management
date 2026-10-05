"""Extra coverage for coinbase/src/config.py — dataclass fallback + CLI branches."""
from __future__ import annotations

import importlib
import shutil
import sys

import pytest

import coinbase.src.config as cfg
from coinbase.src.config import LiveSafetyValidator


def test_config_dataclass_fallback(monkeypatch):
    """Config must work with pydantic unavailable.

    There is no _HAS_PYDANTIC flag any more: config.py does not import pydantic at
    all (zero occurrences), it is a plain dataclass. So the fallback this test used
    to exercise is now the only path, and the meaningful assertion is simply that
    the dataclass still behaves with pydantic blocked.
    """
    with pytest.MonkeyPatch().context() as mp:
        mp.setitem(sys.modules, "pydantic", None)
        importlib.reload(cfg)
        c = cfg.TradingConfig()
        assert c.mode == "paper"
        assert c.dry_run is True
        assert c.kill_switch is True
        assert c.min_confidence == 0.40
        assert c.regime_atr_stop_mult["high_volatility"] == 3.0
        # from_env with dataclass
        mp.setenv("TRADER_MODE", "live")
        mp.setenv("KILL_SWITCH", "false")
        mp.setenv("LIVE_TRADING_ENABLED", "true")
        c2 = cfg.TradingConfig.from_env()
        assert c2.mode == "live"
        LiveSafetyValidator.check(c2)
    # Restore the module for other tests. There is no pydantic-backed variant to
    # return to -- config.py is a plain dataclass and never imports pydantic -- so
    # assert the reload produced a working module rather than a removed flag.
    importlib.reload(cfg)
    assert cfg.TradingConfig().mode == "paper"


def test_live_safety_cli_not_found(monkeypatch):
    for k in ["KILL_SWITCH", "LIVE_TRADING_ENABLED", "TRADER_MODE", "COINBASE_DRY_RUN",
              "MAX_NOTIONAL_PER_TRADE_USD", "RISK_PER_TRADE_PCT", "MAX_DAILY_LOSS_PCT",
              "COINBASE_API_KEY", "COINBASE_API_SECRET"]:
        monkeypatch.setenv(k, {
            "KILL_SWITCH": "false",
            "LIVE_TRADING_ENABLED": "true",
            "TRADER_MODE": "live",
            "COINBASE_DRY_RUN": "false",
            "MAX_NOTIONAL_PER_TRADE_USD": "100",
            "RISK_PER_TRADE_PCT": "0.01",
            "MAX_DAILY_LOSS_PCT": "3",
            "COINBASE_API_KEY": "k",
            "COINBASE_API_SECRET": "s",
        }[k])
    monkeypatch.setattr(shutil, "which", lambda x: None)
    cfg2 = cfg.TradingConfig.from_env()
    issues = LiveSafetyValidator.check(cfg2)
    assert any("not found" in i for i in issues)
    # CLI found branch
    monkeypatch.setattr(shutil, "which", lambda x: "/bin/coinbase")
    issues2 = LiveSafetyValidator.check(cfg2)
    assert not any("not found" in i for i in issues2)
