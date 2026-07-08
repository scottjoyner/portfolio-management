from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

log = logging.getLogger(__name__)

TRUE_VALUES = {"1", "true", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "no", "n", "off"}

_HAS_PYDANTIC = False
try:
    from pydantic import BaseModel, Field, field_validator
    _HAS_PYDANTIC = True
except ImportError:
    BaseModel = object
    def Field(default=None, **kw): return default
    def field_validator(*a, **kw): return lambda f: f


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in TRUE_VALUES


def _env_float(name: str, default: float = 0.0) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


def _env_int(name: str, default: int = 0) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


if _HAS_PYDANTIC:
    from pydantic import BaseModel as _BM
    class TradingConfig(_BM):
        """Runtime trading configuration — overridable via env vars."""
        mode: str = Field(default="paper")
        dry_run: bool = Field(default=True)
        kill_switch: bool = Field(default=True)
        live_trading_enabled: bool = Field(default=False)
        require_approvals: bool = Field(default=True)
        products: str = Field(default="")
        risk_per_trade_pct: float = Field(default=0.01)
        max_notional_per_trade_usd: float = Field(default=100.0)
        max_notional_per_tick_usd: float = Field(default=500.0)
        max_positions: int = Field(default=6)
        max_position_pct: float = Field(default=0.10)
        max_daily_loss_pct: float = Field(default=0.03)
        max_drawdown_pct: float = Field(default=0.10)
        max_consecutive_losses: int = Field(default=5)
        min_risk_reward: float = Field(default=1.5)
        min_edge_bps: float = Field(default=15.0)
        min_confidence: float = Field(default=0.40)
        min_win_rate: float = Field(default=0.50)
        min_sharpe: float = Field(default=0.5)
        kelly_fraction: float = Field(default=0.25)
        bracket_stop_atr_mult: float = Field(default=2.5)
        bracket_target_atr_mult: float = Field(default=4.0)
        breakeven_r_multiple: float = Field(default=1.5)
        # Regime-specific ATR multipliers (override base when regime detected)
        regime_atr_stop_mult: Dict[str, float] = Field(default_factory=lambda: {
            "high_volatility": 3.0,
            "ranging": 1.5,
            "trending": 2.0,
            "trending_bullish": 2.0,
            "trending_bearish": 2.5,
        })
        regime_atr_target_mult: Dict[str, float] = Field(default_factory=lambda: {
            "high_volatility": 5.0,
            "ranging": 2.5,
            "trending": 4.0,
            "trending_bullish": 4.5,
            "trending_bearish": 3.5,
        })
        coinbase_api_key: str = ""
        coinbase_api_secret: str = ""
        coinbase_cli_path: str = Field(default="coinbase")
        coinbase_cli_env: str = Field(default="live")
        products_override: Optional[List[str]] = None

        @field_validator("mode")
        @classmethod
        def _validate_mode(cls, v: str) -> str:
            v = v.lower().strip()
            if v not in ("paper", "approval", "live"):
                raise ValueError(f"mode must be paper|approval|live, got {v!r}")
            return v

        @classmethod
        def from_env(cls) -> TradingConfig:
            return cls(
                mode=os.getenv("TRADER_MODE", "paper"),
                dry_run=_env_bool("COINBASE_DRY_RUN", True),
                kill_switch=_env_bool("KILL_SWITCH", True),
                live_trading_enabled=_env_bool("LIVE_TRADING_ENABLED", False),
                require_approvals=_env_bool("REQUIRE_APPROVALS", True),
                products=os.getenv("PRODUCTS", ""),
                risk_per_trade_pct=float(os.getenv("RISK_PER_TRADE_PCT", "0.01")),
                max_notional_per_trade_usd=float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "100")),
                max_notional_per_tick_usd=float(os.getenv("TRADER_MAX_NOTIONAL_PER_TICK", "500")),
                max_positions=int(os.getenv("MAX_POSITIONS", "30")),
                max_position_pct=float(os.getenv("MAX_POSITION_PCT", "0.10")),
                max_daily_loss_pct=float(os.getenv("MAX_DAILY_LOSS_PCT", "3")) / 100.0,
                max_drawdown_pct=float(os.getenv("MAX_DRAWDOWN_PCT", "10")) / 100.0,
                max_consecutive_losses=int(os.getenv("MAX_CONSECUTIVE_LOSSES", "5")),
                min_risk_reward=float(os.getenv("MIN_RISK_REWARD", "1.5")),
                min_edge_bps=float(os.getenv("MIN_EDGE_BPS", "15")),
                min_confidence=float(os.getenv("PAPER_MIN_CONFIDENCE", "0.40")),
                min_win_rate=float(os.getenv("PAPER_MIN_WIN_RATE", "0.50")),
                min_sharpe=float(os.getenv("PAPER_MIN_SHARPE", "0.5")),
                kelly_fraction=float(os.getenv("KELLY_FRACTION", "0.25")),
                bracket_stop_atr_mult=float(os.getenv("BRACKET_STOP_ATR_MULT", "2.5")),
                bracket_target_atr_mult=float(os.getenv("BRACKET_TARGET_ATR_MULT", "4.0")),
                breakeven_r_multiple=float(os.getenv("BREAKEVEN_R_MULTIPLE", "1.5")),
                coinbase_api_key=os.getenv("COINBASE_API_KEY", ""),
                coinbase_api_secret=os.getenv("COINBASE_API_SECRET", ""),
                coinbase_cli_path=os.getenv("COINBASE_CLI_PATH", "coinbase"),
                coinbase_cli_env=os.getenv("COINBASE_CLI_ENV", "live"),
            )
else:
    @dataclass
    class TradingConfig:
        mode: str = "paper"
        dry_run: bool = True
        kill_switch: bool = True
        live_trading_enabled: bool = False
        require_approvals: bool = True
        products: str = ""
        risk_per_trade_pct: float = 0.01
        max_notional_per_trade_usd: float = 100.0
        max_notional_per_tick_usd: float = 500.0
        max_positions: int = 6
        max_position_pct: float = 0.10
        max_daily_loss_pct: float = 0.03
        max_drawdown_pct: float = 0.10
        max_consecutive_losses: int = 5
        min_risk_reward: float = 1.5
        min_edge_bps: float = 15.0
        min_confidence: float = 0.40
        min_win_rate: float = 0.50
        min_sharpe: float = 0.5
        kelly_fraction: float = 0.25
        bracket_stop_atr_mult: float = 2.5
        bracket_target_atr_mult: float = 4.0
        breakeven_r_multiple: float = 1.5
        regime_atr_stop_mult: Dict[str, float] = field(default_factory=lambda: {
            "high_volatility": 3.0,
            "ranging": 1.5,
            "trending": 2.0,
            "trending_bullish": 2.0,
            "trending_bearish": 2.5,
        })
        regime_atr_target_mult: Dict[str, float] = field(default_factory=lambda: {
            "high_volatility": 5.0,
            "ranging": 2.5,
            "trending": 4.0,
            "trending_bullish": 4.5,
            "trending_bearish": 3.5,
        })
        coinbase_api_key: str = ""
        coinbase_api_secret: str = ""
        coinbase_cli_path: str = "coinbase"
        coinbase_cli_env: str = "live"
        products_override: Optional[List[str]] = None

        @classmethod
        def from_env(cls) -> TradingConfig:
            return cls(
                mode=os.getenv("TRADER_MODE", "paper"),
                dry_run=_env_bool("COINBASE_DRY_RUN", True),
                kill_switch=_env_bool("KILL_SWITCH", True),
                live_trading_enabled=_env_bool("LIVE_TRADING_ENABLED", False),
                require_approvals=_env_bool("REQUIRE_APPROVALS", True),
                products=os.getenv("PRODUCTS", ""),
                risk_per_trade_pct=float(os.getenv("RISK_PER_TRADE_PCT", "0.01")),
                max_notional_per_trade_usd=float(os.getenv("MAX_NOTIONAL_PER_TRADE_USD", "100")),
                max_notional_per_tick_usd=float(os.getenv("TRADER_MAX_NOTIONAL_PER_TICK", "500")),
                max_positions=int(os.getenv("MAX_POSITIONS", "30")),
                max_position_pct=float(os.getenv("MAX_POSITION_PCT", "0.10")),
                max_daily_loss_pct=float(os.getenv("MAX_DAILY_LOSS_PCT", "3")) / 100.0,
                max_drawdown_pct=float(os.getenv("MAX_DRAWDOWN_PCT", "10")) / 100.0,
                max_consecutive_losses=int(os.getenv("MAX_CONSECUTIVE_LOSSES", "5")),
                min_risk_reward=float(os.getenv("MIN_RISK_REWARD", "1.5")),
                min_edge_bps=float(os.getenv("MIN_EDGE_BPS", "15")),
                min_confidence=float(os.getenv("PAPER_MIN_CONFIDENCE", "0.40")),
                min_win_rate=float(os.getenv("PAPER_MIN_WIN_RATE", "0.50")),
                min_sharpe=float(os.getenv("PAPER_MIN_SHARPE", "0.5")),
                kelly_fraction=float(os.getenv("KELLY_FRACTION", "0.25")),
                bracket_stop_atr_mult=float(os.getenv("BRACKET_STOP_ATR_MULT", "2.5")),
                bracket_target_atr_mult=float(os.getenv("BRACKET_TARGET_ATR_MULT", "4.0")),
                breakeven_r_multiple=float(os.getenv("BREAKEVEN_R_MULTIPLE", "1.5")),
                coinbase_api_key=os.getenv("COINBASE_API_KEY", ""),
                coinbase_api_secret=os.getenv("COINBASE_API_SECRET", ""),
                coinbase_cli_path=os.getenv("COINBASE_CLI_PATH", "coinbase"),
                coinbase_cli_env=os.getenv("COINBASE_CLI_ENV", "live"),
            )


class LiveSafetyValidator:
    """Validates that the system is safe to go live."""

    @staticmethod
    def check(config: TradingConfig) -> List[str]:
        issues: List[str] = []

        if config.kill_switch:
            issues.append("KILL_SWITCH is active — set KILL_SWITCH=false to enable live trading")
        if not config.live_trading_enabled:
            issues.append("LIVE_TRADING_ENABLED is false — set to true for live mode")
        if config.mode == "live" and config.dry_run:
            issues.append("mode=live but COINBASE_DRY_RUN=true — orders will be preview-only")

        if config.max_notional_per_trade_usd <= 0:
            issues.append("MAX_NOTIONAL_PER_TRADE_USD must be > 0")
        if config.risk_per_trade_pct <= 0 or config.risk_per_trade_pct > 0.5:
            issues.append(f"RISK_PER_TRADE_PCT={config.risk_per_trade_pct} is out of range (0.001-0.50)")

        if config.max_daily_loss_pct <= 0 or config.max_daily_loss_pct > 0.5:
            issues.append(f"MAX_DAILY_LOSS_PCT={config.max_daily_loss_pct*100:.1f}% is out of range")

        import shutil
        if config.mode in ("live", "approval"):
            if not config.coinbase_api_key or not config.coinbase_api_secret:
                issues.append("COINBASE_API_KEY or COINBASE_API_SECRET not set")
            cli = shutil.which(config.coinbase_cli_path)
            if not cli:
                cli = shutil.which("coinbase")
            if not cli:
                issues.append(f"Coinbase CLI '{config.coinbase_cli_path}' not found on PATH")

        return issues

    @staticmethod
    def assert_safe(config: TradingConfig) -> None:
        issues = LiveSafetyValidator.check(config)
        if issues:
            for i in issues:
                log.error("LIVE SAFETY: %s", i)
            raise RuntimeError(
                f"Live safety checks failed ({len(issues)} issues):\n  "
                + "\n  ".join(issues)
            )


class KillSwitch:
    """File-based kill switch — touch data/trading_kill_switch to halt."""

    KILL_PATH = Path("data/trading_kill_switch")

    @classmethod
    def is_active(cls) -> bool:
        if _env_bool("KILL_SWITCH", True):
            return True
        return cls.KILL_PATH.exists()

    @classmethod
    def engage(cls) -> None:
        cls.KILL_PATH.touch()

    @classmethod
    def disengage(cls) -> None:
        cls.KILL_PATH.unlink(missing_ok=True)
