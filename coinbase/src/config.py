from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

TRUE_VALUES = {"1", "true", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "no", "n", "off"}

KILL_SWITCH_ENV = "KILL_SWITCH"
LEGACY_KILL_SWITCH_ENV = "TRADER_KILL_SWITCH"
KILL_SWITCH_PATH_ENV = "TRADER_KILL_SWITCH_PATH"

VALID_ORDER_SIDES = {"BUY", "SELL"}
EVENT_ARBITRAGE_SIDE = "PAIR"


def _parse_bool(raw: object, *, default: bool = False, fail_closed: bool = False) -> bool:
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in TRUE_VALUES:
        return True
    if value in FALSE_VALUES:
        return False
    return True if fail_closed else default


def _env_bool(name: str, default: bool = False) -> bool:
    return _parse_bool(os.getenv(name), default=default)


def _env_float(name: str, default: float = 0.0) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (ValueError, TypeError):
        return default


def _env_int(name: str, default: int = 0) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except (ValueError, TypeError):
        return default


def _configured_kill_switch_value(default: bool = False) -> bool:
    """Resolve the canonical and legacy kill-switch environment names.

    ``KILL_SWITCH`` is canonical. ``TRADER_KILL_SWITCH`` remains supported for
    older deployment and challenge configurations. An explicitly active value
    on either name wins. Invalid explicit values fail closed.
    """
    explicit_values: list[bool] = []
    for name in (KILL_SWITCH_ENV, LEGACY_KILL_SWITCH_ENV):
        if name in os.environ:
            explicit_values.append(
                _parse_bool(os.environ.get(name), fail_closed=True)
            )
    if not explicit_values:
        return default
    return any(explicit_values)


def is_kill_switch_active() -> bool:
    """Return the unified environment-or-file kill-switch state.

    Environment configuration does not replace the sentinel file: either an
    active environment value or an existing sentinel halts execution. When no
    environment value is supplied, the file is the source of truth rather than
    silently treating every development and paper process as halted.
    """
    if _configured_kill_switch_value(default=False):
        return True

    kill_path = Path(
        os.getenv(KILL_SWITCH_PATH_ENV, str(KillSwitch.KILL_PATH))
    )
    try:
        return kill_path.exists()
    except OSError:
        # An unreadable configured safety boundary is ambiguous, so fail closed.
        return True


@dataclass
class TradingConfig:
    """Runtime trading configuration, overridable through environment values."""

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
    regime_atr_stop_mult: Dict[str, float] = field(
        default_factory=lambda: {
            "high_volatility": 3.0,
            "ranging": 1.5,
            "trending": 2.0,
            "trending_bullish": 2.0,
            "trending_bearish": 2.5,
        }
    )
    regime_atr_target_mult: Dict[str, float] = field(
        default_factory=lambda: {
            "high_volatility": 5.0,
            "ranging": 2.5,
            "trending": 4.0,
            "trending_bullish": 4.5,
            "trending_bearish": 3.5,
        }
    )
    coinbase_api_key: str = ""
    coinbase_api_secret: str = ""
    coinbase_cli_path: str = "coinbase"
    coinbase_cli_env: str = "live"
    products_override: Optional[List[str]] = None

    def __post_init__(self) -> None:
        self.mode = str(self.mode).strip().lower()
        if self.mode not in {"paper", "approval", "live"}:
            raise ValueError(
                f"mode must be paper|approval|live, got {self.mode!r}"
            )
        self.require_approvals = bool(self.require_approvals)

    @classmethod
    def from_env(cls) -> "TradingConfig":
        if KILL_SWITCH_ENV in os.environ or LEGACY_KILL_SWITCH_ENV in os.environ:
            kill_switch = _configured_kill_switch_value(default=True)
        else:
            # Configuration remains fail-safe by default even though the shared
            # runtime helper uses the sentinel file when no env value is present.
            kill_switch = True

        return cls(
            mode=os.getenv("TRADER_MODE", "paper"),
            dry_run=_env_bool("COINBASE_DRY_RUN", True),
            kill_switch=kill_switch,
            live_trading_enabled=_env_bool("LIVE_TRADING_ENABLED", False),
            require_approvals=(
                _env_bool("REQUIRE_APPROVALS", True)
                or _env_bool("REQUIRE_APPROVAL", False)
            ),
            products=os.getenv("PRODUCTS", ""),
            risk_per_trade_pct=_env_float("RISK_PER_TRADE_PCT", 0.01),
            max_notional_per_trade_usd=_env_float(
                "MAX_NOTIONAL_PER_TRADE_USD", 100.0
            ),
            max_notional_per_tick_usd=_env_float(
                "TRADER_MAX_NOTIONAL_PER_TICK", 500.0
            ),
            max_positions=_env_int("MAX_POSITIONS", 30),
            max_position_pct=_env_float("MAX_POSITION_PCT", 0.10),
            max_daily_loss_pct=_env_float("MAX_DAILY_LOSS_PCT", 3.0) / 100.0,
            max_drawdown_pct=_env_float("MAX_DRAWDOWN_PCT", 10.0) / 100.0,
            max_consecutive_losses=_env_int("MAX_CONSECUTIVE_LOSSES", 5),
            min_risk_reward=_env_float("MIN_RISK_REWARD", 1.5),
            min_edge_bps=_env_float("MIN_EDGE_BPS", 15.0),
            min_confidence=_env_float("PAPER_MIN_CONFIDENCE", 0.40),
            min_win_rate=_env_float("PAPER_MIN_WIN_RATE", 0.50),
            min_sharpe=_env_float("PAPER_MIN_SHARPE", 0.5),
            kelly_fraction=_env_float("KELLY_FRACTION", 0.25),
            bracket_stop_atr_mult=_env_float("BRACKET_STOP_ATR_MULT", 2.5),
            bracket_target_atr_mult=_env_float("BRACKET_TARGET_ATR_MULT", 4.0),
            breakeven_r_multiple=_env_float("BREAKEVEN_R_MULTIPLE", 1.5),
            coinbase_api_key=os.getenv("COINBASE_API_KEY", ""),
            coinbase_api_secret=os.getenv("COINBASE_API_SECRET", ""),
            coinbase_cli_path=os.getenv("COINBASE_CLI_PATH", "coinbase"),
            coinbase_cli_env=os.getenv("COINBASE_CLI_ENV", "live"),
        )


class LiveSafetyValidator:
    """Validate explicit prerequisites before enabling live execution."""

    @staticmethod
    def check(config: TradingConfig) -> List[str]:
        issues: List[str] = []

        if config.kill_switch:
            issues.append(
                "KILL_SWITCH is active — set KILL_SWITCH=false to enable live trading"
            )
        if not config.live_trading_enabled:
            issues.append(
                "LIVE_TRADING_ENABLED is false — set to true for live mode"
            )
        if config.mode == "live" and config.dry_run:
            issues.append(
                "mode=live but COINBASE_DRY_RUN=true — dry-run orders are preview-only"
            )

        if config.max_notional_per_trade_usd <= 0:
            issues.append("MAX_NOTIONAL_PER_TRADE_USD must be > 0")
        if not 0 < config.risk_per_trade_pct <= 0.5:
            issues.append(
                f"RISK_PER_TRADE_PCT={config.risk_per_trade_pct} is out of range (0.001-0.50)"
            )
        if not 0 < config.max_daily_loss_pct <= 0.5:
            issues.append(
                f"MAX_DAILY_LOSS_PCT={config.max_daily_loss_pct * 100:.1f}% is out of range"
            )

        if config.mode in {"live", "approval"}:
            if not config.coinbase_api_key or not config.coinbase_api_secret:
                issues.append("COINBASE_API_KEY or COINBASE_API_SECRET not set")
            # Preview-only mode may use the internal deterministic request
            # assembler. Actual live submission still requires the executable.
            if not config.dry_run:
                cli = shutil.which(config.coinbase_cli_path) or shutil.which(
                    "coinbase"
                )
                if not cli:
                    issues.append(
                        f"Coinbase CLI '{config.coinbase_cli_path}' not found on PATH"
                    )

        return issues

    @staticmethod
    def assert_safe(config: TradingConfig) -> None:
        issues = LiveSafetyValidator.check(config)
        if issues:
            for issue in issues:
                log.error("LIVE SAFETY: %s", issue)
            raise RuntimeError(
                f"Live safety checks failed ({len(issues)} issues):\n  "
                + "\n  ".join(issues)
            )

    @staticmethod
    def assert_kill_switch_resolved(config: TradingConfig) -> None:
        if config.mode != "live" or config.dry_run:
            return
        if KillSwitch.is_active():
            raise RuntimeError(
                "Live trading blocked: kill switch is active "
                "(set KILL_SWITCH=false, set TRADER_KILL_SWITCH=false, and remove the sentinel file)."
            )


class KillSwitch:
    """File-backed execution halt shared by all execution paths."""

    KILL_PATH = Path("data/trading_kill_switch")

    @classmethod
    def is_active(cls) -> bool:
        return is_kill_switch_active()

    @classmethod
    def engage(cls) -> None:
        cls.KILL_PATH.parent.mkdir(parents=True, exist_ok=True)
        cls.KILL_PATH.touch(exist_ok=True)

    @classmethod
    def disengage(cls) -> None:
        cls.KILL_PATH.unlink(missing_ok=True)


def validate_opportunity_side(side: str) -> str:
    """Reject values that cannot be submitted as Coinbase order sides."""
    if side is None:
        raise ValueError("order side is None (expected BUY or SELL)")
    normalized = str(side).strip().upper()
    if normalized not in VALID_ORDER_SIDES:
        raise ValueError(
            f"order side {side!r} is not a valid Coinbase order side "
            f"(expected one of {sorted(VALID_ORDER_SIDES)})"
        )
    return normalized
