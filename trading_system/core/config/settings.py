import os
from enum import Enum

from pydantic import BaseModel, Field, field_validator, model_validator


class TradingMode(str, Enum):
    SIMULATION = "SIMULATION"
    PAPER = "PAPER"
    LIVE_APPROVAL_REQUIRED = "LIVE_APPROVAL_REQUIRED"
    LIVE_SEMI_AUTO = "LIVE_SEMI_AUTO"
    LIVE_AUTO = "LIVE_AUTO"
    SHADOW = "SHADOW"
    CANARY = "CANARY"


TRUE_VALUES = {"1", "true", "yes", "y", "on"}
FALSE_VALUES = {"0", "false", "no", "n", "off"}


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in TRUE_VALUES:
        return True
    if normalized in FALSE_VALUES:
        return False
    raise ValueError(f"Invalid boolean for {name}: {raw!r}")


class Settings(BaseModel):
    app_env: str = "dev"
    trading_mode: TradingMode = TradingMode.PAPER
    coinbase_api_key: str = ""
    coinbase_api_secret: str = ""
    coinbase_passphrase: str = ""
    coinbase_portfolio_ids: str = "default"
    database_url: str = "postgresql://trader:trader@localhost:5432/trading"
    redis_url: str = "redis://localhost:6379/0"
    require_approvals: bool = True
    live_trading_enabled: bool = False
    low_latency_mode: bool = False
    gpu_enabled: bool = False
    queue_model: str = "simple"
    canary_rollout_pct: float = Field(default=0.0, ge=0, le=100)

    @field_validator("queue_model")
    @classmethod
    def _queue_model_supported(cls, value: str) -> str:
        allowed = {"simple", "priority", "pro_rata"}
        normalized = value.strip().lower()
        if normalized not in allowed:
            raise ValueError(f"QUEUE_MODEL must be one of {sorted(allowed)}")
        return normalized

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            app_env=os.getenv("APP_ENV", "dev"),
            trading_mode=TradingMode(os.getenv("TRADING_MODE", TradingMode.PAPER.value)),
            coinbase_api_key=os.getenv("COINBASE_API_KEY", ""),
            coinbase_api_secret=os.getenv("COINBASE_API_SECRET", ""),
            coinbase_passphrase=os.getenv("COINBASE_PASSPHRASE", ""),
            coinbase_portfolio_ids=os.getenv("COINBASE_PORTFOLIO_IDS", "default"),
            database_url=os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/trading"),
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            require_approvals=_parse_bool_env("REQUIRE_APPROVALS", True),
            live_trading_enabled=_parse_bool_env("LIVE_TRADING_ENABLED", False),
            low_latency_mode=_parse_bool_env("LOW_LATENCY_MODE", False),
            gpu_enabled=_parse_bool_env("GPU_ENABLED", False),
            queue_model=os.getenv("QUEUE_MODEL", "simple"),
            canary_rollout_pct=float(os.getenv("CANARY_ROLLOUT_PCT", "0")),
        )

    @model_validator(mode="after")
    def validate_safety(self) -> "Settings":
        if self.trading_mode.name.startswith("LIVE") and not self.live_trading_enabled:
            raise ValueError("Live mode requested while LIVE_TRADING_ENABLED is false")
        if self.trading_mode is TradingMode.LIVE_AUTO and not self.require_approvals:
            raise ValueError("LIVE_AUTO requires REQUIRE_APPROVALS=true in this build")
        if self.trading_mode is TradingMode.CANARY and self.canary_rollout_pct <= 0:
            raise ValueError("CANARY mode requires CANARY_ROLLOUT_PCT > 0")
        if self.trading_mode is not TradingMode.CANARY and self.canary_rollout_pct > 0:
            raise ValueError("CANARY_ROLLOUT_PCT can only be set when TRADING_MODE=CANARY")
        return self
