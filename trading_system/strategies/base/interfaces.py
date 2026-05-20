from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field, field_validator


class StrategySignal(BaseModel):
    strategy_id: str
    product_id: str
    score: float
    reason: str
    confidence: float = Field(default=0.5, ge=0, le=1)
    warmup_passed: bool = True
    tags: list[str] = Field(default_factory=list)
    features: dict[str, float | str | int | bool] = Field(default_factory=dict)


class StrategyMetadata(BaseModel):
    strategy_id: str
    strategy_type: str
    status: str = "implemented"
    paper_mode: bool = True
    live_supported: bool = False
    replay_supported: bool = True
    backtest_supported: bool = True
    products: list[str] = Field(default_factory=lambda: ["BTC-USD"])
    data_requirements: list[str] = Field(default_factory=lambda: ["product_id", "score"])
    risk_mode_hint: str = "NORMAL"
    capital_bucket: str = "ACTIVE_TRADING"


class StrategyConfig(BaseModel):
    threshold: float = Field(default=0.1, ge=-1.0, le=1.0)
    cooldown_seconds: float = Field(default=0.0, ge=0.0, le=3600.0)
    warmup_period: int = Field(default=0, ge=0, le=20_000)
    max_abs_score: float = Field(default=10.0, gt=0)
    enabled: bool = True

    @field_validator("max_abs_score")
    @classmethod
    def check_score_safety(cls, value: float) -> float:
        if value < 0.5:
            raise ValueError("max_abs_score is unrealistically tight")
        return value


class Strategy(ABC):
    strategy_id: str

    @abstractmethod
    def metadata(self) -> dict: ...

    @abstractmethod
    def generate_signal(self, market_state: dict) -> StrategySignal | None: ...

    @abstractmethod
    def explain_trade(self, signal: StrategySignal) -> str: ...

    def sizing_hints(self, market_state: dict) -> dict[str, float | str | bool]:
        return {}

    def order_intents(self, signal: StrategySignal, market_state: dict) -> list[dict[str, float | str | bool]]:
        return []

    def risk_hints(self, market_state: dict) -> dict[str, float | str | bool]:
        return {}

    def required_inputs(self) -> set[str]:
        return {"product_id", "score"}

    def supports_mode(self, mode: str) -> bool:
        mode_lower = mode.lower()
        metadata = self.metadata()
        return {
            "live": bool(metadata.get("live_supported", False)),
            "paper": bool(metadata.get("paper_mode", True)),
            "replay": bool(metadata.get("replay_supported", True)),
            "backtest": bool(metadata.get("backtest_supported", True)),
        }.get(mode_lower, False)

    def in_cooldown(self) -> bool:
        return False

    def is_disabled(self, market_state: dict) -> tuple[bool, str]:
        return False, "enabled"

    def approvals_required(self) -> bool:
        return bool(self.metadata().get("live_supported", False))

    def on_paper_fill(self, fill_event: dict) -> None:
        return None

    def replay_hooks(self) -> dict[str, str]:
        return {}

    def analytics_tags(self) -> dict[str, str]:
        return {}

    def serialize_state(self) -> dict:
        return {}

    def restore_state(self, state: dict) -> None:
        return None
