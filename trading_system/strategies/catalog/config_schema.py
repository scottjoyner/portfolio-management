from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


ALLOWED_RISK_TIERS: set[str] = {
    "TIER_0_CAPITAL_PRESERVATION",
    "TIER_1_LOW_RISK",
    "TIER_2_MODERATE_RISK",
    "TIER_3_HIGH_RISK",
    "TIER_4_EXPERT_HIGH_RISK",
    "TIER_5_RESEARCH_ONLY",
}

ALLOWED_SIZING_MODELS: set[str] = {
    "fixed_fraction",
    "volatility_targeting",
    "atr_based",
    "kelly_capped",
    "risk_parity",
    "equal_risk_contribution",
    "drawdown_adjusted",
    "inventory_aware",
    "liquidity_adjusted",
    "regime_aware",
}

TIER_MAX_CAPITAL_FRACTION: dict[str, float] = {
    "TIER_0_CAPITAL_PRESERVATION": 0.40,
    "TIER_1_LOW_RISK": 0.30,
    "TIER_2_MODERATE_RISK": 0.20,
    "TIER_3_HIGH_RISK": 0.10,
    "TIER_4_EXPERT_HIGH_RISK": 0.03,
    "TIER_5_RESEARCH_ONLY": 0.01,
}


class StrategyRuntimeFlags(BaseModel):
    backtest_enabled: bool = True
    paper_enabled: bool = True
    live_enabled: bool = False


class StrategyConfig(BaseModel):
    strategy_id: str
    enabled: bool = False
    supported_products: list[str] = Field(default_factory=lambda: ["BTC-USD"])
    risk_tier: str
    max_capital_fraction: float = Field(ge=0.0, le=1.0)
    sizing_model: str
    min_size: float = Field(gt=0.0)
    max_size: float = Field(gt=0.0)
    max_exposure_by_asset: float = Field(default=0.2, gt=0.0, le=1.0)
    max_exposure_by_correlated_group: float = Field(default=0.3, gt=0.0, le=1.0)
    max_turnover: float = Field(default=5.0, ge=0.0)
    entry_threshold: float
    exit_threshold: float
    stop_loss_bps: float = Field(ge=0.0)
    take_profit_bps: float = Field(ge=0.0)
    trailing_take_profit_bps: float = Field(ge=0.0)
    cooldown_bars: int = Field(ge=0)
    warmup_bars: int = Field(ge=0)
    approvals_required: bool = False
    telemetry_enabled: bool = True
    live_requires_risk_gates: bool = True
    runtime_flags: StrategyRuntimeFlags = Field(default_factory=StrategyRuntimeFlags)

    @model_validator(mode="after")
    def validate_thresholds(self) -> "StrategyConfig":
        if self.risk_tier not in ALLOWED_RISK_TIERS:
            raise ValueError("unsupported risk tier")
        if self.sizing_model not in ALLOWED_SIZING_MODELS:
            raise ValueError("unsupported sizing model")
        if self.enabled and not (self.runtime_flags.backtest_enabled or self.runtime_flags.paper_enabled or self.runtime_flags.live_enabled):
            raise ValueError("enabled strategy must run in at least one runtime mode")
        if self.max_size < self.min_size:
            raise ValueError("max_size must be >= min_size")
        if self.exit_threshold > self.entry_threshold:
            raise ValueError("exit_threshold must be <= entry_threshold")
        if not self.supported_products:
            raise ValueError("supported_products must not be empty")
        if self.max_capital_fraction > TIER_MAX_CAPITAL_FRACTION[self.risk_tier]:
            raise ValueError("max_capital_fraction exceeds risk-tier ceiling")
        if self.runtime_flags.live_enabled and not self.enabled:
            raise ValueError("live_enabled strategy must also be enabled")
        if self.runtime_flags.live_enabled and not self.approvals_required:
            raise ValueError("live_enabled requires approvals_required")
        if self.runtime_flags.live_enabled and self.risk_tier in {"TIER_4_EXPERT_HIGH_RISK", "TIER_5_RESEARCH_ONLY"}:
            raise ValueError("expert/research tiers cannot be directly enabled live")
        if self.max_exposure_by_correlated_group < self.max_exposure_by_asset:
            raise ValueError("correlated group exposure must be >= asset exposure")
        if self.warmup_bars < self.cooldown_bars:
            raise ValueError("warmup_bars must be >= cooldown_bars")
        if self.live_requires_risk_gates and self.runtime_flags.live_enabled and self.max_turnover > 10:
            raise ValueError("live strategy turnover cap too high for safety gates")
        if self.runtime_flags.live_enabled and self.stop_loss_bps <= 0:
            raise ValueError("live strategy requires positive stop_loss_bps")
        if self.take_profit_bps == 0 and self.trailing_take_profit_bps > 0:
            raise ValueError("trailing take profit requires non-zero take_profit_bps")
        return self
