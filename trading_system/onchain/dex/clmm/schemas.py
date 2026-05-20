from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class HedgeMode(str, Enum):
    NONE = "NONE"
    DELTA_NEUTRAL = "DELTA_NEUTRAL"
    BAND_HEDGE = "BAND_HEDGE"
    PARTIAL_HEDGE = "PARTIAL_HEDGE"
    DIRECTIONAL_BIAS = "DIRECTIONAL_BIAS"
    EMERGENCY_FLATTEN = "EMERGENCY_FLATTEN"


class TokenRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    chain: str
    symbol: str
    address: str = Field(min_length=4)
    decimals: int = Field(ge=0, le=36)


class PoolRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    chain: str
    protocol: str
    pool_address: str
    token0: TokenRef
    token1: TokenRef
    fee_tier_bps: int = Field(ge=0, le=10_000)
    tick_spacing: int = Field(gt=0)


class CLMMRange(BaseModel):
    lower_tick: int
    upper_tick: int

    @model_validator(mode="after")
    def _validate_ticks(self) -> "CLMMRange":
        if self.lower_tick >= self.upper_tick:
            raise ValueError("lower_tick must be less than upper_tick")
        return self


class CLMMPositionSnapshot(BaseModel):
    position_id: str
    pool: PoolRef
    owner_wallet: str
    range: CLMMRange
    liquidity: Decimal = Field(ge=0)
    amount0: Decimal = Field(ge=0)
    amount1: Decimal = Field(ge=0)
    uncollected_fee0: Decimal = Field(ge=0, default=Decimal("0"))
    uncollected_fee1: Decimal = Field(ge=0, default=Decimal("0"))
    opened_at: datetime
    updated_at: datetime
    mark_price: Decimal = Field(gt=0)


class LPInventoryState(BaseModel):
    token0_units: Decimal
    token1_units: Decimal
    token0_usd: Decimal
    token1_usd: Decimal
    total_usd: Decimal = Field(ge=0)
    imbalance_ratio: Decimal = Field(description="(token1_usd - token0_usd) / total")


class LPFeeState(BaseModel):
    fee0_realized: Decimal = Field(ge=0)
    fee1_realized: Decimal = Field(ge=0)
    fee_usd_realized: Decimal = Field(ge=0)
    fee_usd_unrealized: Decimal = Field(ge=0)
    collection_count: int = Field(ge=0, default=0)


class ILDecomposition(BaseModel):
    hodl_value_usd: Decimal
    lp_terminal_value_usd: Decimal
    il_usd: Decimal
    fee_income_usd: Decimal
    net_lp_edge_usd: Decimal
    fee_to_il_ratio: Decimal


class HedgePlan(BaseModel):
    hedge_mode: HedgeMode
    base_asset: str
    quote_asset: str
    target_delta_usd: Decimal
    hedge_notional_usd: Decimal
    max_slippage_bps: Decimal = Field(ge=0)
    urgency_score: Decimal = Field(ge=0, le=1)
    cooldown_seconds: int = Field(ge=0)


class HedgeExecutionLink(BaseModel):
    opportunity_id: str
    onchain_action_id: str
    hedge_order_preview_id: str
    sequencing: Literal["ONCHAIN_FIRST", "COINBASE_FIRST"]
    semi_atomic_score: Decimal = Field(ge=0, le=1)


class RouteStep(BaseModel):
    venue: str
    contract: str
    method: str
    token_in: str
    token_out: str
    amount_in: Decimal = Field(ge=0)
    min_amount_out: Decimal = Field(ge=0)


class RouteGraph(BaseModel):
    route_id: str
    steps: list[RouteStep]
    quote_age_ms: int = Field(ge=0)


class ActionSimulationRequest(BaseModel):
    action_type: str
    wallet: str
    pool: PoolRef | None = None
    route: RouteGraph | None = None
    amount_usd: Decimal = Field(gt=0)
    slippage_bps_limit: Decimal = Field(gt=0, le=5_000)
    deadline_ts: datetime


class RouteFallbackPlan(BaseModel):
    reason: str
    fallback_route: RouteGraph | None = None
    expected_net_delta_usd: Decimal


class ActionSimulationResult(BaseModel):
    action_type: str
    contracts_touched: list[str]
    tokens_touched: list[str]
    approvals_required: list[str]
    estimated_gas: int = Field(ge=0)
    gas_cost_usd: Decimal = Field(ge=0)
    slippage_bps: Decimal = Field(ge=0)
    price_impact_bps: Decimal = Field(ge=0)
    route_fragility_score: Decimal = Field(ge=0, le=1)
    pool_liquidity_quality: Decimal = Field(ge=0, le=1)
    contract_trust_score: Decimal = Field(ge=0, le=1)
    token_safety_score: Decimal = Field(ge=0, le=1)
    expected_gross_value_change_usd: Decimal
    expected_net_value_change_usd: Decimal
    worst_case_downside_usd: Decimal = Field(ge=0)
    break_even_gas_usd: Decimal
    break_even_slippage_bps: Decimal
    confidence_score: Decimal = Field(ge=0, le=1)
    fallback: RouteFallbackPlan | None = None


class ActionProfitabilityReport(BaseModel):
    opportunity_id: str
    simulation: ActionSimulationResult
    is_profitable: bool
    rejection_reason: str | None = None


class LPRebalanceDecision(BaseModel):
    should_rebalance: bool
    reason: str
    distance_to_boundary_bps: Decimal = Field(ge=0)
    expected_rebalance_cost_usd: Decimal = Field(ge=0)
    expected_rebalance_edge_usd: Decimal


class ApprovalPacketOnchainMM(BaseModel):
    opportunity_id: str
    strategy_name: str
    action_type: str
    protocol: str
    chain: str
    pool: str
    wallet: str
    current_position_summary: str
    proposed_position_summary: str
    contracts_touched: list[str]
    tokens_touched: list[str]
    approvals_required: list[str]
    expected_fee_capture: Decimal
    expected_gas_cost: Decimal
    expected_slippage: Decimal
    expected_net_benefit: Decimal
    worst_case_loss: Decimal
    hedge_required: bool
    hedge_plan: HedgePlan | None = None
    route_trust_score: Decimal = Field(ge=0, le=1)
    fragility_score: Decimal = Field(ge=0, le=1)
    token_risk_score: Decimal = Field(ge=0, le=1)
    contract_risk_score: Decimal = Field(ge=0, le=1)
    expiration_time: datetime
    rollback_plan: str
    concise_voice_summary: str = Field(min_length=8)
    detailed_operator_summary: str = Field(min_length=16)


class ProfitSweepDecision(BaseModel):
    should_sweep: bool
    should_compound: bool
    sweep_amount_usd: Decimal = Field(ge=0)
    compound_amount_usd: Decimal = Field(ge=0)
    destination_bucket: str
    reason: str


class PoolVolatilitySnapshot(BaseModel):
    realized_vol_1d: Decimal = Field(ge=0)
    realized_vol_7d: Decimal = Field(ge=0)
    short_horizon_vol: Decimal = Field(ge=0)
    regime: Literal["CALM", "NORMAL", "ELEVATED", "CRISIS"]


class PoolLiquiditySnapshot(BaseModel):
    depth_usd_50bps: Decimal = Field(ge=0)
    depth_usd_200bps: Decimal = Field(ge=0)
    daily_volume_usd: Decimal = Field(ge=0)
    quality_score: Decimal = Field(ge=0, le=1)


class HybridExposureSnapshot(BaseModel):
    onchain_delta_usd: Decimal
    cex_delta_usd: Decimal
    net_delta_usd: Decimal
    hedge_coverage_ratio: Decimal


class StressScenarioInput(BaseModel):
    name: str
    spot_shock_pct: Decimal
    vol_multiplier: Decimal = Field(gt=0)
    gas_multiplier: Decimal = Field(gt=0)
    liquidity_haircut_pct: Decimal = Field(ge=0, le=100)


class StressScenarioResult(BaseModel):
    name: str
    pnl_usd: Decimal
    max_drawdown_usd: Decimal
    time_out_of_range_pct: Decimal = Field(ge=0, le=100)
    hedge_failures: int = Field(ge=0)
    fragility_score: Decimal = Field(ge=0, le=1)


class RangeSelectionInput(BaseModel):
    mark_price: Decimal = Field(gt=0)
    tick_spacing: int = Field(gt=0)
    realized_vol: Decimal = Field(ge=0)
    short_vol: Decimal = Field(ge=0)
    market_regime: Literal["CALM", "NORMAL", "ELEVATED", "CRISIS"]
    inventory_skew: Decimal = Field(ge=-1, le=1)
    gas_regime_score: Decimal = Field(ge=0, le=1)


class RangeCandidate(BaseModel):
    lower_tick: int
    upper_tick: int
    width_bps: Decimal
    expected_utilization: Decimal = Field(ge=0, le=1)
    expected_fee_density: Decimal = Field(ge=0)
    expected_rebalance_pressure: Decimal = Field(ge=0)
    expected_il_pressure: Decimal = Field(ge=0)
    expected_hedge_drag: Decimal = Field(ge=0)
    confidence_score: Decimal = Field(ge=0, le=1)

    @field_validator("upper_tick")
    @classmethod
    def _upper_gt_lower(cls, v: int, info):
        lower = info.data.get("lower_tick")
        if lower is not None and v <= lower:
            raise ValueError("upper_tick must exceed lower_tick")
        return v
