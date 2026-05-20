from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class ActionType(str, Enum):
    SWAP = "swap"
    ADD_LIQUIDITY = "add_liquidity"
    REMOVE_LIQUIDITY = "remove_liquidity"
    REBALANCE_POSITION = "rebalance_position"
    HARVEST_FEES = "harvest_fees"
    APPROVE_TOKEN = "approve_token"
    REVOKE_APPROVAL = "revoke_approval"
    BRIDGE_TRANSFER = "bridge_transfer"
    WRAP_UNWRAP = "wrap_unwrap"
    STAKE = "stake"
    UNSTAKE = "unstake"
    BORROW = "borrow"
    REPAY = "repay"
    HEDGE_HANDOFF = "hedge_handoff"
    MULTICALL = "multicall"


class SafetyState(str, Enum):
    TRUSTED = "trusted"
    WATCHED = "watched"
    QUARANTINED = "quarantined"
    DENIED = "denied"


class AtomicityClass(str, Enum):
    EFFECTIVELY_ATOMIC = "effectively_atomic"
    SEMI_ATOMIC = "semi_atomic"
    SEQUENTIAL = "sequential"
    UNSAFE_SEQUENTIAL = "unsafe_sequential"


class ContractProfile(BaseModel):
    chain: str
    address: str
    protocol: str
    version: str = "unknown"
    selectors_allowlist: set[str] = Field(default_factory=set)
    codehash: str
    verified_abi: bool = False
    is_proxy: bool = False
    upgradeable: bool = False
    admin_keys_present: bool = False
    pause_controls: bool = False
    risk_score: float = Field(default=0.5, ge=0, le=1)
    safety_state: SafetyState = SafetyState.WATCHED


class TokenProfile(BaseModel):
    chain: str
    address: str
    symbol: str
    decimals: int = Field(ge=0, le=36)
    transfer_quirks: list[str] = Field(default_factory=list)
    risk_score: float = Field(default=0.5, ge=0, le=1)
    safety_state: SafetyState = SafetyState.WATCHED


class RouteEdge(BaseModel):
    protocol: str
    pool: str
    token_in: str
    token_out: str
    fee_bps: float = Field(default=0.0, ge=0)
    liquidity_score: float = Field(default=0.0, ge=0, le=1)


class ExecutionRoute(BaseModel):
    action_type: ActionType
    chain: str
    protocol: str
    contracts_touched: list[str]
    tokens_touched: list[str]
    approvals_required: list[str] = Field(default_factory=list)
    route_graph: list[RouteEdge] = Field(default_factory=list)
    fallback_routes: list[str] = Field(default_factory=list)
    deadline_seconds: int = 60


class RouteEconomics(BaseModel):
    expected_gross_edge: float
    expected_gas_cost: float
    expected_priority_fee: float
    expected_slippage_cost: float
    expected_price_impact: float
    expected_lp_fee_capture: float = 0.0
    expected_hedge_cost: float = 0.0
    expected_bridge_cost: float = 0.0
    expected_net_edge: float
    worst_case_downside: float
    break_even_slippage_bps: float
    break_even_gas: float


class RouteDiagnostics(BaseModel):
    path_trust_score: float = Field(ge=0, le=1)
    pool_liquidity_quality: float = Field(ge=0, le=1)
    route_fragility: float = Field(ge=0, le=1)
    token_risk_score: float = Field(ge=0, le=1)
    contract_risk_score: float = Field(ge=0, le=1)
    oracle_freshness_seconds: int
    quote_staleness_ms: int
    fill_confidence: float = Field(ge=0, le=1)
    mev_reorder_risk: float = Field(ge=0, le=1)
    bridge_settlement_risk: float = Field(ge=0, le=1)
    revert_probability: float = Field(ge=0, le=1)


class RiskDiagnostics(BaseModel):
    capital_at_risk: float
    inventory_impact: dict[str, float]
    exposure_delta: dict[str, float]
    strategy_cap_ok: bool
    sleeve_cap_ok: bool
    reserve_lock_ok: bool
    drawdown_mode_ok: bool
    unwind_plan: str


class SimulationResult(BaseModel):
    success: bool
    simulation_hash: str
    gas_used: int
    min_out_respected: bool
    revert_reason: str | None = None


class ExecutionPlan(BaseModel):
    opportunity_id: str
    strategy_name: str
    route: ExecutionRoute
    economics: RouteEconomics
    diagnostics: RouteDiagnostics
    risk: RiskDiagnostics
    simulation: SimulationResult
    executable: bool
    fail_reasons: list[str] = Field(default_factory=list)


class OnchainApprovalPayload(BaseModel):
    opportunity_id: str
    strategy_name: str
    action_type: ActionType
    chain: str
    protocol: str
    wallet: str
    contracts_touched: list[str]
    tokens_touched: list[str]
    approvals_required: list[str]
    expected_gross_pnl: float
    expected_net_pnl: float
    gas_estimate: float
    slippage_estimate: float
    worst_case_loss: float
    capital_at_risk: float
    hedge_plan: str
    route_trust_score: float
    token_risk_score: float
    contract_risk_score: float
    revert_risk: float
    expiration_time: str
    simulation_hash: str
    rollback_plan: str
    reason: str
    urgency: str
    operator_actions_available: list[str]
    concise_summary: str
    detailed_summary: str


class Opportunity(BaseModel):
    opportunity_id: str
    strategy_name: str
    chain: str
    protocol: str
    action_type: ActionType
    token_pair: str
    gross_edge: float
    capital_required: float
    confidence: float = Field(ge=0, le=1)
    age_ms: int = 0


class OpportunityScore(BaseModel):
    opportunity: Opportunity
    estimated_net_edge: float
    route_trust_score: float
    fragility: float
    executable: bool
    reject_reason: str | None = None
