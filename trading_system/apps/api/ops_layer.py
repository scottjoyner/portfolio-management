from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Literal
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from storage.postgres.repository import OpsRepository
from storage.postgres.session import get_db
from strategies.lifecycle import StrategyLifecycleManager
from storage.postgres.models import (
    AuditEvent as AuditEventModel,
    Order as OrderModel,
    PortfolioSleeve,
    StrategyRun as StrategyRunModel,
)


class CapitalStatus(str, Enum):
    IDLE = "idle"
    WORKING = "working"
    LOCKED = "locked"
    HEDGED = "hedged"
    PENDING = "pending"


class FeedState(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"


class ThemeMode(str, Enum):
    DARK = "dark"
    LIGHT = "light"


class CapitalBucketView(BaseModel):
    name: str
    amount: float
    status: CapitalStatus


class FeedHealth(BaseModel):
    feed: str
    state: FeedState
    freshness_ms: int
    update_rate_hz: float
    dropped_messages_1m: int
    failover_active: bool


class DashboardIssue(BaseModel):
    issue_id: str
    severity: Literal["low", "medium", "high", "critical"]
    title: str
    impacted_entities: list[str]
    recommended_action: str


class ActionShortcut(BaseModel):
    action_id: str
    label: str
    requires_approval: bool
    endpoint: str


class PortfolioSummary(BaseModel):
    portfolio_id: str
    name: str
    objective: str
    nav: float
    available_capital: float
    locked_capital: float
    realized_pnl: float
    unrealized_pnl: float
    liquidity_score: float = Field(ge=0, le=1)
    capital_efficiency: float = Field(ge=0, le=1)


class DashboardSnapshot(BaseModel):
    generated_at: datetime
    total_nav: float
    daily_pnl: float
    realized_pnl: float
    unrealized_pnl: float
    risk_mode: str
    exchange_trust_state: str
    open_orders: int
    fills_last_15m: int
    approval_queue: int
    latency_health: float
    reconciliation_health: float
    liquidity_availability_score: float
    idle_capital_score: float
    working_capital_score: float
    capital_buckets: list[CapitalBucketView]
    feed_health: list[FeedHealth]
    active_issues: list[DashboardIssue]
    quick_actions: list[ActionShortcut]
    portfolios: list[PortfolioSummary]


class DashboardDelta(BaseModel):
    generated_at: datetime
    nav_delta_5m: float
    pnl_delta_5m: float
    fills_delta_5m: int
    open_orders_delta_5m: int
    new_issues: list[DashboardIssue]


class PortfolioDetail(BaseModel):
    summary: PortfolioSummary
    sleeves: dict[str, float]
    strategy_allocations: dict[str, float]
    transfers_24h: list[dict]
    risk_budget_used: float
    what_changed: dict[str, float]


class TreasuryTransferPreviewRequest(BaseModel):
    source_portfolio: str
    destination_portfolio: str
    asset: str
    amount: float = Field(gt=0)
    rationale: str


class TreasuryTransferPreview(BaseModel):
    preview_id: str
    source_portfolio: str
    destination_portfolio: str
    asset: str
    amount: float
    approvals_required: list[str]
    resulting_liquidity_change: dict[str, float]
    resulting_risk_budget_change: dict[str, float]
    rollback_guidance: str


class TreasuryTransferExecuteRequest(BaseModel):
    preview_id: str


class LiquidityRecommendation(BaseModel):
    recommendation_id: str
    action: str
    expected_efficiency_delta: float
    expected_liquidity_score_delta: float
    confidence: float


class LiquidityMapNode(BaseModel):
    node_id: str
    node_type: Literal["portfolio", "sleeve", "asset"]
    utilization: float
    productive: bool


class LiquidityMapEdge(BaseModel):
    source: str
    target: str
    suggested_move: float


class LiquidityMapSnapshot(BaseModel):
    as_of: datetime
    nodes: list[LiquidityMapNode]
    edges: list[LiquidityMapEdge]


class OrderPreviewRequest(BaseModel):
    portfolio_id: str
    sleeve_id: str
    strategy_id: str
    product_id: str
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit"]
    size: float = Field(gt=0)
    limit_price: float | None = None


class OrderPreviewResponse(BaseModel):
    preview_id: str
    estimated_commission: float
    estimated_slippage: float
    expected_fee_impact: float
    resulting_exposure: float
    resulting_risk_usage: float
    approval_required: bool
    confidence: float


class SubmitOrderRequest(BaseModel):
    preview_id: str


class OrderRecord(BaseModel):
    order_id: str
    preview_id: str
    strategy_id: str
    portfolio_id: str
    sleeve_id: str
    product_id: str
    side: str
    size: float
    remaining_size: float
    order_type: str
    status: str
    maker_taker_expectation: str
    queue_age_s: int
    created_at: datetime


class FillRecord(BaseModel):
    fill_id: str
    order_id: str
    product_id: str
    size: float
    price: float
    slippage_bps: float
    fee: float
    at: datetime


class ThemeSettings(BaseModel):
    mode: ThemeMode
    lightweight: bool
    animation_level: Literal["none", "reduced", "standard"]
    table_density: Literal["compact", "standard"]
    chart_render_mode: Literal["canvas", "svg"]


class BacktestRequest(BaseModel):
    strategy_id: str
    universe: list[str]
    lookback_days: int = Field(ge=1, le=365)
    capital: float = Field(gt=0)


class StrategyActionResponse(BaseModel):
    task_id: str
    strategy_id: str
    status: str
    queued_at: datetime


class RealtimeStrategyOutcome(BaseModel):
    strategy_id: str
    status: Literal["running", "paused", "cooldown"]
    pnl_1h: float
    fill_quality_score: float
    consumed_capital: float
    latest_decision: str
    at: datetime


class _PreviewCache:
    def __init__(self) -> None:
        self.preview_cache: dict[str, TreasuryTransferPreview | OrderPreviewResponse] = {}

    def put(self, key: str, value: TreasuryTransferPreview | OrderPreviewResponse) -> None:
        self.preview_cache[key] = value

    def get(self, key: str) -> TreasuryTransferPreview | OrderPreviewResponse | None:
        return self.preview_cache.get(key)


_preview_cache = _PreviewCache()


def _repo(db: Session) -> OpsRepository:
    return OpsRepository(db)


router = APIRouter(prefix="/ops", tags=["ops"])


@router.get("/dashboard/snapshot", response_model=DashboardSnapshot)
def dashboard_snapshot(db: Session = Depends(get_db)) -> DashboardSnapshot:
    repo = _repo(db)
    portfolios = repo.list_portfolios()
    orders = repo.list_orders()
    fills = repo.list_fills()
    feed_health = repo.list_feed_health()
    approvals = repo.list_approvals()
    issues: list[DashboardIssue] = []
    stale_or_degraded = [f for f in feed_health if f.state != "healthy"]
    if stale_or_degraded:
        issues.append(
            DashboardIssue(
                issue_id="issue-feed-quality",
                severity="medium",
                title="One or more realtime feeds are degraded",
                impacted_entities=[f.feed_name for f in stale_or_degraded],
                recommended_action="Switch to conservative routing and inspect feed failover lanes.",
            )
        )
    for p in portfolios:
        if p.available_capital < 200_000:
            issues.append(
                DashboardIssue(
                    issue_id=f"issue-capital-low-{p.id}",
                    severity="high",
                    title="Portfolio available capital below threshold",
                    impacted_entities=[p.id],
                    recommended_action="Trigger treasury replenish preview before new strategy allocations.",
                )
            )
    open_orders = [o for o in orders if o.status == "open"]
    total_nav = sum(p.nav for p in portfolios)
    total_realized = sum(p.realized_pnl for p in portfolios)
    total_unrealized = sum(p.unrealized_pnl for p in portfolios)
    idle = 750_000
    working = 1_450_000
    bucket_total = idle + working + 900_000 + 500_000 + 125_000
    return DashboardSnapshot(
        generated_at=datetime.now(timezone.utc),
        total_nav=total_nav,
        daily_pnl=total_realized + total_unrealized,
        realized_pnl=total_realized,
        unrealized_pnl=total_unrealized,
        risk_mode="MARKET_MAKING_PRO",
        exchange_trust_state="HEALTHY",
        open_orders=len(open_orders),
        fills_last_15m=len(fills),
        approval_queue=len(approvals),
        latency_health=0.96,
        reconciliation_health=0.98,
        liquidity_availability_score=round((working + idle) / bucket_total, 4),
        idle_capital_score=round(idle / bucket_total, 4),
        working_capital_score=round(working / bucket_total, 4),
        capital_buckets=[
            CapitalBucketView(name="locked_reserve", amount=900_000, status=CapitalStatus.LOCKED),
            CapitalBucketView(name="active_trading", amount=1_450_000, status=CapitalStatus.WORKING),
            CapitalBucketView(name="hedging", amount=500_000, status=CapitalStatus.HEDGED),
            CapitalBucketView(name="cash_buffer", amount=750_000, status=CapitalStatus.IDLE),
            CapitalBucketView(name="pending_transfer", amount=125_000, status=CapitalStatus.PENDING),
        ],
        feed_health=[FeedHealth(feed=f.feed_name, state=FeedState(f.state), freshness_ms=f.freshness_ms, update_rate_hz=float(f.update_rate_hz), dropped_messages_1m=f.dropped_messages_1m, failover_active=f.failover_active) for f in feed_health],
        active_issues=issues,
        quick_actions=[
            ActionShortcut(action_id="preview_treasury_move", label="Preview Treasury Move", requires_approval=True, endpoint="/ops/treasury/preview"),
            ActionShortcut(action_id="preview_order", label="Preview Order", requires_approval=False, endpoint="/ops/orders/preview"),
            ActionShortcut(action_id="start_strategy_backtest", label="Start Strategy Backtest", requires_approval=False, endpoint="/ops/strategies/backtest/start"),
        ],
        portfolios=[PortfolioSummary(portfolio_id=p.id, name=p.name, objective=p.objective, nav=float(p.nav), available_capital=float(p.available_capital), locked_capital=float(p.locked_capital), realized_pnl=float(p.realized_pnl), unrealized_pnl=float(p.unrealized_pnl), liquidity_score=float(p.liquidity_score), capital_efficiency=float(p.capital_efficiency)) for p in portfolios],
    )


@router.get("/dashboard/delta", response_model=DashboardDelta)
def dashboard_delta() -> DashboardDelta:
    return DashboardDelta(
        generated_at=datetime.now(timezone.utc),
        nav_delta_5m=21_200,
        pnl_delta_5m=1_240,
        fills_delta_5m=14,
        open_orders_delta_5m=3,
        new_issues=[],
    )


@router.get("/feeds/health", response_model=list[FeedHealth])
def feeds_health(db: Session = Depends(get_db)) -> list[FeedHealth]:
    repo = _repo(db)
    feeds = repo.list_feed_health()
    return [FeedHealth(feed=f.feed_name, state=FeedState(f.state), freshness_ms=f.freshness_ms, update_rate_hz=float(f.update_rate_hz), dropped_messages_1m=f.dropped_messages_1m, failover_active=f.failover_active) for f in feeds]


@router.get("/portfolios", response_model=list[PortfolioSummary])
def list_portfolios(db: Session = Depends(get_db)) -> list[PortfolioSummary]:
    repo = _repo(db)
    return [PortfolioSummary(portfolio_id=p.id, name=p.name, objective=p.objective, nav=float(p.nav), available_capital=float(p.available_capital), locked_capital=float(p.locked_capital), realized_pnl=float(p.realized_pnl), unrealized_pnl=float(p.unrealized_pnl), liquidity_score=float(p.liquidity_score), capital_efficiency=float(p.capital_efficiency)) for p in repo.list_portfolios()]


@router.get("/portfolios/{portfolio_id}", response_model=PortfolioDetail)
def get_portfolio_detail(portfolio_id: str, db: Session = Depends(get_db)) -> PortfolioDetail:
    repo = _repo(db)
    p = repo.get_portfolio(portfolio_id)
    if not p:
        raise HTTPException(status_code=404, detail="portfolio not found")
    sleeves = {s.name: float(s.weight) for s in db.query(PortfolioSleeve).filter(PortfolioSleeve.portfolio_id == portfolio_id).all()}
    return PortfolioDetail(
        summary=PortfolioSummary(portfolio_id=p.id, name=p.name, objective=p.objective, nav=float(p.nav), available_capital=float(p.available_capital), locked_capital=float(p.locked_capital), realized_pnl=float(p.realized_pnl), unrealized_pnl=float(p.unrealized_pnl), liquidity_score=float(p.liquidity_score), capital_efficiency=float(p.capital_efficiency)),
        sleeves=sleeves,
        strategy_allocations={},
        transfers_24h=[],
        risk_budget_used=0.0,
        what_changed={},
    )


@router.post("/treasury/preview", response_model=TreasuryTransferPreview)
def treasury_preview(req: TreasuryTransferPreviewRequest) -> TreasuryTransferPreview:
    if req.source_portfolio == req.destination_portfolio:
        raise HTTPException(status_code=400, detail="source and destination must differ")
    preview = TreasuryTransferPreview(
        preview_id=f"tr-prev-{uuid4().hex[:10]}",
        source_portfolio=req.source_portfolio,
        destination_portfolio=req.destination_portfolio,
        asset=req.asset,
        amount=req.amount,
        approvals_required=["treasury_officer", "risk_desk"],
        resulting_liquidity_change={req.source_portfolio: -req.amount, req.destination_portfolio: req.amount},
        resulting_risk_budget_change={req.source_portfolio: -0.04, req.destination_portfolio: 0.03},
        rollback_guidance="Submit reverse transfer using original preview id lineage and freeze non-essential orders.",
    )
    _preview_cache.put(preview.preview_id, preview)
    return preview


@router.post("/treasury/execute")
def treasury_execute(req: TreasuryTransferExecuteRequest, db: Session = Depends(get_db)) -> dict:
    preview = _preview_cache.get(req.preview_id)
    if not isinstance(preview, TreasuryTransferPreview):
        raise HTTPException(status_code=404, detail="transfer preview not found")
    repo = _repo(db)
    source = repo.get_portfolio(preview.source_portfolio)
    destination = repo.get_portfolio(preview.destination_portfolio)
    if not source or not destination:
        raise HTTPException(status_code=404, detail="portfolio not found")
    if source.available_capital < preview.amount:
        raise HTTPException(status_code=400, detail="insufficient available capital in source portfolio")
    source.available_capital = float(source.available_capital) - preview.amount
    destination.available_capital = float(destination.available_capital) + preview.amount
    repo.upsert_portfolio(source)
    repo.upsert_portfolio(destination)
    repo.create_audit_event(
        AuditEventModel(
            event_type="treasury_transfer_executed",
            details=f"{preview.source_portfolio} -> {preview.destination_portfolio}: {preview.amount}",
        )
    )
    return {"status": "executed", "preview_id": preview.preview_id}


@router.get("/liquidity/map", response_model=LiquidityMapSnapshot)
def liquidity_map() -> LiquidityMapSnapshot:
    return LiquidityMapSnapshot(
        as_of=datetime.now(timezone.utc),
        nodes=[
            LiquidityMapNode(node_id="cb-core-mm", node_type="portfolio", utilization=0.78, productive=True),
            LiquidityMapNode(node_id="cb-hedge", node_type="portfolio", utilization=0.54, productive=True),
            LiquidityMapNode(node_id="maker", node_type="sleeve", utilization=0.83, productive=True),
            LiquidityMapNode(node_id="USDC", node_type="asset", utilization=0.49, productive=False),
        ],
        edges=[
            LiquidityMapEdge(source="cb-core-mm", target="cb-hedge", suggested_move=120_000),
            LiquidityMapEdge(source="USDC", target="maker", suggested_move=75_000),
        ],
    )


@router.get("/liquidity/recommendations", response_model=list[LiquidityRecommendation])
def liquidity_recommendations() -> list[LiquidityRecommendation]:
    return [
        LiquidityRecommendation(
            recommendation_id="liq-001",
            action="Move 120k USDC from cb-core-mm to cb-hedge delta sleeve",
            expected_efficiency_delta=0.07,
            expected_liquidity_score_delta=0.05,
            confidence=0.82,
        ),
        LiquidityRecommendation(
            recommendation_id="liq-002",
            action="Cancel stale BTC-USD maker inventory and recycle 80k capital",
            expected_efficiency_delta=0.04,
            expected_liquidity_score_delta=0.03,
            confidence=0.76,
        ),
    ]


@router.post("/orders/preview", response_model=OrderPreviewResponse)
def preview_order(req: OrderPreviewRequest) -> OrderPreviewResponse:
    if req.order_type == "limit" and req.limit_price is None:
        raise HTTPException(status_code=400, detail="limit price required for limit order")
    notional = req.size * (req.limit_price or 100)
    preview = OrderPreviewResponse(
        preview_id=f"ord-prev-{uuid4().hex[:10]}",
        estimated_commission=notional * 0.0008,
        estimated_slippage=notional * 0.0006,
        expected_fee_impact=notional * 0.0014,
        resulting_exposure=notional * (1 if req.side == "buy" else -1),
        resulting_risk_usage=0.57,
        approval_required=notional > 250_000,
        confidence=0.88,
    )
    _preview_cache.put(preview.preview_id, preview)
    return preview


@router.post("/orders/submit", response_model=OrderRecord)
def submit_order(req: SubmitOrderRequest, db: Session = Depends(get_db)) -> OrderRecord:
    preview = _preview_cache.get(req.preview_id)
    if not isinstance(preview, OrderPreviewResponse):
        raise HTTPException(status_code=404, detail="order preview not found")
    repo = _repo(db)
    order = OrderModel(
        order_id=f"ord-{uuid4().hex[:10]}",
        preview_id=preview.preview_id,
        strategy_id="adaptive_spread_mm",
        portfolio_id="cb-core-mm",
        sleeve_id="maker",
        product_id="BTC-USD",
        side="buy",
        size=0.5,
        remaining_size=0.5,
        order_type="limit",
        status="open",
        maker_taker_expectation="maker",
        queue_age_s=0,
    )
    repo.create_order(order)
    repo.create_audit_event(
        AuditEventModel(event_type="order_submitted", resource_type="order", resource_id=order.order_id)
    )
    return OrderRecord(
        order_id=order.order_id,
        preview_id=order.preview_id or "",
        strategy_id=order.strategy_id,
        portfolio_id=order.portfolio_id,
        sleeve_id=order.sleeve_id or "",
        product_id=order.product_id,
        side=order.side,
        size=float(order.size),
        remaining_size=float(order.remaining_size),
        order_type=order.order_type,
        status=order.status,
        maker_taker_expectation=order.maker_taker_expectation or "",
        queue_age_s=order.queue_age_s,
        created_at=order.created_at,
    )


@router.get("/orders/open", response_model=list[OrderRecord])
def open_orders(db: Session = Depends(get_db)) -> list[OrderRecord]:
    repo = _repo(db)
    return [
        OrderRecord(
            order_id=o.order_id,
            preview_id=o.preview_id or "",
            strategy_id=o.strategy_id,
            portfolio_id=o.portfolio_id,
            sleeve_id=o.sleeve_id or "",
            product_id=o.product_id,
            side=o.side,
            size=float(o.size),
            remaining_size=float(o.remaining_size),
            order_type=o.order_type,
            status=o.status,
            maker_taker_expectation=o.maker_taker_expectation or "",
            queue_age_s=o.queue_age_s,
            created_at=o.created_at,
        )
        for o in repo.list_orders()
        if o.status == "open"
    ]


@router.post("/orders/{order_id}/cancel")
def cancel_order(order_id: str, db: Session = Depends(get_db)) -> dict:
    repo = _repo(db)
    order = repo.update_order(order_id, status="canceled", remaining_size=0)
    if not order:
        raise HTTPException(status_code=404, detail="order not found")
    repo.create_audit_event(
        AuditEventModel(event_type="order_canceled", resource_type="order", resource_id=order_id)
    )
    return {"status": "canceled", "order_id": order_id}


@router.get("/fills", response_model=list[FillRecord])
def fills(db: Session = Depends(get_db)) -> list[FillRecord]:
    repo = _repo(db)
    return [
        FillRecord(
            fill_id=f.fill_id,
            order_id=f.order_id,
            product_id=f.product_id,
            size=float(f.size),
            price=float(f.price),
            slippage_bps=float(f.slippage_bps),
            fee=float(f.fee),
            at=f.created_at,
        )
        for f in repo.list_fills()
    ]


@router.post("/strategies/backtest/start", response_model=StrategyActionResponse)
def start_backtest(req: BacktestRequest, db: Session = Depends(get_db)) -> StrategyActionResponse:
    repo = _repo(db)
    task_id = f"bt-{uuid4().hex[:10]}"
    run = StrategyRunModel(task_id=task_id, strategy_id=req.strategy_id, status="queued")
    repo.create_strategy_run(run)
    repo.create_audit_event(
        AuditEventModel(
            event_type="strategy_backtest_started",
            resource_type="strategy_run",
            resource_id=task_id,
            details=f"lookback_days={req.lookback_days}, capital={req.capital}",
        )
    )
    return StrategyActionResponse(task_id=task_id, strategy_id=req.strategy_id, status="queued", queued_at=run.queued_at)


@router.post("/strategies/{strategy_id}/start", response_model=StrategyActionResponse)
def start_strategy(strategy_id: str, db: Session = Depends(get_db)) -> StrategyActionResponse:
    mgr = StrategyLifecycleManager(repo=_repo(db))
    run = mgr.start(strategy_id)
    return StrategyActionResponse(task_id=run.task_id, strategy_id=strategy_id, status=run.status, queued_at=run.queued_at)


@router.post("/strategies/{strategy_id}/stop", response_model=StrategyActionResponse)
def stop_strategy(strategy_id: str, db: Session = Depends(get_db)) -> StrategyActionResponse:
    mgr = StrategyLifecycleManager(repo=_repo(db))
    repo = _repo(db)
    latest = repo.db.query(StrategyRunModel).filter(
        StrategyRunModel.strategy_id == strategy_id,
        StrategyRunModel.status == "running",
    ).order_by(StrategyRunModel.queued_at.desc()).first()
    if not latest:
        raise HTTPException(status_code=404, detail=f"no running run for strategy {strategy_id}")
    run = mgr.stop(latest.task_id)
    return StrategyActionResponse(task_id=run.task_id, strategy_id=strategy_id, status="stopped", queued_at=run.queued_at)


@router.post("/strategies/{strategy_id}/pause", response_model=StrategyActionResponse)
def pause_strategy(strategy_id: str, db: Session = Depends(get_db)) -> StrategyActionResponse:
    mgr = StrategyLifecycleManager(repo=_repo(db))
    repo = _repo(db)
    latest = repo.db.query(StrategyRunModel).filter(
        StrategyRunModel.strategy_id == strategy_id,
        StrategyRunModel.status == "running",
    ).order_by(StrategyRunModel.queued_at.desc()).first()
    if not latest:
        raise HTTPException(status_code=404, detail=f"no running run for strategy {strategy_id}")
    run = mgr.pause(latest.task_id)
    return StrategyActionResponse(task_id=run.task_id, strategy_id=strategy_id, status="paused", queued_at=run.queued_at)


@router.post("/strategies/{strategy_id}/resume", response_model=StrategyActionResponse)
def resume_strategy(strategy_id: str, db: Session = Depends(get_db)) -> StrategyActionResponse:
    mgr = StrategyLifecycleManager(repo=_repo(db))
    repo = _repo(db)
    latest = repo.db.query(StrategyRunModel).filter(
        StrategyRunModel.strategy_id == strategy_id,
        StrategyRunModel.status == "paused",
    ).order_by(StrategyRunModel.queued_at.desc()).first()
    if not latest:
        raise HTTPException(status_code=404, detail=f"no paused run for strategy {strategy_id}")
    run = mgr.resume(latest.task_id)
    return StrategyActionResponse(task_id=run.task_id, strategy_id=strategy_id, status="running", queued_at=run.queued_at)


@router.post("/strategies/{strategy_id}/enable")
def enable_strategy(strategy_id: str, db: Session = Depends(get_db)) -> dict:
    mgr = StrategyLifecycleManager(repo=_repo(db))
    result = mgr.enable(strategy_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"strategy {strategy_id} not found")
    return {"strategy_id": strategy_id, "status": "enabled"}


@router.post("/strategies/{strategy_id}/disable")
def disable_strategy(strategy_id: str, db: Session = Depends(get_db)) -> dict:
    mgr = StrategyLifecycleManager(repo=_repo(db))
    result = mgr.disable(strategy_id)
    if not result:
        raise HTTPException(status_code=404, detail=f"strategy {strategy_id} not found")
    return {"strategy_id": strategy_id, "status": "disabled"}


@router.get("/strategies/outcomes/realtime", response_model=list[RealtimeStrategyOutcome])
def strategy_outcomes_realtime() -> list[RealtimeStrategyOutcome]:
    return [
        RealtimeStrategyOutcome(
            strategy_id="adaptive_spread_mm",
            status="running",
            pnl_1h=3200,
            fill_quality_score=0.84,
            consumed_capital=690_000,
            latest_decision="tighten_quotes_due_to_improving_depth",
            at=datetime.now(timezone.utc),
        ),
        RealtimeStrategyOutcome(
            strategy_id="hybrid_hedge",
            status="running",
            pnl_1h=910,
            fill_quality_score=0.78,
            consumed_capital=280_000,
            latest_decision="increase_hedge_ratio_for_btc_inventory",
            at=datetime.now(timezone.utc),
        ),
    ]


@router.get("/ui/theme", response_model=ThemeSettings)
def theme_settings() -> ThemeSettings:
    return ThemeSettings(
        mode=ThemeMode.DARK,
        lightweight=True,
        animation_level="reduced",
        table_density="compact",
        chart_render_mode="canvas",
    )


@router.get("/ui/labels")
def ui_labels() -> dict[str, str]:
    return {
        "portfolio": "portfolios",
        "strategy": "strategies",
        "order": "orders",
        "fill": "fills",
        "approval": "approvals",
        "incident": "incidents",
    }


@router.get("/risk/summary")
def risk_summary() -> dict:
    return {
        "mode": "MARKET_MAKING_PRO",
        "drawdown": 0.021,
        "limit_usage": {"portfolio": 0.61, "strategy": 0.55},
        "warnings": ["minor liquidity-quality warning on ETH-USD"],
        "exchange_trust_state": "HEALTHY",
    }


@router.get("/approvals")
def approvals(db: Session = Depends(get_db)) -> list[dict]:
    repo = _repo(db)
    return [
        {
            "approval_id": a.approval_id,
            "type": a.approval_type,
            "summary": a.summary,
            "capital_affected": float(a.capital_affected),
            "liquidity_impact": a.liquidity_impact,
            "risk_impact": a.risk_impact,
            "expiration": a.expires_at.isoformat() if a.expires_at else None,
        }
        for a in repo.list_approvals()
    ]


@router.get("/alerts")
def alerts(db: Session = Depends(get_db)) -> list[dict]:
    repo = _repo(db)
    return [
        {
            "alert_id": a.alert_id,
            "severity": a.severity,
            "summary": a.summary,
            "acknowledged": a.acknowledged,
        }
        for a in repo.list_alerts()
    ]


@router.get("/incidents")
def incidents(db: Session = Depends(get_db)) -> list[dict]:
    repo = _repo(db)
    return [
        {
            "incident_id": i.incident_id,
            "severity": i.severity,
            "summary": i.summary,
            "status": i.status,
        }
        for i in repo.list_incidents()
    ]


@router.get("/audit")
def audit_events(db: Session = Depends(get_db)) -> list[dict]:
    repo = _repo(db)
    return [
        {
            "event_type": e.event_type,
            "actor": e.actor,
            "resource_type": e.resource_type,
            "resource_id": e.resource_id,
            "details": e.details,
            "created_at": e.created_at.isoformat(),
        }
        for e in repo.list_audit_events()
    ]
