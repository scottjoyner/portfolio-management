from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class ReadModelEnvelope(BaseModel):
    stream: str
    sequence: int
    as_of: datetime
    payload: dict


class DashboardReadModel(BaseModel):
    total_nav: float
    daily_pnl: float
    risk_mode: str
    exchange_trust_state: str
    liquidity_availability_score: float
    idle_capital_score: float
    working_capital_score: float
    feed_health_summary: list[dict]
    active_issues: list[dict]


class PortfolioReadModel(BaseModel):
    portfolio_id: str
    nav: float
    available_capital: float
    liquidity_score: float


class LiquidityMapReadModel(BaseModel):
    nodes: list[dict]
    edges: list[dict]


class OpenOrdersReadModel(BaseModel):
    total_open: int
    by_strategy: dict[str, int]


class ApprovalQueueReadModel(BaseModel):
    pending_count: int
    urgent_count: int


class StrategyOutcomeReadModel(BaseModel):
    strategy_id: str
    status: str
    pnl_1h: float
    fill_quality_score: float
    consumed_capital: float
    latest_decision: str


class ThemeSettingsReadModel(BaseModel):
    mode: str
    lightweight: bool
    animation_level: str
    table_density: str
    chart_render_mode: str
