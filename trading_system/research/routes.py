from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from research.approval import ApprovalService
from research.certification import BacktestCertificationService
from research.hypothesis_registry import HypothesisRegistry
from research.incubation import IncubationService
from storage.postgres.repository import OpsRepository
from storage.postgres.session import get_db

router = APIRouter(prefix="/research", tags=["research"])


class RegisterHypothesisRequest(BaseModel):
    strategy_id: str
    philosophy: str
    target_instruments: list[str]
    timeframe: str
    holding_period: str
    signal_rules: str
    exit_rules: str
    risk_constraints: str
    expected_edge: str
    author: str = "system"


class CertifyRequest(BaseModel):
    strategy_id: str


def _reg(db: Session) -> HypothesisRegistry:
    return HypothesisRegistry(db)


@router.post("/hypotheses")
def register_hypothesis(req: RegisterHypothesisRequest, db: Session = Depends(get_db)) -> dict:
    hyp = _reg(db).register_hypothesis(
        strategy_id=req.strategy_id, philosophy=req.philosophy,
        target_instruments=req.target_instruments, timeframe=req.timeframe,
        holding_period=req.holding_period, signal_rules=req.signal_rules,
        exit_rules=req.exit_rules, risk_constraints=req.risk_constraints,
        expected_edge=req.expected_edge, author=req.author,
    )
    return {
        "hypothesis_id": hyp.hypothesis_id,
        "strategy_id": hyp.strategy_id,
        "config_hash": hyp.config_hash,
        "status": "registered",
    }


@router.get("/hypotheses")
def list_hypotheses(active_only: bool = True, db: Session = Depends(get_db)) -> list[dict]:
    return [
        {
            "hypothesis_id": h.hypothesis_id,
            "strategy_id": h.strategy_id,
            "philosophy": h.philosophy,
            "timeframe": h.timeframe,
            "expected_edge": h.expected_edge,
            "active": h.active,
            "created_at": h.created_at.isoformat(),
        }
        for h in _reg(db).list_hypotheses(active_only=active_only)
    ]


@router.get("/hypotheses/{strategy_id}")
def get_strategy_hypotheses(strategy_id: str, db: Session = Depends(get_db)) -> list[dict]:
    return [
        {
            "hypothesis_id": h.hypothesis_id,
            "philosophy": h.philosophy,
            "target_instruments": h.target_instruments,
            "timeframe": h.timeframe,
            "holding_period": h.holding_period,
            "signal_rules": h.signal_rules,
            "exit_rules": h.exit_rules,
            "risk_constraints": h.risk_constraints,
            "expected_edge": h.expected_edge,
            "config_hash": h.config_hash,
            "active": h.active,
            "version": h.version,
            "created_at": h.created_at.isoformat(),
        }
        for h in _reg(db).get_strategy_hypotheses(strategy_id)
    ]


@router.post("/certify")
def certify_strategy(req: CertifyRequest, db: Session = Depends(get_db)) -> dict:
    svc = BacktestCertificationService(db)
    result = svc.certify(req.strategy_id)
    return {
        "strategy_id": req.strategy_id,
        "status": result.status,
        "sharpe": result.sharpe,
        "max_drawdown": result.max_drawdown,
        "total_return": result.total_return,
        "live_transfer_confidence": result.live_transfer_confidence,
        "fragility_score": result.fragility_score,
        "check_details": result.check_details,
        "rejection_reason": result.rejection_reason,
    }


@router.get("/certifications/{strategy_id}")
def get_certifications(strategy_id: str, db: Session = Depends(get_db)) -> list[dict]:
    return [
        {
            "id": c.id,
            "hypothesis_id": c.hypothesis_id,
            "status": c.status,
            "sharpe": float(c.sharpe) if c.sharpe else None,
            "max_drawdown": float(c.max_drawdown) if c.max_drawdown else None,
            "live_transfer_confidence": float(c.live_transfer_confidence) if c.live_transfer_confidence else None,
            "fragility_score": float(c.fragility_score) if c.fragility_score else None,
            "rejection_reason": c.rejection_reason,
            "certified_at": c.certified_at.isoformat() if c.certified_at else None,
        }
        for c in _reg(db).get_certifications(strategy_id)
    ]


@router.get("/verify-backtest/{strategy_id}")
def verify_backtest_eligible(strategy_id: str, db: Session = Depends(get_db)) -> dict:
    eligible, reason = _reg(db).verify_backtest_eligible(strategy_id)
    return {"strategy_id": strategy_id, "eligible": eligible, "reason": reason}


@router.get("/verify-live/{strategy_id}")
def verify_live_eligible(strategy_id: str, db: Session = Depends(get_db)) -> dict:
    eligible, reason = _reg(db).verify_live_eligible(strategy_id)
    return {"strategy_id": strategy_id, "eligible": eligible, "reason": reason}


class TrackRunRequest(BaseModel):
    strategy_id: str
    total_orders: int = 0
    total_fills: int = 0
    total_fill_value: float = 0
    avg_slippage_bps: float = 0
    realized_pnl: float = 0
    backtest_assumptions: dict[str, Any] = {}


@router.post("/incubate/track")
def track_incubation(req: TrackRunRequest, db: Session = Depends(get_db)) -> dict:
    from storage.postgres.models import StrategyRun
    run = db.query(StrategyRun).filter(
        StrategyRun.strategy_id == req.strategy_id,
        StrategyRun.mode == "paper",
        StrategyRun.status == "running",
    ).order_by(StrategyRun.queued_at.desc()).first()

    if not run:
        task_id = f"inc-{req.strategy_id}-{uuid4().hex[:8]}"
        run = StrategyRun(task_id=task_id, strategy_id=req.strategy_id, mode="paper", status="running")
        db.add(run)
        db.commit()

    svc = IncubationService(db)

    class _MockEngine:
        class _MockPosition:
            realized_pnl = Decimal(str(req.realized_pnl))
        def __init__(self):
            self.fills = []
            self.positions = {"default": self._MockPosition()}

    report = svc.track_run(run.task_id, _MockEngine(), req.backtest_assumptions)
    return {
        "strategy_id": req.strategy_id,
        "task_id": report.task_id,
        "total_orders": report.total_orders,
        "total_fills": report.total_fills,
        "avg_slippage_bps": round(report.avg_slippage_bps, 2),
        "fill_rate": round(report.fill_rate, 4),
        "realized_pnl": float(report.realized_pnl),
        "slippage_drift_bps": round(report.slippage_drift_bps, 2),
        "fill_quality_ratio": round(report.fill_quality_ratio, 4),
        "started_at": report.started_at.isoformat(),
    }


@router.post("/incubate/approve-live/{task_id}")
def approve_incubation(task_id: str, db: Session = Depends(get_db)) -> dict:
    svc = IncubationService(db)
    ok, msg = svc.approve_for_live(task_id)
    if not ok:
        raise HTTPException(status_code=400, detail=msg)
    return {"task_id": task_id, "status": "approved_for_live"}


class ShadowRequest(BaseModel):
    strategy_id: str
    product_id: str
    side: str
    size: float
    limit_price: float | None = None


@router.post("/shadow")
def generate_shadow_payload(req: ShadowRequest, db: Session = Depends(get_db)) -> dict:
    svc = IncubationService(db)
    market_data = {
        "price": 60000.0,
        "spread_bps": 8.0,
        "volume_24h": 1_500_000,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    intent = {
        "strategy_id": req.strategy_id,
        "product_id": req.product_id,
        "side": req.side,
        "size": req.size,
        "limit_price": req.limit_price,
    }
    return svc.shadow_payload(market_data, intent)


# ---------------------------------------------------------------------------
# P5 — Approval endpoints
# ---------------------------------------------------------------------------


class CreateStrategyApprovalRequest(BaseModel):
    strategy_id: str
    hypothesis_id: str | None = None
    required_approver: str = "admin"
    expires_in_days: int = 30


class ApproveRejectRequest(BaseModel):
    actor: str
    reason: str | None = None


class CreateTradeApprovalRequest(BaseModel):
    strategy_id: str
    strategy_approval_id: str
    account: str = "default"
    venue: str = "coinbase"
    product_id: str
    side: str
    order_type: str = "limit"
    expected_slippage_bps: float = 0
    expected_fee_bps: float = 0
    spread_bps: float = 0
    liquidity_score: float = 0.8
    fill_risk_score: float = 0.1
    position_exposure_impact_pct: float = 0
    portfolio_exposure_impact_pct: float = 0
    holding_period: str | None = None
    exit_plan: str | None = None
    fair_value_low: float | None = None
    fair_value_high: float | None = None


def _aps(db: Session) -> ApprovalService:
    return ApprovalService(db)


@router.post("/approvals/strategy")
def create_strategy_approval(req: CreateStrategyApprovalRequest, db: Session = Depends(get_db)) -> dict:
    ap = _aps(db).create_strategy_approval(
        strategy_id=req.strategy_id, hypothesis_id=req.hypothesis_id,
        required_approver=req.required_approver, expires_in_days=req.expires_in_days,
    )
    return {
        "approval_id": ap.approval_id,
        "strategy_id": ap.strategy_id,
        "status": ap.status,
        "required_approver": ap.required_approver,
        "expires_at": ap.expires_at.isoformat() if ap.expires_at else None,
    }


@router.get("/approvals/strategy")
def list_strategy_approvals(
    strategy_id: str | None = None, status: str | None = None, db: Session = Depends(get_db),
) -> list[dict]:
    return [
        {
            "approval_id": a.approval_id,
            "strategy_id": a.strategy_id,
            "hypothesis_id": a.hypothesis_id,
            "status": a.status,
            "required_approver": a.required_approver,
            "approved_by": a.approved_by,
            "status_reason": a.status_reason,
            "expires_at": a.expires_at.isoformat() if a.expires_at else None,
            "created_at": a.created_at.isoformat(),
        }
        for a in _aps(db).list_strategy_approvals(strategy_id=strategy_id, status=status)
    ]


@router.post("/approvals/strategy/{approval_id}/approve")
def approve_strategy(approval_id: str, req: ApproveRejectRequest, db: Session = Depends(get_db)) -> dict:
    ap = _aps(db).approve_strategy(approval_id, req.actor, req.reason)
    return {"approval_id": ap.approval_id, "status": ap.status, "approved_by": ap.approved_by}


@router.post("/approvals/strategy/{approval_id}/reject")
def reject_strategy(approval_id: str, req: ApproveRejectRequest, db: Session = Depends(get_db)) -> dict:
    ap = _aps(db).reject_strategy(approval_id, req.actor, req.reason or "Rejected")
    return {"approval_id": ap.approval_id, "status": ap.status, "status_reason": ap.status_reason}


@router.post("/approvals/expire-pending")
def expire_pending_approvals(db: Session = Depends(get_db)) -> dict:
    n = _aps(db).expire_pending()
    return {"expired_count": n}


@router.post("/approvals/trade")
def create_trade_approval(req: CreateTradeApprovalRequest, db: Session = Depends(get_db)) -> dict:
    ta = _aps(db).create_trade_approval(
        strategy_id=req.strategy_id, strategy_approval_id=req.strategy_approval_id,
        account=req.account, venue=req.venue, product_id=req.product_id,
        side=req.side, order_type=req.order_type,
        expected_slippage_bps=req.expected_slippage_bps, expected_fee_bps=req.expected_fee_bps,
        spread_bps=req.spread_bps, liquidity_score=req.liquidity_score,
        fill_risk_score=req.fill_risk_score,
        position_exposure_impact_pct=req.position_exposure_impact_pct,
        portfolio_exposure_impact_pct=req.portfolio_exposure_impact_pct,
        holding_period=req.holding_period, exit_plan=req.exit_plan,
        fair_value_low=req.fair_value_low, fair_value_high=req.fair_value_high,
    )
    return {
        "approval_id": ta.approval_id,
        "strategy_id": ta.strategy_id,
        "product_id": ta.product_id,
        "status": ta.status,
    }


@router.get("/approvals/trade")
def list_trade_approvals(
    strategy_id: str | None = None, status: str | None = None, db: Session = Depends(get_db),
) -> list[dict]:
    return [
        {
            "approval_id": a.approval_id,
            "strategy_approval_id": a.strategy_approval_id,
            "strategy_id": a.strategy_id,
            "product_id": a.product_id,
            "side": a.side,
            "order_type": a.order_type,
            "status": a.status,
            "expected_slippage_bps": float(a.expected_slippage_bps) if a.expected_slippage_bps else None,
            "fill_risk_score": float(a.fill_risk_score) if a.fill_risk_score else None,
            "approved_by": a.approved_by,
            "created_at": a.created_at.isoformat(),
        }
        for a in _aps(db).list_trade_approvals(strategy_id=strategy_id, status=status)
    ]


@router.post("/approvals/trade/{approval_id}/approve")
def approve_trade(approval_id: str, req: ApproveRejectRequest, db: Session = Depends(get_db)) -> dict:
    ta = _aps(db).approve_trade(approval_id, req.actor)
    return {"approval_id": ta.approval_id, "status": ta.status}


@router.post("/approvals/trade/{approval_id}/reject")
def reject_trade(approval_id: str, req: ApproveRejectRequest, db: Session = Depends(get_db)) -> dict:
    ta = _aps(db).reject_trade(approval_id, req.actor, req.reason or "Rejected")
    return {"approval_id": ta.approval_id, "status": ta.status, "status_reason": ta.status_reason}


@router.get("/approvals/check-strategy/{strategy_id}")
def check_strategy_approved(strategy_id: str, db: Session = Depends(get_db)) -> dict:
    ok, ref = _aps(db).check_strategy_approved(strategy_id)
    return {"strategy_id": strategy_id, "approved": ok, "reference": ref}


@router.get("/approvals/check-trade/{strategy_id}/{product_id}")
def check_trade_approved(strategy_id: str, product_id: str, db: Session = Depends(get_db)) -> dict:
    ok, ref = _aps(db).check_trade_approved(strategy_id, product_id)
    return {"strategy_id": strategy_id, "product_id": product_id, "approved": ok, "reference": ref}
