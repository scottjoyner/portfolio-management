from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy.orm import Session

from research.certification import BacktestCertificationService
from research.hypothesis_registry import HypothesisRegistry
from storage.postgres.models import StrategyApproval, TradeApproval, StrategyConfig, StrategyHypothesis
from storage.postgres.repository import OpsRepository


class ApprovalService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.repo = OpsRepository(db)
        self.registry = HypothesisRegistry(db)
        self.cert_svc = BacktestCertificationService(db)

    def create_strategy_approval(
        self,
        strategy_id: str,
        hypothesis_id: str | None = None,
        required_approver: str = "admin",
        expires_in_days: int = 30,
    ) -> StrategyApproval:
        cfg = self.db.query(StrategyConfig).filter(
            StrategyConfig.strategy_id == strategy_id
        ).first()

        hyp: StrategyHypothesis | None = None
        if hypothesis_id:
            hyp = self.db.query(StrategyHypothesis).filter(
                StrategyHypothesis.hypothesis_id == hypothesis_id
            ).first()
        elif cfg and cfg.hypothesis_id:
            hyp = self.db.query(StrategyHypothesis).filter(
                StrategyHypothesis.hypothesis_id == cfg.hypothesis_id
            ).first()

        certs = self.registry.get_certifications(strategy_id)
        latest_cert = certs[-1] if certs else None

        backtest_evidence = {
            "sharpe": float(latest_cert.sharpe) if latest_cert and latest_cert.sharpe else None,
            "max_drawdown": float(latest_cert.max_drawdown) if latest_cert and latest_cert.max_drawdown else None,
            "total_return": float(latest_cert.total_return) if latest_cert and latest_cert.total_return else None,
            "live_transfer_confidence": float(latest_cert.live_transfer_confidence) if latest_cert and latest_cert.live_transfer_confidence else None,
            "fragility_score": float(latest_cert.fragility_score) if latest_cert and latest_cert.fragility_score else None,
            "certification_status": latest_cert.status if latest_cert else None,
        } if latest_cert else {}

        now = datetime.now(timezone.utc)
        approval = StrategyApproval(
            approval_id=f"sa-{strategy_id}-{now.strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:6]}",
            strategy_id=strategy_id,
            hypothesis_id=hyp.hypothesis_id if hyp else None,
            config_hash=cfg.config_hash if cfg and cfg.config_hash else hyp.config_hash if hyp else None,
            philosophy=hyp.philosophy if hyp else None,
            target_instruments=hyp.target_instruments if hyp else None,
            signal_rules=hyp.signal_rules if hyp else None,
            backtest_evidence_json=json.dumps(backtest_evidence) if backtest_evidence else None,
            expected_return_range=hyp.expected_edge if hyp else None,
            holding_period=hyp.holding_period if hyp else None,
            exit_criteria=hyp.exit_rules if hyp else None,
            required_approver=required_approver,
            expires_at=datetime.fromtimestamp(now.timestamp() + expires_in_days * 86400, tz=timezone.utc),
        )
        self.db.add(approval)
        self.db.commit()
        self.db.refresh(approval)
        return approval

    def get_strategy_approval(self, approval_id: str) -> StrategyApproval | None:
        return self.db.query(StrategyApproval).filter(
            StrategyApproval.approval_id == approval_id
        ).first()

    def list_strategy_approvals(
        self, strategy_id: str | None = None, status: str | None = None,
    ) -> list[StrategyApproval]:
        q = self.db.query(StrategyApproval)
        if strategy_id:
            q = q.filter(StrategyApproval.strategy_id == strategy_id)
        if status:
            q = q.filter(StrategyApproval.status == status)
        return q.order_by(StrategyApproval.created_at.desc()).all()

    def approve_strategy(self, approval_id: str, approved_by: str, notes: str | None = None) -> StrategyApproval:
        approval = self.get_strategy_approval(approval_id)
        if not approval:
            raise ValueError(f"approval {approval_id} not found")
        if approval.status != "pending":
            raise ValueError(f"approval {approval_id} is {approval.status}, not pending")
        approval.status = "approved"
        approval.approved_by = approved_by
        approval.status_reason = notes
        approval.decided_at = datetime.now(timezone.utc)
        self.db.commit()
        self.db.refresh(approval)

        cfg = self.db.query(StrategyConfig).filter(
            StrategyConfig.strategy_id == approval.strategy_id
        ).first()
        if cfg:
            cfg.status = "approved"
            self.db.commit()
        return approval

    def reject_strategy(self, approval_id: str, rejected_by: str, reason: str) -> StrategyApproval:
        approval = self.get_strategy_approval(approval_id)
        if not approval:
            raise ValueError(f"approval {approval_id} not found")
        approval.status = "rejected"
        approval.approved_by = rejected_by
        approval.status_reason = reason
        approval.decided_at = datetime.now(timezone.utc)
        self.db.commit()
        self.db.refresh(approval)
        return approval

    def expire_pending(self) -> int:
        now = datetime.now(timezone.utc)
        all_pending = self.db.query(StrategyApproval).filter(
            StrategyApproval.status == "pending",
        ).all()
        expired = [a for a in all_pending if a.expires_at is not None]
        expired = [a for a in expired if (
            a.expires_at.tzinfo is None
            and a.expires_at.replace(tzinfo=timezone.utc) < now
        ) or (
            a.expires_at.tzinfo is not None and a.expires_at < now
        )]
        for a in expired:
            a.status = "expired"
            a.status_reason = "Expired without decision"
            a.decided_at = now
        self.db.commit()
        return len(expired)

    def check_strategy_approved(self, strategy_id: str) -> tuple[bool, str]:
        active = self.db.query(StrategyApproval).filter(
            StrategyApproval.strategy_id == strategy_id,
            StrategyApproval.status == "approved",
        ).order_by(StrategyApproval.created_at.desc()).first()
        if not active:
            return False, "no active strategy approval"
        if active.expires_at:
            exp = active.expires_at
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if exp < datetime.now(timezone.utc):
                active.status = "expired"
                self.db.commit()
                return False, "strategy approval expired"
        return True, active.approval_id

    # ------------------------------------------------------------------
    # Trade approvals
    # ------------------------------------------------------------------

    def create_trade_approval(
        self,
        strategy_id: str,
        strategy_approval_id: str,
        account: str,
        venue: str,
        product_id: str,
        side: str,
        order_type: str,
        expected_slippage_bps: float = 0.0,
        expected_fee_bps: float = 0.0,
        spread_bps: float = 0.0,
        liquidity_score: float = 0.0,
        fill_risk_score: float = 0.0,
        position_exposure_impact_pct: float = 0.0,
        portfolio_exposure_impact_pct: float = 0.0,
        holding_period: str | None = None,
        exit_plan: str | None = None,
        fair_value_low: float | None = None,
        fair_value_high: float | None = None,
        order_bounds: dict[str, Any] | None = None,
    ) -> TradeApproval:
        now = datetime.now(timezone.utc)
        approval = TradeApproval(
            approval_id=f"ta-{strategy_id}-{now.strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:8]}",
            strategy_approval_id=strategy_approval_id,
            strategy_id=strategy_id,
            account=account,
            venue=venue,
            product_id=product_id,
            side=side,
            order_type=order_type,
            order_bounds_json=json.dumps(order_bounds) if order_bounds else None,
            fair_value_low=fair_value_low,
            fair_value_high=fair_value_high,
            expected_slippage_bps=expected_slippage_bps,
            expected_fee_bps=expected_fee_bps,
            spread_bps=spread_bps,
            liquidity_score=liquidity_score,
            fill_risk_score=fill_risk_score,
            position_exposure_impact_pct=position_exposure_impact_pct,
            portfolio_exposure_impact_pct=portfolio_exposure_impact_pct,
            holding_period=holding_period,
            exit_plan=exit_plan,
        )
        self.db.add(approval)
        self.db.commit()
        self.db.refresh(approval)
        return approval

    def approve_trade(self, approval_id: str, approved_by: str) -> TradeApproval:
        approval = self.db.query(TradeApproval).filter(
            TradeApproval.approval_id == approval_id
        ).first()
        if not approval:
            raise ValueError(f"trade approval {approval_id} not found")
        approval.status = "approved"
        approval.approved_by = approved_by
        approval.decided_at = datetime.now(timezone.utc)
        self.db.commit()
        self.db.refresh(approval)
        return approval

    def reject_trade(self, approval_id: str, rejected_by: str, reason: str) -> TradeApproval:
        approval = self.db.query(TradeApproval).filter(
            TradeApproval.approval_id == approval_id
        ).first()
        if not approval:
            raise ValueError(f"trade approval {approval_id} not found")
        approval.status = "rejected"
        approval.approved_by = rejected_by
        approval.status_reason = reason
        approval.decided_at = datetime.now(timezone.utc)
        self.db.commit()
        self.db.refresh(approval)
        return approval

    def check_trade_approved(self, strategy_id: str, product_id: str) -> tuple[bool, str]:
        active = self.db.query(TradeApproval).filter(
            TradeApproval.strategy_id == strategy_id,
            TradeApproval.product_id == product_id,
            TradeApproval.status == "approved",
        ).order_by(TradeApproval.created_at.desc()).first()
        if not active:
            return False, "no active trade approval for this product"
        return True, active.approval_id

    def list_trade_approvals(
        self, strategy_id: str | None = None, status: str | None = None,
    ) -> list[TradeApproval]:
        q = self.db.query(TradeApproval)
        if strategy_id:
            q = q.filter(TradeApproval.strategy_id == strategy_id)
        if status:
            q = q.filter(TradeApproval.status == status)
        return q.order_by(TradeApproval.created_at.desc()).all()
