from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from storage.postgres.models import StrategyCertification, StrategyConfig, StrategyHypothesis


def compute_config_hash(config: dict[str, Any]) -> str:
    raw = json.dumps(config, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


class HypothesisRegistry:
    def __init__(self, db: Session) -> None:
        self.db = db

    def register_hypothesis(
        self,
        strategy_id: str,
        philosophy: str,
        target_instruments: list[str],
        timeframe: str,
        holding_period: str,
        signal_rules: str,
        exit_rules: str,
        risk_constraints: str,
        expected_edge: str,
        author: str = "system",
        config: dict[str, Any] | None = None,
    ) -> StrategyHypothesis:
        config = config or {}
        config_hash = compute_config_hash(config)
        hypothesis_id = f"hyp-{strategy_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

        hyp = StrategyHypothesis(
            hypothesis_id=hypothesis_id,
            strategy_id=strategy_id,
            config_hash=config_hash,
            philosophy=philosophy,
            target_instruments=json.dumps(target_instruments),
            timeframe=timeframe,
            holding_period=holding_period,
            signal_rules=signal_rules,
            exit_rules=exit_rules,
            risk_constraints=risk_constraints,
            expected_edge=expected_edge,
            config_snapshot=json.dumps(config, default=str),
            author=author,
        )
        self.db.add(hyp)

        cfg = self.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
        if not cfg:
            cfg = StrategyConfig(strategy_id=strategy_id, strategy_type=philosophy, status="implemented")
            self.db.add(cfg)
        cfg.hypothesis_id = hypothesis_id
        cfg.config_hash = config_hash

        self.db.commit()
        return hyp

    def get_hypothesis(self, hypothesis_id: str) -> StrategyHypothesis | None:
        return self.db.query(StrategyHypothesis).filter(StrategyHypothesis.hypothesis_id == hypothesis_id).first()

    def get_strategy_hypotheses(self, strategy_id: str) -> list[StrategyHypothesis]:
        return self.db.query(StrategyHypothesis).filter(
            StrategyHypothesis.strategy_id == strategy_id
        ).order_by(StrategyHypothesis.created_at.desc()).all()

    def list_hypotheses(self, active_only: bool = True) -> list[StrategyHypothesis]:
        q = self.db.query(StrategyHypothesis)
        if active_only:
            q = q.filter(StrategyHypothesis.active.is_(True))
        return q.order_by(StrategyHypothesis.created_at.desc()).all()

    def verify_backtest_eligible(self, strategy_id: str) -> tuple[bool, str]:
        hyp = self.db.query(StrategyHypothesis).filter(
            StrategyHypothesis.strategy_id == strategy_id,
            StrategyHypothesis.active.is_(True),
        ).first()
        if not hyp:
            return False, f"strategy {strategy_id} has no active registered hypothesis"

        cfg = self.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
        if cfg and cfg.hypothesis_id != hyp.hypothesis_id:
            cfg.hypothesis_id = hyp.hypothesis_id

        return True, "eligible"

    def record_certification(
        self,
        hypothesis_id: str,
        strategy_id: str,
        status: str,
        sharpe: float | None = None,
        max_drawdown: float | None = None,
        total_return: float | None = None,
        win_rate: float | None = None,
        profit_factor: float | None = None,
        live_transfer_confidence: float | None = None,
        fragility_score: float | None = None,
        check_details: str | None = None,
        rejection_reason: str | None = None,
    ) -> StrategyCertification:
        cert = StrategyCertification(
            hypothesis_id=hypothesis_id,
            strategy_id=strategy_id,
            status=status,
            sharpe=sharpe,
            max_drawdown=max_drawdown,
            total_return=total_return,
            win_rate=win_rate,
            profit_factor=profit_factor,
            live_transfer_confidence=live_transfer_confidence,
            fragility_score=fragility_score,
            check_details=check_details,
            rejection_reason=rejection_reason,
            certified_at=datetime.now(timezone.utc) if status == "certified" else None,
        )
        self.db.add(cert)

        if status == "certified":
            cfg = self.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
            if cfg:
                cfg.certification_status = "certified"

        self.db.commit()
        return cert

    def get_certifications(self, strategy_id: str) -> list[StrategyCertification]:
        return self.db.query(StrategyCertification).filter(
            StrategyCertification.strategy_id == strategy_id
        ).order_by(StrategyCertification.created_at.desc()).all()

    def verify_live_eligible(self, strategy_id: str) -> tuple[bool, str]:
        cfg = self.db.query(StrategyConfig).filter(StrategyConfig.strategy_id == strategy_id).first()
        if not cfg:
            return False, "strategy not found"
        if cfg.certification_status != "certified":
            return False, f"strategy {strategy_id} is {cfg.certification_status}; must be certified for live"
        if not cfg.enabled:
            return False, f"strategy {strategy_id} is disabled"
        return True, "eligible"
