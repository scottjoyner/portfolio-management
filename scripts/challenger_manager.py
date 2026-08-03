#!/usr/bin/env python3
"""Versioned challenger evaluation, promotion, and rollback for the paid agent."""
from __future__ import annotations

import json
import math
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.learning_lineage import LineageStore

ROOT = Path(__file__).resolve().parent.parent
REGISTRY_PATH = ROOT / "data" / "learning" / "challengers.json"
ACTIVE_CONFIG_PATH = ROOT / "data" / "agent_runtime_config.json"

DEFAULT_THRESHOLDS = {
    "min_total_trades": 30,
    "min_out_of_sample_trades": 10,
    "min_regimes": 3,
    "min_profit_factor": 1.10,
    "min_cost_coverage_ratio": 1.0,
    "max_drawdown_increase_pct_points": 1.0,
    "min_net_pnl_improvement_usd": 1.0,
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _num(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def evaluate_challenger(
    incumbent: dict[str, Any],
    challenger: dict[str, Any],
    thresholds: dict[str, Any] | None = None,
) -> dict[str, Any]:
    limits = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    reasons: list[str] = []
    if int(_num(challenger.get("total_trades"))) < int(limits["min_total_trades"]):
        reasons.append("insufficient_total_trades")
    if int(_num(challenger.get("out_of_sample_trades"))) < int(limits["min_out_of_sample_trades"]):
        reasons.append("insufficient_out_of_sample_trades")
    if int(_num(challenger.get("regimes_tested"))) < int(limits["min_regimes"]):
        reasons.append("insufficient_regime_diversity")
    if _num(challenger.get("profit_factor")) < _num(limits["min_profit_factor"]):
        reasons.append("profit_factor_below_floor")
    if _num(challenger.get("cost_coverage_ratio")) < _num(limits["min_cost_coverage_ratio"]):
        reasons.append("agent_cost_not_covered")
    pnl_improvement = _num(challenger.get("net_pnl_after_cost_usd")) - _num(incumbent.get("net_pnl_after_cost_usd"))
    if pnl_improvement < _num(limits["min_net_pnl_improvement_usd"]):
        reasons.append("net_pnl_improvement_below_floor")
    dd_increase = _num(challenger.get("max_drawdown_pct")) - _num(incumbent.get("max_drawdown_pct"))
    if dd_increase > _num(limits["max_drawdown_increase_pct_points"]):
        reasons.append("drawdown_regression")
    if challenger.get("walk_forward_passed") is not True:
        reasons.append("walk_forward_failed")
    if challenger.get("accounting_invariants_ok") is not True:
        reasons.append("accounting_invariants_failed")
    if challenger.get("lineage_verified") is not True:
        reasons.append("lineage_verification_failed")
    return {
        "approved": not reasons,
        "reasons": reasons,
        "pnl_improvement_usd": round(pnl_improvement, 8),
        "drawdown_increase_pct_points": round(dd_increase, 8),
        "thresholds": limits,
        "evaluated_at": _utc_now(),
    }


class ChallengerRegistry:
    def __init__(
        self,
        registry_path: Path | str = REGISTRY_PATH,
        active_config_path: Path | str = ACTIVE_CONFIG_PATH,
        lineage: LineageStore | None = None,
    ):
        self.registry_path = Path(registry_path)
        self.active_config_path = Path(active_config_path)
        self.lineage = lineage or LineageStore()

    def load(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return {"schema_version": 1, "incumbent_id": None, "challengers": [], "promotions": []}
        payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("challenger registry root must be an object")
        payload.setdefault("challengers", [])
        payload.setdefault("promotions", [])
        return payload

    def save(self, payload: dict[str, Any]) -> None:
        payload["updated_at"] = _utc_now()
        _atomic(self.registry_path, payload)

    def propose(
        self,
        parameters: dict[str, Any],
        *,
        rationale: str,
        model_request_id: str,
        evidence_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        registry = self.load()
        challenger = {
            "id": f"challenger-{uuid.uuid4().hex[:12]}",
            "version": len(registry["challengers"]) + 1,
            "status": "proposed",
            "parameters": parameters,
            "rationale": rationale,
            "model_request_id": model_request_id,
            "evidence_ids": evidence_ids or [],
            "created_at": _utc_now(),
            "evaluation": None,
        }
        proposal = self.lineage.append(
            "proposal",
            {"challenger_id": challenger["id"], "parameters": parameters, "rationale": rationale},
            actor="openrouter-agent",
            parents=[model_request_id, *(evidence_ids or [])],
        )
        challenger["proposal_lineage_id"] = proposal["id"]
        registry["challengers"].append(challenger)
        self.save(registry)
        return challenger

    def evaluate(
        self,
        challenger_id: str,
        incumbent_metrics: dict[str, Any],
        challenger_metrics: dict[str, Any],
        thresholds: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        registry = self.load()
        challenger = next((row for row in registry["challengers"] if row["id"] == challenger_id), None)
        if not challenger:
            raise KeyError(challenger_id)
        result = evaluate_challenger(incumbent_metrics, challenger_metrics, thresholds)
        event = self.lineage.append(
            "evaluation",
            {
                "challenger_id": challenger_id,
                "incumbent_metrics": incumbent_metrics,
                "challenger_metrics": challenger_metrics,
                "result": result,
            },
            actor="promotion-gate",
            parents=[challenger["proposal_lineage_id"]],
        )
        challenger["status"] = "approved" if result["approved"] else "rejected"
        challenger["evaluation"] = result
        challenger["evaluation_lineage_id"] = event["id"]
        self.save(registry)
        return result

    def promote(self, challenger_id: str, *, canary_fraction: float = 0.10) -> dict[str, Any]:
        registry = self.load()
        challenger = next((row for row in registry["challengers"] if row["id"] == challenger_id), None)
        if not challenger:
            raise KeyError(challenger_id)
        if challenger.get("status") != "approved":
            raise ValueError("challenger has not passed the promotion gate")
        previous = None
        if self.active_config_path.exists():
            previous = json.loads(self.active_config_path.read_text(encoding="utf-8"))
        config = {
            "schema_version": 1,
            "active_challenger_id": challenger_id,
            "parameters": challenger["parameters"],
            "deployment": "canary",
            "canary_fraction": max(0.01, min(1.0, _num(canary_fraction, 0.10))),
            "promoted_at": _utc_now(),
            "rollback_config": previous,
            "promotion_lineage_id": None,
        }
        event = self.lineage.append(
            "promotion",
            {"challenger_id": challenger_id, "canary_fraction": config["canary_fraction"]},
            actor="promotion-gate",
            parents=[challenger["evaluation_lineage_id"]],
        )
        config["promotion_lineage_id"] = event["id"]
        _atomic(self.active_config_path, config)
        challenger["status"] = "canary"
        challenger["promotion_lineage_id"] = event["id"]
        registry["incumbent_id"] = challenger_id
        registry["promotions"].append({"challenger_id": challenger_id, "at": _utc_now(), "lineage_id": event["id"]})
        self.save(registry)
        return config

    def rollback(self, reason: str) -> dict[str, Any]:
        if not self.active_config_path.exists():
            raise FileNotFoundError(self.active_config_path)
        current = json.loads(self.active_config_path.read_text(encoding="utf-8"))
        previous = current.get("rollback_config")
        if previous is None:
            raise ValueError("no rollback configuration is available")
        _atomic(self.active_config_path, previous)
        promotion_lineage_id = current.get("promotion_lineage_id")
        event = self.lineage.append(
            "rollback",
            {"rolled_back_challenger_id": current.get("active_challenger_id"), "reason": reason},
            actor="risk-controller",
            parents=[promotion_lineage_id] if promotion_lineage_id else [],
        )
        return {"rolled_back": True, "reason": reason, "restored": previous, "lineage_id": event["id"]}
