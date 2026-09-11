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

from scripts.alpha_validation import evidence_to_challenger_metrics, verify_alpha_validation_evidence
from scripts.backtest_framework.canonical_replay import verify_evidence_replay_binding
from scripts.learning_lineage import LineageStore
from scripts.research_tournament import verify_terminal_holdout_evidence
from scripts.runtime_research_certification import build_runtime_research_certification

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
    """Low-level metric gate.

    New promotion workflows must reach this function through
    :func:`evaluate_challenger_evidence`; the registry fails closed when no
    canonical alpha-validation evidence is supplied.
    """

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


def evaluate_challenger_evidence(
    incumbent: dict[str, Any],
    evidence: Any,
    thresholds: dict[str, Any] | None = None,
    *,
    require_replay_provenance: bool = False,
) -> dict[str, Any]:
    """Verify immutable alpha evidence before applying the metric gate."""

    valid, evidence_reasons = verify_alpha_validation_evidence(evidence)
    if not valid:
        return {
            "approved": False,
            "reasons": [f"alpha_validation_evidence_invalid:{reason}" for reason in evidence_reasons],
            "pnl_improvement_usd": 0.0,
            "drawdown_increase_pct_points": 0.0,
            "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
            "evaluated_at": _utc_now(),
            "evidence_hash": evidence.get("evidence_hash") if isinstance(evidence, dict) else None,
            "evidence_schema_version": evidence.get("schema_version") if isinstance(evidence, dict) else None,
        }

    if require_replay_provenance:
        if evidence.get("replay_provenance_bound") is not True:
            return {
                "approved": False,
                "reasons": ["alpha_validation_replay_provenance_required"],
                "pnl_improvement_usd": 0.0,
                "drawdown_increase_pct_points": 0.0,
                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
                "evaluated_at": _utc_now(),
                "evidence_hash": evidence.get("evidence_hash"),
                "evidence_schema_version": evidence.get("schema_version"),
            }
        attestation = evidence.get("replay_attestation")
        if not isinstance(attestation, dict) or attestation.get("execution_config_bound") is not True:
            return {
                "approved": False,
                "reasons": ["alpha_validation_execution_config_binding_required"],
                "pnl_improvement_usd": 0.0,
                "drawdown_increase_pct_points": 0.0,
                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
                "evaluated_at": _utc_now(),
                "evidence_hash": evidence.get("evidence_hash"),
                "evidence_schema_version": evidence.get("schema_version"),
            }
        if attestation.get("strategy_config") != evidence.get("candidate_config"):
            return {
                "approved": False,
                "reasons": ["alpha_validation_execution_config_mismatch"],
                "pnl_improvement_usd": 0.0,
                "drawdown_increase_pct_points": 0.0,
                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
                "evaluated_at": _utc_now(),
                "evidence_hash": evidence.get("evidence_hash"),
                "evidence_schema_version": evidence.get("schema_version"),
            }
        replay_valid, replay_reasons = verify_evidence_replay_binding(
            evidence, reverify_source=True
        )
        if not replay_valid:
            return {
                "approved": False,
                "reasons": [
                    f"alpha_validation_replay_invalid:{reason}"
                    for reason in replay_reasons
                ],
                "pnl_improvement_usd": 0.0,
                "drawdown_increase_pct_points": 0.0,
                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
                "evaluated_at": _utc_now(),
                "evidence_hash": evidence.get("evidence_hash"),
                "evidence_schema_version": evidence.get("schema_version"),
            }

    challenger_metrics = evidence_to_challenger_metrics(evidence)
    result = evaluate_challenger(incumbent, challenger_metrics, thresholds)
    detailed_failures = [f"alpha_validation:{reason}" for reason in evidence.get("fail_reasons", [])]
    if detailed_failures:
        result["reasons"] = list(dict.fromkeys([*result["reasons"], *detailed_failures]))
        result["approved"] = False
    result["evidence_hash"] = evidence["evidence_hash"]
    result["evidence_schema_version"] = evidence["schema_version"]
    result["candidate_id"] = evidence["candidate_id"]
    result["challenger_metrics"] = challenger_metrics
    return result


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
        challenger_metrics: dict[str, Any] | None = None,
        thresholds: dict[str, Any] | None = None,
        *,
        validation_evidence: Any = None,
        terminal_holdout_evidence: Any = None,
    ) -> dict[str, Any]:
        """Evaluate a challenger using canonical search and untouched-terminal evidence.

        `challenger_metrics` is retained only for call compatibility and audit
        visibility. It cannot authorize promotion. Canonical alpha/replay
        evidence is evaluated first so its historical failure reasons remain
        stable; terminal-holdout evidence is required only when that gate would
        otherwise approve.
        """

        registry = self.load()
        challenger = next((row for row in registry["challengers"] if row["id"] == challenger_id), None)
        if not challenger:
            raise KeyError(challenger_id)

        if validation_evidence is None:
            result = {
                "approved": False,
                "reasons": ["alpha_validation_evidence_required"],
                "pnl_improvement_usd": 0.0,
                "drawdown_increase_pct_points": 0.0,
                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
                "evaluated_at": _utc_now(),
                "evidence_hash": None,
                "evidence_schema_version": None,
            }
        elif not isinstance(validation_evidence, dict):
            result = evaluate_challenger_evidence(incumbent_metrics, validation_evidence, thresholds)
        elif validation_evidence.get("candidate_id") != challenger_id:
            result = {
                "approved": False,
                "reasons": ["alpha_validation_candidate_mismatch"],
                "pnl_improvement_usd": 0.0,
                "drawdown_increase_pct_points": 0.0,
                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
                "evaluated_at": _utc_now(),
                "evidence_hash": validation_evidence.get("evidence_hash"),
                "evidence_schema_version": validation_evidence.get("schema_version"),
            }
        elif validation_evidence.get("candidate_config") != challenger.get("parameters"):
            result = {
                "approved": False,
                "reasons": ["alpha_validation_candidate_config_mismatch"],
                "pnl_improvement_usd": 0.0,
                "drawdown_increase_pct_points": 0.0,
                "thresholds": {**DEFAULT_THRESHOLDS, **(thresholds or {})},
                "evaluated_at": _utc_now(),
                "evidence_hash": validation_evidence.get("evidence_hash"),
                "evidence_schema_version": validation_evidence.get("schema_version"),
            }
        else:
            result = evaluate_challenger_evidence(
                incumbent_metrics,
                validation_evidence,
                thresholds,
                require_replay_provenance=True,
            )

        terminal_verified = False
        terminal_hash = terminal_holdout_evidence.get("evidence_hash") if isinstance(terminal_holdout_evidence, dict) else None
        terminal_schema = terminal_holdout_evidence.get("schema_version") if isinstance(terminal_holdout_evidence, dict) else None
        if result.get("approved") is True:
            terminal_reasons: list[str] = []
            if terminal_holdout_evidence is None:
                terminal_reasons.append("terminal_holdout_evidence_required")
            else:
                terminal_valid, verification_reasons = verify_terminal_holdout_evidence(
                    terminal_holdout_evidence,
                    lineage=self.lineage,
                    reverify_source=True,
                )
                if not terminal_valid:
                    terminal_reasons.extend(
                        f"terminal_holdout_invalid:{reason}" for reason in verification_reasons
                    )
                if isinstance(terminal_holdout_evidence, dict):
                    if terminal_holdout_evidence.get("candidate_id") != challenger_id:
                        terminal_reasons.append("terminal_holdout_candidate_mismatch")
                    if terminal_holdout_evidence.get("candidate_config") != challenger.get("parameters"):
                        terminal_reasons.append("terminal_holdout_candidate_config_mismatch")
                    if terminal_holdout_evidence.get("candidate_source_sha") != validation_evidence.get("candidate_source_sha"):
                        terminal_reasons.append("terminal_holdout_candidate_source_mismatch")
                    if terminal_holdout_evidence.get("alpha_validation_evidence_hash") != validation_evidence.get("evidence_hash"):
                        terminal_reasons.append("terminal_holdout_alpha_evidence_mismatch")
                    if terminal_holdout_evidence.get("passed") is not True:
                        failure_reasons = terminal_holdout_evidence.get("reasons") or ["policy_failed"]
                        terminal_reasons.extend(
                            f"terminal_holdout_policy_failed:{reason}" for reason in failure_reasons
                        )
            if terminal_reasons:
                result["approved"] = False
                result["reasons"] = list(dict.fromkeys([*result.get("reasons", []), *terminal_reasons]))
            else:
                terminal_verified = True

        result["terminal_holdout_evidence_hash"] = terminal_hash
        result["terminal_holdout_schema_version"] = terminal_schema
        result["terminal_holdout_verified"] = terminal_verified

        event_parents = [challenger["proposal_lineage_id"]]
        if terminal_verified and isinstance(terminal_holdout_evidence, dict):
            terminal_lineage_id = terminal_holdout_evidence.get("terminal_lineage_id")
            if terminal_lineage_id:
                event_parents.append(terminal_lineage_id)
        event = self.lineage.append(
            "evaluation",
            {
                "challenger_id": challenger_id,
                "incumbent_metrics": incumbent_metrics,
                "legacy_challenger_metrics": challenger_metrics or {},
                "alpha_validation_evidence_hash": result.get("evidence_hash"),
                "terminal_holdout_evidence_hash": terminal_hash,
                "result": result,
            },
            actor="promotion-gate",
            parents=event_parents,
        )
        challenger["status"] = "approved" if result["approved"] else "rejected"
        challenger["evaluation"] = result
        challenger["evaluation_lineage_id"] = event["id"]
        if result["approved"] and isinstance(validation_evidence, dict) and isinstance(terminal_holdout_evidence, dict):
            challenger["alpha_validation_evidence_hash"] = result["evidence_hash"]
            challenger["alpha_validation_evidence"] = validation_evidence
            challenger["terminal_holdout_evidence_hash"] = terminal_hash
            challenger["terminal_holdout_evidence"] = terminal_holdout_evidence
        else:
            challenger.pop("alpha_validation_evidence_hash", None)
            challenger.pop("alpha_validation_evidence", None)
            challenger.pop("terminal_holdout_evidence_hash", None)
            challenger.pop("terminal_holdout_evidence", None)
        self.save(registry)
        return result

    def promote(self, challenger_id: str, *, canary_fraction: float = 0.10) -> dict[str, Any]:
        registry = self.load()
        challenger = next((row for row in registry["challengers"] if row["id"] == challenger_id), None)
        if not challenger:
            raise KeyError(challenger_id)
        if challenger.get("status") != "approved" or challenger.get("evaluation", {}).get("approved") is not True:
            raise ValueError("challenger has not passed the promotion gate")

        evidence = challenger.get("alpha_validation_evidence")
        evidence_hash = challenger.get("alpha_validation_evidence_hash")
        valid, evidence_reasons = verify_alpha_validation_evidence(evidence)
        if not valid:
            raise ValueError("challenger alpha-validation evidence is invalid: " + ",".join(evidence_reasons))
        if evidence.get("replay_provenance_bound") is not True:
            raise ValueError("challenger alpha-validation replay provenance is required")
        attestation = evidence.get("replay_attestation")
        if not isinstance(attestation, dict) or attestation.get("execution_config_bound") is not True:
            raise ValueError("challenger alpha-validation executable config binding is required")
        if attestation.get("strategy_config") != challenger.get("parameters"):
            raise ValueError("challenger alpha-validation executable config mismatch")
        replay_valid, replay_reasons = verify_evidence_replay_binding(
            evidence, reverify_source=True
        )
        if not replay_valid:
            raise ValueError(
                "challenger alpha-validation replay provenance is invalid: "
                + ",".join(replay_reasons)
            )
        if evidence.get("candidate_id") != challenger_id:
            raise ValueError("challenger alpha-validation candidate mismatch")
        if evidence.get("candidate_config") != challenger.get("parameters"):
            raise ValueError("challenger alpha-validation candidate config mismatch")
        if evidence.get("evidence_hash") != evidence_hash:
            raise ValueError("challenger alpha-validation evidence hash mismatch")
        if challenger.get("evaluation", {}).get("evidence_hash") != evidence_hash:
            raise ValueError("challenger evaluation/evidence hash mismatch")

        terminal_evidence = challenger.get("terminal_holdout_evidence")
        terminal_hash = challenger.get("terminal_holdout_evidence_hash")
        terminal_valid, terminal_reasons = verify_terminal_holdout_evidence(
            terminal_evidence,
            lineage=self.lineage,
            reverify_source=True,
        )
        if not terminal_valid:
            raise ValueError(
                "challenger terminal-holdout evidence is invalid: "
                + ",".join(terminal_reasons)
            )
        if terminal_evidence.get("passed") is not True:
            raise ValueError("challenger terminal-holdout policy did not pass")
        if terminal_evidence.get("candidate_id") != challenger_id:
            raise ValueError("challenger terminal-holdout candidate mismatch")
        if terminal_evidence.get("candidate_config") != challenger.get("parameters"):
            raise ValueError("challenger terminal-holdout candidate config mismatch")
        if terminal_evidence.get("candidate_source_sha") != evidence.get("candidate_source_sha"):
            raise ValueError("challenger terminal-holdout candidate source mismatch")
        if terminal_evidence.get("alpha_validation_evidence_hash") != evidence_hash:
            raise ValueError("challenger terminal-holdout alpha evidence mismatch")
        if terminal_evidence.get("evidence_hash") != terminal_hash:
            raise ValueError("challenger terminal-holdout evidence hash mismatch")
        if challenger.get("evaluation", {}).get("terminal_holdout_evidence_hash") != terminal_hash:
            raise ValueError("challenger evaluation/terminal evidence hash mismatch")
        if challenger.get("evaluation", {}).get("terminal_holdout_verified") is not True:
            raise ValueError("challenger evaluation did not verify terminal holdout")

        research_certification = build_runtime_research_certification(
            challenger, evidence, terminal_evidence
        )

        previous = None
        if self.active_config_path.exists():
            previous = json.loads(self.active_config_path.read_text(encoding="utf-8"))
        config = {
            "schema_version": 1,
            "active_challenger_id": challenger_id,
            "parameters": challenger["parameters"],
            "deployment": "canary",
            "canary_fraction": max(0.01, min(1.0, _num(canary_fraction, 0.10))),
            "alpha_validation_evidence_hash": evidence_hash,
            "terminal_holdout_evidence_hash": terminal_hash,
            "research_certification": research_certification,
            "promoted_at": _utc_now(),
            "rollback_config": previous,
            "promotion_lineage_id": None,
        }
        event = self.lineage.append(
            "promotion",
            {
                "challenger_id": challenger_id,
                "canary_fraction": config["canary_fraction"],
                "alpha_validation_evidence_hash": evidence_hash,
                "terminal_holdout_evidence_hash": terminal_hash,
                "research_certification_hash": research_certification.get("certification_hash") if research_certification else None,
            },
            actor="promotion-gate",
            parents=[challenger["evaluation_lineage_id"]],
        )
        config["promotion_lineage_id"] = event["id"]
        _atomic(self.active_config_path, config)
        challenger["status"] = "canary"
        challenger["promotion_lineage_id"] = event["id"]
        registry["incumbent_id"] = challenger_id
        registry["promotions"].append(
            {
                "challenger_id": challenger_id,
                "at": _utc_now(),
                "lineage_id": event["id"],
                "alpha_validation_evidence_hash": evidence_hash,
                "terminal_holdout_evidence_hash": terminal_hash,
                "research_certification_hash": research_certification.get("certification_hash") if research_certification else None,
            }
        )
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