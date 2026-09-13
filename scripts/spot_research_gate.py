"""Precommitted research gate for spot-long-only execution authority.

This module is additive to the historical alpha/terminal evidence path. It does
not reinterpret legacy evidence. A research experiment may explicitly commit to
this gate before candidate search; only then can spot execution evidence be
created and later promoted into execution authority.
"""
from __future__ import annotations

import math
from typing import Any

from scripts.alpha_validation import performance_metrics, stable_hash
from scripts.backtest_framework.canonical_replay import (
    load_canonical_snapshot,
    normalize_strategy_config,
)
from scripts.backtest_framework.spot_execution_replay import (
    SPOT_EXECUTION_METHOD,
    SPOT_POSITION_MODE,
    attest_spot_snapshot_replay,
    replay_spot_long_only_rust,
    verify_spot_replay_attestation,
)
from scripts.selection_bias import assess_candidate_significance, flatten_fold_returns

EXECUTION_LIFECYCLE_KEY = "spot_long_only_v1"
COMMITMENT_SCHEMA_VERSION = 1
COMMITMENT_METHOD = "precommitted_spot_execution_research_v1"
SEARCH_EVIDENCE_SCHEMA_VERSION = 1
SEARCH_EVIDENCE_TYPE = "spot_execution_search_evidence_v1"
TERMINAL_EVIDENCE_SCHEMA_VERSION = 1
TERMINAL_EVIDENCE_TYPE = "spot_execution_terminal_evidence_v1"

_DEFAULT_SEARCH_POLICY = {
    "require_positive_total_return": True,
    "min_profit_factor": 1.0,
    "max_drawdown_pct": 25.0,
}
_DEFAULT_TERMINAL_POLICY = {
    "min_trades": 5,
    "require_positive_total_return": True,
    "min_profit_factor": 1.0,
    "max_drawdown_pct": 25.0,
}


def build_execution_lifecycle_commitment(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    if value != EXECUTION_LIFECYCLE_KEY:
        raise ValueError("unsupported execution lifecycle commitment")
    core = {
        "schema_version": COMMITMENT_SCHEMA_VERSION,
        "method": COMMITMENT_METHOD,
        "lifecycle_key": EXECUTION_LIFECYCLE_KEY,
        "lifecycle_method": SPOT_EXECUTION_METHOD,
        "position_mode": SPOT_POSITION_MODE,
        "search_policy": dict(_DEFAULT_SEARCH_POLICY),
        "terminal_policy": dict(_DEFAULT_TERMINAL_POLICY),
        "required_for_promotion": True,
    }
    return {**core, "commitment_hash": stable_hash(core)}


def verify_execution_lifecycle_commitment(value: Any) -> tuple[bool, list[str]]:
    if not isinstance(value, dict):
        return False, ["execution_lifecycle_commitment_missing"]
    reasons: list[str] = []
    try:
        expected = build_execution_lifecycle_commitment(value.get("lifecycle_key"))
        if expected != value:
            reasons.append("execution_lifecycle_commitment_mismatch")
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("execution_lifecycle_commitment_invalid")
    return not reasons, reasons


def _performance_reasons(metrics: dict[str, Any], policy: dict[str, Any], *, prefix: str) -> list[str]:
    reasons: list[str] = []
    if int(metrics.get("trade_count", 0)) < int(policy.get("min_trades", 0)):
        reasons.append(f"{prefix}_insufficient_trades")
    if policy.get("require_positive_total_return") is True and float(metrics.get("total_return_pct", 0.0)) <= 0.0:
        reasons.append(f"{prefix}_nonpositive_return")
    if float(metrics.get("profit_factor", 0.0)) < float(policy.get("min_profit_factor", 0.0)):
        reasons.append(f"{prefix}_profit_factor_below_floor")
    if float(metrics.get("max_drawdown_pct", math.inf)) > float(policy.get("max_drawdown_pct", math.inf)):
        reasons.append(f"{prefix}_drawdown_above_limit")
    return reasons


def build_spot_search_evidence(
    *,
    commitment: dict[str, Any],
    plan_search_dataset: dict[str, Any],
    multiple_testing_policy: dict[str, Any],
    validation_evidence: dict[str, Any],
    periods_per_year: int | None = None,
) -> dict[str, Any]:
    ok, reasons = verify_execution_lifecycle_commitment(commitment)
    if not ok:
        raise ValueError("invalid execution lifecycle commitment: " + ",".join(reasons))
    replay = validation_evidence.get("replay_attestation")
    if not isinstance(replay, dict):
        raise ValueError("canonical replay attestation required before spot search replay")
    dataset = replay.get("dataset")
    if dataset != plan_search_dataset:
        raise ValueError("spot search dataset must equal committed search dataset")
    strategy_name = replay.get("strategy_name")
    strategy_config = normalize_strategy_config(strategy_name, replay.get("strategy_config"))
    if not strategy_config:
        raise ValueError("spot execution certification requires configured strategy replay")
    if validation_evidence.get("candidate_config") != strategy_config:
        raise ValueError("spot search candidate config mismatch")

    snapshot = load_canonical_snapshot(
        kind=dataset["kind"],
        symbol=dataset["symbol"],
        granularity=int(dataset["granularity"]),
        start_ts=float(dataset["start_ts"]),
        end_ts=float(dataset["end_ts"]),
    )
    if snapshot["manifest"] != dataset:
        raise ValueError("spot search source no longer matches committed dataset")

    fold_returns, attestation = attest_spot_snapshot_replay(
        snapshot,
        strategy_name=strategy_name,
        n_folds=int(replay["n_folds"]),
        purge_size=int(replay["purge_size"]),
        embargo_size=int(replay["embargo_size"]),
        warmup=int(replay["warmup"]),
        fee_bps=float(replay["fee_bps"]),
        max_hold_bars=int(replay["max_hold_bars"]),
        strategy_config=strategy_config,
    )
    multiplicity = assess_candidate_significance(fold_returns, multiple_testing_policy)
    flat_returns = flatten_fold_returns(fold_returns)
    ppy = int(periods_per_year or validation_evidence.get("periods_per_year", 252))
    metrics = performance_metrics(flat_returns, periods_per_year=ppy)
    failure_reasons = []
    if multiplicity.get("passed") is not True:
        failure_reasons.extend(
            f"spot_search:{reason}"
            for reason in (multiplicity.get("reasons") or ["multiple_testing_failed"])
        )
    failure_reasons.extend(
        _performance_reasons(metrics, commitment["search_policy"], prefix="spot_search")
    )
    core = {
        "schema_version": SEARCH_EVIDENCE_SCHEMA_VERSION,
        "evidence_type": SEARCH_EVIDENCE_TYPE,
        "commitment": commitment,
        "candidate_id": validation_evidence.get("candidate_id"),
        "candidate_source_sha": validation_evidence.get("candidate_source_sha"),
        "candidate_config": strategy_config,
        "candidate_config_hash": stable_hash(strategy_config),
        "alpha_validation_evidence_hash": validation_evidence.get("evidence_hash"),
        "search_dataset": dataset,
        "strategy_name": strategy_name,
        "spot_replay_attestation": attestation,
        "fold_returns": fold_returns,
        "fold_returns_hash": stable_hash(fold_returns),
        "multiple_testing_policy": multiple_testing_policy,
        "multiple_testing": multiplicity,
        "periods_per_year": ppy,
        "metrics": metrics,
        "passed": not failure_reasons,
        "reasons": failure_reasons,
    }
    return {**core, "evidence_hash": stable_hash(core)}


def verify_spot_search_evidence(
    evidence: Any,
    *,
    commitment: dict[str, Any] | None = None,
    plan_search_dataset: dict[str, Any] | None = None,
    multiple_testing_policy: dict[str, Any] | None = None,
    validation_evidence: dict[str, Any] | None = None,
    reverify_source: bool = True,
) -> tuple[bool, list[str]]:
    if not isinstance(evidence, dict):
        return False, ["spot_search_evidence_missing"]
    reasons: list[str] = []
    required = {
        "schema_version", "evidence_type", "commitment", "candidate_id",
        "candidate_source_sha", "candidate_config", "candidate_config_hash",
        "alpha_validation_evidence_hash", "search_dataset", "strategy_name",
        "spot_replay_attestation", "fold_returns", "fold_returns_hash",
        "multiple_testing_policy", "multiple_testing", "periods_per_year",
        "metrics", "passed", "reasons", "evidence_hash",
    }
    for name in sorted(required - set(evidence)):
        reasons.append(f"spot_search_missing_field:{name}")
    if reasons:
        return False, reasons
    if evidence["schema_version"] != SEARCH_EVIDENCE_SCHEMA_VERSION or evidence["evidence_type"] != SEARCH_EVIDENCE_TYPE:
        reasons.append("spot_search_schema_mismatch")
    try:
        core = dict(evidence)
        supplied = core.pop("evidence_hash")
        if stable_hash(core) != supplied:
            reasons.append("spot_search_evidence_hash_mismatch")
        if stable_hash(evidence["fold_returns"]) != evidence["fold_returns_hash"]:
            reasons.append("spot_search_returns_hash_mismatch")
        normalized = normalize_strategy_config(evidence["strategy_name"], evidence["candidate_config"])
        if normalized != evidence["candidate_config"] or stable_hash(normalized) != evidence["candidate_config_hash"]:
            reasons.append("spot_search_strategy_config_mismatch")
        expected_mult = assess_candidate_significance(evidence["fold_returns"], evidence["multiple_testing_policy"])
        if expected_mult != evidence["multiple_testing"]:
            reasons.append("spot_search_multiple_testing_mismatch")
        metrics = performance_metrics(
            flatten_fold_returns(evidence["fold_returns"]),
            periods_per_year=int(evidence["periods_per_year"]),
        )
        if metrics != evidence["metrics"]:
            reasons.append("spot_search_metrics_mismatch")
        expected_reasons = []
        if expected_mult.get("passed") is not True:
            expected_reasons.extend(
                f"spot_search:{reason}"
                for reason in (expected_mult.get("reasons") or ["multiple_testing_failed"])
            )
        expected_reasons.extend(
            _performance_reasons(metrics, evidence["commitment"]["search_policy"], prefix="spot_search")
        )
        if expected_reasons != evidence["reasons"] or (not expected_reasons) is not evidence["passed"]:
            reasons.append("spot_search_pass_state_mismatch")
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("spot_search_evidence_invalid")

    commitment_ok, commitment_reasons = verify_execution_lifecycle_commitment(evidence.get("commitment"))
    if not commitment_ok:
        reasons.extend(commitment_reasons)
    if commitment is not None and evidence.get("commitment") != commitment:
        reasons.append("spot_search_commitment_mismatch")
    if plan_search_dataset is not None and evidence.get("search_dataset") != plan_search_dataset:
        reasons.append("spot_search_dataset_mismatch")
    if multiple_testing_policy is not None and evidence.get("multiple_testing_policy") != multiple_testing_policy:
        reasons.append("spot_search_multiple_testing_policy_mismatch")
    if validation_evidence is not None:
        if evidence.get("candidate_id") != validation_evidence.get("candidate_id"):
            reasons.append("spot_search_candidate_mismatch")
        if evidence.get("candidate_source_sha") != validation_evidence.get("candidate_source_sha"):
            reasons.append("spot_search_candidate_source_mismatch")
        if evidence.get("candidate_config") != validation_evidence.get("candidate_config"):
            reasons.append("spot_search_candidate_config_binding_mismatch")
        if evidence.get("alpha_validation_evidence_hash") != validation_evidence.get("evidence_hash"):
            reasons.append("spot_search_alpha_evidence_mismatch")

    if reverify_source and not reasons:
        valid, attestation_reasons = verify_spot_replay_attestation(
            evidence["spot_replay_attestation"],
            evidence["fold_returns"],
            reverify_source=True,
        )
        if not valid:
            reasons.extend(f"spot_search_replay:{reason}" for reason in attestation_reasons)
    return not reasons, list(dict.fromkeys(reasons))


def build_spot_terminal_evidence(
    *,
    commitment: dict[str, Any],
    search_evidence: dict[str, Any],
    terminal_dataset: dict[str, Any],
    terminal_rows: list[list[float]],
    periods_per_year: int,
) -> dict[str, Any]:
    ok, reasons = verify_spot_search_evidence(search_evidence, commitment=commitment, reverify_source=False)
    if not ok or search_evidence.get("passed") is not True:
        raise ValueError("spot search evidence must pass before terminal evaluation: " + ",".join(reasons or search_evidence.get("reasons", [])))
    attestation = search_evidence["spot_replay_attestation"]
    lifecycle = attestation["lifecycle"]
    replay = replay_spot_long_only_rust(
        search_evidence["strategy_name"],
        terminal_rows,
        warmup=int(lifecycle["warmup_bars"]),
        fee_bps=float(lifecycle["fee_bps"]),
        max_hold_bars=int(lifecycle["max_hold_bars"]),
        strategy_config=search_evidence["candidate_config"],
    )
    metrics = performance_metrics(replay["returns"], periods_per_year=int(periods_per_year))
    failed = _performance_reasons(metrics, commitment["terminal_policy"], prefix="spot_terminal")
    core = {
        "schema_version": TERMINAL_EVIDENCE_SCHEMA_VERSION,
        "evidence_type": TERMINAL_EVIDENCE_TYPE,
        "commitment": commitment,
        "candidate_id": search_evidence["candidate_id"],
        "candidate_source_sha": search_evidence["candidate_source_sha"],
        "candidate_config": search_evidence["candidate_config"],
        "candidate_config_hash": search_evidence["candidate_config_hash"],
        "alpha_validation_evidence_hash": search_evidence["alpha_validation_evidence_hash"],
        "spot_search_evidence_hash": search_evidence["evidence_hash"],
        "spot_replay_attestation_hash": attestation["attestation_hash"],
        "spot_execution_lifecycle": lifecycle,
        "spot_execution_lifecycle_hash": attestation["lifecycle_hash"],
        "terminal_dataset": terminal_dataset,
        "returns": replay["returns"],
        "returns_hash": stable_hash(replay["returns"]),
        "events_hash": stable_hash(replay["events"]),
        "periods_per_year": int(periods_per_year),
        "metrics": metrics,
        "policy": commitment["terminal_policy"],
        "passed": not failed,
        "reasons": failed,
    }
    return {**core, "evidence_hash": stable_hash(core)}


def verify_spot_terminal_evidence(
    evidence: Any,
    *,
    search_evidence: dict[str, Any] | None = None,
    reverify_source: bool = True,
) -> tuple[bool, list[str]]:
    if not isinstance(evidence, dict):
        return False, ["spot_terminal_evidence_missing"]
    reasons: list[str] = []
    required = {
        "schema_version", "evidence_type", "commitment", "candidate_id",
        "candidate_source_sha", "candidate_config", "candidate_config_hash",
        "alpha_validation_evidence_hash", "spot_search_evidence_hash",
        "spot_replay_attestation_hash", "spot_execution_lifecycle",
        "spot_execution_lifecycle_hash", "terminal_dataset", "returns",
        "returns_hash", "events_hash", "periods_per_year", "metrics", "policy",
        "passed", "reasons", "evidence_hash",
    }
    for name in sorted(required - set(evidence)):
        reasons.append(f"spot_terminal_missing_field:{name}")
    if reasons:
        return False, reasons
    if evidence["schema_version"] != TERMINAL_EVIDENCE_SCHEMA_VERSION or evidence["evidence_type"] != TERMINAL_EVIDENCE_TYPE:
        reasons.append("spot_terminal_schema_mismatch")
    try:
        core = dict(evidence)
        supplied = core.pop("evidence_hash")
        if stable_hash(core) != supplied:
            reasons.append("spot_terminal_evidence_hash_mismatch")
        if stable_hash(evidence["returns"]) != evidence["returns_hash"]:
            reasons.append("spot_terminal_returns_hash_mismatch")
        if stable_hash(evidence["spot_execution_lifecycle"]) != evidence["spot_execution_lifecycle_hash"]:
            reasons.append("spot_terminal_lifecycle_hash_mismatch")
        normalized = normalize_strategy_config(
            evidence.get("spot_execution_lifecycle", {}).get("strategy_name") or "rsi_revert",
            evidence["candidate_config"],
        )
        if stable_hash(normalized) != evidence["candidate_config_hash"]:
            reasons.append("spot_terminal_strategy_config_hash_mismatch")
        metrics = performance_metrics(evidence["returns"], periods_per_year=int(evidence["periods_per_year"]))
        if metrics != evidence["metrics"]:
            reasons.append("spot_terminal_metrics_mismatch")
        expected_reasons = _performance_reasons(metrics, evidence["policy"], prefix="spot_terminal")
        if expected_reasons != evidence["reasons"] or (not expected_reasons) is not evidence["passed"]:
            reasons.append("spot_terminal_pass_state_mismatch")
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("spot_terminal_evidence_invalid")

    commitment_ok, commitment_reasons = verify_execution_lifecycle_commitment(evidence.get("commitment"))
    if not commitment_ok:
        reasons.extend(commitment_reasons)
    if evidence.get("policy") != evidence.get("commitment", {}).get("terminal_policy"):
        reasons.append("spot_terminal_policy_not_precommitted")
    if search_evidence is not None:
        search_ok, search_reasons = verify_spot_search_evidence(
            search_evidence,
            commitment=evidence.get("commitment"),
            reverify_source=reverify_source,
        )
        if not search_ok:
            reasons.extend(f"spot_terminal_search:{reason}" for reason in search_reasons)
        if evidence.get("spot_search_evidence_hash") != search_evidence.get("evidence_hash"):
            reasons.append("spot_terminal_search_evidence_mismatch")
        if evidence.get("candidate_id") != search_evidence.get("candidate_id"):
            reasons.append("spot_terminal_candidate_mismatch")
        if evidence.get("candidate_config") != search_evidence.get("candidate_config"):
            reasons.append("spot_terminal_candidate_config_mismatch")

    if reverify_source and not reasons:
        try:
            dataset = evidence["terminal_dataset"]
            snapshot = load_canonical_snapshot(
                kind=dataset["kind"],
                symbol=dataset["symbol"],
                granularity=int(dataset["granularity"]),
                start_ts=float(dataset["start_ts"]),
                end_ts=float(dataset["end_ts"]),
            )
            if snapshot["manifest"] != dataset:
                reasons.append("spot_terminal_dataset_source_mismatch")
            else:
                lifecycle = evidence["spot_execution_lifecycle"]
                replay = replay_spot_long_only_rust(
                    search_evidence["strategy_name"] if search_evidence else "rsi_revert",
                    snapshot["rows"],
                    warmup=int(lifecycle["warmup_bars"]),
                    fee_bps=float(lifecycle["fee_bps"]),
                    max_hold_bars=int(lifecycle["max_hold_bars"]),
                    strategy_config=evidence["candidate_config"],
                )
                if replay["returns"] != evidence["returns"]:
                    reasons.append("spot_terminal_regenerated_returns_mismatch")
                if stable_hash(replay["events"]) != evidence["events_hash"]:
                    reasons.append("spot_terminal_regenerated_events_mismatch")
        except (ImportError, RuntimeError, TypeError, ValueError, KeyError, OverflowError):
            reasons.append("spot_terminal_source_reverification_failed")
    return not reasons, list(dict.fromkeys(reasons))
