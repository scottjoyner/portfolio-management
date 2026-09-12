#!/usr/bin/env python3
"""Research tournament with precommitted search control and one-shot holdout.

This module contains no broker/order-routing authority. It closes two research
trust gaps before challenger promotion: candidate search must pay a precommitted
multiple-testing penalty, and the final terminal window remains untouched until
one candidate has been selected from search-only evidence.
"""
from __future__ import annotations

import json
import math
import os
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from functools import wraps
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

try:
    import fcntl
except ImportError:  # pragma: no cover - supported deployment targets are POSIX
    fcntl = None

from scripts.alpha_validation import performance_metrics, stable_hash, verify_alpha_validation_evidence
from scripts.backtest_framework.canonical_replay import (
    DATASET_KIND,
    load_canonical_snapshot,
    normalize_candle_rows,
    normalize_strategy_config,
    replay_trade_returns_rust,
    snapshot_from_rows,
    verify_evidence_replay_binding,
)
from scripts.learning_lineage import LineageStore
from scripts.selection_bias import (
    BLOCK_METHOD,
    METHOD as MULTIPLE_TESTING_METHOD,
    SearchMultiplicityPolicy,
    assess_candidate_significance,
    flatten_fold_returns,
    policy_from_artifact,
    verify_candidate_significance,
)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REGISTRY_PATH = ROOT / "data" / "learning" / "research_experiments.json"
REGISTRY_SCHEMA_VERSION = 1
EXPERIMENT_SCHEMA_VERSION = 2
TERMINAL_EVIDENCE_SCHEMA_VERSION = 2
SELECTION_POLICY_VERSION = "familywise_significant_net_pnl_then_quality_v2"
TERMINAL_METHOD = "canonical_one_shot_terminal_holdout_v2"


@dataclass(frozen=True)
class TerminalHoldoutPolicy:
    min_trades: int = 5
    require_positive_total_return: bool = True
    min_profit_factor: float = 1.0
    max_drawdown_pct: float = 25.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass


def _finite(value: Any, *, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _manifest(snapshot: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("manifest"), dict):
        raise TypeError("snapshot must contain a canonical manifest")
    return snapshot["manifest"]


def _invalid_multiple_testing_assessment(policy_artifact: dict[str, Any]) -> dict[str, Any]:
    return {
        "method": MULTIPLE_TESTING_METHOD,
        "policy": policy_artifact,
        "positive_trades": 0,
        "negative_trades": 0,
        "zero_trades": 0,
        "nonzero_trades": 0,
        "raw_p_value": 1.0,
        "marginal_adjusted_p_value": 1.0,
        "dependence": {
            "method": BLOCK_METHOD,
            "block_size": int(policy_artifact.get("dependence_block_size", 5)),
            "total_blocks": 0,
            "positive_blocks": 0,
            "negative_blocks": 0,
            "zero_blocks": 0,
            "nonzero_blocks": 0,
            "raw_p_value": 1.0,
            "adjusted_p_value": 1.0,
        },
        "adjusted_p_value": 1.0,
        "passed": False,
        "reasons": ["multiple_testing_assessment_invalid"],
    }


def build_experiment_plan_from_snapshot(
    snapshot: dict[str, Any],
    *,
    experiment_id: str,
    strategy_name: str,
    holdout_bars: int,
    embargo_bars: int = 0,
    min_search_bars: int = 200,
    max_candidate_trials: int = 20,
    familywise_alpha: float = 0.05,
    min_sign_test_trades: int = 10,
    dependence_block_size: int = 5,
    min_nonzero_blocks: int = 5,
    created_at: str | None = None,
) -> dict[str, Any]:
    if not experiment_id or not strategy_name:
        raise ValueError("experiment_id and strategy_name are required")
    holdout_bars = int(holdout_bars)
    embargo_bars = int(embargo_bars)
    min_search_bars = int(min_search_bars)
    if holdout_bars <= 0 or embargo_bars < 0 or min_search_bars <= 0:
        raise ValueError("holdout_bars/min_search_bars must be positive and embargo_bars non-negative")
    multiplicity_policy = SearchMultiplicityPolicy(
        max_candidate_trials=max_candidate_trials,
        familywise_alpha=familywise_alpha,
        min_nonzero_trades=min_sign_test_trades,
        dependence_block_size=dependence_block_size,
        min_nonzero_blocks=min_nonzero_blocks,
    ).artifact()
    full_manifest = _manifest(snapshot)
    rows = normalize_candle_rows(snapshot.get("rows", []))
    rebuilt = snapshot_from_rows(
        rows,
        kind=full_manifest["kind"],
        symbol=full_manifest["symbol"],
        granularity=int(full_manifest["granularity"]),
    )
    if rebuilt["manifest"] != full_manifest:
        raise ValueError("full experiment snapshot manifest does not match rows")
    search_end = len(rows) - holdout_bars - embargo_bars
    holdout_start = len(rows) - holdout_bars
    if search_end < min_search_bars:
        raise ValueError("experiment snapshot does not leave enough search history")
    if holdout_bars < 10:
        raise ValueError("terminal holdout must contain at least 10 bars")
    search_rows = rows[:search_end]
    embargo_rows = rows[search_end:holdout_start]
    terminal_rows = rows[holdout_start:]
    search_snapshot = snapshot_from_rows(
        search_rows,
        kind=full_manifest["kind"],
        symbol=full_manifest["symbol"],
        granularity=int(full_manifest["granularity"]),
    )
    terminal_snapshot = snapshot_from_rows(
        terminal_rows,
        kind=full_manifest["kind"],
        symbol=full_manifest["symbol"],
        granularity=int(full_manifest["granularity"]),
    )
    if float(search_snapshot["manifest"]["end_ts"]) >= float(terminal_snapshot["manifest"]["start_ts"]):
        raise ValueError("search and terminal windows overlap")
    core = {
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "experiment_id": str(experiment_id),
        "strategy_name": str(strategy_name),
        "selection_policy_version": SELECTION_POLICY_VERSION,
        "multiple_testing_policy": multiplicity_policy,
        "full_dataset": full_manifest,
        "search_dataset": search_snapshot["manifest"],
        "embargo_bars": embargo_bars,
        "embargo_rows_hash": stable_hash(embargo_rows),
        "terminal_holdout_bars": holdout_bars,
        "terminal_holdout_commitment": terminal_snapshot["manifest"],
        "created_at": created_at or _utc_now(),
    }
    return {**core, "experiment_hash": stable_hash(core)}


def _selection_sort_key(trial: dict[str, Any]) -> tuple[Any, ...]:
    metrics = trial.get("selection_metrics") or {}
    multiplicity = trial.get("multiple_testing") or {}
    return (
        float(multiplicity.get("adjusted_p_value", 1.0)),
        -float(metrics.get("net_pnl_after_cost_usd", 0.0)),
        -float(metrics.get("profit_factor", 0.0)),
        float(metrics.get("max_drawdown_pct", 1e30)),
        -float(metrics.get("annualized_return_pct", 0.0)),
        str(trial.get("validation_evidence_hash") or ""),
        str(trial.get("candidate_id") or ""),
    )


def _derive_terminal_result(
    returns: Sequence[float], *, periods_per_year: int, policy: TerminalHoldoutPolicy
) -> tuple[dict[str, Any], bool, list[str]]:
    metrics = performance_metrics(returns, periods_per_year=periods_per_year)
    reasons: list[str] = []
    if metrics["trade_count"] < int(policy.min_trades):
        reasons.append("terminal_insufficient_trades")
    if policy.require_positive_total_return and metrics["total_return_pct"] <= 0.0:
        reasons.append("terminal_nonpositive_return")
    if metrics["profit_factor"] < float(policy.min_profit_factor):
        reasons.append("terminal_profit_factor_below_floor")
    if metrics["max_drawdown_pct"] > float(policy.max_drawdown_pct):
        reasons.append("terminal_drawdown_above_limit")
    return metrics, not reasons, reasons


def _terminal_result_core(evidence: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "schema_version", "method", "experiment_id", "experiment_hash",
        "selection_hash", "selection_policy_version", "candidate_id",
        "candidate_source_sha", "candidate_config", "candidate_config_hash",
        "alpha_validation_evidence_hash", "multiple_testing_policy",
        "selected_multiple_testing", "search_oos_returns", "search_oos_returns_hash",
        "trial_count", "max_candidate_trials", "terminal_dataset", "strategy_name",
        "strategy_config", "strategy_config_hash", "warmup", "fee_bps",
        "max_hold_bars", "periods_per_year", "returns", "returns_hash",
        "metrics", "policy", "passed", "reasons", "evaluated_at",
    )
    return {name: evidence.get(name) for name in fields}


def verify_terminal_holdout_evidence(
    evidence: Any, *, lineage: LineageStore | None = None, reverify_source: bool = True
) -> tuple[bool, list[str]]:
    if not isinstance(evidence, dict):
        return False, ["terminal_evidence_not_object"]
    required = set(_terminal_result_core({}).keys()) | {
        "terminal_result_hash", "experiment_lineage_id", "experiment_lineage_event_hash",
        "selection_lineage_id", "selection_lineage_event_hash", "terminal_lineage_id",
        "terminal_lineage_event_hash", "evidence_hash",
    }
    reasons: list[str] = []
    for name in sorted(required - set(evidence)):
        reasons.append(f"terminal_missing_field:{name}")
    if reasons:
        return False, reasons
    if evidence.get("schema_version") != TERMINAL_EVIDENCE_SCHEMA_VERSION:
        reasons.append("terminal_schema_mismatch")
    if evidence.get("method") != TERMINAL_METHOD:
        reasons.append("terminal_method_mismatch")
    if evidence.get("selection_policy_version") != SELECTION_POLICY_VERSION:
        reasons.append("terminal_selection_policy_mismatch")
    try:
        final_core = dict(evidence)
        supplied = final_core.pop("evidence_hash", None)
        if stable_hash(final_core) != supplied:
            reasons.append("terminal_evidence_hash_mismatch")
        if stable_hash(_terminal_result_core(evidence)) != evidence.get("terminal_result_hash"):
            reasons.append("terminal_result_hash_mismatch")
        if stable_hash(evidence["returns"]) != evidence.get("returns_hash"):
            reasons.append("terminal_returns_hash_mismatch")
        if stable_hash(evidence["search_oos_returns"]) != evidence.get("search_oos_returns_hash"):
            reasons.append("terminal_search_oos_returns_hash_mismatch")
        mp = policy_from_artifact(evidence["multiple_testing_policy"])
        if int(evidence["max_candidate_trials"]) != mp.max_candidate_trials:
            reasons.append("terminal_search_budget_mismatch")
        trial_count = int(evidence["trial_count"])
        if trial_count <= 0 or trial_count > mp.max_candidate_trials:
            reasons.append("terminal_search_budget_violation")
        expected = assess_candidate_significance(
            [evidence["search_oos_returns"]], evidence["multiple_testing_policy"]
        )
        if expected != evidence["selected_multiple_testing"]:
            reasons.append("terminal_multiple_testing_assessment_mismatch")
        if expected.get("passed") is not True:
            reasons.append("terminal_selected_candidate_not_familywise_significant")
        normalized = normalize_strategy_config(evidence["strategy_name"], evidence["strategy_config"])
        if normalized != evidence["strategy_config"]:
            reasons.append("terminal_strategy_config_not_normalized")
        if stable_hash(normalized) != evidence["strategy_config_hash"]:
            reasons.append("terminal_strategy_config_hash_mismatch")
        policy = TerminalHoldoutPolicy(**evidence["policy"])
        metrics, passed, failed = _derive_terminal_result(
            evidence["returns"], periods_per_year=int(evidence["periods_per_year"]), policy=policy
        )
        if metrics != evidence["metrics"]:
            reasons.append("terminal_metrics_mismatch")
        if passed is not evidence["passed"]:
            reasons.append("terminal_pass_state_mismatch")
        if failed != evidence["reasons"]:
            reasons.append("terminal_reasons_mismatch")
    except (TypeError, ValueError, KeyError, OverflowError):
        reasons.append("terminal_evidence_invalid")
    dataset = evidence.get("terminal_dataset")
    if reverify_source and not reasons:
        try:
            snapshot = load_canonical_snapshot(
                kind=dataset["kind"], symbol=dataset["symbol"],
                granularity=int(dataset["granularity"]),
                start_ts=float(dataset["start_ts"]), end_ts=float(dataset["end_ts"]),
            )
            if snapshot["manifest"] != dataset:
                reasons.append("terminal_dataset_source_mismatch")
            else:
                regenerated = replay_trade_returns_rust(
                    evidence["strategy_name"], snapshot["rows"],
                    warmup=int(evidence["warmup"]), fee_bps=float(evidence["fee_bps"]),
                    max_hold_bars=int(evidence["max_hold_bars"]),
                    strategy_config=evidence["strategy_config"],
                )
                if regenerated != evidence["returns"]:
                    reasons.append("terminal_regenerated_returns_mismatch")
        except (ImportError, RuntimeError, TypeError, ValueError, KeyError, OverflowError):
            reasons.append("terminal_source_reverification_failed")
    if lineage is not None:
        if lineage.verify().get("ok") is not True:
            reasons.append("terminal_lineage_chain_invalid")
        events = lineage.events()
        by_id = {row.get("id"): row for row in events}
        experiment_event = by_id.get(evidence.get("experiment_lineage_id"))
        selection_event = by_id.get(evidence.get("selection_lineage_id"))
        terminal_event = by_id.get(evidence.get("terminal_lineage_id"))
        if not experiment_event or experiment_event.get("type") != "research_experiment":
            reasons.append("terminal_experiment_lineage_missing")
        else:
            payload = experiment_event.get("payload") or {}
            if experiment_event.get("event_hash") != evidence.get("experiment_lineage_event_hash"):
                reasons.append("terminal_experiment_lineage_hash_mismatch")
            if payload.get("experiment_id") != evidence.get("experiment_id") or payload.get("experiment_hash") != evidence.get("experiment_hash"):
                reasons.append("terminal_experiment_lineage_binding_mismatch")
            if payload.get("multiple_testing_policy") != evidence.get("multiple_testing_policy"):
                reasons.append("terminal_multiple_testing_policy_lineage_mismatch")
        trial_events = [
            row for row in events
            if row.get("type") == "candidate_trial"
            and (row.get("payload") or {}).get("experiment_id") == evidence.get("experiment_id")
        ]
        if len(trial_events) != int(evidence.get("trial_count", -1)):
            reasons.append("terminal_trial_lineage_count_mismatch")
        if len(trial_events) > int(evidence.get("max_candidate_trials", -1)):
            reasons.append("terminal_trial_lineage_budget_violation")
        selected_trial = next((
            row for row in trial_events
            if (row.get("payload") or {}).get("candidate_id") == evidence.get("candidate_id")
            and (row.get("payload") or {}).get("validation_evidence_hash") == evidence.get("alpha_validation_evidence_hash")
        ), None)
        if selected_trial is None:
            reasons.append("terminal_selected_trial_lineage_missing")
        elif (selected_trial.get("payload") or {}).get("multiple_testing") != evidence.get("selected_multiple_testing"):
            reasons.append("terminal_selected_multiple_testing_lineage_mismatch")
        if not selection_event or selection_event.get("type") != "candidate_selection":
            reasons.append("terminal_selection_lineage_missing")
        else:
            payload = selection_event.get("payload") or {}
            if selection_event.get("event_hash") != evidence.get("selection_lineage_event_hash"):
                reasons.append("terminal_selection_lineage_hash_mismatch")
            if payload.get("selection_hash") != evidence.get("selection_hash") or payload.get("candidate_id") != evidence.get("candidate_id"):
                reasons.append("terminal_selection_lineage_binding_mismatch")
            if payload.get("multiple_testing_policy") != evidence.get("multiple_testing_policy"):
                reasons.append("terminal_selection_multiple_testing_policy_mismatch")
            if payload.get("selected_multiple_testing") != evidence.get("selected_multiple_testing"):
                reasons.append("terminal_selection_multiple_testing_mismatch")
            if set(selection_event.get("parents", [])) != {row.get("id") for row in trial_events}:
                reasons.append("terminal_selection_trial_parent_set_mismatch")
        terminal_events = [
            row for row in events
            if row.get("type") == "terminal_holdout"
            and (row.get("payload") or {}).get("experiment_id") == evidence.get("experiment_id")
        ]
        if len(terminal_events) != 1:
            reasons.append("terminal_one_shot_lineage_violation")
        if not terminal_event or terminal_event.get("type") != "terminal_holdout":
            reasons.append("terminal_holdout_lineage_missing")
        else:
            payload = terminal_event.get("payload") or {}
            if terminal_event.get("event_hash") != evidence.get("terminal_lineage_event_hash"):
                reasons.append("terminal_holdout_lineage_hash_mismatch")
            if payload.get("terminal_result_hash") != evidence.get("terminal_result_hash") or payload.get("candidate_id") != evidence.get("candidate_id"):
                reasons.append("terminal_holdout_lineage_binding_mismatch")
            if selection_event and evidence.get("selection_lineage_id") not in terminal_event.get("parents", []):
                reasons.append("terminal_holdout_parent_mismatch")
    return not reasons, list(dict.fromkeys(reasons))


@contextmanager
def _exclusive_file_lock(path: Path) -> Iterator[None]:
    if fcntl is None:
        raise RuntimeError("interprocess tournament locking is unavailable on this platform")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def _registry_mutation(method):
    """Serialize one complete tournament state transition."""

    @wraps(method)
    def wrapped(self, *args, **kwargs):
        with _exclusive_file_lock(self.registry_lock_path):
            return method(self, *args, **kwargs)

    return wrapped


class ResearchTournament:
    def __init__(self, registry_path: Path | str = DEFAULT_REGISTRY_PATH, *, lineage: LineageStore | None = None):
        self.registry_path = Path(registry_path)
        self.registry_lock_path = self.registry_path.with_name(f".{self.registry_path.name}.lock")
        self.lineage = lineage or LineageStore()

    def load(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return {"schema_version": REGISTRY_SCHEMA_VERSION, "experiments": []}
        payload = json.loads(self.registry_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("research tournament registry root must be an object")
        if payload.get("schema_version") != REGISTRY_SCHEMA_VERSION:
            raise ValueError("unsupported research tournament registry schema")
        payload.setdefault("experiments", [])
        return payload

    def save(self, payload: dict[str, Any]) -> None:
        payload["updated_at"] = _utc_now()
        _atomic(self.registry_path, payload)

    def _experiment(self, registry: dict[str, Any], experiment_id: str) -> dict[str, Any]:
        row = next((item for item in registry["experiments"] if item.get("id") == experiment_id), None)
        if row is None:
            raise KeyError(experiment_id)
        return row

    @_registry_mutation
    def create_from_snapshot(
        self, snapshot: dict[str, Any], *, strategy_name: str, holdout_bars: int,
        embargo_bars: int = 0, min_search_bars: int = 200,
        max_candidate_trials: int = 20, familywise_alpha: float = 0.05,
        min_sign_test_trades: int = 10, dependence_block_size: int = 5,
        min_nonzero_blocks: int = 5, experiment_id: str | None = None,
        actor: str = "research-controller",
    ) -> dict[str, Any]:
        registry = self.load()
        experiment_id = experiment_id or f"experiment-{uuid.uuid4().hex[:12]}"
        if any(row.get("id") == experiment_id for row in registry["experiments"]):
            raise ValueError("research experiment already exists")
        plan = build_experiment_plan_from_snapshot(
            snapshot, experiment_id=experiment_id, strategy_name=strategy_name,
            holdout_bars=holdout_bars, embargo_bars=embargo_bars,
            min_search_bars=min_search_bars, max_candidate_trials=max_candidate_trials,
            familywise_alpha=familywise_alpha, min_sign_test_trades=min_sign_test_trades,
            dependence_block_size=dependence_block_size, min_nonzero_blocks=min_nonzero_blocks,
        )
        event = self.lineage.append(
            "research_experiment",
            {
                "experiment_id": experiment_id,
                "experiment_hash": plan["experiment_hash"],
                "strategy_name": strategy_name,
                "full_dataset_hash": plan["full_dataset"]["dataset_hash"],
                "search_dataset_hash": plan["search_dataset"]["dataset_hash"],
                "terminal_holdout_dataset_hash": plan["terminal_holdout_commitment"]["dataset_hash"],
                "embargo_bars": plan["embargo_bars"],
                "multiple_testing_policy": plan["multiple_testing_policy"],
            }, actor=actor,
        )
        record = {
            "id": experiment_id, "status": "searching", "plan": plan,
            "experiment_lineage_id": event["id"],
            "experiment_lineage_event_hash": event["event_hash"], "trials": [],
            "selection": None, "terminal_holdout_evidence": None, "created_at": _utc_now(),
        }
        registry["experiments"].append(record)
        self.save(registry)
        return record

    def create(
        self, *, strategy_name: str, symbol: str, granularity: int, holdout_bars: int,
        embargo_bars: int = 0, min_search_bars: int = 200,
        max_candidate_trials: int = 20, familywise_alpha: float = 0.05,
        min_sign_test_trades: int = 10, dependence_block_size: int = 5,
        min_nonzero_blocks: int = 5, start_ts: float | None = None,
        end_ts: float | None = None, window_bars: int | None = None,
        experiment_id: str | None = None,
    ) -> dict[str, Any]:
        snapshot = load_canonical_snapshot(
            kind=DATASET_KIND, symbol=symbol, granularity=int(granularity),
            start_ts=start_ts, end_ts=end_ts, window_bars=window_bars,
        )
        return self.create_from_snapshot(
            snapshot, strategy_name=strategy_name, holdout_bars=holdout_bars,
            embargo_bars=embargo_bars, min_search_bars=min_search_bars,
            max_candidate_trials=max_candidate_trials, familywise_alpha=familywise_alpha,
            min_sign_test_trades=min_sign_test_trades, dependence_block_size=dependence_block_size,
            min_nonzero_blocks=min_nonzero_blocks, experiment_id=experiment_id,
        )

    @_registry_mutation
    def register_candidate(
        self, experiment_id: str, validation_evidence: Any, *,
        actor: str = "research-gate", reverify_source: bool = True,
    ) -> dict[str, Any]:
        registry = self.load()
        experiment = self._experiment(registry, experiment_id)
        if experiment.get("status") != "searching":
            raise ValueError("research experiment is no longer accepting candidate trials")
        if not isinstance(validation_evidence, dict) or not validation_evidence.get("candidate_id"):
            raise ValueError("candidate validation evidence with candidate_id is required")
        plan = experiment["plan"]
        mp_artifact = plan.get("multiple_testing_policy")
        mp = policy_from_artifact(mp_artifact)
        if len(experiment["trials"]) >= mp.max_candidate_trials:
            raise ValueError("candidate search budget exhausted")
        candidate_id = str(validation_evidence["candidate_id"])
        if any(row.get("candidate_id") == candidate_id for row in experiment["trials"]):
            raise ValueError("candidate already registered in this experiment")
        reasons: list[str] = []
        evidence_is_canonical = True
        try:
            json.dumps(validation_evidence, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError, OverflowError):
            evidence_is_canonical = False

        if evidence_is_canonical:
            evidence_for_storage = validation_evidence
            try:
                alpha_valid, alpha_reasons = verify_alpha_validation_evidence(validation_evidence)
            except (TypeError, ValueError, KeyError, AttributeError, OverflowError):
                alpha_valid, alpha_reasons = False, ["verification_error"]
            if not alpha_valid:
                reasons.extend(f"alpha:{reason}" for reason in alpha_reasons)
            if validation_evidence.get("replay_provenance_bound") is not True:
                reasons.append("replay_provenance_required")
            if alpha_valid and validation_evidence.get("replay_provenance_bound") is True:
                try:
                    replay_valid, replay_reasons = verify_evidence_replay_binding(
                        validation_evidence, reverify_source=reverify_source
                    )
                except (TypeError, ValueError, KeyError, AttributeError, OverflowError, RuntimeError, ImportError):
                    replay_valid, replay_reasons = False, ["verification_error"]
                if not replay_valid:
                    reasons.extend(f"replay:{reason}" for reason in replay_reasons)
            attestation = validation_evidence.get("replay_attestation") or {}
            if not isinstance(attestation, dict):
                reasons.append("candidate_replay_attestation_invalid")
                attestation = {}
            if validation_evidence.get("dataset_hash") != plan["search_dataset"]["dataset_hash"]:
                reasons.append("candidate_search_dataset_hash_mismatch")
            if validation_evidence.get("dataset_id") != plan["search_dataset"]["dataset_id"]:
                reasons.append("candidate_search_dataset_id_mismatch")
            if attestation.get("strategy_name") != plan["strategy_name"]:
                reasons.append("candidate_strategy_mismatch")
            if validation_evidence.get("walk_forward_passed") is not True:
                reasons.append("candidate_walk_forward_failed")
            try:
                multiplicity = assess_candidate_significance(
                    validation_evidence.get("fold_returns"), mp_artifact
                )
            except (TypeError, ValueError, OverflowError):
                multiplicity = _invalid_multiple_testing_assessment(mp_artifact)
        else:
            evidence_for_storage = {
                "candidate_id": candidate_id,
                "canonicalization_failed": True,
            }
            reasons.append("candidate_evidence_not_canonical")
            multiplicity = _invalid_multiple_testing_assessment(mp_artifact)

        if multiplicity.get("passed") is not True:
            reasons.extend(multiplicity.get("reasons") or ["multiple_testing_failed"])

        metrics_invalid = not evidence_is_canonical
        if not metrics_invalid:
            try:
                metrics = {
                    "net_pnl_after_cost_usd": float(
                        validation_evidence.get("net_pnl_after_cost_usd", 0.0)
                    ),
                    "profit_factor": float(validation_evidence.get("profit_factor", 0.0)),
                    "max_drawdown_pct": float(
                        validation_evidence.get("max_drawdown_pct", 0.0)
                    ),
                    "annualized_return_pct": float(
                        validation_evidence.get("annualized_return_pct", 0.0)
                    ),
                    "out_of_sample_trades": int(
                        validation_evidence.get("out_of_sample_trades", 0)
                    ),
                }
                finite_metric_names = (
                    "net_pnl_after_cost_usd",
                    "profit_factor",
                    "max_drawdown_pct",
                    "annualized_return_pct",
                )
                if not all(math.isfinite(metrics[name]) for name in finite_metric_names):
                    raise ValueError("selection metrics must be finite")
                if metrics["out_of_sample_trades"] < 0:
                    raise ValueError("out_of_sample_trades must be non-negative")
            except (TypeError, ValueError, OverflowError):
                metrics_invalid = True

        if metrics_invalid:
            metrics = {
                "net_pnl_after_cost_usd": 0.0,
                "profit_factor": 0.0,
                "max_drawdown_pct": 1e30,
                "annualized_return_pct": 0.0,
                "out_of_sample_trades": 0,
            }
            if evidence_is_canonical:
                reasons.append("candidate_selection_metrics_invalid")

        trial = {
            "trial_index": len(experiment["trials"]) + 1,
            "candidate_id": candidate_id,
            "candidate_source_sha": evidence_for_storage.get("candidate_source_sha"),
            "candidate_config": evidence_for_storage.get("candidate_config"),
            "candidate_config_hash": evidence_for_storage.get("candidate_config_hash"),
            "validation_evidence_hash": (
                evidence_for_storage.get("evidence_hash") if evidence_is_canonical else None
            ),
            "validation_evidence": evidence_for_storage,
            "selection_metrics": metrics,
            "multiple_testing": multiplicity,
            "eligible": not reasons,
            "reasons": list(dict.fromkeys(reasons)),
            "registered_at": _utc_now(),
        }
        # Prove the complete registry trial can be encoded before lineage
        # changes. This prevents malformed evidence from leaving an orphan
        # candidate_trial event when strict registry persistence rejects it.
        json.dumps(trial, sort_keys=True, allow_nan=False)
        event = self.lineage.append(
            "candidate_trial",
            {
                "experiment_id": experiment_id, "experiment_hash": plan["experiment_hash"],
                "trial_index": trial["trial_index"], "candidate_id": candidate_id,
                "validation_evidence_hash": trial["validation_evidence_hash"],
                "candidate_config_hash": trial["candidate_config_hash"],
                "multiple_testing": multiplicity, "eligible": trial["eligible"],
                "reasons": trial["reasons"], "selection_metrics": metrics,
            }, actor=actor, parents=[experiment["experiment_lineage_id"]],
        )
        trial["lineage_id"] = event["id"]
        trial["lineage_event_hash"] = event["event_hash"]
        experiment["trials"].append(trial)
        self.save(registry)
        return trial

    @_registry_mutation
    def seal_selection(self, experiment_id: str, *, actor: str = "research-selector") -> dict[str, Any]:
        registry = self.load()
        experiment = self._experiment(registry, experiment_id)
        if experiment.get("status") != "searching":
            raise ValueError("research experiment cannot be selected from its current state")
        mp = policy_from_artifact(experiment["plan"]["multiple_testing_policy"])
        if len(experiment["trials"]) > mp.max_candidate_trials:
            raise ValueError("candidate search budget exceeded")
        eligible = [row for row in experiment["trials"] if row.get("eligible") is True]
        if not eligible:
            raise ValueError("research experiment has no eligible candidate")
        for row in eligible:
            valid, verify_reasons = verify_candidate_significance(
                row.get("multiple_testing"),
                (row.get("validation_evidence") or {}).get("fold_returns"),
                experiment["plan"]["multiple_testing_policy"],
            )
            if not valid or row.get("multiple_testing", {}).get("passed") is not True:
                raise ValueError(
                    "eligible candidate failed multiple-testing reverification: "
                    + ",".join(verify_reasons or ["not_significant"])
                )
        ranked = sorted(eligible, key=_selection_sort_key)
        selected = ranked[0]
        core = {
            "policy_version": SELECTION_POLICY_VERSION,
            "experiment_id": experiment_id,
            "experiment_hash": experiment["plan"]["experiment_hash"],
            "multiple_testing_policy": experiment["plan"]["multiple_testing_policy"],
            "candidate_id": selected["candidate_id"],
            "candidate_source_sha": selected["candidate_source_sha"],
            "candidate_config_hash": selected["candidate_config_hash"],
            "alpha_validation_evidence_hash": selected["validation_evidence_hash"],
            "selected_multiple_testing": selected["multiple_testing"],
            "trial_count": len(experiment["trials"]),
            "max_candidate_trials": mp.max_candidate_trials,
            "trial_budget_remaining": mp.max_candidate_trials - len(experiment["trials"]),
            "eligible_trial_count": len(eligible),
            "ranked_candidate_ids": [row["candidate_id"] for row in ranked],
            "selected_metrics": selected["selection_metrics"], "selected_at": _utc_now(),
        }
        selection = {**core, "selection_hash": stable_hash(core)}
        event = self.lineage.append(
            "candidate_selection",
            {
                "experiment_id": experiment_id,
                "experiment_hash": experiment["plan"]["experiment_hash"],
                "candidate_id": selected["candidate_id"],
                "alpha_validation_evidence_hash": selected["validation_evidence_hash"],
                "selection_hash": selection["selection_hash"],
                "multiple_testing_policy": experiment["plan"]["multiple_testing_policy"],
                "selected_multiple_testing": selected["multiple_testing"],
                "trial_count": len(experiment["trials"]),
                "max_candidate_trials": mp.max_candidate_trials,
                "eligible_trial_count": len(eligible),
            }, actor=actor, parents=[row["lineage_id"] for row in experiment["trials"]],
        )
        selection["lineage_id"] = event["id"]
        selection["lineage_event_hash"] = event["event_hash"]
        experiment["selection"] = selection
        experiment["status"] = "selected"
        self.save(registry)
        return selection

    @_registry_mutation
    def run_terminal_holdout(
        self, experiment_id: str, *, policy: TerminalHoldoutPolicy | None = None,
        actor: str = "terminal-holdout-runner",
    ) -> dict[str, Any]:
        registry = self.load()
        experiment = self._experiment(registry, experiment_id)
        if experiment.get("status") != "selected" or not isinstance(experiment.get("selection"), dict):
            raise ValueError("research experiment must be selected before terminal holdout")
        existing = [
            row for row in self.lineage.events()
            if row.get("type") == "terminal_holdout"
            and (row.get("payload") or {}).get("experiment_id") == experiment_id
        ]
        if existing or experiment.get("terminal_holdout_evidence") is not None:
            raise ValueError("terminal holdout is one-shot and has already been evaluated")
        selection = experiment["selection"]
        selected_trial = next(
            row for row in experiment["trials"]
            if row.get("candidate_id") == selection.get("candidate_id")
            and row.get("validation_evidence_hash") == selection.get("alpha_validation_evidence_hash")
        )
        alpha = selected_trial["validation_evidence"]
        mp_artifact = experiment["plan"]["multiple_testing_policy"]
        mp = policy_from_artifact(mp_artifact)
        valid, verify_reasons = verify_candidate_significance(
            selected_trial.get("multiple_testing"), alpha.get("fold_returns"), mp_artifact
        )
        if not valid or selected_trial.get("multiple_testing", {}).get("passed") is not True:
            raise ValueError(
                "selected candidate failed multiple-testing gate: "
                + ",".join(verify_reasons or ["not_significant"])
            )
        if len(experiment["trials"]) > mp.max_candidate_trials:
            raise ValueError("candidate search budget exceeded before terminal holdout")
        search_returns = flatten_fold_returns(alpha.get("fold_returns"))
        replay = alpha.get("replay_attestation") or {}
        terminal_dataset = experiment["plan"]["terminal_holdout_commitment"]
        snapshot = load_canonical_snapshot(
            kind=terminal_dataset["kind"], symbol=terminal_dataset["symbol"],
            granularity=int(terminal_dataset["granularity"]),
            start_ts=float(terminal_dataset["start_ts"]), end_ts=float(terminal_dataset["end_ts"]),
        )
        if snapshot["manifest"] != terminal_dataset:
            raise ValueError("terminal holdout source no longer matches experiment commitment")
        strategy_name = replay.get("strategy_name")
        if strategy_name != experiment["plan"]["strategy_name"]:
            raise ValueError("selected strategy does not match experiment strategy")
        strategy_config = normalize_strategy_config(strategy_name, replay.get("strategy_config"))
        returns = replay_trade_returns_rust(
            strategy_name, snapshot["rows"], warmup=int(replay.get("warmup", 30)),
            fee_bps=_finite(replay.get("fee_bps", 0.0), name="fee_bps"),
            max_hold_bars=int(replay.get("max_hold_bars", 0)), strategy_config=strategy_config,
        )
        periods_per_year = int(alpha.get("periods_per_year", 252))
        active_policy = policy or TerminalHoldoutPolicy()
        metrics, passed, failure_reasons = _derive_terminal_result(
            returns, periods_per_year=periods_per_year, policy=active_policy
        )
        core = {
            "schema_version": TERMINAL_EVIDENCE_SCHEMA_VERSION, "method": TERMINAL_METHOD,
            "experiment_id": experiment_id, "experiment_hash": experiment["plan"]["experiment_hash"],
            "selection_hash": selection["selection_hash"], "selection_policy_version": SELECTION_POLICY_VERSION,
            "candidate_id": selected_trial["candidate_id"],
            "candidate_source_sha": selected_trial["candidate_source_sha"],
            "candidate_config": selected_trial["candidate_config"],
            "candidate_config_hash": selected_trial["candidate_config_hash"],
            "alpha_validation_evidence_hash": selected_trial["validation_evidence_hash"],
            "multiple_testing_policy": mp_artifact,
            "selected_multiple_testing": selected_trial["multiple_testing"],
            "search_oos_returns": search_returns, "search_oos_returns_hash": stable_hash(search_returns),
            "trial_count": len(experiment["trials"]), "max_candidate_trials": mp.max_candidate_trials,
            "terminal_dataset": terminal_dataset, "strategy_name": strategy_name,
            "strategy_config": strategy_config, "strategy_config_hash": stable_hash(strategy_config),
            "warmup": int(replay.get("warmup", 30)),
            "fee_bps": _finite(replay.get("fee_bps", 0.0), name="fee_bps"),
            "max_hold_bars": int(replay.get("max_hold_bars", 0)), "periods_per_year": periods_per_year,
            "returns": returns, "returns_hash": stable_hash(returns), "metrics": metrics,
            "policy": asdict(active_policy), "passed": passed, "reasons": failure_reasons,
            "evaluated_at": _utc_now(),
        }
        result_hash = stable_hash(core)
        event = self.lineage.append(
            "terminal_holdout",
            {
                "experiment_id": experiment_id,
                "experiment_hash": experiment["plan"]["experiment_hash"],
                "candidate_id": selected_trial["candidate_id"],
                "selection_hash": selection["selection_hash"],
                "alpha_validation_evidence_hash": selected_trial["validation_evidence_hash"],
                "selected_adjusted_p_value": selected_trial["multiple_testing"]["adjusted_p_value"],
                "terminal_result_hash": result_hash, "passed": passed, "reasons": failure_reasons,
            }, actor=actor, parents=[selection["lineage_id"]],
        )
        evidence = {
            **core, "terminal_result_hash": result_hash,
            "experiment_lineage_id": experiment["experiment_lineage_id"],
            "experiment_lineage_event_hash": experiment["experiment_lineage_event_hash"],
            "selection_lineage_id": selection["lineage_id"],
            "selection_lineage_event_hash": selection["lineage_event_hash"],
            "terminal_lineage_id": event["id"], "terminal_lineage_event_hash": event["event_hash"],
        }
        evidence["evidence_hash"] = stable_hash(evidence)
        valid, reasons = verify_terminal_holdout_evidence(
            evidence, lineage=self.lineage, reverify_source=True
        )
        if not valid:
            raise ValueError("terminal holdout evidence failed self-verification: " + ",".join(reasons))
        experiment["terminal_holdout_evidence"] = evidence
        experiment["status"] = "terminal_passed" if passed else "terminal_failed"
        self.save(registry)
        return evidence
