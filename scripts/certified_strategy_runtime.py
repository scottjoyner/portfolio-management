#!/usr/bin/env python3
"""Fail-closed execution adapter for tournament-certified strategy identities.

This module has no broker/order-routing authority.  It turns the existing
promotion artifacts into a runtime strategy identity only after re-verifying the
stored alpha evidence, canonical replay, untouched terminal holdout, source
ancestry and the exact compiled Rust evaluator that will emit the signal.

The important trust rule is that ``agent_runtime_config.json`` is a selector,
not an authority.  Certification is re-derived from the promotion registry and
research evidence at runtime.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any, Callable

from scripts.alpha_validation import stable_hash, verify_alpha_validation_evidence
from scripts.backtest_framework.canonical_replay import (
    normalize_strategy_config,
    verify_evidence_replay_binding,
)
from scripts.learning_lineage import LineageStore
from scripts.research_tournament import verify_terminal_holdout_evidence
from strategy_engine import Signal

ROOT = Path(__file__).resolve().parents[1]
ACTIVE_CONFIG_PATH = ROOT / "data" / "agent_runtime_config.json"
CHALLENGER_REGISTRY_PATH = ROOT / "data" / "learning" / "challengers.json"
RUNTIME_IDENTITY_SCHEMA_VERSION = 1
SUPPORTED_RUNTIME_EVALUATOR = "rust_core.run_rsi_revert_opens_configured_py"
STRATEGY_SOURCE_PATHS = (
    "rust_core/src",
    "scripts/backtest_framework/canonical_replay.py",
)


class CertifiedRuntimeError(RuntimeError):
    """Raised when a promoted runtime identity cannot be verified exactly."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rust_binary_path() -> Path:
    try:
        import rust_core
    except ImportError as exc:  # pragma: no cover - exercised in non-Rust environments
        raise CertifiedRuntimeError("compiled_rust_core_required") from exc
    path = Path(getattr(rust_core, "__file__", ""))
    if not path.is_file():
        raise CertifiedRuntimeError("compiled_rust_core_path_invalid")
    return path.resolve()


def runtime_binary_sha256() -> str:
    """Hash the exact compiled evaluator imported by this Python process."""

    return _sha256_file(_rust_binary_path())


def verify_source_ancestry(candidate_source_sha: str, *, root: Path = ROOT) -> None:
    """Require certified strategy source to be an unchanged ancestor of runtime.

    Runtime-binding/orchestration code may be newer than the research candidate,
    but the Rust strategy implementation and canonical replay source may not have
    drifted since the candidate SHA.
    """

    source_sha = str(candidate_source_sha or "").lower()
    if len(source_sha) != 40 or any(ch not in "0123456789abcdef" for ch in source_sha):
        raise CertifiedRuntimeError("candidate_source_sha_invalid")
    try:
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", source_sha, "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        subprocess.run(
            ["git", "diff", "--quiet", source_sha, "--", *STRATEGY_SOURCE_PATHS],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except subprocess.CalledProcessError as exc:
        raise CertifiedRuntimeError("certified_strategy_source_drift") from exc
    except (OSError, subprocess.SubprocessError) as exc:
        raise CertifiedRuntimeError("git_source_identity_unavailable") from exc


def _load_json(path: Path, *, missing_reason: str, invalid_reason: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CertifiedRuntimeError(missing_reason) from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise CertifiedRuntimeError(invalid_reason) from exc
    if not isinstance(payload, dict):
        raise CertifiedRuntimeError(invalid_reason)
    return payload


def _challenger_for_active_config(
    config: dict[str, Any], registry: dict[str, Any]
) -> dict[str, Any]:
    candidate_id = config.get("active_challenger_id")
    if not candidate_id:
        raise CertifiedRuntimeError("active_challenger_id_required")
    challengers = registry.get("challengers")
    if not isinstance(challengers, list):
        raise CertifiedRuntimeError("challenger_registry_invalid")
    challenger = next(
        (row for row in challengers if isinstance(row, dict) and row.get("id") == candidate_id),
        None,
    )
    if challenger is None:
        raise CertifiedRuntimeError("active_challenger_not_found")
    if challenger.get("status") not in {"canary", "active"}:
        raise CertifiedRuntimeError("active_challenger_not_promoted")
    evaluation = challenger.get("evaluation")
    if not isinstance(evaluation, dict) or evaluation.get("approved") is not True:
        raise CertifiedRuntimeError("active_challenger_evaluation_not_approved")
    return challenger


def derive_runtime_identity(
    config: dict[str, Any],
    registry: dict[str, Any],
    *,
    binary_sha256: str,
    alpha_verifier: Callable[[Any], tuple[bool, list[str]]] = verify_alpha_validation_evidence,
    replay_verifier: Callable[..., tuple[bool, list[str]]] = verify_evidence_replay_binding,
    terminal_verifier: Callable[..., tuple[bool, list[str]]] = verify_terminal_holdout_evidence,
    source_verifier: Callable[..., None] = verify_source_ancestry,
    lineage: LineageStore | None = None,
    verify_source: bool = True,
    root: Path = ROOT,
) -> dict[str, Any]:
    """Re-derive the immutable runtime identity from promotion-time evidence.

    Callers cannot supply a claimed certification object.  Every field is taken
    from the promoted challenger and independently cross-checked before an
    identity hash can exist.
    """

    challenger = _challenger_for_active_config(config, registry)
    candidate_id = str(challenger["id"])
    alpha = challenger.get("alpha_validation_evidence")
    terminal = challenger.get("terminal_holdout_evidence")
    if not isinstance(alpha, dict):
        raise CertifiedRuntimeError("alpha_validation_evidence_required")
    if not isinstance(terminal, dict):
        raise CertifiedRuntimeError("terminal_holdout_evidence_required")

    alpha_hash = challenger.get("alpha_validation_evidence_hash")
    terminal_hash = challenger.get("terminal_holdout_evidence_hash")
    if not isinstance(alpha_hash, str) or alpha.get("evidence_hash") != alpha_hash:
        raise CertifiedRuntimeError("alpha_validation_evidence_hash_mismatch")
    if not isinstance(terminal_hash, str) or terminal.get("evidence_hash") != terminal_hash:
        raise CertifiedRuntimeError("terminal_holdout_evidence_hash_mismatch")
    if config.get("alpha_validation_evidence_hash") != alpha_hash:
        raise CertifiedRuntimeError("active_config_alpha_evidence_mismatch")
    if config.get("terminal_holdout_evidence_hash") != terminal_hash:
        raise CertifiedRuntimeError("active_config_terminal_evidence_mismatch")
    if challenger.get("evaluation", {}).get("evidence_hash") != alpha_hash:
        raise CertifiedRuntimeError("evaluation_alpha_evidence_mismatch")
    if challenger.get("evaluation", {}).get("terminal_holdout_evidence_hash") != terminal_hash:
        raise CertifiedRuntimeError("evaluation_terminal_evidence_mismatch")
    if challenger.get("evaluation", {}).get("terminal_holdout_verified") is not True:
        raise CertifiedRuntimeError("evaluation_terminal_holdout_not_verified")

    alpha_ok, alpha_reasons = alpha_verifier(alpha)
    if not alpha_ok:
        raise CertifiedRuntimeError(
            "alpha_validation_evidence_invalid:" + ",".join(alpha_reasons)
        )
    if alpha.get("replay_provenance_bound") is not True:
        raise CertifiedRuntimeError("alpha_validation_replay_provenance_required")
    replay = alpha.get("replay_attestation")
    if not isinstance(replay, dict) or replay.get("execution_config_bound") is not True:
        raise CertifiedRuntimeError("alpha_validation_execution_config_binding_required")

    strategy_name = str(replay.get("strategy_name") or "")
    strategy_config = normalize_strategy_config(strategy_name, replay.get("strategy_config"))
    if not strategy_config or strategy_name != "rsi_revert":
        raise CertifiedRuntimeError("certified_runtime_strategy_unsupported")
    if strategy_config != alpha.get("candidate_config"):
        raise CertifiedRuntimeError("alpha_validation_execution_config_mismatch")
    if strategy_config != challenger.get("parameters"):
        raise CertifiedRuntimeError("challenger_runtime_config_mismatch")
    if strategy_config != terminal.get("strategy_config"):
        raise CertifiedRuntimeError("terminal_runtime_config_mismatch")
    if strategy_name != terminal.get("strategy_name"):
        raise CertifiedRuntimeError("terminal_runtime_strategy_mismatch")

    if alpha.get("candidate_id") != candidate_id or terminal.get("candidate_id") != candidate_id:
        raise CertifiedRuntimeError("certified_runtime_candidate_mismatch")
    candidate_source_sha = str(alpha.get("candidate_source_sha") or "")
    if terminal.get("candidate_source_sha") != candidate_source_sha:
        raise CertifiedRuntimeError("terminal_candidate_source_mismatch")
    if terminal.get("alpha_validation_evidence_hash") != alpha_hash:
        raise CertifiedRuntimeError("terminal_alpha_evidence_mismatch")
    if terminal.get("passed") is not True:
        raise CertifiedRuntimeError("terminal_holdout_policy_failed")

    try:
        replay_ok, replay_reasons = replay_verifier(alpha, reverify_source=verify_source)
    except TypeError:
        replay_ok, replay_reasons = replay_verifier(alpha)
    if not replay_ok:
        raise CertifiedRuntimeError(
            "canonical_replay_verification_failed:" + ",".join(replay_reasons)
        )
    try:
        terminal_ok, terminal_reasons = terminal_verifier(
            terminal, lineage=lineage, reverify_source=verify_source
        )
    except TypeError:
        terminal_ok, terminal_reasons = terminal_verifier(terminal)
    if not terminal_ok:
        raise CertifiedRuntimeError(
            "terminal_holdout_verification_failed:" + ",".join(terminal_reasons)
        )

    if verify_source:
        source_verifier(candidate_source_sha, root=root)
    elif len(candidate_source_sha) != 40:
        raise CertifiedRuntimeError("candidate_source_sha_invalid")

    evaluator = SUPPORTED_RUNTIME_EVALUATOR
    replay_attestation_hash = replay.get("attestation_hash")
    if not isinstance(replay_attestation_hash, str):
        raise CertifiedRuntimeError("replay_attestation_hash_required")
    strategy_config_hash = stable_hash(strategy_config)
    if replay.get("strategy_config_hash") != strategy_config_hash:
        raise CertifiedRuntimeError("replay_strategy_config_hash_mismatch")
    if terminal.get("strategy_config_hash") != strategy_config_hash:
        raise CertifiedRuntimeError("terminal_strategy_config_hash_mismatch")

    identity = {
        "schema_version": RUNTIME_IDENTITY_SCHEMA_VERSION,
        "candidate_id": candidate_id,
        "candidate_source_sha": candidate_source_sha,
        "strategy_name": strategy_name,
        "strategy_config": strategy_config,
        "strategy_config_hash": strategy_config_hash,
        "alpha_validation_evidence_hash": alpha_hash,
        "terminal_holdout_evidence_hash": terminal_hash,
        "replay_attestation_hash": replay_attestation_hash,
        "promotion_lineage_id": config.get("promotion_lineage_id"),
        "runtime_binary_sha256": str(binary_sha256),
        "runtime_evaluator": evaluator,
    }
    if not identity["runtime_binary_sha256"]:
        raise CertifiedRuntimeError("runtime_binary_sha256_required")
    return identity


def load_certified_runtime_context(
    config_path: Path | str = ACTIVE_CONFIG_PATH,
    registry_path: Path | str = CHALLENGER_REGISTRY_PATH,
    *,
    binary_sha256: str | None = None,
    verify_source: bool = True,
    lineage: LineageStore | None = None,
    root: Path = ROOT,
) -> dict[str, Any]:
    """Load promotion state and return a verified strategy/deployment context."""

    config = _load_json(
        Path(config_path),
        missing_reason="active_runtime_config_missing",
        invalid_reason="active_runtime_config_invalid",
    )
    registry = _load_json(
        Path(registry_path),
        missing_reason="challenger_registry_missing",
        invalid_reason="challenger_registry_invalid",
    )
    active_lineage = lineage if lineage is not None else LineageStore()
    identity = derive_runtime_identity(
        config,
        registry,
        binary_sha256=binary_sha256 or runtime_binary_sha256(),
        lineage=active_lineage,
        verify_source=verify_source,
        root=root,
    )
    fraction = float(config.get("canary_fraction", 0.0))
    if config.get("deployment") != "canary" or not (0.0 < fraction <= 1.0):
        raise CertifiedRuntimeError("certified_runtime_deployment_invalid")
    return {
        "identity": identity,
        "runtime_identity_hash": stable_hash(identity),
        "deployment": "canary",
        "canary_fraction": fraction,
    }


def load_certified_runtime_identity(
    config_path: Path | str = ACTIVE_CONFIG_PATH,
    registry_path: Path | str = CHALLENGER_REGISTRY_PATH,
    **kwargs: Any,
) -> dict[str, Any]:
    return load_certified_runtime_context(config_path, registry_path, **kwargs)["identity"]


def canary_selected(identity: dict[str, Any], key: str, *, fraction: float) -> bool:
    """Deterministically route a market key into the certified canary."""

    if not (0.0 < fraction <= 1.0):
        raise CertifiedRuntimeError("certified_runtime_canary_fraction_invalid")
    bucket = int(
        hashlib.sha256(f"{identity['candidate_id']}:{key}".encode("utf-8")).hexdigest()[:16],
        16,
    )
    return (bucket / float(0xFFFFFFFFFFFFFFFF)) < fraction


def runtime_certification(identity: dict[str, Any]) -> dict[str, Any]:
    """Return the portable, hash-verifiable identity carried by a signal."""

    return {
        "certified": True,
        "runtime_identity_hash": stable_hash(identity),
        "candidate_id": identity["candidate_id"],
        "candidate_source_sha": identity["candidate_source_sha"],
        "strategy_name": identity["strategy_name"],
        "strategy_config_hash": identity["strategy_config_hash"],
        "alpha_validation_evidence_hash": identity["alpha_validation_evidence_hash"],
        "terminal_holdout_evidence_hash": identity["terminal_holdout_evidence_hash"],
        "replay_attestation_hash": identity["replay_attestation_hash"],
        "promotion_lineage_id": identity.get("promotion_lineage_id"),
        "runtime_binary_sha256": identity["runtime_binary_sha256"],
        "runtime_evaluator": identity["runtime_evaluator"],
    }


def run_certified_strategy(
    identity: dict[str, Any],
    *,
    closes: list[float],
    volumes: list[float] | None = None,
    highs: list[float] | None = None,
    lows: list[float] | None = None,
) -> list[Signal]:
    """Run the exact configured evaluator attested by canonical research replay."""

    if identity.get("runtime_evaluator") != SUPPORTED_RUNTIME_EVALUATOR:
        raise CertifiedRuntimeError("certified_runtime_evaluator_unsupported")
    config = normalize_strategy_config(identity["strategy_name"], identity["strategy_config"])
    if identity["strategy_name"] != "rsi_revert" or not config:
        raise CertifiedRuntimeError("certified_runtime_strategy_unsupported")
    if stable_hash(config) != identity.get("strategy_config_hash"):
        raise CertifiedRuntimeError("certified_runtime_strategy_config_hash_mismatch")
    if len(closes) < int(config["period"]) + 2:
        return []

    try:
        import rust_core
    except ImportError as exc:  # pragma: no cover
        raise CertifiedRuntimeError("compiled_rust_core_required") from exc
    evaluator = getattr(rust_core, "run_rsi_revert_opens_configured_py", None)
    if not callable(evaluator):
        raise CertifiedRuntimeError("certified_runtime_evaluator_missing")
    if runtime_binary_sha256() != identity.get("runtime_binary_sha256"):
        raise CertifiedRuntimeError("certified_runtime_binary_changed")

    result = evaluator(
        list(closes),
        list(closes),
        list(volumes or []),
        list(highs or []),
        list(lows or []),
        period=int(config["period"]),
        oversold=float(config["oversold"]),
        overbought=float(config["overbought"]),
    )
    if result is None:
        return []
    action, confidence, reason = result
    if action == "HOLD":
        return []
    if action not in {"BUY", "SELL"}:
        raise CertifiedRuntimeError("certified_runtime_action_invalid")

    signal = Signal(
        action=str(action),
        price=float(closes[-1]),
        confidence=float(confidence),
        reason=str(reason),
        strategy=str(identity["strategy_name"]),
    )
    signal.runtime_certification = runtime_certification(identity)
    return [signal]
