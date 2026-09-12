#!/usr/bin/env python3
"""Fail-closed execution adapter for tournament-certified strategy identities.

This module does not place orders. It binds a promoted research identity to the
exact configured Rust strategy entry point that produced canonical replay
results, verifies the promotion-time binary identity, and emits signals carrying
the immutable certification identity for downstream paper/shadow admission.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

from scripts.alpha_validation import stable_hash
from scripts.backtest_framework.canonical_replay import normalize_strategy_config
from strategy_engine import Signal

ROOT = Path(__file__).resolve().parents[1]
ACTIVE_CONFIG_PATH = ROOT / "data" / "agent_runtime_config.json"
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
    return _sha256_file(_rust_binary_path())


def _git_output(*args: str, cwd: Path = ROOT) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=cwd,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise CertifiedRuntimeError("git_source_identity_unavailable") from exc
    return result.stdout.strip()


def verify_source_ancestry(candidate_source_sha: str, *, root: Path = ROOT) -> None:
    """Require the certified source to be an ancestor and strategy code unchanged.

    The runtime-binding glue may be newer than the certified candidate, but the
    strategy implementation and canonical replay source must be byte-identical
    to the candidate source. This prevents a later orchestration-only commit from
    invalidating the identity while still failing closed on strategy drift.
    """

    if len(candidate_source_sha) != 40 or any(ch not in "0123456789abcdef" for ch in candidate_source_sha.lower()):
        raise CertifiedRuntimeError("candidate_source_sha_invalid")
    try:
        subprocess.run(
            ["git", "merge-base", "--is-ancestor", candidate_source_sha, "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        subprocess.run(
            ["git", "diff", "--quiet", candidate_source_sha, "--", *STRATEGY_SOURCE_PATHS],
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


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CertifiedRuntimeError("active_runtime_config_missing") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise CertifiedRuntimeError("active_runtime_config_invalid") from exc
    if not isinstance(payload, dict):
        raise CertifiedRuntimeError("active_runtime_config_invalid")
    return payload


def validate_runtime_identity(
    config: dict[str, Any],
    *,
    binary_sha256: str | None = None,
    verify_source: bool = True,
    root: Path = ROOT,
) -> dict[str, Any]:
    """Validate and return the immutable runtime identity from a promoted config."""

    identity = config.get("runtime_identity")
    supplied_hash = config.get("runtime_identity_hash")
    if not isinstance(identity, dict) or not isinstance(supplied_hash, str):
        raise CertifiedRuntimeError("certified_runtime_identity_required")
    if identity.get("schema_version") != RUNTIME_IDENTITY_SCHEMA_VERSION:
        raise CertifiedRuntimeError("certified_runtime_identity_schema_mismatch")
    if stable_hash(identity) != supplied_hash:
        raise CertifiedRuntimeError("certified_runtime_identity_hash_mismatch")
    if identity.get("candidate_id") != config.get("active_challenger_id"):
        raise CertifiedRuntimeError("certified_runtime_candidate_mismatch")
    if identity.get("alpha_validation_evidence_hash") != config.get("alpha_validation_evidence_hash"):
        raise CertifiedRuntimeError("certified_runtime_alpha_evidence_mismatch")
    if identity.get("terminal_holdout_evidence_hash") != config.get("terminal_holdout_evidence_hash"):
        raise CertifiedRuntimeError("certified_runtime_terminal_evidence_mismatch")
    if identity.get("runtime_evaluator") != SUPPORTED_RUNTIME_EVALUATOR:
        raise CertifiedRuntimeError("certified_runtime_evaluator_unsupported")

    strategy_name = identity.get("strategy_name")
    strategy_config = identity.get("strategy_config")
    normalized = normalize_strategy_config(str(strategy_name or ""), strategy_config)
    if normalized != strategy_config or not normalized:
        raise CertifiedRuntimeError("certified_runtime_strategy_config_invalid")
    if stable_hash(normalized) != identity.get("strategy_config_hash"):
        raise CertifiedRuntimeError("certified_runtime_strategy_config_hash_mismatch")

    expected_binary = identity.get("runtime_binary_sha256")
    actual_binary = binary_sha256 or runtime_binary_sha256()
    if not isinstance(expected_binary, str) or expected_binary != actual_binary:
        raise CertifiedRuntimeError("certified_runtime_binary_mismatch")

    candidate_source_sha = str(identity.get("candidate_source_sha") or "")
    if verify_source:
        verify_source_ancestry(candidate_source_sha, root=root)
    elif len(candidate_source_sha) != 40:
        raise CertifiedRuntimeError("candidate_source_sha_invalid")

    return dict(identity)


def load_certified_runtime_identity(
    path: Path | str = ACTIVE_CONFIG_PATH,
    *,
    binary_sha256: str | None = None,
    verify_source: bool = True,
    root: Path = ROOT,
) -> dict[str, Any]:
    return validate_runtime_identity(
        _load_json(Path(path)),
        binary_sha256=binary_sha256,
        verify_source=verify_source,
        root=root,
    )


def canary_selected(identity: dict[str, Any], key: str, *, fraction: float) -> bool:
    if not (0.0 < fraction <= 1.0):
        raise CertifiedRuntimeError("certified_runtime_canary_fraction_invalid")
    bucket = int(hashlib.sha256(f"{identity['candidate_id']}:{key}".encode("utf-8")).hexdigest()[:16], 16)
    return (bucket / float(0xFFFFFFFFFFFFFFFF)) < fraction


def runtime_certification(identity: dict[str, Any]) -> dict[str, Any]:
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
    if len(closes) < int(config["period"]) + 2:
        return []

    try:
        import rust_core
    except ImportError as exc:  # pragma: no cover
        raise CertifiedRuntimeError("compiled_rust_core_required") from exc
    evaluator = getattr(rust_core, "run_rsi_revert_opens_configured_py", None)
    if not callable(evaluator):
        raise CertifiedRuntimeError("certified_runtime_evaluator_missing")

    opens = list(closes)
    result = evaluator(
        list(closes),
        opens,
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
