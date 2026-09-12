#!/usr/bin/env python3
"""Public certified-runtime API with deployable evidence binding.

Research promotion remains the authority.  On a research/development checkout we
re-verify the promotion registry, canonical replay, terminal holdout, lineage,
Git ancestry and the loaded native evaluator exactly as before.

Production images intentionally do not carry ``.git`` or the historical replay
feed.  A deployment-preparation step therefore performs that expensive/full
reverification once and emits a compact, hash-bound deployment bundle.  At
runtime we fail closed unless the bundle matches the deployed strategy source
files and the exact native Rust extension loaded by the process.

The bundle does *not* grant execution authority.  It preserves the existing
signal-only certification boundary used by shadow attribution; the overseer
still requires explicit execution-lifecycle certification for automated entry.
"""
from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
from typing import Any

from scripts import certified_strategy_runtime_impl as _impl


ROOT = Path(__file__).resolve().parents[1]
DEPLOYMENT_BUNDLE_SCHEMA_VERSION = 1
DEPLOYMENT_BUNDLE_TYPE = "certified_runtime_deployment_bundle"
DEFAULT_DEPLOYMENT_BUNDLE_PATH = ROOT / "deploy" / "generated" / "certified-runtime-bundle.json"

_original_derive_runtime_identity = _impl.derive_runtime_identity
_original_runtime_certification = _impl.runtime_certification
_original_canary_selected = _impl.canary_selected


CertifiedRuntimeError = _impl.CertifiedRuntimeError
STRATEGY_SOURCE_PATHS = _impl.STRATEGY_SOURCE_PATHS
SUPPORTED_RUNTIME_EVALUATOR = _impl.SUPPORTED_RUNTIME_EVALUATOR


def _native_runtime_binary_path() -> Path:
    try:
        package = importlib.import_module("rust_core")
        if getattr(package, "RUST_CORE_AVAILABLE", False) is not True:
            raise CertifiedRuntimeError("compiled_rust_core_required")
        native = importlib.import_module("rust_core.rust_core")
    except (ImportError, ModuleNotFoundError) as exc:
        raise CertifiedRuntimeError("compiled_rust_core_required") from exc
    path = Path(getattr(native, "__file__", ""))
    if not path.is_file() or path.suffix.lower() not in {".so", ".pyd", ".dylib"}:
        raise CertifiedRuntimeError("compiled_rust_core_path_invalid")
    return path.resolve()


def runtime_binary_sha256() -> str:
    """Hash the exact native extension that owns the configured evaluator."""

    return _impl._sha256_file(_native_runtime_binary_path())


def strategy_source_manifest(*, root: Path = ROOT) -> dict[str, Any]:
    """Hash every deployed file whose drift invalidates research certification."""

    entries: list[dict[str, str]] = []
    root = Path(root).resolve()
    for relative in STRATEGY_SOURCE_PATHS:
        target = (root / relative).resolve()
        if not target.exists():
            raise CertifiedRuntimeError(f"certified_strategy_source_missing:{relative}")
        if target.is_file():
            files = [target]
        else:
            files = sorted(path for path in target.rglob("*") if path.is_file())
        if not files:
            raise CertifiedRuntimeError(f"certified_strategy_source_empty:{relative}")
        for path in files:
            try:
                relative_path = path.relative_to(root).as_posix()
            except ValueError as exc:
                raise CertifiedRuntimeError("certified_strategy_source_outside_root") from exc
            entries.append({
                "path": relative_path,
                "sha256": _impl._sha256_file(path),
            })
    entries.sort(key=lambda row: row["path"])
    return {
        "schema_version": 1,
        "paths": list(STRATEGY_SOURCE_PATHS),
        "files": entries,
    }


def strategy_source_manifest_hash(*, root: Path = ROOT) -> str:
    return _impl.stable_hash(strategy_source_manifest(root=root))


def _canonical_replay_execution_config(
    active_config: dict[str, Any], registry: dict[str, Any]
) -> dict[str, Any]:
    candidate_id = active_config.get("active_challenger_id")
    challengers = registry.get("challengers")
    if not isinstance(challengers, list):
        raise CertifiedRuntimeError("challenger_registry_invalid")
    challenger = next(
        (row for row in challengers if isinstance(row, dict) and row.get("id") == candidate_id),
        None,
    )
    replay = (
        challenger.get("alpha_validation_evidence", {}).get("replay_attestation")
        if isinstance(challenger, dict)
        else None
    )
    if not isinstance(replay, dict):
        raise CertifiedRuntimeError("replay_attestation_required_for_shadow_lifecycle")
    dataset = replay.get("dataset")
    if not isinstance(dataset, dict):
        raise CertifiedRuntimeError("replay_dataset_required_for_shadow_lifecycle")

    try:
        lifecycle = {
            "method": "canonical_replay_lifecycle_v1",
            "dataset_kind": str(dataset["kind"]),
            "dataset_symbol": str(dataset["symbol"]),
            "granularity_seconds": int(dataset["granularity"]),
            "warmup_bars": int(replay["warmup"]),
            "fee_bps": float(replay["fee_bps"]),
            "max_hold_bars": int(replay["max_hold_bars"]),
            "exit_on_opposite_signal": True,
            "close_open_trade_at_observation_end": True,
        }
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise CertifiedRuntimeError("replay_lifecycle_config_invalid") from exc

    if not lifecycle["dataset_symbol"]:
        raise CertifiedRuntimeError("replay_dataset_symbol_invalid")
    if lifecycle["granularity_seconds"] <= 0:
        raise CertifiedRuntimeError("replay_granularity_invalid")
    if lifecycle["warmup_bars"] < 0 or lifecycle["max_hold_bars"] < 0:
        raise CertifiedRuntimeError("replay_lifecycle_bars_invalid")
    if lifecycle["fee_bps"] < 0:
        raise CertifiedRuntimeError("replay_fee_bps_invalid")
    return lifecycle


def derive_runtime_identity(
    config: dict[str, Any], registry: dict[str, Any], **kwargs: Any
) -> dict[str, Any]:
    """Derive the proven strategy identity plus its canonical shadow lifecycle."""

    identity = _original_derive_runtime_identity(config, registry, **kwargs)
    lifecycle = _canonical_replay_execution_config(config, registry)
    return {
        **identity,
        "replay_execution_config": lifecycle,
        "replay_execution_config_hash": _impl.stable_hash(lifecycle),
    }


def _validate_identity_core(identity: Any) -> dict[str, Any]:
    if not isinstance(identity, dict):
        raise CertifiedRuntimeError("deployment_bundle_identity_required")
    required = {
        "schema_version", "candidate_id", "candidate_source_sha", "strategy_name",
        "strategy_config", "strategy_config_hash", "alpha_validation_evidence_hash",
        "terminal_holdout_evidence_hash", "replay_attestation_hash",
        "runtime_evaluator", "replay_execution_config", "replay_execution_config_hash",
    }
    missing = sorted(required - set(identity))
    if missing:
        raise CertifiedRuntimeError("deployment_bundle_identity_missing:" + ",".join(missing))
    source_sha = str(identity.get("candidate_source_sha") or "").lower()
    if len(source_sha) != 40 or any(ch not in "0123456789abcdef" for ch in source_sha):
        raise CertifiedRuntimeError("candidate_source_sha_invalid")
    if identity.get("runtime_evaluator") != SUPPORTED_RUNTIME_EVALUATOR:
        raise CertifiedRuntimeError("certified_runtime_evaluator_unsupported")
    strategy_name = str(identity.get("strategy_name") or "")
    strategy_config = _impl.normalize_strategy_config(strategy_name, identity.get("strategy_config"))
    if strategy_name != "rsi_revert" or not strategy_config:
        raise CertifiedRuntimeError("certified_runtime_strategy_unsupported")
    if strategy_config != identity.get("strategy_config"):
        raise CertifiedRuntimeError("deployment_bundle_strategy_config_not_normalized")
    if _impl.stable_hash(strategy_config) != identity.get("strategy_config_hash"):
        raise CertifiedRuntimeError("certified_runtime_strategy_config_hash_mismatch")
    lifecycle = identity.get("replay_execution_config")
    if not isinstance(lifecycle, dict):
        raise CertifiedRuntimeError("replay_lifecycle_identity_required")
    if lifecycle.get("method") != "canonical_replay_lifecycle_v1":
        raise CertifiedRuntimeError("replay_lifecycle_method_mismatch")
    if not str(lifecycle.get("dataset_symbol") or ""):
        raise CertifiedRuntimeError("replay_dataset_symbol_invalid")
    try:
        if int(lifecycle.get("granularity_seconds", 0)) <= 0:
            raise CertifiedRuntimeError("replay_granularity_invalid")
        if int(lifecycle.get("warmup_bars", -1)) < 0 or int(lifecycle.get("max_hold_bars", -1)) < 0:
            raise CertifiedRuntimeError("replay_lifecycle_bars_invalid")
        if float(lifecycle.get("fee_bps", -1.0)) < 0.0:
            raise CertifiedRuntimeError("replay_fee_bps_invalid")
    except (TypeError, ValueError, OverflowError) as exc:
        raise CertifiedRuntimeError("replay_lifecycle_config_invalid") from exc
    if lifecycle.get("exit_on_opposite_signal") is not True:
        raise CertifiedRuntimeError("replay_lifecycle_opposite_exit_required")
    if _impl.stable_hash(lifecycle) != identity.get("replay_execution_config_hash"):
        raise CertifiedRuntimeError("replay_lifecycle_identity_hash_mismatch")
    return dict(identity)


def validate_deployment_bundle(
    bundle: Any,
    *,
    root: Path = ROOT,
    binary_sha256: str | None = None,
) -> dict[str, Any]:
    """Validate a compact bundle against deployed source and native binary."""

    if not isinstance(bundle, dict):
        raise CertifiedRuntimeError("deployment_bundle_invalid")
    if bundle.get("schema_version") != DEPLOYMENT_BUNDLE_SCHEMA_VERSION:
        raise CertifiedRuntimeError("deployment_bundle_schema_mismatch")
    if bundle.get("bundle_type") != DEPLOYMENT_BUNDLE_TYPE:
        raise CertifiedRuntimeError("deployment_bundle_type_mismatch")
    supplied_hash = bundle.get("bundle_hash")
    if not isinstance(supplied_hash, str):
        raise CertifiedRuntimeError("deployment_bundle_hash_required")
    core = dict(bundle)
    core.pop("bundle_hash", None)
    if _impl.stable_hash(core) != supplied_hash:
        raise CertifiedRuntimeError("deployment_bundle_hash_mismatch")

    verification = bundle.get("verification")
    if not isinstance(verification, dict):
        raise CertifiedRuntimeError("deployment_bundle_verification_required")
    required_verifications = (
        "full_research_reverification",
        "canonical_replay_reverified",
        "terminal_holdout_reverified",
        "lineage_reverified",
        "source_ancestry_verified",
    )
    missing_verifications = [name for name in required_verifications if verification.get(name) is not True]
    if missing_verifications:
        raise CertifiedRuntimeError(
            "deployment_bundle_verification_incomplete:" + ",".join(missing_verifications)
        )

    expected_manifest = bundle.get("strategy_source_manifest")
    if not isinstance(expected_manifest, dict):
        raise CertifiedRuntimeError("deployment_bundle_source_manifest_required")
    expected_manifest_hash = bundle.get("strategy_source_manifest_hash")
    if _impl.stable_hash(expected_manifest) != expected_manifest_hash:
        raise CertifiedRuntimeError("deployment_bundle_source_manifest_hash_mismatch")
    actual_manifest = strategy_source_manifest(root=Path(root))
    if actual_manifest != expected_manifest:
        raise CertifiedRuntimeError("certified_strategy_source_drift")

    identity = _validate_identity_core(bundle.get("identity"))
    deployment = bundle.get("deployment")
    if not isinstance(deployment, dict) or deployment.get("mode") != "canary":
        raise CertifiedRuntimeError("certified_runtime_deployment_invalid")
    try:
        fraction = float(deployment.get("canary_fraction", 0.0))
    except (TypeError, ValueError, OverflowError) as exc:
        raise CertifiedRuntimeError("certified_runtime_deployment_invalid") from exc
    if not (0.0 < fraction <= 1.0):
        raise CertifiedRuntimeError("certified_runtime_deployment_invalid")

    identity.pop("runtime_binary_sha256", None)
    identity["runtime_binary_sha256"] = str(binary_sha256 or runtime_binary_sha256())
    if not identity["runtime_binary_sha256"]:
        raise CertifiedRuntimeError("runtime_binary_sha256_required")
    identity["deployment_bundle_hash"] = supplied_hash
    identity["strategy_source_manifest_hash"] = expected_manifest_hash
    return {
        "identity": identity,
        "runtime_identity_hash": _impl.stable_hash(identity),
        "deployment": "canary",
        "canary_fraction": fraction,
        "deployment_bundle_hash": supplied_hash,
    }


def load_deployment_runtime_context(
    path: Path | str = DEFAULT_DEPLOYMENT_BUNDLE_PATH,
    *,
    root: Path = ROOT,
    binary_sha256: str | None = None,
) -> dict[str, Any]:
    try:
        bundle = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise CertifiedRuntimeError("certified_runtime_deployment_bundle_missing") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise CertifiedRuntimeError("certified_runtime_deployment_bundle_invalid") from exc
    return validate_deployment_bundle(
        bundle,
        root=Path(root),
        binary_sha256=binary_sha256,
    )


def load_local_reverified_runtime_context(
    config_path: Path | str = _impl.ACTIVE_CONFIG_PATH,
    registry_path: Path | str = _impl.CHALLENGER_REGISTRY_PATH,
    **kwargs: Any,
) -> dict[str, Any]:
    """Force the full research/data/lineage/Git verification path."""

    return _impl.load_certified_runtime_context(config_path, registry_path, **kwargs)


def _resolved_deployment_bundle_path(explicit: Path | str | None = None) -> Path | None:
    if explicit is not None:
        return Path(explicit)
    configured = os.environ.get("CERTIFIED_RUNTIME_BUNDLE_PATH")
    if configured:
        return Path(configured)
    if DEFAULT_DEPLOYMENT_BUNDLE_PATH.exists():
        return DEFAULT_DEPLOYMENT_BUNDLE_PATH
    return None


def load_certified_runtime_context(
    config_path: Path | str = _impl.ACTIVE_CONFIG_PATH,
    registry_path: Path | str = _impl.CHALLENGER_REGISTRY_PATH,
    *,
    deployment_bundle_path: Path | str | None = None,
    binary_sha256: str | None = None,
    verify_source: bool = True,
    lineage: Any = None,
    root: Path = ROOT,
) -> dict[str, Any]:
    """Load a production bundle when present, else use full local verification."""

    bundle_path = _resolved_deployment_bundle_path(deployment_bundle_path)
    required = os.environ.get("CERTIFIED_RUNTIME_BUNDLE_REQUIRED", "").lower() in {
        "1", "true", "yes", "on"
    }
    if bundle_path is not None:
        if not bundle_path.exists():
            raise CertifiedRuntimeError("certified_runtime_deployment_bundle_missing")
        return load_deployment_runtime_context(
            bundle_path,
            root=Path(root),
            binary_sha256=binary_sha256,
        )
    if required:
        raise CertifiedRuntimeError("certified_runtime_deployment_bundle_required")
    return load_local_reverified_runtime_context(
        config_path,
        registry_path,
        binary_sha256=binary_sha256,
        verify_source=verify_source,
        lineage=lineage,
        root=Path(root),
    )


def load_certified_runtime_identity(
    config_path: Path | str = _impl.ACTIVE_CONFIG_PATH,
    registry_path: Path | str = _impl.CHALLENGER_REGISTRY_PATH,
    **kwargs: Any,
) -> dict[str, Any]:
    return load_certified_runtime_context(config_path, registry_path, **kwargs)["identity"]


def runtime_certification(identity: dict[str, Any]) -> dict[str, Any]:
    """Carry strategy identity, replay lifecycle and deployment binding downstream."""

    certification = _original_runtime_certification(identity)
    lifecycle = identity.get("replay_execution_config")
    lifecycle_hash = identity.get("replay_execution_config_hash")
    if not isinstance(lifecycle, dict) or not isinstance(lifecycle_hash, str):
        raise CertifiedRuntimeError("replay_lifecycle_identity_required")
    if _impl.stable_hash(lifecycle) != lifecycle_hash:
        raise CertifiedRuntimeError("replay_lifecycle_identity_hash_mismatch")
    output = {
        **certification,
        "replay_execution_config": lifecycle,
        "replay_execution_config_hash": lifecycle_hash,
    }
    if identity.get("deployment_bundle_hash"):
        output["deployment_bundle_hash"] = identity["deployment_bundle_hash"]
    if identity.get("strategy_source_manifest_hash"):
        output["strategy_source_manifest_hash"] = identity["strategy_source_manifest_hash"]
    return output


def canary_selected(identity: dict[str, Any], key: str, *, fraction: float) -> bool:
    """Route certified canary traffic only inside the replayed evidence domain."""

    lifecycle = identity.get("replay_execution_config")
    if not isinstance(lifecycle, dict):
        raise CertifiedRuntimeError("replay_lifecycle_identity_required")
    certified_symbol = str(lifecycle.get("dataset_symbol") or "")
    if not certified_symbol:
        raise CertifiedRuntimeError("replay_dataset_symbol_invalid")
    if str(key) != certified_symbol:
        return False
    return _original_canary_selected(identity, key, fraction=fraction)


# Functions defined in the implementation resolve globals from that module.
# Patch those seams before re-exporting the public surface so all local/full
# reverification callers use the strengthened identity automatically.
_impl.runtime_binary_sha256 = runtime_binary_sha256
_impl.derive_runtime_identity = derive_runtime_identity
_impl.runtime_certification = runtime_certification
_impl.canary_selected = canary_selected

for _name in dir(_impl):
    if not _name.startswith("__"):
        globals().setdefault(_name, getattr(_impl, _name))

# Ensure strengthened public wrappers win over implementation originals.
globals()["runtime_binary_sha256"] = runtime_binary_sha256
globals()["derive_runtime_identity"] = derive_runtime_identity
globals()["runtime_certification"] = runtime_certification
globals()["canary_selected"] = canary_selected
globals()["load_certified_runtime_context"] = load_certified_runtime_context
globals()["load_certified_runtime_identity"] = load_certified_runtime_identity
