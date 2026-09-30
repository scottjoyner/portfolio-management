#!/usr/bin/env python3
"""Public certified-runtime API with native binary and replay-lifecycle binding.

The implementation lives in ``certified_strategy_runtime_impl`` so this module
can add deployment-time bindings without disturbing the already-certified
research implementation:

* hash the actual PyO3 extension loaded by the process, not the Python package
  that merely re-exports it;
* carry the canonical replay execution semantics (bar granularity, warmup,
  round-trip fee assumption and maximum hold) into the immutable runtime
  identity used by shadow attribution;
* constrain certified canary routing to the exact market symbol represented by
  the replay evidence instead of treating certification as cross-market proof.

The replay lifecycle remains *shadow evidence*, not automated execution
authority.  The overseer still requires explicit execution-lifecycle
certification before an automated entry can pass.
"""
from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from scripts import certified_strategy_runtime_impl as _impl


_original_derive_runtime_identity = _impl.derive_runtime_identity
_original_runtime_certification = _impl.runtime_certification
_original_canary_selected = _impl.canary_selected


def _native_runtime_binary_path() -> Path:
    try:
        package = importlib.import_module("rust_core")
        if getattr(package, "RUST_CORE_AVAILABLE", False) is not True:
            raise _impl.CertifiedRuntimeError("compiled_rust_core_required")
        native = importlib.import_module("rust_core.rust_core")
    except (ImportError, ModuleNotFoundError) as exc:
        raise _impl.CertifiedRuntimeError("compiled_rust_core_required") from exc
    path = Path(getattr(native, "__file__", ""))
    if not path.is_file() or path.suffix.lower() not in {".so", ".pyd", ".dylib"}:
        raise _impl.CertifiedRuntimeError("compiled_rust_core_path_invalid")
    return path.resolve()


def runtime_binary_sha256() -> str:
    """Hash the exact native extension that owns the configured evaluator."""

    return _impl._sha256_file(_native_runtime_binary_path())


def _canonical_replay_execution_config(
    active_config: dict[str, Any], registry: dict[str, Any]
) -> dict[str, Any]:
    candidate_id = active_config.get("active_challenger_id")
    challengers = registry.get("challengers")
    if not isinstance(challengers, list):
        raise _impl.CertifiedRuntimeError("challenger_registry_invalid")
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
        raise _impl.CertifiedRuntimeError("replay_attestation_required_for_shadow_lifecycle")
    dataset = replay.get("dataset")
    if not isinstance(dataset, dict):
        raise _impl.CertifiedRuntimeError("replay_dataset_required_for_shadow_lifecycle")

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
        raise _impl.CertifiedRuntimeError("replay_lifecycle_config_invalid") from exc

    if not lifecycle["dataset_symbol"]:
        raise _impl.CertifiedRuntimeError("replay_dataset_symbol_invalid")
    if lifecycle["granularity_seconds"] <= 0:
        raise _impl.CertifiedRuntimeError("replay_granularity_invalid")
    if lifecycle["warmup_bars"] < 0 or lifecycle["max_hold_bars"] < 0:
        raise _impl.CertifiedRuntimeError("replay_lifecycle_bars_invalid")
    if lifecycle["fee_bps"] < 0:
        raise _impl.CertifiedRuntimeError("replay_fee_bps_invalid")
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


def runtime_certification(identity: dict[str, Any]) -> dict[str, Any]:
    """Carry strategy identity and canonical replay lifecycle downstream."""

    certification = _original_runtime_certification(identity)
    lifecycle = identity.get("replay_execution_config")
    lifecycle_hash = identity.get("replay_execution_config_hash")
    if not isinstance(lifecycle, dict) or not isinstance(lifecycle_hash, str):
        raise _impl.CertifiedRuntimeError("replay_lifecycle_identity_required")
    if _impl.stable_hash(lifecycle) != lifecycle_hash:
        raise _impl.CertifiedRuntimeError("replay_lifecycle_identity_hash_mismatch")
    return {
        **certification,
        "replay_execution_config": lifecycle,
        "replay_execution_config_hash": lifecycle_hash,
    }


def canary_selected(identity: dict[str, Any], key: str, *, fraction: float) -> bool:
    """Route certified canary traffic only inside the replayed evidence domain."""

    lifecycle = identity.get("replay_execution_config")
    if not isinstance(lifecycle, dict):
        raise _impl.CertifiedRuntimeError("replay_lifecycle_identity_required")
    certified_symbol = str(lifecycle.get("dataset_symbol") or "")
    if not certified_symbol:
        raise _impl.CertifiedRuntimeError("replay_dataset_symbol_invalid")
    if str(key) != certified_symbol:
        return False
    return _original_canary_selected(identity, key, fraction=fraction)


# Functions defined in the implementation resolve globals from that module.
# Patch those seams before re-exporting the public surface so all callers,
# including load_certified_runtime_context(), scanner dispatch and
# run_certified_strategy(), use the strengthened identity automatically.
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
