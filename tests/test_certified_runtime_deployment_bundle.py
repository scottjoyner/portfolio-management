from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts.alpha_validation import stable_hash
from scripts.certified_strategy_runtime import (
    CertifiedRuntimeError,
    DEPLOYMENT_BUNDLE_SCHEMA_VERSION,
    DEPLOYMENT_BUNDLE_TYPE,
    strategy_source_manifest,
    validate_deployment_bundle,
)


STRATEGY_CONFIG = {"period": 14, "oversold": 30.0, "overbought": 70.0}
LIFECYCLE = {
    "method": "canonical_replay_lifecycle_v1",
    "dataset_kind": "spot",
    "dataset_symbol": "BTC-USD",
    "granularity_seconds": 3600,
    "warmup_bars": 30,
    "fee_bps": 7.5,
    "max_hold_bars": 12,
    "exit_on_opposite_signal": True,
    "close_open_trade_at_observation_end": True,
}


def _root(tmp_path: Path) -> Path:
    (tmp_path / "rust_core" / "src").mkdir(parents=True)
    (tmp_path / "scripts" / "backtest_framework").mkdir(parents=True)
    (tmp_path / "rust_core" / "src" / "lib.rs").write_text("// certified rust source\n", encoding="utf-8")
    (tmp_path / "scripts" / "backtest_framework" / "canonical_replay.py").write_text(
        "# certified replay source\n", encoding="utf-8"
    )
    return tmp_path


def _bundle(root: Path) -> dict:
    manifest = strategy_source_manifest(root=root)
    identity = {
        "schema_version": 1,
        "candidate_id": "challenger-runtime-deploy",
        "candidate_source_sha": "a" * 40,
        "strategy_name": "rsi_revert",
        "strategy_config": copy.deepcopy(STRATEGY_CONFIG),
        "strategy_config_hash": stable_hash(STRATEGY_CONFIG),
        "alpha_validation_evidence_hash": "b" * 64,
        "terminal_holdout_evidence_hash": "c" * 64,
        "replay_attestation_hash": "d" * 64,
        "promotion_lineage_id": "lin-promotion-1",
        "runtime_evaluator": "rust_core.run_rsi_revert_opens_configured_py",
        "replay_execution_config": copy.deepcopy(LIFECYCLE),
        "replay_execution_config_hash": stable_hash(LIFECYCLE),
    }
    core = {
        "schema_version": DEPLOYMENT_BUNDLE_SCHEMA_VERSION,
        "bundle_type": DEPLOYMENT_BUNDLE_TYPE,
        "generated_at": "2026-09-12T20:15:00+00:00",
        "identity": identity,
        "deployment": {"mode": "canary", "canary_fraction": 0.25},
        "strategy_source_manifest": manifest,
        "strategy_source_manifest_hash": stable_hash(manifest),
        "verification": {
            "full_research_reverification": True,
            "canonical_replay_reverified": True,
            "terminal_holdout_reverified": True,
            "lineage_reverified": True,
            "source_ancestry_verified": True,
            "host_native_runtime_sha256": "e" * 64,
        },
    }
    return {**core, "bundle_hash": stable_hash(core)}


def _rehash(bundle: dict) -> None:
    core = dict(bundle)
    core.pop("bundle_hash", None)
    bundle["bundle_hash"] = stable_hash(core)


def test_deployment_bundle_binds_deployed_source_and_runtime_binary(tmp_path):
    root = _root(tmp_path)
    bundle = _bundle(root)

    context = validate_deployment_bundle(bundle, root=root, binary_sha256="f" * 64)

    assert context["deployment"] == "canary"
    assert context["canary_fraction"] == 0.25
    assert context["deployment_bundle_hash"] == bundle["bundle_hash"]
    assert context["identity"]["runtime_binary_sha256"] == "f" * 64
    assert context["identity"]["deployment_bundle_hash"] == bundle["bundle_hash"]
    assert context["identity"]["strategy_source_manifest_hash"] == bundle["strategy_source_manifest_hash"]
    assert context["runtime_identity_hash"] == stable_hash(context["identity"])


def test_deployment_bundle_rejects_bundle_tampering_even_when_source_is_unchanged(tmp_path):
    root = _root(tmp_path)
    bundle = _bundle(root)
    bundle["deployment"]["canary_fraction"] = 0.75

    with pytest.raises(CertifiedRuntimeError, match="deployment_bundle_hash_mismatch"):
        validate_deployment_bundle(bundle, root=root, binary_sha256="f" * 64)


def test_deployment_bundle_rejects_strategy_source_drift_after_valid_rehash(tmp_path):
    root = _root(tmp_path)
    bundle = _bundle(root)
    (root / "rust_core" / "src" / "lib.rs").write_text("// drifted source\n", encoding="utf-8")

    with pytest.raises(CertifiedRuntimeError, match="certified_strategy_source_drift"):
        validate_deployment_bundle(bundle, root=root, binary_sha256="f" * 64)


def test_deployment_bundle_rejects_missing_full_reverification_claim(tmp_path):
    root = _root(tmp_path)
    bundle = _bundle(root)
    bundle["verification"]["terminal_holdout_reverified"] = False
    _rehash(bundle)

    with pytest.raises(CertifiedRuntimeError, match="deployment_bundle_verification_incomplete"):
        validate_deployment_bundle(bundle, root=root, binary_sha256="f" * 64)


def test_deployment_bundle_rejects_cross_market_lifecycle_tamper_even_if_rehashed(tmp_path):
    root = _root(tmp_path)
    bundle = _bundle(root)
    bundle["identity"]["replay_execution_config"]["dataset_symbol"] = "ETH-USD"
    _rehash(bundle)

    with pytest.raises(CertifiedRuntimeError, match="replay_lifecycle_identity_hash_mismatch"):
        validate_deployment_bundle(bundle, root=root, binary_sha256="f" * 64)


def test_bundle_is_json_canonicalizable(tmp_path):
    root = _root(tmp_path)
    bundle = _bundle(root)
    encoded = json.dumps(bundle, sort_keys=True, allow_nan=False)
    assert bundle["bundle_hash"] in encoded
