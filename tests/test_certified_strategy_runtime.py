from __future__ import annotations

import copy

import pytest

from scripts.alpha_validation import stable_hash
from scripts.certified_strategy_runtime import (
    CertifiedRuntimeError,
    canary_selected,
    derive_runtime_identity,
    runtime_certification,
)


CANDIDATE_ID = "challenger-certified-runtime"
SOURCE_SHA = "a" * 40
STRATEGY_CONFIG = {"period": 14, "oversold": 30.0, "overbought": 70.0}


def _fixture() -> tuple[dict, dict]:
    replay = {
        "execution_config_bound": True,
        "strategy_name": "rsi_revert",
        "strategy_config": copy.deepcopy(STRATEGY_CONFIG),
        "strategy_config_hash": stable_hash(STRATEGY_CONFIG),
        "attestation_hash": "replay-attestation-1",
    }
    alpha = {
        "candidate_id": CANDIDATE_ID,
        "candidate_source_sha": SOURCE_SHA,
        "candidate_config": copy.deepcopy(STRATEGY_CONFIG),
        "replay_provenance_bound": True,
        "replay_attestation": replay,
        "evidence_hash": "alpha-evidence-1",
    }
    terminal = {
        "candidate_id": CANDIDATE_ID,
        "candidate_source_sha": SOURCE_SHA,
        "candidate_config": copy.deepcopy(STRATEGY_CONFIG),
        "strategy_name": "rsi_revert",
        "strategy_config": copy.deepcopy(STRATEGY_CONFIG),
        "strategy_config_hash": stable_hash(STRATEGY_CONFIG),
        "alpha_validation_evidence_hash": alpha["evidence_hash"],
        "passed": True,
        "evidence_hash": "terminal-evidence-1",
    }
    challenger = {
        "id": CANDIDATE_ID,
        "status": "canary",
        "parameters": copy.deepcopy(STRATEGY_CONFIG),
        "evaluation": {
            "approved": True,
            "evidence_hash": alpha["evidence_hash"],
            "terminal_holdout_evidence_hash": terminal["evidence_hash"],
            "terminal_holdout_verified": True,
        },
        "alpha_validation_evidence_hash": alpha["evidence_hash"],
        "alpha_validation_evidence": alpha,
        "terminal_holdout_evidence_hash": terminal["evidence_hash"],
        "terminal_holdout_evidence": terminal,
    }
    registry = {"schema_version": 1, "incumbent_id": CANDIDATE_ID, "challengers": [challenger]}
    config = {
        "schema_version": 1,
        "active_challenger_id": CANDIDATE_ID,
        "parameters": copy.deepcopy(STRATEGY_CONFIG),
        "deployment": "canary",
        "canary_fraction": 0.25,
        "alpha_validation_evidence_hash": alpha["evidence_hash"],
        "terminal_holdout_evidence_hash": terminal["evidence_hash"],
        "promotion_lineage_id": "promotion-lineage-1",
    }
    return config, registry


def _derive(config: dict, registry: dict):
    source_calls: list[tuple[str, object]] = []

    def source_verifier(source_sha, *, root):
        source_calls.append((source_sha, root))

    identity = derive_runtime_identity(
        config,
        registry,
        binary_sha256="b" * 64,
        alpha_verifier=lambda evidence: (True, []),
        replay_verifier=lambda evidence, reverify_source=True: (True, []),
        terminal_verifier=lambda evidence, lineage=None, reverify_source=True: (True, []),
        source_verifier=source_verifier,
        lineage=None,
        verify_source=True,
    )
    return identity, source_calls


def test_runtime_identity_is_derived_from_promoted_evidence():
    config, registry = _fixture()
    identity, source_calls = _derive(config, registry)

    assert identity["candidate_id"] == CANDIDATE_ID
    assert identity["candidate_source_sha"] == SOURCE_SHA
    assert identity["strategy_name"] == "rsi_revert"
    assert identity["strategy_config"] == STRATEGY_CONFIG
    assert identity["strategy_config_hash"] == stable_hash(STRATEGY_CONFIG)
    assert identity["runtime_evaluator"] == "rust_core.run_rsi_revert_opens_configured_py"
    assert identity["runtime_binary_sha256"] == "b" * 64
    assert source_calls and source_calls[0][0] == SOURCE_SHA

    carried = runtime_certification(identity)
    assert carried["certified"] is True
    assert carried["runtime_identity_hash"] == stable_hash(identity)
    assert carried["alpha_validation_evidence_hash"] == "alpha-evidence-1"
    assert carried["terminal_holdout_evidence_hash"] == "terminal-evidence-1"


def test_runtime_identity_rejects_active_config_evidence_drift():
    config, registry = _fixture()
    config["alpha_validation_evidence_hash"] = "tampered"
    with pytest.raises(CertifiedRuntimeError, match="active_config_alpha_evidence_mismatch"):
        _derive(config, registry)


def test_runtime_identity_rejects_candidate_parameter_drift():
    config, registry = _fixture()
    registry["challengers"][0]["parameters"]["oversold"] = 25.0
    with pytest.raises(CertifiedRuntimeError, match="challenger_runtime_config_mismatch"):
        _derive(config, registry)


def test_runtime_identity_rejects_failed_replay_reverification():
    config, registry = _fixture()
    with pytest.raises(CertifiedRuntimeError, match="canonical_replay_verification_failed:drift"):
        derive_runtime_identity(
            config,
            registry,
            binary_sha256="b" * 64,
            alpha_verifier=lambda evidence: (True, []),
            replay_verifier=lambda evidence, reverify_source=True: (False, ["drift"]),
            terminal_verifier=lambda evidence, lineage=None, reverify_source=True: (True, []),
            source_verifier=lambda source_sha, root: None,
            lineage=None,
            verify_source=True,
        )


def test_canary_selection_is_deterministic_and_bounded():
    config, registry = _fixture()
    identity, _ = _derive(config, registry)
    first = canary_selected(identity, "BTC-USD", fraction=0.25)
    assert canary_selected(identity, "BTC-USD", fraction=0.25) is first
    assert canary_selected(identity, "BTC-USD", fraction=1.0) is True
    with pytest.raises(CertifiedRuntimeError, match="canary_fraction_invalid"):
        canary_selected(identity, "BTC-USD", fraction=0.0)
