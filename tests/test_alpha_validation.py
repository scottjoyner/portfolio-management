from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts.alpha_validation import (
    ValidationPolicy,
    build_alpha_validation_evidence,
    generate_walk_forward_splits,
    stable_hash,
    verify_alpha_validation_evidence,
)
from scripts.challenger_manager import ChallengerRegistry, evaluate_challenger_evidence


GOOD_FOLDS = [
    [0.020, 0.015, -0.003, 0.010, 0.012, -0.002, 0.008, 0.007, 0.006, -0.002],
    [0.018, 0.013, -0.004, 0.011, 0.009, -0.002, 0.007, 0.006, 0.005, -0.002],
    [0.021, 0.014, -0.003, 0.009, 0.010, -0.002, 0.008, 0.007, 0.006, -0.001],
]


def _evidence(candidate_id: str = "challenger-test") -> dict:
    return build_alpha_validation_evidence(
        candidate_id=candidate_id,
        candidate_source_sha="a" * 40,
        candidate_config={"lookback": 20, "threshold": 0.7},
        dataset_id="coinbase-btc-hourly-2024-2026",
        dataset_hash="b" * 64,
        fold_returns=GOOD_FOLDS,
        net_pnl_after_cost_usd=125.0,
        cost_coverage_ratio=2.5,
        regimes_tested=["trend_up", "trend_down", "range"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        bootstrap_samples=300,
        bootstrap_seed=7,
        created_at="2026-09-10T12:00:00+00:00",
    )


def _fixture_replay_bound(evidence: dict) -> dict:
    """Bind registry-mechanics fixtures without pretending to test real replay.

    The real canonical replay verifier is exercised in test_canonical_replay.py.
    These registry tests only need an alpha artifact that survives re-hashing so
    they can focus on persistence/promotion mechanics while the verifier call is
    explicitly monkeypatched to success.
    """
    bound = copy.deepcopy(evidence)
    bound.pop("evidence_hash", None)
    bound["replay_attestation"] = {
        "fixture": "registry-mechanics-only",
        "execution_config_bound": True,
        "strategy_config": copy.deepcopy(bound["candidate_config"]),
    }
    bound["replay_provenance_bound"] = True
    bound["evidence_hash"] = stable_hash(bound)
    valid, reasons = verify_alpha_validation_evidence(bound)
    assert valid, reasons
    return bound


def test_stable_hash_is_order_independent():
    assert stable_hash({"b": 2, "a": 1}) == stable_hash({"a": 1, "b": 2})


def test_walk_forward_boundaries_purge_and_embargo_are_disjoint():
    folds = generate_walk_forward_splits(
        50,
        train_size=20,
        test_size=5,
        purge_size=2,
        embargo_size=1,
        expanding=True,
    )
    assert folds
    first = folds[0]
    assert (first.train_start, first.train_end) == (0, 20)
    assert (first.purge_start, first.purge_end) == (20, 22)
    assert (first.embargo_start, first.embargo_end) == (22, 23)
    assert (first.test_start, first.test_end) == (23, 28)
    for fold in folds:
        assert fold.train_end <= fold.purge_start <= fold.purge_end
        assert fold.purge_end <= fold.embargo_start <= fold.embargo_end
        assert fold.embargo_end <= fold.test_start < fold.test_end
        assert fold.train_size >= 20


def test_rolling_walk_forward_keeps_fixed_training_window():
    folds = generate_walk_forward_splits(
        60,
        train_size=20,
        test_size=5,
        step_size=5,
        purge_size=2,
        embargo_size=1,
        expanding=False,
    )
    assert len(folds) >= 2
    assert all(fold.train_size == 20 for fold in folds)
    assert folds[1].train_start > folds[0].train_start


def test_generated_evidence_is_self_hashing_and_passes_policy():
    evidence = _evidence()
    valid, reasons = verify_alpha_validation_evidence(evidence)
    assert valid, reasons
    assert evidence["walk_forward_passed"] is True
    assert evidence["trade_count"] == 30
    assert evidence["out_of_sample_trades"] == 30
    assert evidence["regimes_tested"] == 3
    assert evidence["bootstrap_positive_fraction"] >= 0.55
    assert evidence["probability_of_ruin"] <= 0.05
    assert evidence["evidence_hash"] == stable_hash({k: v for k, v in evidence.items() if k != "evidence_hash"})


def test_tampered_evidence_fails_integrity_check():
    evidence = _evidence()
    tampered = copy.deepcopy(evidence)
    tampered["net_pnl_after_cost_usd"] = 999999.0
    valid, reasons = verify_alpha_validation_evidence(tampered)
    assert not valid
    assert "evidence_hash_mismatch" in reasons


def test_required_cost_stress_can_fail_walk_forward_evidence():
    weak_folds = [[0.001, 0.001, -0.0005, 0.001, 0.001] for _ in range(3)]
    evidence = build_alpha_validation_evidence(
        candidate_id="weak",
        candidate_source_sha="c" * 40,
        candidate_config={"x": 1},
        dataset_id="fixture",
        dataset_hash="d" * 64,
        fold_returns=weak_folds,
        net_pnl_after_cost_usd=10.0,
        cost_coverage_ratio=2.0,
        regimes_tested=["up", "down", "range"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        policy=ValidationPolicy(min_out_of_sample_trades=10),
        bootstrap_samples=100,
        bootstrap_seed=3,
        created_at="2026-09-10T12:00:00+00:00",
    )
    assert evidence["walk_forward_passed"] is False
    assert "required_cost_stress_unprofitable" in evidence["fail_reasons"]


def test_challenger_metric_gate_consumes_verified_evidence():
    evidence = _evidence()
    result = evaluate_challenger_evidence(
        {"net_pnl_after_cost_usd": 0.0, "max_drawdown_pct": 0.0},
        evidence,
    )
    assert result["approved"] is True, result["reasons"]
    assert result["evidence_hash"] == evidence["evidence_hash"]
    assert result["challenger_metrics"]["out_of_sample_trades"] == 30


def test_challenger_metric_gate_rejects_tampered_evidence():
    evidence = _evidence()
    evidence["profit_factor"] = 999.0
    result = evaluate_challenger_evidence(
        {"net_pnl_after_cost_usd": 0.0, "max_drawdown_pct": 0.0},
        evidence,
    )
    assert result["approved"] is False
    assert any(reason.startswith("alpha_validation_evidence_invalid:") for reason in result["reasons"])


class FakeLineage:
    def __init__(self):
        self.rows = []

    def append(self, event_type, payload, *, actor, parents):
        row = {
            "id": f"lineage-{len(self.rows) + 1}",
            "event_type": event_type,
            "payload": payload,
            "actor": actor,
            "parents": parents,
        }
        self.rows.append(row)
        return row


def _registry(tmp_path: Path) -> tuple[ChallengerRegistry, dict]:
    registry = ChallengerRegistry(
        registry_path=tmp_path / "challengers.json",
        active_config_path=tmp_path / "active.json",
        lineage=FakeLineage(),
    )
    challenger = registry.propose(
        {"lookback": 20, "threshold": 0.7},
        rationale="test",
        model_request_id="model-request-1",
    )
    return registry, challenger


def test_registry_fails_closed_without_alpha_evidence(tmp_path):
    registry, challenger = _registry(tmp_path)
    legacy_metrics = {
        "total_trades": 1000,
        "out_of_sample_trades": 1000,
        "regimes_tested": 10,
        "profit_factor": 99,
        "cost_coverage_ratio": 99,
        "net_pnl_after_cost_usd": 1_000_000,
        "max_drawdown_pct": 0,
        "walk_forward_passed": True,
        "accounting_invariants_ok": True,
        "lineage_verified": True,
    }
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        legacy_metrics,
    )
    assert result["approved"] is False
    assert result["reasons"] == ["alpha_validation_evidence_required"]
    with pytest.raises(ValueError, match="promotion gate"):
        registry.promote(challenger["id"])


def test_registry_rejects_non_object_evidence(tmp_path):
    registry, challenger = _registry(tmp_path)
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=["not", "evidence"],
    )
    assert result["approved"] is False
    assert "alpha_validation_evidence_invalid:evidence_not_object" in result["reasons"]


def test_registry_rejects_evidence_for_different_candidate(tmp_path):
    registry, challenger = _registry(tmp_path)
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=_evidence("some-other-challenger"),
    )
    assert result["approved"] is False
    assert result["reasons"] == ["alpha_validation_candidate_mismatch"]


def test_registry_rejects_evidence_for_different_candidate_config(tmp_path):
    registry, challenger = _registry(tmp_path)
    evidence = _evidence(challenger["id"])
    evidence["candidate_config"] = {"lookback": 99, "threshold": 0.7}
    evidence["candidate_config_hash"] = stable_hash(evidence["candidate_config"])
    evidence.pop("evidence_hash", None)
    evidence["evidence_hash"] = stable_hash(evidence)
    valid, reasons = verify_alpha_validation_evidence(evidence)
    assert valid, reasons

    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=evidence,
    )
    assert result["approved"] is False
    assert result["reasons"] == ["alpha_validation_candidate_config_mismatch"]


def test_registry_rejects_unbound_alpha_evidence(tmp_path):
    registry, challenger = _registry(tmp_path)
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=_evidence(challenger["id"]),
    )
    assert result["approved"] is False
    assert result["reasons"] == ["alpha_validation_replay_provenance_required"]


def test_registry_rejects_replay_without_executable_config_binding(tmp_path, monkeypatch):
    registry, challenger = _registry(tmp_path)
    evidence = _fixture_replay_bound(_evidence(challenger["id"]))
    evidence["replay_attestation"]["execution_config_bound"] = False
    evidence.pop("evidence_hash", None)
    evidence["evidence_hash"] = stable_hash(evidence)
    monkeypatch.setattr(
        "scripts.challenger_manager.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=evidence,
    )
    assert result["approved"] is False
    assert result["reasons"] == ["alpha_validation_execution_config_binding_required"]


def test_registry_rejects_attested_execution_config_mismatch(tmp_path, monkeypatch):
    registry, challenger = _registry(tmp_path)
    evidence = _fixture_replay_bound(_evidence(challenger["id"]))
    evidence["replay_attestation"]["strategy_config"] = {"lookback": 99, "threshold": 0.7}
    evidence.pop("evidence_hash", None)
    evidence["evidence_hash"] = stable_hash(evidence)
    monkeypatch.setattr(
        "scripts.challenger_manager.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=evidence,
    )
    assert result["approved"] is False
    assert result["reasons"] == ["alpha_validation_execution_config_mismatch"]


def test_registry_canary_carries_verified_evidence_hash(tmp_path, monkeypatch):
    registry, challenger = _registry(tmp_path)
    evidence = _fixture_replay_bound(_evidence(challenger["id"]))
    monkeypatch.setattr(
        "scripts.challenger_manager.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=evidence,
    )
    assert result["approved"] is True, result["reasons"]
    stored = registry.load()["challengers"][0]
    assert stored["alpha_validation_evidence"] == evidence
    assert stored["alpha_validation_evidence_hash"] == evidence["evidence_hash"]

    config = registry.promote(challenger["id"], canary_fraction=0.05)
    assert config["deployment"] == "canary"
    assert config["canary_fraction"] == 0.05
    assert config["alpha_validation_evidence_hash"] == evidence["evidence_hash"]
    persisted = json.loads((tmp_path / "active.json").read_text())
    assert persisted["alpha_validation_evidence_hash"] == evidence["evidence_hash"]


def test_registry_reverifies_persisted_evidence_at_promotion(tmp_path, monkeypatch):
    registry, challenger = _registry(tmp_path)
    evidence = _fixture_replay_bound(_evidence(challenger["id"]))
    monkeypatch.setattr(
        "scripts.challenger_manager.verify_evidence_replay_binding",
        lambda evidence, reverify_source=True: (True, []),
    )
    result = registry.evaluate(
        challenger["id"],
        {"net_pnl_after_cost_usd": 0, "max_drawdown_pct": 0},
        validation_evidence=evidence,
    )
    assert result["approved"] is True

    state = registry.load()
    state["challengers"][0]["alpha_validation_evidence"]["net_pnl_after_cost_usd"] = 9_999_999.0
    registry.save(state)

    with pytest.raises(ValueError, match="evidence is invalid"):
        registry.promote(challenger["id"])
