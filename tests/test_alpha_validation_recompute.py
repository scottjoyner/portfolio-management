from __future__ import annotations

import copy

from scripts.alpha_validation import (
    build_alpha_validation_evidence,
    stable_hash,
    verify_alpha_validation_evidence,
)


FOLDS = [
    [0.020, 0.015, -0.003, 0.010, 0.012, -0.002, 0.008, 0.007, 0.006, -0.002],
    [0.018, 0.013, -0.004, 0.011, 0.009, -0.002, 0.007, 0.006, 0.005, -0.002],
    [0.021, 0.014, -0.003, 0.009, 0.010, -0.002, 0.008, 0.007, 0.006, -0.001],
]


def _evidence() -> dict:
    return build_alpha_validation_evidence(
        candidate_id="challenger-recompute",
        candidate_source_sha="a" * 40,
        candidate_config={"lookback": 20, "threshold": 0.7},
        dataset_id="fixture",
        dataset_hash="b" * 64,
        fold_returns=FOLDS,
        net_pnl_after_cost_usd=125.0,
        cost_coverage_ratio=2.5,
        regimes_tested=["trend_up", "trend_down", "range"],
        accounting_invariants_ok=True,
        lineage_verified=True,
        parameter_stability_score=0.9,
        bootstrap_samples=200,
        bootstrap_seed=7,
        created_at="2026-09-10T12:00:00+00:00",
    )


def _rehash(payload: dict) -> None:
    payload.pop("evidence_hash", None)
    payload["evidence_hash"] = stable_hash(payload)


def test_verifier_recomputes_reported_profit_factor_even_after_rehash():
    evidence = _evidence()
    forged = copy.deepcopy(evidence)
    forged["profit_factor"] = 999.0
    _rehash(forged)

    valid, reasons = verify_alpha_validation_evidence(forged)
    assert not valid
    assert "derived_metric_mismatch:profit_factor" in reasons


def test_verifier_recomputes_pass_boolean_even_after_rehash():
    evidence = _evidence()
    forged = copy.deepcopy(evidence)
    forged["walk_forward_passed"] = False
    _rehash(forged)

    valid, reasons = verify_alpha_validation_evidence(forged)
    assert not valid
    assert "derived_metric_mismatch:walk_forward_passed" in reasons


def test_candidate_config_hash_is_recomputed_even_after_rehash():
    evidence = _evidence()
    forged = copy.deepcopy(evidence)
    forged["candidate_config"]["lookback"] = 999
    _rehash(forged)

    valid, reasons = verify_alpha_validation_evidence(forged)
    assert not valid
    assert "candidate_config_hash_mismatch" in reasons


def test_raw_oos_return_edit_requires_all_statistics_to_be_recomputed():
    evidence = _evidence()
    forged = copy.deepcopy(evidence)
    forged["fold_returns"][0][0] = -0.50
    _rehash(forged)

    valid, reasons = verify_alpha_validation_evidence(forged)
    assert not valid
    assert any(reason.startswith("derived_metric_mismatch:") for reason in reasons)
