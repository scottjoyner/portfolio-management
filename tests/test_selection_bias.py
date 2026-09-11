from __future__ import annotations

import copy

import pytest

from scripts.selection_bias import (
    SearchMultiplicityPolicy,
    assess_candidate_significance,
    exact_one_sided_sign_test,
    flatten_fold_returns,
    policy_from_artifact,
    verify_candidate_significance,
)


def test_policy_commits_full_search_budget_and_per_trial_alpha():
    policy = SearchMultiplicityPolicy(
        max_candidate_trials=20,
        familywise_alpha=0.05,
        min_nonzero_trades=10,
    )
    artifact = policy.artifact()
    assert artifact["max_candidate_trials"] == 20
    assert artifact["familywise_alpha"] == 0.05
    assert artifact["per_trial_alpha"] == 0.0025
    assert policy_from_artifact(artifact) == policy


def test_exact_sign_test_known_tail_probability():
    result = exact_one_sided_sign_test([1.0] * 10)
    assert result["positive_trades"] == 10
    assert result["negative_trades"] == 0
    assert result["nonzero_trades"] == 10
    assert result["raw_p_value"] == pytest.approx(1.0 / 1024.0)


def test_sign_test_ignores_zero_returns_in_effective_n():
    result = exact_one_sided_sign_test([1.0, 1.0, -1.0, 0.0, 0.0])
    assert result["positive_trades"] == 2
    assert result["negative_trades"] == 1
    assert result["zero_trades"] == 2
    assert result["nonzero_trades"] == 3
    assert result["raw_p_value"] == pytest.approx(0.5)


def test_bonferroni_uses_committed_budget_not_realized_trial_count():
    returns = [[0.01] * 12]
    small_family = SearchMultiplicityPolicy(
        max_candidate_trials=5,
        familywise_alpha=0.05,
        min_nonzero_trades=10,
    )
    large_family = SearchMultiplicityPolicy(
        max_candidate_trials=500,
        familywise_alpha=0.05,
        min_nonzero_trades=10,
    )

    small = assess_candidate_significance(returns, small_family)
    large = assess_candidate_significance(returns, large_family)

    assert small["raw_p_value"] == large["raw_p_value"]
    assert small["policy"]["per_trial_alpha"] == pytest.approx(0.01)
    assert large["policy"]["per_trial_alpha"] == pytest.approx(0.0001)
    assert small["adjusted_p_value"] == pytest.approx(small["raw_p_value"] * 5)
    assert large["adjusted_p_value"] == pytest.approx(large["raw_p_value"] * 500)
    assert small["passed"] is True
    assert large["passed"] is False
    assert "multiple_testing_not_familywise_significant" in large["reasons"]


def test_candidate_needs_precommitted_minimum_nonzero_trade_count():
    assessment = assess_candidate_significance(
        [[0.02] * 8],
        SearchMultiplicityPolicy(max_candidate_trials=2, familywise_alpha=0.05, min_nonzero_trades=10),
    )
    assert assessment["passed"] is False
    assert "multiple_testing_insufficient_nonzero_trades" in assessment["reasons"]


def test_assessment_verifier_recomputes_and_rejects_rehashed_style_tampering():
    policy = SearchMultiplicityPolicy(max_candidate_trials=10, familywise_alpha=0.05, min_nonzero_trades=10)
    folds = [[0.01] * 15]
    assessment = assess_candidate_significance(folds, policy)
    assert assessment["passed"] is True
    valid, reasons = verify_candidate_significance(assessment, folds, policy)
    assert valid is True, reasons

    tampered = copy.deepcopy(assessment)
    tampered["raw_p_value"] = 0.0
    tampered["adjusted_p_value"] = 0.0
    valid, reasons = verify_candidate_significance(tampered, folds, policy)
    assert valid is False
    assert reasons == ["multiple_testing_assessment_mismatch"]


def test_flatten_rejects_nonfinite_values():
    with pytest.raises(ValueError, match="finite"):
        flatten_fold_returns([[0.01, float("nan")]])


def test_policy_rejects_post_hoc_or_invalid_family_shapes():
    with pytest.raises(ValueError):
        SearchMultiplicityPolicy(max_candidate_trials=0).artifact()
    with pytest.raises(ValueError):
        SearchMultiplicityPolicy(familywise_alpha=1.0).artifact()
    with pytest.raises(ValueError):
        SearchMultiplicityPolicy(min_nonzero_trades=0).artifact()
