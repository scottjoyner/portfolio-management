from __future__ import annotations

import math

from scripts.selection_bias import (
    SearchMultiplicityPolicy,
    assess_candidate_significance,
    exact_one_sided_sign_test,
)


def test_exact_sign_test_handles_more_than_float_exponent_range():
    returns = [0.01] * 1100 + [-0.01] * 900
    result = exact_one_sided_sign_test(returns)

    assert result["nonzero_trades"] == 2000
    assert result["positive_trades"] == 1100
    assert 0.0 <= result["raw_p_value"] <= 1.0
    assert math.isfinite(result["raw_p_value"])
    assert result["raw_p_value"] < 0.001


def test_large_sample_candidate_can_reach_bonferroni_gate_without_overflow():
    returns = [[0.01] * 1100 + [-0.01] * 900]
    assessment = assess_candidate_significance(
        returns,
        SearchMultiplicityPolicy(
            max_candidate_trials=20,
            familywise_alpha=0.05,
            min_nonzero_trades=10,
        ),
    )

    assert assessment["nonzero_trades"] == 2000
    assert assessment["adjusted_p_value"] <= 1.0
    assert math.isfinite(assessment["adjusted_p_value"])
    assert assessment["passed"] is True
