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


def test_large_clustered_sample_is_finite_but_can_fail_dependence_gate():
    returns = [[0.01] * 1100 + [-0.01] * 900]
    assessment = assess_candidate_significance(
        returns,
        SearchMultiplicityPolicy(
            max_candidate_trials=20,
            familywise_alpha=0.05,
            min_nonzero_trades=10,
            dependence_block_size=5,
            min_nonzero_blocks=5,
        ),
    )

    assert assessment["nonzero_trades"] == 2000
    assert assessment["marginal_adjusted_p_value"] <= 1.0
    assert assessment["adjusted_p_value"] <= 1.0
    assert math.isfinite(assessment["adjusted_p_value"])
    assert assessment["dependence"]["nonzero_blocks"] == 400
    assert assessment["passed"] is False
    assert "dependence_not_familywise_significant" in assessment["reasons"]


def test_large_well_distributed_directional_sample_can_pass_both_gates():
    block = [0.01, 0.01, 0.01, 0.01, -0.01]
    returns = [[value for _ in range(400) for value in block]]
    assessment = assess_candidate_significance(
        returns,
        SearchMultiplicityPolicy(
            max_candidate_trials=20,
            familywise_alpha=0.05,
            min_nonzero_trades=10,
            dependence_block_size=5,
            min_nonzero_blocks=5,
        ),
    )

    assert assessment["nonzero_trades"] == 2000
    assert assessment["dependence"]["positive_blocks"] == 400
    assert assessment["dependence"]["negative_blocks"] == 0
    assert assessment["passed"] is True
