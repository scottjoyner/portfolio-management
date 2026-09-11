#!/usr/bin/env python3
"""Deterministic multiple-testing control for research candidate search.

This module contains no trading or broker authority.  It provides a deliberately
conservative family-wise error gate for candidate tournaments: a search budget
and family-wise alpha are committed before search, each candidate is tested with
an exact one-sided sign test over verified OOS trade returns, and Bonferroni
correction uses the *full committed budget* rather than the number of trials that
happened to be run.

Using the committed family size means stopping early after a favorable result
cannot relax the threshold.  The sign test is distribution-free with respect to
return magnitudes, but it does not solve serial dependence between trades; block
or dependence-aware inference remains a separate validation layer.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Sequence

METHOD = "precommitted_bonferroni_exact_sign_v1"
POLICY_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SearchMultiplicityPolicy:
    """Pre-search family-wise error policy.

    ``max_candidate_trials`` is the committed family size.  Every registered
    candidate consumes one slot regardless of whether its alpha evidence is
    ultimately valid or eligible.
    """

    max_candidate_trials: int = 20
    familywise_alpha: float = 0.05
    min_nonzero_trades: int = 10

    def normalized(self) -> "SearchMultiplicityPolicy":
        max_trials = int(self.max_candidate_trials)
        alpha = float(self.familywise_alpha)
        min_trades = int(self.min_nonzero_trades)
        if max_trials <= 0:
            raise ValueError("max_candidate_trials must be positive")
        if max_trials > 100_000:
            raise ValueError("max_candidate_trials is unreasonably large")
        if not math.isfinite(alpha) or not (0.0 < alpha < 1.0):
            raise ValueError("familywise_alpha must be finite and between 0 and 1")
        if min_trades <= 0:
            raise ValueError("min_nonzero_trades must be positive")
        return SearchMultiplicityPolicy(
            max_candidate_trials=max_trials,
            familywise_alpha=alpha,
            min_nonzero_trades=min_trades,
        )

    def artifact(self) -> dict[str, Any]:
        normalized = self.normalized()
        return {
            "schema_version": POLICY_SCHEMA_VERSION,
            "method": METHOD,
            **asdict(normalized),
            "per_trial_alpha": normalized.familywise_alpha / normalized.max_candidate_trials,
        }


def policy_from_artifact(value: Any) -> SearchMultiplicityPolicy:
    if not isinstance(value, dict):
        raise ValueError("multiple-testing policy must be an object")
    if value.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise ValueError("unsupported multiple-testing policy schema")
    if value.get("method") != METHOD:
        raise ValueError("unsupported multiple-testing method")
    policy = SearchMultiplicityPolicy(
        max_candidate_trials=value.get("max_candidate_trials"),
        familywise_alpha=value.get("familywise_alpha"),
        min_nonzero_trades=value.get("min_nonzero_trades"),
    ).normalized()
    expected = policy.artifact()
    if expected != value:
        raise ValueError("multiple-testing policy artifact is not canonical")
    return policy


def flatten_fold_returns(fold_returns: Any) -> list[float]:
    """Return a finite flat list from canonical fold-grouped OOS returns."""

    if not isinstance(fold_returns, Sequence) or isinstance(fold_returns, (str, bytes)):
        raise ValueError("fold_returns must be a sequence")
    flattened: list[float] = []
    for fold in fold_returns:
        if not isinstance(fold, Sequence) or isinstance(fold, (str, bytes)):
            raise ValueError("each fold return group must be a sequence")
        for raw in fold:
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("OOS returns must be numeric") from exc
            if not math.isfinite(value):
                raise ValueError("OOS returns must be finite")
            flattened.append(value)
    return flattened


def _exact_binomial_upper_tail_half(n: int, k: int) -> float:
    """Return P[Binomial(n, 0.5) >= k] without float(2**n) overflow.

    The combinatorial numerator and denominator stay as arbitrary-precision
    integers until the final bounded division.  A recurrence builds the smaller
    side of the tail, avoiding repeated ``math.comb`` calls while retaining the
    exact discrete probability before conversion to the JSON-safe float result.
    """

    if n < 0 or k < 0 or k > n:
        raise ValueError("invalid binomial tail bounds")
    if k == 0:
        return 1.0
    denominator = 1 << n

    if k <= n // 2:
        # P[X >= k] = 1 - P[X <= k - 1].
        coefficient = 1  # C(n, 0)
        lower_sum = coefficient
        for j in range(0, k - 1):
            coefficient = coefficient * (n - j) // (j + 1)
            lower_sum += coefficient
        return 1.0 - (lower_sum / denominator)

    coefficient = math.comb(n, k)
    upper_sum = coefficient
    for j in range(k, n):
        coefficient = coefficient * (n - j) // (j + 1)
        upper_sum += coefficient
    return upper_sum / denominator


def exact_one_sided_sign_test(returns: Sequence[float]) -> dict[str, Any]:
    """Exact H0: P(return > 0) <= 0.5 versus positive-median alternative.

    Zero returns are omitted from the effective sample.  Under the boundary
    null, the positive count is Binomial(n, 0.5), so the upper-tail p-value is
    exact and deterministic.  Integer tail arithmetic avoids the previous
    large-sample overflow caused by converting ``2**n`` to float.
    """

    positives = 0
    negatives = 0
    zeros = 0
    for raw in returns:
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError("sign-test returns must be finite")
        if value > 0.0:
            positives += 1
        elif value < 0.0:
            negatives += 1
        else:
            zeros += 1
    effective = positives + negatives
    p_value = 1.0 if effective == 0 else _exact_binomial_upper_tail_half(effective, positives)
    return {
        "positive_trades": positives,
        "negative_trades": negatives,
        "zero_trades": zeros,
        "nonzero_trades": effective,
        "raw_p_value": p_value,
    }


def assess_candidate_significance(
    fold_returns: Any,
    policy: SearchMultiplicityPolicy | dict[str, Any],
) -> dict[str, Any]:
    """Build the deterministic candidate-level Bonferroni assessment."""

    if isinstance(policy, dict):
        normalized_policy = policy_from_artifact(policy)
        policy_artifact = policy
    elif isinstance(policy, SearchMultiplicityPolicy):
        normalized_policy = policy.normalized()
        policy_artifact = normalized_policy.artifact()
    else:
        raise ValueError("multiple-testing policy is required")

    flattened = flatten_fold_returns(fold_returns)
    sign = exact_one_sided_sign_test(flattened)
    adjusted = min(1.0, sign["raw_p_value"] * normalized_policy.max_candidate_trials)
    reasons: list[str] = []
    if sign["nonzero_trades"] < normalized_policy.min_nonzero_trades:
        reasons.append("multiple_testing_insufficient_nonzero_trades")
    if sign["raw_p_value"] > policy_artifact["per_trial_alpha"]:
        reasons.append("multiple_testing_not_familywise_significant")
    return {
        "method": METHOD,
        "policy": policy_artifact,
        **sign,
        "adjusted_p_value": adjusted,
        "passed": not reasons,
        "reasons": reasons,
    }


def verify_candidate_significance(
    assessment: Any,
    fold_returns: Any,
    policy: SearchMultiplicityPolicy | dict[str, Any],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if not isinstance(assessment, dict):
        return False, ["multiple_testing_assessment_missing"]
    try:
        expected = assess_candidate_significance(fold_returns, policy)
        if assessment != expected:
            reasons.append("multiple_testing_assessment_mismatch")
    except (TypeError, ValueError, OverflowError):
        reasons.append("multiple_testing_assessment_invalid")
    return not reasons, reasons
