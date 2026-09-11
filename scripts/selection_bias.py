#!/usr/bin/env python3
"""Deterministic multiplicity and serial-dependence control for candidate search.

New research experiments precommit both a family-wise candidate-search budget and
a coarse dependence horizon.  Each candidate must pass two one-sided exact sign
tests against the same Bonferroni per-trial alpha:

1. the marginal trade-sign test over every verified OOS trade; and
2. a non-overlapping block-sign test that gives each chronological block within
   a walk-forward fold one direction vote.

The block companion deliberately reduces the effective sample size when wins are
clustered.  It does not prove independence between blocks or model arbitrary
long-memory processes; it is a conservative, deterministic guard against the
most obvious serial-correlation overcounting.  Legacy v1 policy artifacts remain
verifiable so already-recorded evidence does not silently change semantics.
"""
from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Sequence

LEGACY_METHOD = "precommitted_bonferroni_exact_sign_v1"
LEGACY_POLICY_SCHEMA_VERSION = 1
METHOD = "precommitted_bonferroni_exact_sign_block_v2"
POLICY_SCHEMA_VERSION = 2
BLOCK_METHOD = "nonoverlapping_fold_block_sign_v1"


@dataclass(frozen=True)
class SearchMultiplicityPolicy:
    """Pre-search family-wise and dependence policy.

    ``max_candidate_trials`` is the committed family size. Every registered
    candidate consumes one slot whether its evidence is valid or eligible.
    ``dependence_block_size`` is measured in chronological OOS trades inside a
    fold; blocks never cross walk-forward fold boundaries.
    """

    max_candidate_trials: int = 20
    familywise_alpha: float = 0.05
    min_nonzero_trades: int = 10
    dependence_block_size: int = 5
    min_nonzero_blocks: int = 5

    def normalized(self) -> "SearchMultiplicityPolicy":
        max_trials = int(self.max_candidate_trials)
        alpha = float(self.familywise_alpha)
        min_trades = int(self.min_nonzero_trades)
        block_size = int(self.dependence_block_size)
        min_blocks = int(self.min_nonzero_blocks)
        if max_trials <= 0:
            raise ValueError("max_candidate_trials must be positive")
        if max_trials > 100_000:
            raise ValueError("max_candidate_trials is unreasonably large")
        if not math.isfinite(alpha) or not (0.0 < alpha < 1.0):
            raise ValueError("familywise_alpha must be finite and between 0 and 1")
        if min_trades <= 0:
            raise ValueError("min_nonzero_trades must be positive")
        if block_size < 2:
            raise ValueError("dependence_block_size must be at least 2")
        if block_size > 100_000:
            raise ValueError("dependence_block_size is unreasonably large")
        if min_blocks <= 0:
            raise ValueError("min_nonzero_blocks must be positive")
        return SearchMultiplicityPolicy(
            max_candidate_trials=max_trials,
            familywise_alpha=alpha,
            min_nonzero_trades=min_trades,
            dependence_block_size=block_size,
            min_nonzero_blocks=min_blocks,
        )

    def artifact(self) -> dict[str, Any]:
        normalized = self.normalized()
        return {
            "schema_version": POLICY_SCHEMA_VERSION,
            "method": METHOD,
            **asdict(normalized),
            "per_trial_alpha": normalized.familywise_alpha / normalized.max_candidate_trials,
        }


def _legacy_policy_artifact(policy: SearchMultiplicityPolicy) -> dict[str, Any]:
    return {
        "schema_version": LEGACY_POLICY_SCHEMA_VERSION,
        "method": LEGACY_METHOD,
        "max_candidate_trials": policy.max_candidate_trials,
        "familywise_alpha": policy.familywise_alpha,
        "min_nonzero_trades": policy.min_nonzero_trades,
        "per_trial_alpha": policy.familywise_alpha / policy.max_candidate_trials,
    }


def _is_legacy_policy(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("schema_version") == LEGACY_POLICY_SCHEMA_VERSION
        and value.get("method") == LEGACY_METHOD
    )


def policy_from_artifact(value: Any) -> SearchMultiplicityPolicy:
    if not isinstance(value, dict):
        raise ValueError("multiple-testing policy must be an object")

    if _is_legacy_policy(value):
        policy = SearchMultiplicityPolicy(
            max_candidate_trials=value.get("max_candidate_trials"),
            familywise_alpha=value.get("familywise_alpha"),
            min_nonzero_trades=value.get("min_nonzero_trades"),
            dependence_block_size=5,
            min_nonzero_blocks=5,
        ).normalized()
        if _legacy_policy_artifact(policy) != value:
            raise ValueError("legacy multiple-testing policy artifact is not canonical")
        return policy

    if value.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise ValueError("unsupported multiple-testing policy schema")
    if value.get("method") != METHOD:
        raise ValueError("unsupported multiple-testing method")
    policy = SearchMultiplicityPolicy(
        max_candidate_trials=value.get("max_candidate_trials"),
        familywise_alpha=value.get("familywise_alpha"),
        min_nonzero_trades=value.get("min_nonzero_trades"),
        dependence_block_size=value.get("dependence_block_size"),
        min_nonzero_blocks=value.get("min_nonzero_blocks"),
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
    """Return P[Binomial(n, 0.5) >= k] without float(2**n) overflow."""

    if n < 0 or k < 0 or k > n:
        raise ValueError("invalid binomial tail bounds")
    if k == 0:
        return 1.0
    denominator = 1 << n

    if k <= n // 2:
        coefficient = 1
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
    """Exact H0: P(return > 0) <= 0.5 versus positive-direction alternative."""

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


def exact_nonoverlapping_block_sign_test(
    fold_returns: Any,
    *,
    block_size: int,
) -> dict[str, Any]:
    """Coarsen chronological OOS signs into fold-local block direction votes.

    Blocks never cross fold boundaries.  Within each block, positive and
    negative trade signs are counted; the block votes positive when positives
    exceed negatives, negative when negatives exceed positives, and zero on a
    tie.  The exact sign test is then applied to the block votes.
    """

    block_size = int(block_size)
    if block_size < 2:
        raise ValueError("block_size must be at least 2")
    if not isinstance(fold_returns, Sequence) or isinstance(fold_returns, (str, bytes)):
        raise ValueError("fold_returns must be a sequence")

    block_votes: list[float] = []
    total_blocks = 0
    for fold in fold_returns:
        if not isinstance(fold, Sequence) or isinstance(fold, (str, bytes)):
            raise ValueError("each fold return group must be a sequence")
        values: list[float] = []
        for raw in fold:
            try:
                value = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError("OOS returns must be numeric") from exc
            if not math.isfinite(value):
                raise ValueError("OOS returns must be finite")
            values.append(value)

        for start in range(0, len(values), block_size):
            block = values[start:start + block_size]
            if not block:
                continue
            total_blocks += 1
            positives = sum(value > 0.0 for value in block)
            negatives = sum(value < 0.0 for value in block)
            if positives > negatives:
                block_votes.append(1.0)
            elif negatives > positives:
                block_votes.append(-1.0)
            else:
                block_votes.append(0.0)

    sign = exact_one_sided_sign_test(block_votes)
    return {
        "method": BLOCK_METHOD,
        "block_size": block_size,
        "total_blocks": total_blocks,
        "positive_blocks": sign["positive_trades"],
        "negative_blocks": sign["negative_trades"],
        "zero_blocks": sign["zero_trades"],
        "nonzero_blocks": sign["nonzero_trades"],
        "raw_p_value": sign["raw_p_value"],
    }


def _legacy_assessment(
    fold_returns: Any,
    policy: SearchMultiplicityPolicy,
    policy_artifact: dict[str, Any],
) -> dict[str, Any]:
    flattened = flatten_fold_returns(fold_returns)
    sign = exact_one_sided_sign_test(flattened)
    adjusted = min(1.0, sign["raw_p_value"] * policy.max_candidate_trials)
    reasons: list[str] = []
    if sign["nonzero_trades"] < policy.min_nonzero_trades:
        reasons.append("multiple_testing_insufficient_nonzero_trades")
    if sign["raw_p_value"] > policy_artifact["per_trial_alpha"]:
        reasons.append("multiple_testing_not_familywise_significant")
    return {
        "method": LEGACY_METHOD,
        "policy": policy_artifact,
        **sign,
        "adjusted_p_value": adjusted,
        "passed": not reasons,
        "reasons": reasons,
    }


def assess_candidate_significance(
    fold_returns: Any,
    policy: SearchMultiplicityPolicy | dict[str, Any],
) -> dict[str, Any]:
    """Build deterministic family-wise + block-dependence candidate evidence."""

    legacy = False
    if isinstance(policy, dict):
        legacy = _is_legacy_policy(policy)
        normalized_policy = policy_from_artifact(policy)
        policy_artifact = policy
    elif isinstance(policy, SearchMultiplicityPolicy):
        normalized_policy = policy.normalized()
        policy_artifact = normalized_policy.artifact()
    else:
        raise ValueError("multiple-testing policy is required")

    if legacy:
        return _legacy_assessment(fold_returns, normalized_policy, policy_artifact)

    flattened = flatten_fold_returns(fold_returns)
    sign = exact_one_sided_sign_test(flattened)
    block = exact_nonoverlapping_block_sign_test(
        fold_returns,
        block_size=normalized_policy.dependence_block_size,
    )
    marginal_adjusted = min(
        1.0, sign["raw_p_value"] * normalized_policy.max_candidate_trials
    )
    block_adjusted = min(
        1.0, block["raw_p_value"] * normalized_policy.max_candidate_trials
    )
    conservative_adjusted = max(marginal_adjusted, block_adjusted)

    reasons: list[str] = []
    if sign["nonzero_trades"] < normalized_policy.min_nonzero_trades:
        reasons.append("multiple_testing_insufficient_nonzero_trades")
    if sign["raw_p_value"] > policy_artifact["per_trial_alpha"]:
        reasons.append("multiple_testing_not_familywise_significant")
    if block["nonzero_blocks"] < normalized_policy.min_nonzero_blocks:
        reasons.append("dependence_insufficient_nonzero_blocks")
    if block["raw_p_value"] > policy_artifact["per_trial_alpha"]:
        reasons.append("dependence_not_familywise_significant")

    return {
        "method": METHOD,
        "policy": policy_artifact,
        **sign,
        "marginal_adjusted_p_value": marginal_adjusted,
        "dependence": {
            **block,
            "adjusted_p_value": block_adjusted,
        },
        "adjusted_p_value": conservative_adjusted,
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
