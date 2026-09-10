#!/usr/bin/env python3
"""Deterministic alpha-validation evidence for challenger promotion.

This module deliberately contains no broker/order-routing code.  It turns already
realized out-of-sample trade returns into reproducible validation evidence and
provides leakage-safe walk-forward split boundaries for upstream backtest/replay
runners.

Return convention: decimal return per evaluated trade/period (0.01 == +1%).
All evidence hashes use canonical JSON and reject NaN/Infinity.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WalkForwardFold:
    """Half-open integer boundaries for one leakage-safe fold."""

    fold: int
    train_start: int
    train_end: int
    purge_start: int
    purge_end: int
    embargo_start: int
    embargo_end: int
    test_start: int
    test_end: int

    @property
    def train_size(self) -> int:
        return self.train_end - self.train_start

    @property
    def test_size(self) -> int:
        return self.test_end - self.test_start


@dataclass(frozen=True)
class ValidationPolicy:
    """Conservative defaults for generated evidence, independently overridable."""

    min_folds: int = 3
    min_out_of_sample_trades: int = 10
    min_profit_factor: float = 1.10
    max_drawdown_pct: float = 20.0
    min_positive_folds_fraction: float = 0.50
    min_bootstrap_positive_fraction: float = 0.55
    max_probability_of_ruin: float = 0.05
    required_cost_stress_bps: float = 25.0
    min_parameter_stability_score: float = 0.60


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(payload: Any) -> str:
    """Canonical UTF-8 JSON used for provenance/evidence hashing."""

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def generate_walk_forward_splits(
    n_observations: int,
    *,
    train_size: int,
    test_size: int,
    step_size: int | None = None,
    purge_size: int = 0,
    embargo_size: int = 0,
    expanding: bool = True,
) -> list[WalkForwardFold]:
    """Generate deterministic anchored/rolling walk-forward boundaries.

    Layout of every fold::

        [ training ][ purge ][ embargo ][ test ]

    `purge_size` removes observations immediately after the training sample that
    could still carry information from overlapping labels/holding periods.
    `embargo_size` adds an additional no-train/no-test separation.  Both regions
    are excluded from training and test data.

    When `expanding=True`, every fold is anchored at index 0.  Otherwise a fixed
    rolling train window of `train_size` observations is used.
    """

    values = {
        "n_observations": n_observations,
        "train_size": train_size,
        "test_size": test_size,
        "purge_size": purge_size,
        "embargo_size": embargo_size,
    }
    for name, value in values.items():
        if not isinstance(value, int):
            raise TypeError(f"{name} must be an integer")
    if n_observations <= 0 or train_size <= 0 or test_size <= 0:
        raise ValueError("n_observations, train_size and test_size must be positive")
    if purge_size < 0 or embargo_size < 0:
        raise ValueError("purge_size and embargo_size must be non-negative")
    if step_size is None:
        step_size = test_size
    if not isinstance(step_size, int) or step_size <= 0:
        raise ValueError("step_size must be a positive integer")

    first_test_start = train_size + purge_size + embargo_size
    if first_test_start + test_size > n_observations:
        return []

    folds: list[WalkForwardFold] = []
    test_start = first_test_start
    fold_number = 0
    while test_start + test_size <= n_observations:
        embargo_end = test_start
        embargo_start = embargo_end - embargo_size
        purge_end = embargo_start
        purge_start = purge_end - purge_size
        train_end = purge_start
        train_start = 0 if expanding else train_end - train_size
        if train_start < 0 or train_end - train_start < train_size:
            test_start += step_size
            continue

        folds.append(
            WalkForwardFold(
                fold=fold_number,
                train_start=train_start,
                train_end=train_end,
                purge_start=purge_start,
                purge_end=purge_end,
                embargo_start=embargo_start,
                embargo_end=embargo_end,
                test_start=test_start,
                test_end=test_start + test_size,
            )
        )
        fold_number += 1
        test_start += step_size
    return folds


def _clean_returns(returns: Iterable[float]) -> list[float]:
    cleaned: list[float] = []
    for raw in returns:
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError("trade returns must be finite")
        if value <= -1.0:
            raise ValueError("trade return cannot be <= -100%")
        cleaned.append(value)
    return cleaned


def _max_drawdown_pct(returns: Sequence[float]) -> float:
    equity = 1.0
    peak = 1.0
    worst = 0.0
    for ret in returns:
        equity *= 1.0 + ret
        peak = max(peak, equity)
        if peak > 0:
            worst = max(worst, (peak - equity) / peak)
    return worst * 100.0


def _expected_shortfall_pct(returns: Sequence[float], alpha: float = 0.05) -> float:
    if not returns:
        return 0.0
    ordered = sorted(returns)
    count = max(1, math.ceil(len(ordered) * alpha))
    return statistics.mean(ordered[:count]) * 100.0


def performance_metrics(
    returns: Iterable[float],
    *,
    periods_per_year: int = 252,
) -> dict[str, Any]:
    """Compute finite, JSON-safe metrics from a return sequence."""

    rows = _clean_returns(returns)
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    if not rows:
        return {
            "trade_count": 0,
            "total_return_pct": 0.0,
            "annualized_return_pct": 0.0,
            "mean_return_pct": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "profit_factor": 0.0,
            "max_drawdown_pct": 0.0,
            "calmar": 0.0,
            "worst_trade_pct": 0.0,
            "expected_shortfall_pct": 0.0,
            "win_rate": 0.0,
        }

    equity = math.prod(1.0 + value for value in rows)
    total_return = equity - 1.0
    mean_return = statistics.mean(rows)
    stdev = statistics.stdev(rows) if len(rows) > 1 else 0.0
    downside = [min(value, 0.0) for value in rows]
    downside_dev = math.sqrt(statistics.mean(value * value for value in downside)) if downside else 0.0
    scale = math.sqrt(periods_per_year)
    sharpe = mean_return / stdev * scale if stdev > 0 else (scale if mean_return > 0 else 0.0)
    sortino = mean_return / downside_dev * scale if downside_dev > 0 else (scale if mean_return > 0 else 0.0)

    gross_profit = sum(value for value in rows if value > 0)
    gross_loss = abs(sum(value for value in rows if value < 0))
    profit_factor = gross_profit / max(gross_loss, 1e-12) if gross_profit > 0 else 0.0

    # Annualize the geometric trade/period return only when mathematically valid.
    annualized = equity ** (periods_per_year / len(rows)) - 1.0 if equity > 0 else -1.0
    max_dd = _max_drawdown_pct(rows)
    calmar = (annualized * 100.0) / max_dd if max_dd > 0 else (annualized * 100.0 if annualized > 0 else 0.0)

    return {
        "trade_count": len(rows),
        "total_return_pct": round(total_return * 100.0, 10),
        "annualized_return_pct": round(annualized * 100.0, 10),
        "mean_return_pct": round(mean_return * 100.0, 10),
        "sharpe": round(sharpe, 10),
        "sortino": round(sortino, 10),
        "profit_factor": round(profit_factor, 10),
        "max_drawdown_pct": round(max_dd, 10),
        "calmar": round(calmar, 10),
        "worst_trade_pct": round(min(rows) * 100.0, 10),
        "expected_shortfall_pct": round(_expected_shortfall_pct(rows), 10),
        "win_rate": round(sum(1 for value in rows if value > 0) / len(rows), 10),
    }


def bootstrap_robustness(
    returns: Iterable[float],
    *,
    samples: int = 2000,
    seed: int = 0,
    ruin_fraction: float = 0.50,
) -> dict[str, Any]:
    """Deterministic trade-sequence bootstrap for sign robustness and ruin risk."""

    rows = _clean_returns(returns)
    if not rows:
        return {
            "samples": 0,
            "seed": seed,
            "positive_fraction": 0.0,
            "probability_of_ruin": 1.0,
            "median_terminal_return_pct": 0.0,
            "p05_terminal_return_pct": 0.0,
        }
    if samples <= 0:
        raise ValueError("samples must be positive")
    if not 0.0 < ruin_fraction < 1.0:
        raise ValueError("ruin_fraction must be between 0 and 1")

    rng = random.Random(seed)
    terminals: list[float] = []
    positive = 0
    ruined = 0
    for _ in range(samples):
        equity = 1.0
        hit_ruin = False
        for _ in range(len(rows)):
            equity *= 1.0 + rows[rng.randrange(len(rows))]
            if equity <= ruin_fraction:
                hit_ruin = True
        terminal_return = equity - 1.0
        terminals.append(terminal_return)
        if terminal_return > 0:
            positive += 1
        if hit_ruin:
            ruined += 1

    terminals.sort()
    p05_index = max(0, min(len(terminals) - 1, int(math.floor(0.05 * (len(terminals) - 1)))))
    return {
        "samples": samples,
        "seed": seed,
        "positive_fraction": round(positive / samples, 10),
        "probability_of_ruin": round(ruined / samples, 10),
        "median_terminal_return_pct": round(statistics.median(terminals) * 100.0, 10),
        "p05_terminal_return_pct": round(terminals[p05_index] * 100.0, 10),
    }


def cost_stress_results(
    returns: Iterable[float],
    *,
    stress_bps: Sequence[float] = (0.0, 10.0, 25.0, 50.0),
    periods_per_year: int = 252,
) -> list[dict[str, Any]]:
    """Reprice every observed trade with additional round-trip cost drag."""

    rows = _clean_returns(returns)
    results: list[dict[str, Any]] = []
    for raw_bps in stress_bps:
        bps = float(raw_bps)
        if not math.isfinite(bps) or bps < 0:
            raise ValueError("stress bps must be finite and non-negative")
        drag = bps / 10_000.0
        stressed = [value - drag for value in rows]
        metrics = performance_metrics(stressed, periods_per_year=periods_per_year)
        results.append({"extra_cost_bps": bps, **metrics})
    return results


def _stress_at_or_above(stress_rows: Sequence[dict[str, Any]], required_bps: float) -> dict[str, Any] | None:
    eligible = [row for row in stress_rows if float(row.get("extra_cost_bps", -1)) >= required_bps]
    if not eligible:
        return None
    return min(eligible, key=lambda row: float(row["extra_cost_bps"]))


def build_alpha_validation_evidence(
    *,
    candidate_id: str,
    candidate_source_sha: str,
    candidate_config: dict[str, Any],
    dataset_id: str,
    dataset_hash: str,
    fold_returns: Sequence[Sequence[float]],
    net_pnl_after_cost_usd: float,
    cost_coverage_ratio: float,
    regimes_tested: Sequence[str],
    accounting_invariants_ok: bool,
    lineage_verified: bool,
    parameter_stability_score: float,
    policy: ValidationPolicy | None = None,
    stress_bps: Sequence[float] = (0.0, 10.0, 25.0, 50.0),
    bootstrap_samples: int = 2000,
    bootstrap_seed: int = 0,
    periods_per_year: int = 252,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build a self-hashing, fail-closed validation evidence artifact."""

    limits = policy or ValidationPolicy()
    if not candidate_id or not candidate_source_sha or not dataset_id or not dataset_hash:
        raise ValueError("candidate and dataset provenance fields are required")
    pnl = float(net_pnl_after_cost_usd)
    coverage = float(cost_coverage_ratio)
    stability = float(parameter_stability_score)
    if not all(math.isfinite(value) for value in (pnl, coverage, stability)):
        raise ValueError("economic and stability metrics must be finite")
    if not 0.0 <= stability <= 1.0:
        raise ValueError("parameter_stability_score must be in [0, 1]")

    normalized_folds = [_clean_returns(values) for values in fold_returns]
    fold_metrics = [
        {"fold": index, **performance_metrics(values, periods_per_year=periods_per_year)}
        for index, values in enumerate(normalized_folds)
    ]
    all_returns = [value for fold in normalized_folds for value in fold]
    aggregate = performance_metrics(all_returns, periods_per_year=periods_per_year)
    bootstrap = bootstrap_robustness(
        all_returns,
        samples=bootstrap_samples,
        seed=bootstrap_seed,
    )
    stressed = cost_stress_results(
        all_returns,
        stress_bps=stress_bps,
        periods_per_year=periods_per_year,
    )

    positive_fold_fraction = (
        sum(1 for row in fold_metrics if row["total_return_pct"] > 0) / len(fold_metrics)
        if fold_metrics else 0.0
    )
    required_stress = _stress_at_or_above(stressed, limits.required_cost_stress_bps)

    fail_reasons: list[str] = []
    if len(fold_metrics) < limits.min_folds:
        fail_reasons.append("insufficient_walk_forward_folds")
    if aggregate["trade_count"] < limits.min_out_of_sample_trades:
        fail_reasons.append("insufficient_out_of_sample_trades")
    if aggregate["profit_factor"] < limits.min_profit_factor:
        fail_reasons.append("profit_factor_below_floor")
    if aggregate["max_drawdown_pct"] > limits.max_drawdown_pct:
        fail_reasons.append("max_drawdown_above_limit")
    if positive_fold_fraction < limits.min_positive_folds_fraction:
        fail_reasons.append("insufficient_positive_folds")
    if bootstrap["positive_fraction"] < limits.min_bootstrap_positive_fraction:
        fail_reasons.append("bootstrap_robustness_below_floor")
    if bootstrap["probability_of_ruin"] > limits.max_probability_of_ruin:
        fail_reasons.append("probability_of_ruin_above_limit")
    if required_stress is None:
        fail_reasons.append("required_cost_stress_missing")
    elif required_stress["total_return_pct"] <= 0:
        fail_reasons.append("required_cost_stress_unprofitable")
    if stability < limits.min_parameter_stability_score:
        fail_reasons.append("parameter_instability")
    if accounting_invariants_ok is not True:
        fail_reasons.append("accounting_invariants_failed")
    if lineage_verified is not True:
        fail_reasons.append("lineage_verification_failed")

    unique_regimes = sorted({str(value) for value in regimes_tested if str(value)})
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "candidate_id": candidate_id,
        "candidate_source_sha": candidate_source_sha,
        "candidate_config_hash": stable_hash(candidate_config),
        "dataset_id": dataset_id,
        "dataset_hash": dataset_hash,
        "created_at": created_at or _utc_now(),
        "validation_method": "walk_forward_oos_trade_returns_v1",
        "policy": asdict(limits),
        "folds": fold_metrics,
        "trade_count": aggregate["trade_count"],
        "out_of_sample_trades": aggregate["trade_count"],
        "regimes_tested": len(unique_regimes),
        "regime_labels": unique_regimes,
        "net_pnl_after_cost_usd": round(pnl, 10),
        "annualized_return_pct": aggregate["annualized_return_pct"],
        "sharpe": aggregate["sharpe"],
        "sortino": aggregate["sortino"],
        "profit_factor": aggregate["profit_factor"],
        "max_drawdown_pct": aggregate["max_drawdown_pct"],
        "calmar": aggregate["calmar"],
        "worst_trade_pct": aggregate["worst_trade_pct"],
        "expected_shortfall_pct": aggregate["expected_shortfall_pct"],
        "cost_coverage_ratio": round(coverage, 10),
        "parameter_stability_score": round(stability, 10),
        "positive_folds_fraction": round(positive_fold_fraction, 10),
        "bootstrap_positive_fraction": bootstrap["positive_fraction"],
        "probability_of_ruin": bootstrap["probability_of_ruin"],
        "bootstrap": bootstrap,
        "stress_results": stressed,
        "walk_forward_passed": not fail_reasons,
        "accounting_invariants_ok": accounting_invariants_ok is True,
        "lineage_verified": lineage_verified is True,
        "fail_reasons": fail_reasons,
    }
    payload["evidence_hash"] = stable_hash(payload)
    return payload


_REQUIRED_EVIDENCE_FIELDS = {
    "schema_version",
    "candidate_id",
    "candidate_source_sha",
    "candidate_config_hash",
    "dataset_id",
    "dataset_hash",
    "created_at",
    "validation_method",
    "folds",
    "trade_count",
    "out_of_sample_trades",
    "regimes_tested",
    "net_pnl_after_cost_usd",
    "profit_factor",
    "max_drawdown_pct",
    "cost_coverage_ratio",
    "walk_forward_passed",
    "accounting_invariants_ok",
    "lineage_verified",
    "fail_reasons",
    "evidence_hash",
}


def verify_alpha_validation_evidence(evidence: Any) -> tuple[bool, list[str]]:
    """Verify schema/provenance integrity. Performance failure is not tampering."""

    if not isinstance(evidence, dict):
        return False, ["evidence_not_object"]
    reasons: list[str] = []
    missing = sorted(_REQUIRED_EVIDENCE_FIELDS - set(evidence))
    if missing:
        reasons.extend(f"missing_field:{name}" for name in missing)
    if evidence.get("schema_version") != SCHEMA_VERSION:
        reasons.append("unsupported_schema_version")
    supplied_hash = evidence.get("evidence_hash")
    if not isinstance(supplied_hash, str) or len(supplied_hash) != 64:
        reasons.append("invalid_evidence_hash")
    else:
        unhashed = dict(evidence)
        unhashed.pop("evidence_hash", None)
        try:
            expected = stable_hash(unhashed)
        except (TypeError, ValueError, OverflowError):
            reasons.append("evidence_not_canonicalizable")
        else:
            if expected != supplied_hash:
                reasons.append("evidence_hash_mismatch")
    return not reasons, reasons


def evidence_to_challenger_metrics(evidence: dict[str, Any]) -> dict[str, Any]:
    """Map verified evidence onto the existing challenger gate metric contract."""

    valid, reasons = verify_alpha_validation_evidence(evidence)
    if not valid:
        raise ValueError("invalid alpha validation evidence: " + ",".join(reasons))
    return {
        "total_trades": evidence["trade_count"],
        "out_of_sample_trades": evidence["out_of_sample_trades"],
        "regimes_tested": evidence["regimes_tested"],
        "profit_factor": evidence["profit_factor"],
        "cost_coverage_ratio": evidence["cost_coverage_ratio"],
        "net_pnl_after_cost_usd": evidence["net_pnl_after_cost_usd"],
        "max_drawdown_pct": evidence["max_drawdown_pct"],
        "walk_forward_passed": evidence["walk_forward_passed"] is True,
        "accounting_invariants_ok": evidence["accounting_invariants_ok"] is True,
        "lineage_verified": evidence["lineage_verified"] is True,
        "alpha_validation_evidence_hash": evidence["evidence_hash"],
        "alpha_validation_schema_version": evidence["schema_version"],
    }
