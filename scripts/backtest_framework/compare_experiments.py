"""Compare two backtesting experiments and apply a regression gate.

Given two scorecards (from ``run_experiment.py``), reports per-strategy deltas and
aggregate shifts, then fails the *candidate* if it regresses against the baseline:

  * aggregate mean-sharpe of passing strategies drops below the baseline, OR
  * a strategy that passed on the baseline now fails on the candidate (within the
    shared universe), OR
  * the overall pass rate drops by more than ``--max-pass-rate-drop``.

This is the "consistently test" guarantee: a change to a strategy or threshold
cannot silently degrade the portfolio's evaluated edge.

Usage:
    python3 scripts/backtest_framework/compare_experiments.py baseline_v1 candidate_v1
    python3 scripts/backtest_framework/compare_experiments.py baseline_v1 candidate_v1 \
        --max-pass-rate-drop 0.05 --fail-on-regression
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.backtest_framework.experiment import experiments_dir


def _load(name: str) -> Dict:
    path = os.path.join(experiments_dir(), name, "scorecard.json")
    if not os.path.exists(path):
        raise SystemExit(f"No scorecard for experiment {name!r} at {path}")
    with open(path) as f:
        return json.load(f)


def _strategy_index(results: Dict) -> Dict[str, Dict]:
    out = {}
    for ck, r in results.items():
        out[(r["strategy"], r["currency"])] = r
    return out


def compare(baseline: Dict, candidate: Dict, max_pass_rate_drop: float) -> Dict:
    b_idx = _strategy_index(baseline["results"])
    c_idx = _strategy_index(candidate["results"])

    shared = sorted(set(b_idx) & set(c_idx))
    improved, degraded, new_fail = [], [], []
    for key in shared:
        b, c = b_idx[key], c_idx[key]
        label = f"{key[0]}/{key[1]}"
        if b["passed"] and not c["passed"]:
            new_fail.append(label)
        elif c["passed"] and not b["passed"]:
            improved.append(label)
        elif c["passed"] and b["passed"]:
            if c["sharpe"] > b["sharpe"] + 1e-9:
                improved.append(label)
            elif c["sharpe"] < b["sharpe"] - 1e-9:
                degraded.append(label)

    bs, cs = baseline["mean_sharpe_passed"], candidate["mean_sharpe_passed"]
    bpr, cpr = baseline["pass_rate"], candidate["pass_rate"]
    pass_rate_drop = bpr - cpr

    regressions = []
    if cs < bs - 1e-9:
        regressions.append(f"mean_sharpe passed {bs:.3f} -> {cs:.3f} (regressed)")
    if new_fail:
        regressions.append(f"{len(new_fail)} passing strategy(s) now fail: {', '.join(new_fail)}")
    if pass_rate_drop > max_pass_rate_drop:
        regressions.append(
            f"pass_rate {bpr:.1%} -> {cpr:.1%} (drop {pass_rate_drop:.1%} > {max_pass_rate_drop:.1%})")

    return {
        "baseline": baseline["name"],
        "candidate": candidate["name"],
        "shared_strategies": len(shared),
        "improved": improved,
        "degraded": degraded,
        "new_failures": new_fail,
        "baseline_mean_sharpe": bs,
        "candidate_mean_sharpe": cs,
        "baseline_pass_rate": bpr,
        "candidate_pass_rate": cpr,
        "pass_rate_drop": round(pass_rate_drop, 4),
        "regressions": regressions,
        "passed": len(regressions) == 0,
    }


def _print(report: Dict) -> None:
    print(f"\n=== Compare {report['baseline']} -> {report['candidate']} ===")
    print(f"shared strategies: {report['shared_strategies']}")
    print(f"improved: {len(report['improved'])}  degraded: {len(report['degraded'])}  "
          f"new failures: {len(report['new_failures'])}")
    if report["improved"]:
        print("  + " + ", ".join(report["improved"][:20]))
    if report["new_failures"]:
        print("  X " + ", ".join(report["new_failures"][:20]))
    print(f"mean_sharpe: {report['baseline_mean_sharpe']:.3f} -> "
          f"{report['candidate_mean_sharpe']:.3f}")
    print(f"pass_rate:   {report['baseline_pass_rate']:.1%} -> {report['candidate_pass_rate']:.1%} "
          f"(drop {report['pass_rate_drop']:.1%})")
    if report["regressions"]:
        print("REGRESSIONS:")
        for r in report["regressions"]:
            print(f"  - {r}")
    print(f"RESULT: {'PASS' if report['passed'] else 'FAIL'}")


def cli_main(baseline_name: str, candidate_name: str, max_pass_rate_drop: float = 0.05,
             fail_on_regression: bool = False) -> Dict:
    baseline, candidate = _load(baseline_name), _load(candidate_name)
    report = compare(baseline, candidate, max_pass_rate_drop)
    _print(report)
    if fail_on_regression and not report["passed"]:
        raise SystemExit(1)
    return report


def cli():
    p = argparse.ArgumentParser(description="Compare two backtesting experiments")
    p.add_argument("baseline", help="baseline experiment name")
    p.add_argument("candidate", help="candidate experiment name")
    p.add_argument("--max-pass-rate-drop", type=float, default=0.05, dest="max_pass_rate_drop")
    p.add_argument("--fail-on-regression", action="store_true", dest="fail_on_regression",
                   help="exit non-zero if regressions detected")
    args = p.parse_args()
    cli_main(args.baseline, args.candidate, args.max_pass_rate_drop, args.fail_on_regression)


if __name__ == "__main__":
    cli()
