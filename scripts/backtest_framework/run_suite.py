"""One-command battery of backtesting experiments + regression CI gate.

Runs a standard suite of Experiments (defined inline), persists each scorecard
via ``run_experiment._persist``, then compares every candidate to a designated
baseline using ``compare_experiments.compare``. Exits non-zero if ANY candidate
regresses, making this a drop-in CI step.

Usage:
    python3 scripts/backtest_framework/run_suite.py
    python3 scripts/backtest_framework/run_suite.py --suite my_suite.json \
        --baseline-name baseline_1h --fail-on-regression
    python3 scripts/backtest_framework/run_suite.py --report
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.backtest_framework import run_experiment, compare_experiments, experiment

log = logging.getLogger("run_suite")


# ---------------------------------------------------------------------------
# Built-in standard battery. Each entry is an Experiment-spec dict using only
# the existing Experiment fields. The first entry whose name starts with
# "baseline" is the default regression baseline.
# ---------------------------------------------------------------------------
DEFAULT_SUITE: List[Dict[str, Any]] = [
    {
        "name": "baseline_1h",
        "strategies": "rust",
        "universe": "all-harvested",
        "granularity": 3600,
        "window_bars": 5000,
        "thresholds": {"min_sharpe": 0.3},
    },
    {
        "name": "baseline_1d",
        "strategies": "rust",
        "universe": "all-harvested",
        "granularity": 86400,
        "window_bars": 2000,
        "thresholds": {"min_sharpe": 0.3},
    },
    {
        "name": "walkforward_1h",
        "strategies": "rust",
        "universe": "all-harvested",
        "granularity": 3600,
        "window_bars": 5000,
        "walk_forward_folds": 4,
        "thresholds": {"min_sharpe": 0.3},
    },
    {
        "name": "ensemble_1h",
        "strategies": "rust",
        "universe": "all-harvested",
        "granularity": 3600,
        "window_bars": 5000,
        "ensemble": True,
        "ensemble_min_groups": 2,
        "thresholds": {"min_sharpe": 0.3},
    },
]

# Experiments considered candidates (regression-checked) when no explicit
# baseline_name is supplied. By default everything not a baseline name is a candidate.
MAX_PASS_RATE_DROP = 0.05


def _load_suite(path: str) -> List[Dict[str, Any]]:
    if not path:
        return [dict(s) for s in DEFAULT_SUITE]
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SystemExit("--suite file must contain a JSON list of experiment specs")
    return data


def _build_experiment(spec: Dict[str, Any]) -> "experiment.Experiment":
    thresholds = dict(experiment.DEFAULT_THRESHOLDS)
    for k, v in (spec.get("thresholds") or {}).items():
        thresholds[k] = v
    return experiment.Experiment(
        name=spec["name"],
        strategies=spec.get("strategies", "rust"),
        universe=spec.get("universe", "all-harvested"),
        asset_classes=spec.get("asset_classes", list(experiment.VALID_ASSET_CLASSES)),
        granularity=spec.get("granularity", 3600),
        window_bars=spec.get("window_bars", 5000),
        start_ts=spec.get("start_ts"),
        end_ts=spec.get("end_ts"),
        thresholds=thresholds,
        walk_forward_folds=spec.get("walk_forward_folds", 0) or 0,
        ensemble=bool(spec.get("ensemble", False)),
        ensemble_min_groups=spec.get("ensemble_min_groups", 2) or 2,
        config_ref=spec.get("config_ref", ""),
        notes=spec.get("notes", ""),
    )


def _find_baseline(specs: List[Dict[str, Any]], baseline_name: str | None) -> Dict[str, Any] | None:
    if baseline_name:
        for s in specs:
            if s["name"] == baseline_name:
                return s
        return None
    for s in specs:
        if s["name"].startswith("baseline"):
            return s
    return None


def _load_scorecard(name: str) -> Dict[str, Any]:
    from scripts.backtest_framework.experiment import experiments_dir
    path = os.path.join(experiments_dir(), name, "scorecard.json")
    with open(path) as f:
        return json.load(f)


def run_suite(specs: List[Dict[str, Any]], baseline_name: str | None,
              fail_on_regression: bool, universe_override: str | None = None) -> Dict[str, Any]:
    # Apply universe override to each spec (useful for fast CI / smoke runs).
    if universe_override:
        for s in specs:
            s["universe"] = universe_override

    baseline_spec = _find_baseline(specs, baseline_name)
    if baseline_spec is None:
        log.warning("No baseline found; running suite without regression gate.")

    ran: List[Dict[str, Any]] = []
    for spec in specs:
        exp = _build_experiment(spec)
        exp.validate()
        scorecard, _ = run_experiment.run(exp)
        run_experiment._persist(scorecard, exp)
        ran.append(scorecard)
        print(f"[suite] {scorecard['name']}: "
              f"tested={scorecard['n_strategies_tested']} "
              f"passed={scorecard['n_passed']} "
              f"pass_rate={scorecard['pass_rate']:.1%} "
              f"mean_sharpe={scorecard['mean_sharpe_passed']:.2f}")

    # Regression gate.
    regressions_total = 0
    rows = []
    if baseline_spec is not None:
        baseline_sc = _load_scorecard(baseline_spec["name"])
        print("\n=== Regression gate (baseline: %s) ===" % baseline_spec["name"])
        print(f"{'name':<20}{'pass_rate':>12}{'mean_sharpe':>14}{'regression':>14}")
        for sc in ran:
            if sc["name"] == baseline_spec["name"]:
                rows.append((sc["name"], sc["pass_rate"], sc["mean_sharpe_passed"], "baseline"))
                continue
            try:
                report = compare_experiments.compare(baseline_sc, sc, MAX_PASS_RATE_DROP)
            except KeyError:
                rows.append((sc["name"], sc["pass_rate"], sc["mean_sharpe_passed"], "n/a"))
                continue
            regressed = not report["passed"]
            regressions_total += 1 if regressed else 0
            rows.append((sc["name"], sc["pass_rate"], sc["mean_sharpe_passed"],
                         "YES" if regressed else "no"))
            if regressed:
                for r in report["regressions"]:
                    print(f"  - {sc['name']}: {r}")
        for name, pr, ms, rg in rows:
            print(f"{name:<20}{pr:>11.1%}{ms:>14.2f}{rg:>14}")

    result = {
        "n_ran": len(ran),
        "baseline": baseline_spec["name"] if baseline_spec else None,
        "regressions": regressions_total,
        "passed": regressions_total == 0 if baseline_spec else True,
    }
    print(f"\nSUITE RESULT: {'PASS' if result['passed'] else 'FAIL'} "
          f"(ran={result['n_ran']}, regressions={result['regressions']})")
    if fail_on_regression and not result["passed"]:
        sys.exit(1)
    return result


def report_ledger() -> None:
    """Read scripts/experiments/ledger.jsonl and print a sorted table."""
    from scripts.backtest_framework.experiment import experiments_dir
    ledger = os.path.join(experiments_dir(), "ledger.jsonl")
    entries: List[Dict[str, Any]] = []
    if os.path.exists(ledger):
        with open(ledger) as f:
            for ln, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    log.debug("skipping corrupt ledger line %d", ln)
    if not entries:
        print("No ledger entries found.")
        return
    entries.sort(key=lambda e: e.get("name", ""))
    print(f"{'name':<28}{'pass_rate':>12}{'n_passed':>10}{'mean_sharpe':>14}")
    for e in entries:
        pr = e.get("pass_rate", 0.0)
        np_ = e.get("n_passed", 0)
        ms = e.get("mean_sharpe_passed", 0.0)
        print(f"{str(e.get('name', '')):<28}{pr:>11.1%}{np_:>10}{ms:>14.2f}")


def cli():
    p = argparse.ArgumentParser(description="Run the standard backtest suite + regression gate")
    p.add_argument("--suite", default="", help="path to optional JSON suite override")
    p.add_argument("--baseline-name", default=None, dest="baseline_name",
                   help="explicit baseline experiment name (default: first 'baseline*')")
    p.add_argument("--fail-on-regression", action="store_true", dest="fail_on_regression",
                   default=True, help="exit non-zero if any candidate regresses")
    p.add_argument("--no-fail-on-regression", action="store_false", dest="fail_on_regression",
                   help="report regressions but do not fail the run")
    p.add_argument("--universe", default=None, help="override universe for all experiments")
    p.add_argument("--report", action="store_true", help="print ledger table and exit")
    args = p.parse_args()

    if args.report:
        report_ledger()
        return

    specs = _load_suite(args.suite)
    run_suite(specs, args.baseline_name, args.fail_on_regression, args.universe)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cli()
