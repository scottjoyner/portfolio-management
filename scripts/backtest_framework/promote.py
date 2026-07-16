"""Promote a passing backtesting experiment to a live trading config mode.

A validated experiment (one whose scorecard meets the regression gate) can be
promoted to a ``trading_system/configs/*.yaml`` mode. The yaml records:

  * ``mode`` / ``trading_mode`` (PAPER by default — never auto-promote to live)
  * ``enabled_strategies`` — the strategies that PASSED the experiment
  * provenance (``promoted_from_experiment``, ``promoted_scorecard``)
  * the experiment's thresholds as a structured block the trader can read

The companion ``<name>.promoted.json`` carries the full machine-readable
promotion record (passing strategy list + threshold map) so a future trader
change can load validated edges directly.

Promotion is intentionally conservative: it only emits a PAPER-mode yaml and
never enables live trading on its own.
"""
from __future__ import annotations

import json
import os
from typing import Dict, List


PAPER_MODE = "PAPER"


def _passing_strategies(scorecard: Dict) -> List[str]:
    out = []
    for ck, r in scorecard.get("results", {}).items():
        if r.get("passed"):
            out.append(r["strategy"])
    # de-duplicate, preserve order
    seen, uniq = set(), []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def build_yaml(name: str, scorecard: Dict, mode: str = PAPER_MODE) -> str:
    strategies = _passing_strategies(scorecard)
    lines = [
        f"# Auto-promoted from backtesting experiment '{scorecard.get('name')}'.",
        f"# Passing strategies below survived the experiment thresholds + regression gate.",
        f"mode: {mode}",
        f"trading_mode: {mode}",
        f"require_approvals: true",
        f"promoted_from_experiment: {scorecard.get('name')}",
        f"promoted_pass_rate: {scorecard.get('pass_rate')}",
        f"promoted_mean_sharpe: {scorecard.get('mean_sharpe_passed')}",
        "enabled_strategies: ["
        + ", ".join(strategies) + "]" if strategies else "enabled_strategies: []",
        "",
    ]
    return "\n".join(lines)


def promote(name: str, scorecard_path: str, out_dir: str,
            mode: str = PAPER_MODE) -> Dict[str, str]:
    """Write ``<out_dir>/<name>.yaml`` + ``<name>.promoted.json``. Returns paths."""
    with open(scorecard_path) as f:
        scorecard = json.load(f)
    strategies = _passing_strategies(scorecard)
    yaml_text = build_yaml(name, scorecard, mode=mode)
    os.makedirs(out_dir, exist_ok=True)
    yaml_path = os.path.join(out_dir, f"{name}.yaml")
    json_path = os.path.join(out_dir, f"{name}.promoted.json")
    with open(yaml_path, "w") as f:
        f.write(yaml_text)
    record = {
        "promoted_from_experiment": scorecard.get("name"),
        "mode": mode,
        "pass_rate": scorecard.get("pass_rate"),
        "mean_sharpe_passed": scorecard.get("mean_sharpe_passed"),
        "thresholds": scorecard.get("thresholds"),
        "passing_strategies": strategies,
    }
    with open(json_path, "w") as f:
        json.dump(record, f, indent=2)
    return {"yaml": yaml_path, "json": json_path, "n_strategies": len(strategies)}
