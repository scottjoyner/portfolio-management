"""Ensemble / multi-strategy consensus evaluation for the backtesting framework.

Finding from E14 walk-forward: **no single strategy is stable out-of-sample** on
the harvested window (stable_rate 0.0%). The live trader never bets on one
strategy in isolation — it bets when *multiple independent* strategies agree
(see ``confidence_matrix.ConfidenceMatrix``, which boosts by independence-group
agreement). This module operationalizes that for backtesting:

For each symbol we take the per-strategy verdicts already produced by
``run_experiment`` and ask: how many *independent* strategy groups have at least
one passing strategy? A symbol with consensus across >= ``min_groups`` independent
groups is a genuine tradeable candidate; a symbol where only one group (or none)
passes is noise.

We build a combined "ensemble" metric per symbol:

  * ``passing_groups``      — set of independence groups with >=1 passing strategy
  * ``ensemble_sharpe``     — group-weighted sum of passing sharpes
                              (weights from ``confidence_matrix.CLASS_BOOST``)
  * ``consensus``           — True iff ``len(passing_groups) >= min_groups``
  * ``ensemble_coverage``   — fraction of symbols reaching consensus

This reuses the SAME engine verdicts as the single-strategy run, so it costs
nothing extra to compute, and it directly tests the hypothesis that *combining
independent strategies is more robust than any single one*.
"""
from __future__ import annotations

from typing import Dict, List, Set

try:
    from confidence_matrix import STRATEGY_GROUP, CLASS_BOOST
except Exception:  # pragma: no cover - defensive
    STRATEGY_GROUP = {}
    CLASS_BOOST = {}

DEFAULT_MIN_GROUPS = 2


def _group_of(strategy: str) -> str:
    return STRATEGY_GROUP.get(strategy, "other")


def ensemble_consensus(results: Dict[str, Dict], asset_class: str = "growth",
                       min_groups: int = DEFAULT_MIN_GROUPS) -> Dict[str, Dict]:
    """Compute per-symbol ensemble consensus from a scorecard ``results`` map.

    ``results`` is ``{ "strategy/currency": {strategy, currency, passed, sharpe, ...} }``.
    Returns ``{ currency: {passing_groups, ensemble_sharpe, consensus, n_passing} }``.
    """
    by_currency: Dict[str, List[Dict]] = {}
    for r in results.values():
        by_currency.setdefault(r["currency"], []).append(r)

    out: Dict[str, Dict] = {}
    for currency, rs in by_currency.items():
        passing = [r for r in rs if r.get("passed")]
        groups: Dict[str, float] = {}
        for r in passing:
            g = _group_of(r["strategy"])
            # keep the best (most positive) sharpe seen for that group
            groups[g] = max(groups.get(g, float("-inf")), r.get("sharpe", 0.0))
        boost = CLASS_BOOST.get(asset_class, {})
        # group-weighted combined sharpe; groups that didn't pass contribute 0
        esharpe = 0.0
        for g, sh in groups.items():
            w = boost.get(g, 1.0)
            esharpe += max(0.0, sh) * w
        passing_groups = set(groups.keys())
        out[currency] = {
            "currency": currency,
            "passing_groups": sorted(passing_groups),
            "n_passing_groups": len(passing_groups),
            "n_passing_strategies": len(passing),
            "ensemble_sharpe": round(esharpe, 4),
            "consensus": len(passing_groups) >= min_groups,
            "min_groups": min_groups,
        }
    return out


def ensemble_summary(consensus: Dict[str, Dict]) -> Dict[str, object]:
    """Aggregate ensemble consensus across symbols."""
    n = len(consensus)
    reached = [c for c in consensus.values() if c["consensus"]]
    esharpes = [c["ensemble_sharpe"] for c in consensus.values()]
    best = max(consensus.values(), key=lambda c: c["ensemble_sharpe"]) if consensus else None
    return {
        "n_symbols": n,
        "consensus_coverage": round(len(reached) / n, 4) if n else 0.0,
        "n_consensus": len(reached),
        "mean_ensemble_sharpe": round(sum(esharpes) / n, 4) if n else 0.0,
        "best_symbol": best["currency"] if best else None,
        "best_ensemble_sharpe": best["ensemble_sharpe"] if best else 0.0,
        "consensus_symbols": [c["currency"] for c in reached],
    }
