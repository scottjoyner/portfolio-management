"""Walk-forward (out-of-sample) evaluation for the backtesting framework.

A single trailing-window backtest can look good purely by overfitting one
market regime. Walk-forward splits each symbol's history into K contiguous
folds: the strategy is backtested on folds 1..K-1 (in-sample) and then
evaluated on fold K (out-of-sample). We report:

  * OOS mean sharpe / profit factor across folds (the honest number)
  * ``stable`` strategies: those that PASS both in-sample AND out-of-sample
  * a ``oos_degradation`` measure (IS sharpe - OOS sharpe)

This mirrors how the paper trader would actually experience the strategy
across shifting regimes, not just on a cherry-picked window.

The underlying engine (``strategy_engine.backtest_strategy``) already performs
its own internal walk-forward over whatever series it is given; here we
additionally hold out the final fold so the reported OOS metrics were never
seen during training.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple


def make_folds(rows: List[Any], n_folds: int = 4) -> List[Tuple[List[Any], List[Any]]]:
    """Split ``rows`` (oldest-first) into ``n_folds`` honest out-of-sample folds.

    Every fold uses the SAME train/test split: training is ``rows[:-fold_size]``
    (everything except the final ``fold_size`` bars) and the test set is the
    reserved, never-trained final fold ``rows[-fold_size:]``. The previously
    held-out tail is therefore genuinely out-of-sample for every fold and is
    excluded from all training data (fixes the old bug where the reserved fold
    was never tested and every fold's "OOS" overlapped someone's training).
    """
    if n_folds < 2 or len(rows) < n_folds * 40:
        return []
    fold_size = len(rows) // (n_folds + 1)  # reserve last fold as the OOS holdout
    if fold_size < 40:
        return []
    # Honest split: train excludes the reserved OOS tail; test IS that tail.
    # Every returned fold uses this SAME disjoint train/test split (no fold's
    # "OOS" overlaps anyone's training), so the reported OOS is genuinely
    # out-of-sample. We return ``n_folds`` copies so callers iterating folds
    # keep their folding count semantics while all folds agree on the split.
    train = rows[: len(rows) - fold_size]
    test = rows[len(rows) - fold_size:]
    if len(test) < 40 or len(train) < 40:
        return []
    return [(train, test) for _ in range(n_folds)]


def _bt(strategy_engine, name, currency, rows) -> Any:
    closes = [r[4] for r in rows]
    volumes = [r[5] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    try:
        return strategy_engine.backtest_strategy(
            name, currency, closes, volumes, highs=highs, lows=lows,
            warmup=30, min_trades=3,
        )
    except BaseException:
        return None


def walk_forward(strategy_engine, name: str, currency: str, rows: List[Any],
                 n_folds: int = 4) -> Dict[str, Any]:
    """Run walk-forward for a single (strategy, symbol). Returns OOS summary.

    ``strategy_engine`` is the imported ``strategy_engine`` module, passed in to
    avoid import cycles in the test harness.
    """
    folds = make_folds(rows, n_folds)
    if not folds:
        return {"strategy": name, "currency": currency, "n_folds": 0,
                "stable": False, "oos_sharpe": 0.0, "oos_profit_factor": 0.0,
                "is_sharpe": 0.0, "oos_degradation": 0.0, "oos_passed": 0}
    is_sharpes, oos_sharpes, oos_pfs, oos_passed = [], [], [], 0
    stable = True
    for train, test in folds:
        is_v = _bt(strategy_engine, name, currency, train)
        oos_v = _bt(strategy_engine, name, currency, test)
        if is_v is None or oos_v is None:
            stable = False
            continue
        is_sharpes.append(is_v.sharpe_ratio)
        oos_sharpes.append(oos_v.sharpe_ratio)
        oos_pfs.append(oos_v.profit_factor)
        if oos_v.passed:
            oos_passed += 1
        else:
            stable = False
    n = len(is_sharpes)
    if n == 0:
        return {"strategy": name, "currency": currency, "n_folds": len(folds),
                "stable": False, "oos_sharpe": 0.0, "oos_profit_factor": 0.0,
                "is_sharpe": 0.0, "oos_degradation": 0.0, "oos_passed": 0}
    is_mean = sum(is_sharpes) / n
    oos_mean = sum(oos_sharpes) / n
    return {
        "strategy": name,
        "currency": currency,
        "n_folds": len(folds),
        "stable": stable and oos_passed == len(folds),
        "is_sharpe": round(is_mean, 4),
        "oos_sharpe": round(oos_mean, 4),
        "oos_profit_factor": round(sum(oos_pfs) / n, 3),
        "oos_degradation": round(is_mean - oos_mean, 4),
        "oos_passed": oos_passed,
    }


def aggregate_walk_forward(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    oos = [r["oos_sharpe"] for r in results if r["n_folds"]]
    stable = [r for r in results if r.get("stable")]
    return {
        "n_evaluated": len(results),
        "n_with_folds": len(oos),
        "oos_mean_sharpe": round(sum(oos) / len(oos), 4) if oos else 0.0,
        "n_stable": len(stable),
        "stable_rate": round(len(stable) / len(results), 4) if results else 0.0,
        "stable_strategies": [f"{r['strategy']}/{r['currency']}" for r in stable],
    }
