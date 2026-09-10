"""Walk-forward (out-of-sample) evaluation for the backtesting framework.

A single trailing-window backtest can look good purely by overfitting one market
regime.  This module performs chronological expanding-window evaluation: every
OOS fold is a distinct future interval and no fold trains on observations from
its own future.

The previous implementation returned ``n_folds`` copies of one final holdout.
That preserved an old caller count but made four reported OOS folds equivalent
to testing the same holdout four times.  That can overstate stability and is no
longer permitted.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple

from scripts.alpha_validation import generate_walk_forward_splits


def make_folds(
    rows: List[Any],
    n_folds: int = 4,
    *,
    purge_size: int = 0,
    embargo_size: int = 0,
) -> List[Tuple[List[Any], List[Any]]]:
    """Create distinct chronological expanding walk-forward train/test folds.

    With 1,000 observations and ``n_folds=4`` (no gap), the logical layout is::

        train 0:200   -> test 200:400
        train 0:400   -> test 400:600
        train 0:600   -> test 600:800
        train 0:800   -> test 800:1000

    Earlier OOS intervals may become training data for *later* folds, which is
    valid chronological walk-forward behavior: those observations are already
    in the past by the time the later fold is evaluated.  What is forbidden is
    any observation from the current/future test interval entering that fold's
    training data.

    ``purge_size`` and ``embargo_size`` insert an excluded pre-test gap through
    the canonical split generator.  They are zero by default to preserve the
    historical public call shape while allowing stricter callers to opt in.
    """

    if n_folds < 2:
        return []
    if purge_size < 0 or embargo_size < 0:
        raise ValueError("purge_size and embargo_size must be non-negative")

    # We need n_folds test windows plus at least one initial training window.
    # Account for the one pre-test gap that the canonical expanding splitter
    # applies before each current test boundary.
    available = len(rows) - purge_size - embargo_size
    fold_size = available // (n_folds + 1) if available > 0 else 0
    if fold_size < 40:
        return []

    boundaries = generate_walk_forward_splits(
        len(rows),
        train_size=fold_size,
        test_size=fold_size,
        step_size=fold_size,
        purge_size=purge_size,
        embargo_size=embargo_size,
        expanding=True,
    )
    boundaries = boundaries[:n_folds]
    if len(boundaries) != n_folds:
        return []

    folds: List[Tuple[List[Any], List[Any]]] = []
    seen_tests: set[tuple[int, int]] = set()
    for boundary in boundaries:
        test_key = (boundary.test_start, boundary.test_end)
        if test_key in seen_tests:
            raise RuntimeError("walk-forward generator produced a duplicate OOS interval")
        seen_tests.add(test_key)
        train = rows[boundary.train_start:boundary.train_end]
        test = rows[boundary.test_start:boundary.test_end]
        if len(train) < 40 or len(test) < 40:
            return []
        folds.append((train, test))
    return folds


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
    """Run chronological walk-forward for one (strategy, symbol)."""

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
