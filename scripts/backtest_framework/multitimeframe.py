"""Multi-timeframe confirmation experiment for the iterative backtesting framework.

The paper trader's real edge comes from MULTI-TIMEFRAME confirmation: a signal on
a primary granularity (e.g. 1h) is only traded if a higher confirmation granularity
(e.g. 1d or 4h) trend agrees. This module measures whether requiring a strategy to
pass on BOTH granularities is a more stable filter than single-timeframe passing.

Each strategy "passes" a symbol only if it passes on the primary rows AND the
confirmation rows. We compare the multi-timeframe pass rate against the
single-timeframe pass rate; the ``lift`` (mtf - single) is typically negative,
quantifying how much stricter cross-timeframe agreement is.

Rust panics (e.g. impulse_exh/liq_vac, strategies.rs) are caught per-strategy via
BaseException, exactly like ``run_experiment.run``.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional


def _safe_backtest(S, name, currency, rows, thresholds) -> Optional[Any]:
    """Run strategy_engine.backtest_strategy on ``rows``; return verdict or None.

    Catches BaseException (incl. pyo3 PanicException) so one bad strategy does
    not abort the experiment.
    """
    if len(rows) < 40:
        return None
    closes = [r[4] for r in rows]
    volumes = [r[5] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    min_trades = int(thresholds.get("min_trades", 5))
    try:
        return S.backtest_strategy(
            name, currency, closes, volumes,
            highs=highs, lows=lows, warmup=30, min_trades=min_trades,
        )
    except BaseException:
        return None


def mtf_backtest(strategy_engine_module, strategy_names: List[str], currency: str,
                 rows_primary: List[Any], rows_confirm: List[Any],
                 thresholds: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    """Backtest each strategy on primary + confirm rows.

    Returns dict strategy_name -> {
        "primary_passed": bool, "confirm_passed": bool, "mtf_passed": bool,
        "primary_sharpe": float, "confirm_sharpe": float,
    }.
    """
    S = strategy_engine_module
    out: Dict[str, Dict[str, Any]] = {}
    for name in strategy_names:
        prim_v = _safe_backtest(S, name, currency, rows_primary, thresholds)
        conf_v = _safe_backtest(S, name, currency, rows_confirm, thresholds)

        prim_passed = bool(prim_v is not None and prim_v.passed)
        conf_passed = bool(conf_v is not None and conf_v.passed)
        out[name] = {
            "primary_passed": prim_passed,
            "confirm_passed": conf_passed,
            "mtf_passed": prim_passed and conf_passed,
            "primary_sharpe": round(prim_v.sharpe_ratio, 4) if prim_v is not None else 0.0,
            "confirm_sharpe": round(conf_v.sharpe_ratio, 4) if conf_v is not None else 0.0,
        }
    return out


def mtf_summary(per_symbol_results: Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Any]:
    """Aggregate per-symbol mtf_backtest results into a summary.

    ``per_symbol_results`` is currency -> (strategy -> mtf dict) OR a flat
    strategy -> mtf dict (single symbol). Handles both shapes.

    Returns {
        "n_strategies": int,
        "n_mtf_passed": int, "mtf_pass_rate": float,
        "n_single_passed": int, "single_pass_rate": float,
        "lift": float (mtf_pass_rate - single_pass_rate),
    }.
    """
    # Detect shape: values are dicts of mtf dicts (per-symbol) vs mtf dicts.
    flat: Dict[str, Dict[str, Any]] = {}
    sample = next(iter(per_symbol_results.values())) if per_symbol_results else {}
    nested = bool(sample) and isinstance(next(iter(sample.values())), dict) \
        and "mtf_passed" not in sample

    if nested:
        for sym, stratmap in per_symbol_results.items():
            for strat, d in stratmap.items():
                flat[f"{sym}/{strat}"] = d
    else:
        flat = dict(per_symbol_results)

    n = len(flat)
    if n == 0:
        return {
            "n_strategies": 0, "n_mtf_passed": 0, "mtf_pass_rate": 0.0,
            "n_single_passed": 0, "single_pass_rate": 0.0, "lift": 0.0,
        }

    n_mtf = sum(1 for d in flat.values() if d.get("mtf_passed"))
    n_single = sum(1 for d in flat.values()
                   if d.get("primary_passed") and d.get("confirm_passed") is not None)
    # single-timeframe "passed" = passed on EITHER granularity (union is the
    # relevant single-tf denominator the confirmation is stricter than). Use
    # primary as the canonical single-timeframe measure to match run_experiment.
    n_single = sum(1 for d in flat.values() if d.get("primary_passed"))
    mtf_rate = n_mtf / n
    single_rate = n_single / n
    return {
        "n_strategies": n,
        "n_mtf_passed": n_mtf,
        "mtf_pass_rate": round(mtf_rate, 4),
        "n_single_passed": n_single,
        "single_pass_rate": round(single_rate, 4),
        "lift": round(mtf_rate - single_rate, 4),
    }
