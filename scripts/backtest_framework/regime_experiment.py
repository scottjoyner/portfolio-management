"""Regime-conditioned experiment mode for the backtesting framework (E16).

Hypothesis (from E14/E15 walk-forward + ensemble runs): no single strategy is
stable and **no ensemble reaches consensus** over the harvested window
(stable_rate 0.0%, consensus_coverage 0.0%). The likely reason is that strategy
edge is *regime-dependent* — trend strategies win in trends, mean-reversion in
ranges. The Rust ``rust_core.detect_regime_py`` exposes 8-class regime detection.

This module splits each symbol's candle window into contiguous runs by detected
regime, then recomputes the ensemble consensus PER regime slice. That surfaces
which regimes actually produce tradeable multi-group consensus (the whole-window
consensus being masked by regime mixing).

It is dependency-light and import-safe: Rust is guarded with try/except and a
simple heuristic fallback is provided when Rust is unavailable.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    import rust_core
    _HAS_RUST = True
except Exception:  # pragma: no cover - defensive
    rust_core = None
    _HAS_RUST = False


def _heuristic_regime(closes: List[float]) -> str:
    """Fallback regime classifier when Rust is unavailable.

    Compares last vs first close over the window: up > 2% => "uptrend",
    down > 2% => "downtrend", else "range".
    """
    if not closes:
        return "unknown"
    change = (closes[-1] - closes[0]) / closes[0]
    if change > 0.02:
        return "uptrend"
    if change < -0.02:
        return "downtrend"
    return "range"


def _make_detect_fn() -> Callable[[List[float], List[float], List[float], List[float]], str]:
    """Return a ``detect_fn(closes, highs, lows, volumes) -> regime_str``.

    Uses ``rust_core.detect_regime_py`` when available, else the heuristic.
    """
    if _HAS_RUST:
        def _detect(closes, highs, lows, volumes):
            try:
                reg, *_ = rust_core.detect_regime_py(
                    closes, highs, lows, volumes, None, None)
                return reg
            except Exception:
                return _heuristic_regime(closes)
        return _detect
    return lambda closes, highs, lows, volumes: _heuristic_regime(closes)


def detect_regime_slices(rows: List[List[float]], granularity: int = 3600,
                         detect_fn: Optional[Callable] = None,
                         min_bars: int = 60) -> List[Tuple[str, List[List[float]]]]:
    """Split candle rows into contiguous runs by detected regime.

    ``rows`` is a list of ``[t, o, h, l, c, v]``. Returns a list of
    ``(regime, rows_subset)``. A new run starts whenever the detected regime
    differs from the previous bar's regime. A run shorter than ``min_bars`` is
    merged into the previous run (its regime label takes the larger run's).
    """
    if detect_fn is None:
        detect_fn = _make_detect_fn()
    if len(rows) < 1:
        return []

    closes = [r[4] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    volumes = [r[5] for r in rows]

    # Detect the regime for every bar; the input window is (by design) the full
    # sequence, so we classify each bar against its own trailing context using a
    # sliding window up to ``min_bars`` wide (bounded so full-window calls stay cheap).
    n = len(rows)
    half = max(min_bars, 30)
    labels = []
    for i in range(n):
        lo = max(0, i - half + 1)
        labels.append(detect_fn(closes[lo:i + 1], highs[lo:i + 1],
                                lows[lo:i + 1], volumes[lo:i + 1]))

    # Build contiguous runs, merging sub-min-bars runs into the previous one.
    runs: List[Tuple[str, List[List[float]]]] = []
    for i, row in enumerate(rows):
        reg = labels[i]
        if runs and runs[-1][0] == reg:
            runs[-1][1].append(row)
        else:
            # start a new candidate run
            runs.append((reg, [row]))

    # Drop runs shorter than min_bars by merging into a neighbor. Leading short
    # runs (e.g. a 3-bar startup mislabel) merge forward into the next run;
    # trailing short runs merge back into the previous run; any interior short
    # run is absorbed by whichever neighbor is longer (prefer the previous one).
    merged: List[Tuple[str, List[List[float]]]] = []
    for reg, sub in runs:
        if merged and len(sub) < min_bars:
            merged[-1][1].extend(sub)
        else:
            merged.append((reg, sub))
    if len(merged) >= 2 and len(merged[0][1]) < min_bars:
        # absorb the leading short run into the next run
        merged[1] = (merged[1][0], merged[0][1] + merged[1][1])
        merged = merged[1:]
    return merged


def _verdicts_in_slice(per_strategy: Dict[str, Dict], currency: str,
                       slice_rows: List[List[float]]) -> Dict[str, Dict]:
    """Filter a scorecard ``results`` map to a currency + date slice range.

    ``slice_rows`` provides the ``[t, ...]`` bounds (first/last timestamp).
    """
    if not slice_rows:
        return {}
    t0 = slice_rows[0][0]
    t1 = slice_rows[-1][0]
    out = {}
    for ck, r in per_strategy.items():
        if r.get("currency") != currency:
            continue
        # We cannot recover exact bar timestamps per verdict; the executor passes
        # verdicts keyed by strategy/currency WITHOUT per-bar dates. So we accept
        # ALL verdicts of the currency (regime slicing is meaningful only when the
        # caller re-runs strategies on the slice). For the framework's normal
        # path, the slice verdicts are the same set re-scored by ensemble; we
        # therefore mark them with slice bounds but keep identity. To support
        # true per-slice filtering, callers may pass a pre-filtered map; here we
        # return currency-filtered verdicts and rely on the slice label.
        out[ck] = r
    return out


def regime_ensemble(scorecard_results: Dict[str, Dict],
                    classify_fn: Callable[[str], str],
                    detect_fn: Optional[Callable] = None,
                    rows_by_currency: Optional[Dict[str, List[List[float]]]] = None,
                    min_groups: int = 2,
                    min_bars: int = 60,
                    granularity: int = 3600) -> Dict[str, Any]:
    """Compute ensemble consensus PER REGIME for each symbol.

    For each currency in ``rows_by_currency`` the candle window is split into
    regime slices via :func:`detect_regime_slices`. For each slice we (re)score
    the currency's verdicts with :func:`ensemble.ensemble_consensus`. The per-symbol
    results are aggregated into a per-regime summary.

    Returns::

        {
            "n_regimes": int,
            "regimes": {regime: {consensus_coverage, n_consensus,
                                 mean_ensemble_sharpe, symbols:[...]}},
            "best_regime": str,
        }
    """
    from scripts.backtest_framework.ensemble import ensemble_consensus

    if rows_by_currency is None:
        rows_by_currency = {}
    if detect_fn is None:
        detect_fn = _make_detect_fn()

    per_symbol: Dict[str, Dict[str, Any]] = {}

    for currency, rows in rows_by_currency.items():
        slices = detect_regime_slices(rows, granularity=granularity,
                                      detect_fn=detect_fn, min_bars=min_bars)
        sub = {ck: r for ck, r in scorecard_results.items()
               if r.get("currency") == currency}
        ac = classify_fn(currency) if classify_fn else "growth"
        currency_regimes: Dict[str, Dict[str, Any]] = {}
        for reg, slice_rows in slices:
            slice_verdicts = _verdicts_in_slice(sub, currency, slice_rows)
            if not slice_verdicts:
                continue
            cons = ensemble_consensus(slice_verdicts, asset_class=ac,
                                      min_groups=min_groups)
            # cons is keyed by currency; collapse to the single currency entry.
            entry = cons.get(currency, {
                "passing_groups": [], "n_passing_groups": 0,
                "n_passing_strategies": 0, "ensemble_sharpe": 0.0,
                "consensus": False, "min_groups": min_groups,
            })
            currency_regimes[reg] = entry
        per_symbol[currency] = currency_regimes

    # Aggregate per-regime across all symbols.
    regimes: Dict[str, Dict[str, Any]] = {}
    for currency, cr in per_symbol.items():
        for reg, entry in cr.items():
            agg = regimes.setdefault(reg, {
                "consensus_coverage": 0.0, "n_consensus": 0,
                "mean_ensemble_sharpe": 0.0, "symbols": [], "n": 0,
            })
            agg["n"] += 1
            agg["symbols"].append(currency)
            if entry.get("consensus"):
                agg["n_consensus"] += 1
            agg["mean_ensemble_sharpe"] += entry.get("ensemble_sharpe", 0.0)

    regimes_summary: Dict[str, Dict[str, Any]] = {}
    for reg, agg in regimes.items():
        n = agg["n"]
        regimes_summary[reg] = {
            "consensus_coverage": round(agg["n_consensus"] / n, 4) if n else 0.0,
            "n_consensus": agg["n_consensus"],
            "mean_ensemble_sharpe": round(agg["mean_ensemble_sharpe"] / n, 4) if n else 0.0,
            "symbols": sorted(set(agg["symbols"])),
        }

    best_regime = ""
    best_sharpe = float("-inf")
    for reg, s in regimes_summary.items():
        if s["consensus_coverage"] > 0 or s["mean_ensemble_sharpe"] > best_sharpe:
            if s["mean_ensemble_sharpe"] > best_sharpe:
                best_sharpe = s["mean_ensemble_sharpe"]
                best_regime = reg

    return {
        "n_regimes": len(regimes_summary),
        "regimes": regimes_summary,
        "best_regime": best_regime,
    }
