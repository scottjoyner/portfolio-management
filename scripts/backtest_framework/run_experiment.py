"""Execute a single backtesting Experiment and persist a comparable scorecard.

Reuses ``strategy_engine.batch_backtest_rust`` (the same Rust engine the paper
trader evaluates against) so a backtest result is directly comparable to live
behavior. The scorecard is written to ``experiments/<name>/scorecard.json`` and
appended to ``experiments/ledger.jsonl`` so experiments can be diffed later.

Usage:
    python3 scripts/backtest_framework/run_experiment.py \
        --name baseline_v1 --granularity 3600 --strategies rust \
        --universe all-harvested --window-bars 5000 --min-sharpe 0.3
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import strategy_engine as S
from data.feed_cache import load_candles, _root
from scripts.backtest_framework.experiment import (
    Experiment, resolve_strategies, evaluate_verdict, experiments_dir,
)

log = logging.getLogger("run_experiment")

KIND = "coinbase_candles"


def _discover_symbols(granularity: int, universe: str) -> List[str]:
    if universe and universe != "all-harvested":
        return [s.strip() for s in universe.split(",") if s.strip()]
    base = os.path.join(_root(), KIND)
    if not os.path.isdir(base):
        return []
    out = []
    for name in os.listdir(base):
        if os.path.exists(os.path.join(base, name, f"{granularity}.parquet")):
            out.append(name)
    return sorted(out)


def _load_window(symbol: str, granularity: int, exp: Experiment):
    rows = load_candles(KIND, symbol, granularity)
    if exp.window_bars and len(rows) > exp.window_bars:
        rows = rows[-exp.window_bars:]
    if exp.start_ts or exp.end_ts:
        st, en = exp.start_ts or 0, exp.end_ts or 10 ** 12
        rows = [r for r in rows if st <= r[0] <= en]
    return rows


def run(exp: Experiment) -> Dict[str, Any]:
    exp.validate()
    from scripts.backtest_framework.multitimeframe import mtf_backtest, mtf_summary

    strategy_names = resolve_strategies(exp.strategies)
    symbols = _discover_symbols(exp.granularity, exp.universe)
    log.info("Experiment %s: %d strategies x %d symbols @ %ds",
             exp.name, len(strategy_names), len(symbols), exp.granularity)
    mtf_enabled = exp.confirm_granularity and exp.confirm_granularity > 0
    if mtf_enabled:
        confirm_symbols = _discover_symbols(exp.confirm_granularity, exp.universe)

    # Run one backtest per (strategy, symbol). We deliberately avoid the grouped
    # batch_backtest_rust path here: a known Rust panic in a single strategy
    # (impulse_exh/liq_vac, strategies.rs:1078/1216) aborts the whole product call.
    # Calling backtest_strategy per strategy and catching BaseException (which
    # includes pyo3 PanicException) keeps the experiment resilient, matching
    # scripts/backtest_from_cache.py.
    min_trades = int(exp.thresholds.get("min_trades", 5))
    raw = {}
    wf_rows = {}  # (strategy, currency) -> full rows, for optional walk-forward
    symbol_rows = {}  # currency -> full rows, for regime splitting
    for sym in symbols:
        rows = _load_window(sym, exp.granularity, exp)
        if len(rows) < min_trades + 20:
            continue
        closes = [r[4] for r in rows]
        volumes = [r[5] for r in rows]
        highs = [r[2] for r in rows]
        lows = [r[3] for r in rows]
        currency = sym.split("-")[0]
        symbol_rows[currency] = rows
        for name in strategy_names:
            try:
                v = S.backtest_strategy(
                    name, currency, closes, volumes,
                    highs=highs, lows=lows, warmup=30, min_trades=min_trades,
                )
            except BaseException as e:  # catches Rust PanicException from bad strategies
                log.debug("backtest %s/%s failed: %s", sym, name, e)
                continue
            raw[f"{v.strategy}/{v.currency}"] = v
            wf_rows[(name, currency)] = rows

    # Apply experiment thresholds + aggregate a scorecard.
    per_strategy: Dict[str, Dict[str, Any]] = {}
    passed = 0
    sharpes, pfs, drawdowns = [], [], []
    for ck, v in raw.items():
        ok = evaluate_verdict(v, exp.thresholds)
        if ok:
            passed += 1
            sharpes.append(v.sharpe_ratio)
            pfs.append(v.profit_factor)
            drawdowns.append(v.max_drawdown_pct)
        per_strategy[ck] = {
            "strategy": v.strategy,
            "currency": v.currency,
            "passed": ok,
            "total_trades": v.total_trades,
            "win_rate": round(v.win_rate, 4),
            "total_return_pct": round(v.total_return_pct, 3),
            "sharpe": round(v.sharpe_ratio, 4),
            "profit_factor": round(v.profit_factor, 3),
            "max_drawdown_pct": round(v.max_drawdown_pct, 3),
            "avg_trade_pct": round(getattr(v, "avg_trade_pct", 0.0), 4),
            "regime": v.regime,
        }

    n = len(per_strategy)

    # Optional multi-timeframe confirmation: for each symbol, re-backtest the
    # same strategy set on a confirmation granularity and require BOTH to pass.
    multitimeframe_out = None
    if mtf_enabled:
        per_symbol_mtf: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for sym in symbols:
            if sym not in confirm_symbols:
                continue
            rows_primary = _load_window(sym, exp.granularity, exp)
            rows_confirm = _load_window(sym, exp.confirm_granularity, exp)
            if len(rows_primary) < min_trades + 20 or len(rows_confirm) < min_trades + 20:
                continue
            currency = sym.split("-")[0]
            per_symbol_mtf[sym] = mtf_backtest(S, strategy_names, currency,
                                               rows_primary, rows_confirm, exp.thresholds)
        if per_symbol_mtf:
            summ = mtf_summary(per_symbol_mtf)
            summ["confirm_granularity"] = exp.confirm_granularity
            multitimeframe_out = summ

    # Optional walk-forward out-of-sample evaluation (v2).
    wf_summary = None
    if exp.walk_forward_folds and exp.walk_forward_folds >= 2:
        from scripts.backtest_framework.walk_forward import walk_forward, aggregate_walk_forward
        wf_results = []
        for (name, currency), rows in wf_rows.items():
            wf_results.append(walk_forward(S, name, currency, rows, exp.walk_forward_folds))
        wf_summary = aggregate_walk_forward(wf_results)
        wf_summary["folds"] = exp.walk_forward_folds

    # Ensemble / independence-group consensus (tests the "combining independent
    # strategies is more robust than any single one" hypothesis). Computed from
    # the same per-strategy verdicts, so it is free after the single-strategy run.
    ensemble_summary_out = None
    if exp.ensemble:
        from scripts.backtest_framework.ensemble import ensemble_consensus, ensemble_summary
        try:
            from portfolio_optimizer import classify_asset
        except Exception:
            def classify_asset(c):
                return "growth"
        consensus_all: Dict[str, Dict] = {}
        for currency in {r["currency"] for r in per_strategy.values()}:
            ac = classify_asset(currency)
            sub = {ck: r for ck, r in per_strategy.items() if r["currency"] == currency}
            consensus_all.update(ensemble_consensus(sub, asset_class=ac,
                                                    min_groups=exp.ensemble_min_groups))
        ensemble_summary_out = ensemble_summary(consensus_all)
        ensemble_summary_out["min_groups"] = exp.ensemble_min_groups
        ensemble_summary_out["per_symbol"] = consensus_all

    # Regime-conditioned consensus (E16): recompute ensemble consensus PER regime
    # slice across each symbol's window, surfacing which regimes actually produce
    # tradeable multi-group consensus (whole-window consensus is masked by mixing).
    regime_summary_out = None
    if exp.regime:
        from scripts.backtest_framework.regime_experiment import regime_ensemble
        try:
            from portfolio_optimizer import classify_asset
        except Exception:
            def classify_asset(c):
                return "growth"
        regime_summary_out = regime_ensemble(
            per_strategy, classify_fn=classify_asset,
            rows_by_currency=symbol_rows, min_groups=exp.ensemble_min_groups,
            min_bars=exp.regime_min_bars, granularity=exp.granularity,
        )

    scorecard = {
        "name": exp.name,
        "config_ref": exp.config_ref,
        "strategies": exp.strategies,
        "universe": exp.universe,
        "asset_classes": exp.asset_classes,
        "granularity": exp.granularity,
        "window_bars": exp.window_bars,
        "thresholds": exp.thresholds,
        "n_symbols": len(symbols),
        "n_strategies_tested": len(per_strategy),
        "n_passed": passed,
        "pass_rate": round(passed / n, 4) if n else 0.0,
        "mean_sharpe_passed": round(sum(sharpes) / len(sharpes), 4) if sharpes else 0.0,
        "mean_profit_factor_passed": round(sum(pfs) / len(pfs), 3) if pfs else 0.0,
        "worst_drawdown_passed": round(max(drawdowns), 3) if drawdowns else 0.0,
        "walk_forward": wf_summary,
        "ensemble": ensemble_summary_out,
        "regime": regime_summary_out,
        "multitimeframe": multitimeframe_out,
        "results": per_strategy,
    }
    return scorecard, raw


def _write_bt_cache(db_path: str, raw: Dict[str, Any], only_passed: bool) -> int:
    """Persist backtest verdicts into the optimizer's StateStore bt_cache so
    ConfidenceMatrix picks up per-strategy weights. Keyed ``strategy/currency``.
    Returns the number of rows written.
    """
    from state_store import StateStore
    store = StateStore(db_path=db_path)
    n = 0
    for v in raw.values():
        if only_passed and not getattr(v, "passed", False):
            continue
        key = f"{v.strategy}/{v.currency}"
        try:
            store.save_bt_cache(key, v)
            n += 1
        except Exception as e:  # pragma: no cover - defensive
            log.debug("bt_cache save failed for %s: %s", key, e)
    return n


def _persist(scorecard: Dict[str, Any], exp: Experiment) -> str:
    base = os.path.join(experiments_dir(), exp.name)
    os.makedirs(base, exist_ok=True)
    sc_path = os.path.join(base, "scorecard.json")
    with open(sc_path, "w") as f:
        json.dump(scorecard, f, indent=2)
    ledger = os.path.join(experiments_dir(), "ledger.jsonl")
    with open(ledger, "a") as f:
        f.write(json.dumps({"name": exp.name, "scorecard": sc_path,
                            "pass_rate": scorecard["pass_rate"],
                            "n_passed": scorecard["n_passed"],
                            "mean_sharpe_passed": scorecard["mean_sharpe_passed"]}) + "\n")
    return sc_path


def cli():
    p = argparse.ArgumentParser(description="Run a single backtesting experiment")
    p.add_argument("--name", required=True, help="experiment name (also the output dir)")
    p.add_argument("--strategies", default="rust", help="'rust' | 'all' | comma list")
    p.add_argument("--universe", default="all-harvested", help="comma list or 'all-harvested'")
    p.add_argument("--asset-classes", default="safe,growth,speculative")
    p.add_argument("--granularity", type=int, default=3600)
    p.add_argument("--window-bars", type=int, default=5000)
    p.add_argument("--start-ts", type=float, default=None, dest="start_ts")
    p.add_argument("--end-ts", type=float, default=None, dest="end_ts")
    p.add_argument("--min-win-rate", type=float, default=None, dest="min_win_rate")
    p.add_argument("--min-sharpe", type=float, default=None, dest="min_sharpe")
    p.add_argument("--min-profit-factor", type=float, default=None, dest="min_profit_factor")
    p.add_argument("--min-trades", type=float, default=None, dest="min_trades")
    p.add_argument("--max-drawdown-pct", type=float, default=None, dest="max_drawdown_pct")
    p.add_argument("--min-avg-trade-pct", type=float, default=None, dest="min_avg_trade_pct",
                   help="minimum average per-trade return pct (net of fees) to pass")
    p.add_argument("--walk-forward", type=int, default=0, dest="walk_forward_folds",
                   help="walk-forward folds (>=2) for out-of-sample scoring")
    p.add_argument("--ensemble", action="store_true", dest="ensemble",
                   help="compute independence-group consensus across strategies")
    p.add_argument("--ensemble-min-groups", type=int, default=2, dest="ensemble_min_groups",
                    help="min independent groups agreeing for consensus")
    p.add_argument("--confirm-granularity", type=int, default=0, dest="confirm_granularity",
                    help="require strategy to ALSO pass on this confirmation granularity (0=off)")
    p.add_argument("--regime", action="store_true", dest="regime",
                   help="E16: compute ensemble consensus PER detected regime across each symbol window")
    p.add_argument("--regime-min-bars", type=int, default=60, dest="regime_min_bars",
                   help="min contiguous bars per regime slice (guard against noise)")
    p.add_argument("--config-ref", default="", dest="config_ref", help="provenance: live yaml mode")
    p.add_argument("--notes", default="")
    p.add_argument("--bt-cache-db", default="", dest="bt_cache_db",
                   help="optional StateStore db to persist verdicts into (feeds ConfidenceMatrix)")
    p.add_argument("--only-passed-cache", action="store_true", dest="only_passed_cache",
                   help="only write passing verdicts to bt_cache")
    args = p.parse_args()

    exp = Experiment.from_args(args)
    scorecard, raw = run(exp)
    sc_path = _persist(scorecard, exp)

    if args.bt_cache_db:
        n = _write_bt_cache(args.bt_cache_db, raw, args.only_passed_cache)
        print(f"bt_cache: wrote {n} verdicts -> {args.bt_cache_db}")

    print(f"\n=== Experiment {scorecard['name']} ===")
    print(f"symbols={scorecard['n_symbols']} tested={scorecard['n_strategies_tested']} "
          f"passed={scorecard['n_passed']} pass_rate={scorecard['pass_rate']:.1%}")
    print(f"mean_sharpe(passed)={scorecard['mean_sharpe_passed']:.2f} "
          f"mean_PF={scorecard['mean_profit_factor_passed']:.2f} "
          f"worst_DD={scorecard['worst_drawdown_passed']:.1f}%")
    wf = scorecard.get("walk_forward")
    if wf:
        print(f"walk_forward: OOS_mean_sharpe={wf['oos_mean_sharpe']:.3f} "
              f"stable={wf['n_stable']}/{wf['n_evaluated']} "
              f"stable_rate={wf['stable_rate']:.1%} folds={wf['folds']}")
    ens = scorecard.get("ensemble")
    if ens:
        print(f"ensemble: coverage={ens['consensus_coverage']:.1%} "
              f"n_consensus={ens['n_consensus']}/{ens['n_symbols']} "
              f"mean_ens_sharpe={ens['mean_ensemble_sharpe']:.3f} "
              f"best={ens['best_symbol']}({ens['best_ensemble_sharpe']:.2f}) "
              f"min_groups={ens['min_groups']}")
        if ens.get("consensus_symbols"):
            print(f"  consensus symbols: {', '.join(ens['consensus_symbols'])}")
    mtf = scorecard.get("multitimeframe")
    if mtf:
        print(f"multitimeframe(confirm={mtf['confirm_granularity']}s): "
              f"mtf_passed={mtf['n_mtf_passed']}/{mtf['n_strategies']} "
              f"mtf_rate={mtf['mtf_pass_rate']:.1%} "
              f"single_rate={mtf['single_pass_rate']:.1%} lift={mtf['lift']:+.1%}")
    reg = scorecard.get("regime")
    if reg:
        print(f"regime: n_regimes={reg['n_regimes']} best_regime={reg['best_regime']}")
        for rname, rinfo in sorted(reg["regimes"].items()):
            print(f"  {rname}: coverage={rinfo['consensus_coverage']:.1%} "
                  f"n_consensus={rinfo['n_consensus']} "
                  f"mean_ens_sharpe={rinfo['mean_ensemble_sharpe']:.3f} "
                  f"symbols={','.join(rinfo['symbols']) or '-'}")
    print(f"Scorecard -> {sc_path}")
    return scorecard


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cli()
