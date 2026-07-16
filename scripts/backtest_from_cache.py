#!/usr/bin/env python3
"""Backtest the 74 strategies against harvested feed_cache data — no API needed.

Reads candles persisted by ``scripts/collect_backtest_data.py`` or the live
paper trader (both write to ``data.feed_cache`` / ``NAS_FEED_ROOT``) and runs
``strategy_engine.backtest_strategy`` for every strategy on every symbol,
producing a ranked pass/fail report.

This is the read side of the data factory: harvest -> store -> backtest.

Usage:
    # All symbols present in coinbase_candles at 1h, all strategies
    python3 scripts/backtest_from_cache.py --granularity 3600

    # Explicit symbols + a specific strategy whitelist
    python3 scripts/backtest_from_cache.py --symbols BTC-USD,ETH-USD --granularity 3600

    # Restrict to a strategy subset and emit JSON
    python3 scripts/backtest_from_cache.py --granularity 3600 --strategies ema_cross,rsi_revert --json out.json
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import strategy_engine as S
from data.feed_cache import load_candles, _root

log = logging.getLogger("backtest_cache")

# feed_cache kind where collect_backtest_data.py / the trader persist candles
KIND = "coinbase_candles"


def _list_symbols(granularity: int, symbols: Optional[List[str]] = None) -> List[str]:
    if symbols:
        return symbols
    root = _root()
    base = os.path.join(root, KIND)
    if not os.path.isdir(base):
        return []
    out = []
    for name in os.listdir(base):
        d = os.path.join(base, name)
        if os.path.isdir(d) and os.path.exists(os.path.join(d, f"{granularity}.parquet")):
            out.append(name)
    return sorted(out)


def _run(symbol: str, granularity: int, strategies: List[str], warmup: int,
         min_trades: int, max_bars: int = 5000):
    """Return (rows, verdicts) where verdicts are raw BacktestVerdict objects."""
    rows = load_candles(KIND, symbol, granularity)
    if max_bars and len(rows) > max_bars:
        rows = rows[-max_bars:]  # trailing window for fast, meaningful validation
    if len(rows) < warmup + 20:
        return [], []
    closes = [r[4] for r in rows]
    opens = [r[1] for r in rows]
    highs = [r[2] for r in rows]
    lows = [r[3] for r in rows]
    volumes = [r[5] for r in rows]
    currency = symbol.split("-")[0]
    out = []
    verdicts = []
    for name in strategies:
        try:
            v = S.backtest_strategy(
                name, currency, closes, volumes,
                highs=highs, lows=lows, warmup=warmup, min_trades=min_trades,
            )
        except BaseException as e:  # catches Rust PanicException from bad strategies
            log.debug("backtest %s/%s failed: %s", symbol, name, e)
            continue
        verdicts.append(v)
        out.append({
            "symbol": symbol,
            "strategy": v.strategy,
            "passed": v.passed,
            "total_trades": v.total_trades,
            "win_rate": round(v.win_rate, 4),
            "total_return_pct": round(v.total_return_pct, 3),
            "sharpe": round(v.sharpe_ratio, 4),
            "profit_factor": round(v.profit_factor, 3),
            "max_drawdown_pct": round(v.max_drawdown_pct, 3),
            "regime": v.regime,
            "reason": v.reason,
        })
    return out, verdicts


def _args_for(granularity=3600, symbols="", strategies="", warmup=30,
            min_trades=3, max_bars=5000, include_external=False, top=25,
            bt_cache_db="", only_passed=False):
    """Build a lightweight args namespace for programmatic/test use."""
    class _A:
        pass
    a = _A()
    a.granularity = granularity
    a.symbols = symbols
    a.strategies = strategies
    a.warmup = warmup
    a.min_trades = min_trades
    a.max_bars = max_bars
    a.include_external = include_external
    a.top = top
    a.bt_cache_db = bt_cache_db
    a.only_passed = only_passed
    return a


def _write_bt_cache(db_path: str, verdicts, only_passed: bool) -> int:
    """Persist backtest verdicts into the optimizer's StateStore bt_cache so
    ConfidenceMatrix picks up per-strategy weights. Keyed ``strategy/currency``.
    Returns the number of rows written.
    """
    try:
        from state_store import StateStore
    except Exception as e:  # pragma: no cover - defensive
        log.warning("bt_cache write skipped (state_store unavailable): %s", e)
        return 0
    store = StateStore(db_path=db_path)
    n = 0
    for v in verdicts:
        if only_passed and not getattr(v, "passed", False):
            continue
        key = f"{v.strategy}/{v.currency}"
        try:
            store.save_bt_cache(key, v)
            n += 1
        except Exception as e:  # pragma: no cover - defensive
            log.debug("bt_cache save failed for %s: %s", key, e)
    return n


def run(args) -> Dict[str, Any]:
    if args.strategies:
        strategies = [s.strip() for s in args.strategies.split(",")]
    elif args.include_external:
        # All 83 strategies, including the 9 external-data / order-flow ones
        # that use the slower Python walk-forward backtester.
        strategies = list(S.ALL_STRATEGIES.keys())
    else:
        # Default: Rust-native strategies only (fast walk-forward in Rust).
        strategies = [s for s in S.ALL_STRATEGIES.keys() if s in S._RUST_STRATEGIES]
    symbols = _list_symbols(args.granularity,
                            [s.strip() for s in args.symbols.split(",")] if args.symbols else None)
    log.info("Backtesting %d symbols x %d strategies @ %ds", len(symbols), len(strategies), args.granularity)
    all_rows: List[Dict[str, Any]] = []
    all_verdicts = []
    for sym in symbols:
        rows, verdicts = _run(sym, args.granularity, strategies, args.warmup, args.min_trades, args.max_bars)
        all_rows.extend(rows)
        all_verdicts.extend(verdicts)
    if args.bt_cache_db:
        _write_bt_cache(args.bt_cache_db, all_verdicts, args.only_passed)
        log.info("Wrote %d verdicts to bt_cache db %s", len(all_verdicts), args.bt_cache_db)

    passed = [r for r in all_rows if r["passed"]]
    # Rank passing strategies by sharpe then profit factor.
    passed.sort(key=lambda r: (r["sharpe"], r["profit_factor"]), reverse=True)
    all_rows.sort(key=lambda r: (r["passed"], r["sharpe"], r["profit_factor"]), reverse=True)

    summary = {
        "granularity": args.granularity,
        "symbols": symbols,
        "n_symbols": len(symbols),
        "n_strategies_tested": len(strategies),
        "n_signals": len(all_rows),
        "n_passed": len(passed),
        "pass_rate": round(len(passed) / len(all_rows), 4) if all_rows else 0.0,
        "results": all_rows,
    }
    return summary


def cli():
    p = argparse.ArgumentParser(description="Backtest strategies against harvested feed_cache data")
    p.add_argument("--granularity", type=int, default=3600, help="candle granularity (seconds)")
    p.add_argument("--symbols", default="", help="comma-separated product ids (default: all in cache)")
    p.add_argument("--strategies", default="", help="comma-separated strategy names (default: all)")
    p.add_argument("--warmup", type=int, default=30, help="warmup bars before first signal")
    p.add_argument("--min-trades", type=int, default=3, dest="min_trades", help="min trades to score")
    p.add_argument("--max-bars", type=int, default=5000, dest="max_bars",
                   help="cap candles to the trailing N bars for fast, meaningful validation")
    p.add_argument("--include-external", action="store_true",
                   help="also backtest the 9 external-data/order-flow strategies (slower Python path)")
    p.add_argument("--bt-cache-db", default="",
                   help="write verdicts into this optimizer StateStore db (e.g. optimizer_state.db) "
                        "so ConfidenceMatrix uses them as bt_weights")
    p.add_argument("--only-passed", action="store_true",
                   help="with --bt-cache-db, only persist strategies that passed backtest")
    p.add_argument("--json", default="", help="write full JSON report to this path")
    p.add_argument("--top", type=int, default=25, help="print top N passing strategies")
    args = p.parse_args()

    summary = run(args)

    print(f"\n=== Backtest report ({summary['granularity']}s) ===")
    print(f"symbols={summary['n_symbols']} strategies={summary['n_strategies_tested']} "
          f"signals={summary['n_signals']} passed={summary['n_passed']} "
          f"pass_rate={summary['pass_rate']:.1%}")
    print(f"\nTop {args.top} passing strategies (sharpe desc):")
    ranked = [r for r in summary["results"] if r["passed"]][:args.top]
    for r in ranked:
        print(f"  {r['symbol']:10s} {r['strategy']:18s} "
              f"wr={r['win_rate']:.0%} ret={r['total_return_pct']:+.1f}% "
              f"sharpe={r['sharpe']:.2f} pf={r['profit_factor']:.2f} "
              f"mdd={r['max_drawdown_pct']:.1f}%")

    if args.json:
        with open(args.json, "w") as f:
            json.dump(summary, f, indent=2)
        log.info("Wrote JSON report -> %s", args.json)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cli()
