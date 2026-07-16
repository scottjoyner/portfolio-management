"""Standalone walk-forward backtest harness for the 12 new trading strategies.

This script does NOT use the heavy Coinbase backtester. It calls each new
strategy's `generate_signal(market_state)` directly on historical OHLCV and
simulates a simple next-bar exit position.

Pure stdlib only: random, csv, argparse, sys.
"""
from __future__ import annotations

import argparse
import csv
import random
import sys

from trading_system.strategies.registry.registry import load_strategies

TARGET_IDS = [
    "BollingerBandReversionStrategy",
    "RsiBounceReversionStrategy",
    "DonchianMeanReversionStrategy",
    "EmaMacdMomentumStrategy",
    "AdxDiStrengthStrategy",
    "AroonBreakoutMomentumStrategy",
    "KeltnerVolBreakoutStrategy",
    "BollingerSqueezeVolExpansionStrategy",
    "DonchianChoppinessVolBreakoutStrategy",
    "TradeFlowImbalanceStrategy",
    "SpreadCompressionStrategy",
    "CvdExhaustionStrategy",
]


def _load_target_strategies():
    all_strategies = load_strategies()
    by_id = {}
    for s in all_strategies:
        by_id[getattr(s, "strategy_id", None)] = s
    selected = []
    for sid in TARGET_IDS:
        if sid in by_id:
            selected.append(by_id[sid])
        else:
            sys.stderr.write(f"WARNING: strategy id {sid} not found in registry\n")
    return selected


def _build_market_state(i, closes, highs, lows, volumes, product_id):
    close = closes[i]
    ohlc_history = [
        {
            "close": closes[j],
            "high": highs[j] if highs else closes[j],
            "low": lows[j] if lows else closes[j],
            "volume": volumes[j] if volumes else 0.0,
            "open": closes[max(0, j - 1)],
        }
        for j in range(i + 1)
    ]
    return {
        "product_id": product_id,
        "currency": product_id.replace("-USD", ""),
        "close": close,
        "closes": closes[: i + 1],
        "highs": highs[: i + 1] if highs else [],
        "lows": lows[: i + 1] if lows else [],
        "volumes": volumes[: i + 1] if volumes else [],
        "open": closes[0],
        "price": close,
        "score": 0.0,
        "warmup_complete": True,
        "ohlc_history": ohlc_history,
        "best_bid": close * 0.999,
        "best_ask": close * 1.001,
        "mid_price": close,
        "spread_bps": 2.0,
        "bid_volume": 1.0,
        "ask_volume": 1.0,
        "trade_flow_imbalance": 0.0,
        "imbalance": 0.0,
        "cumulative_delta": 0.0,
        "baseline_spread_bps": 2.0,
        "book_pressure": 0.0,
        "buy_volume": 1.0,
        "sell_volume": 1.0,
        "delta_scale": 1.0,
    }


def run_walkforward(strategy, closes, highs=None, lows=None, volumes=None,
                    product_id="BTC-USD", initial_capital=1000.0):
    if highs is None:
        highs = []
    if lows is None:
        lows = []
    if volumes is None:
        volumes = []
    if not closes or len(closes) < 2:
        return _empty_metrics()

    warmup = getattr(getattr(strategy, "config", None), "warmup_period", 0)
    if not warmup or warmup <= 0:
        warmup = 30
    warmup = min(warmup, len(closes) - 1)

    trades = 0
    wins = 0
    gross_profit = 0.0
    gross_loss = 0.0
    total_return_pct = 0.0
    equity = initial_capital
    peak_equity = initial_capital
    max_drawdown_pct = 0.0

    for i in range(warmup, len(closes) - 1):
        market_state = _build_market_state(i, closes, highs, lows, volumes, product_id)
        try:
            sig = strategy.generate_signal(market_state)
        except Exception:
            sig = None
        if not sig or abs(sig.score) <= 0:
            continue

        direction_sign = 1.0 if sig.score > 0 else -1.0
        entry = closes[i]
        exit_price = closes[i + 1]
        if entry <= 0:
            continue
        pnl = direction_sign * (exit_price - entry) / entry * initial_capital
        trades += 1
        if pnl > 0:
            wins += 1
            gross_profit += pnl
        else:
            gross_loss += -pnl
        total_return_pct += pnl / initial_capital * 100.0
        equity += pnl
        if equity > peak_equity:
            peak_equity = equity
        if peak_equity > 0:
            dd = (peak_equity - equity) / peak_equity * 100.0
            if dd > max_drawdown_pct:
                max_drawdown_pct = dd

    win_rate = (wins / trades * 100.0) if trades else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    passed = bool(
        trades > 0
        and win_rate >= 40.0
        and profit_factor > 1.0
        and total_return_pct > -20.0
    )

    return {
        "strategy_id": getattr(strategy, "strategy_id", ""),
        "total_trades": trades,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "total_return_pct": total_return_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "passed": passed,
    }


def _empty_metrics():
    return {
        "strategy_id": "",
        "total_trades": 0,
        "win_rate": 0.0,
        "profit_factor": 0.0,
        "total_return_pct": 0.0,
        "max_drawdown_pct": 0.0,
        "passed": False,
    }


def _gen_synthetic(n, seed=42):
    rng = random.Random(seed)
    price = 100.0
    closes, highs, lows, volumes = [], [], [], []
    for _ in range(n):
        drift = rng.gauss(0.0, 1.0)
        price = max(1.0, price * (1.0 + drift * 0.01))
        hi = price * (1.0 + abs(rng.gauss(0, 0.005)))
        lo = price * (1.0 - abs(rng.gauss(0, 0.005)))
        vol = rng.uniform(100.0, 1000.0)
        closes.append(price)
        highs.append(hi)
        lows.append(lo)
        volumes.append(vol)
    return closes, highs, lows, volumes


def _load_csv(path):
    closes, highs, lows, volumes = [], [], [], []
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            closes.append(float(row["close"]))
            highs.append(float(row.get("high", row["close"])))
            lows.append(float(row.get("low", row["close"])))
            volumes.append(float(row.get("volume", 0.0)))
    return closes, highs, lows, volumes


def _print_table(results):
    header = f"{'strategy_id':<38} {'trades':>6} {'win%':>7} {'pf':>8} {'ret%':>9} {'pass':>5}"
    print(header)
    print("-" * len(header))
    for r in results:
        pf = r["profit_factor"]
        pf_s = "inf" if pf == float("inf") else f"{pf:>8.2f}"
        print(
            f"{r['strategy_id']:<38} {r['total_trades']:>6} "
            f"{r['win_rate']:>7.1f} {pf_s} {r['total_return_pct']:>9.2f} "
            f"{'Y' if r['passed'] else 'N':>5}"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description="Walk-forward backtest for the 12 new strategies.")
    parser.add_argument("--product", default="BTC-USD")
    parser.add_argument("--bars", type=int, default=200)
    parser.add_argument("--csv", default=None, help="CSV with close,high,low,volume columns")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    if args.csv:
        closes, highs, lows, volumes = _load_csv(args.csv)
    else:
        closes, highs, lows, volumes = _gen_synthetic(args.bars, seed=args.seed)

    strategies = _load_target_strategies()
    results = []
    for s in strategies:
        metrics = run_walkforward(s, closes, highs, lows, volumes, product_id=args.product)
        results.append(metrics)

    _print_table(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
