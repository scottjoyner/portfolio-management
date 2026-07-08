#!/usr/bin/env python3
"""Unified backtesting CLI — runs strategy_engine.py + niche strategies
through EnhancedBacktestEngine with short selling and futures support.

Examples:
  python3 -m coinbase.src.backtest.run_backtest_v2 --products BTC-USD,ETH-USD --short --futures
  python3 -m coinbase.src.backtest.run_backtest_v2 --strategies ema_cross,rsi_revert --dry
  python3 -m coinbase.src.backtest.run_backtest_v2 --list-strategies
"""
from __future__ import annotations
import argparse
import json
import sys
from typing import List, Optional

try:
    from ..cb_client import CBClient
except Exception:
    from src.cb_client import CBClient

from ..protocols import Direction
from .engine_v2 import EnhancedBacktestEngine, BacktestConfig, DataPortalV2
from .niche_adapter import wrap_all_niche_strategies, NicheStrategyWrapper
from ..opportunity_scanner import (
    OpportunityScanner, StrategyEngineAdapter, AlphaSetupAdapter,
    FuturesSignalAdapter, ScannerConfig,
)
from ..fill_model import AdaptiveFillModel


def list_available_strategies():
    strategies = {
        "alpha_setup": "AlphaSetupAdapter (7 bracket strategies: donchian, rsi pullback, etc.)",
        "ema_cross": "EMA Crossover trend",
        "rsi_revert": "RSI mean reversion",
        "boll_break": "Bollinger Band breakout",
        "zscore_revert": "Z-score mean reversion",
        "vol_mom": "Volume momentum",
        "macd": "MACD crossover",
        "vwap_revert": "VWAP mean reversion",
        "obv_div": "OBV divergence",
        "cmo": "Chande Momentum Oscillator",
        "trix": "Triple EMA",
        "adx": "ADX trend strength",
        "keltner": "Keltner Channels",
        "chaikin_mf": "Chaikin Money Flow",
        "williams_r": "Williams %R",
        "psar": "Parabolic SAR",
        "hma": "Hull MA",
        "force_idx": "Force Index",
        "vpt": "Volume Price Trend",
        "donchian": "Donchian Channels",
        "aroon": "Aroon indicator",
        "futures_signal": "Futures/perps momentum signal",
    }
    print("Available strategies for --strategies:")
    print("=" * 70)
    for name, desc in strategies.items():
        print(f"  {name:20s}  {desc}")
    print()
    print("Niche strategies (always included):")
    for n in [
        "MultiTimeframeRSIMomentumStrategy",
        "BollingerSqueezeBreakoutStrategy",
        "RegimeAwareAdaptiveStrategy",
        "AnchoredVWAPMeanReversionStrategy",
        "LiquidityVacuumReversalStrategy",
        "DonchianPullbackContinuationStrategy",
        "RSIFailureSwingReversalStrategy",
        "VolatilityCompressionBreakoutStrategy",
        "ImpulseExhaustionReversalStrategy",
        "VolRegimeSwitchStrategy",
        "SentimentMomentumCompositeStrategy",
        "OnChainRegimeWhaleFlowStrategy",
    ]:
        print(f"  {n}")


def run_backtest_v2(args):
    cb = CBClient()
    products = [p.strip() for p in args.products.split(",")]

    portal = DataPortalV2(
        cb, products, args.granularity,
        args.lookback_days, args.start, args.end,
    )

    cfg = BacktestConfig(
        initial_cash=args.cash,
        risk_per_trade=args.risk,
        max_positions=args.max_positions,
        min_notional=args.min_notional,
        enable_short=args.short,
        enable_futures=args.futures,
        max_leverage=args.leverage,
        fee_bps=args.fee_bps,
    )

    engine = EnhancedBacktestEngine(portal, cfg)

    strategies = []

    if args.strategies:
        strategy_keys = [s.strip() for s in args.strategies.split(",")]
        for key in strategy_keys:
            if key == "alpha_setup":
                strategies.append(AlphaSetupAdapter())
            elif key == "futures_signal":
                strategies.append(FuturesSignalAdapter())
            else:
                strategies.append(StrategyEngineAdapter(key))
    else:
        strategies = [
            AlphaSetupAdapter(),
            StrategyEngineAdapter("ema_cross"),
            StrategyEngineAdapter("rsi_revert"),
            StrategyEngineAdapter("boll_break"),
            StrategyEngineAdapter("donchian"),
            StrategyEngineAdapter("macd"),
            StrategyEngineAdapter("adx"),
        ]
        if args.futures:
            strategies.append(FuturesSignalAdapter())

    if not args.no_niche:
        strategies.extend(wrap_all_niche_strategies())

    results = engine.run(strategies, warmup=args.warmup)

    metrics = results["metrics"]
    trades = results["trades"]
    eq = results["equity_curve"]

    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"Products: {', '.join(products)}")
    print(f"Short selling: {'ON' if args.short else 'OFF'}")
    print(f"Futures/Perps: {'ON' if args.futures else 'OFF'}")
    print(f"Initial cash: ${args.cash:,.2f}")
    print(f"Max leverage: {args.leverage}x")
    print("-" * 60)
    print(f"Total Return: {metrics['TotalReturn']*100:+.2f}%")
    print(f"CAGR:         {metrics['CAGR']*100:+.2f}%")
    print(f"Max DD:       {metrics['MaxDrawdown']*100:.2f}%")
    print(f"Sharpe:       {metrics['Sharpe']:.3f}")
    print(f"Sortino:      {metrics['Sortino']:.3f}")
    print(f"Calmar:       {metrics['Calmar']:.3f}")
    print("-" * 60)

    if isinstance(trades, list):
        n_trades = len(trades)
    else:
        n_trades = len(trades) if hasattr(trades, '__len__') else 0
    print(f"Total trades: {n_trades}")
    print(f"Final equity: ${eq.iloc[-1]:,.2f}" if len(eq) > 1 else "")

    if args.output:
        output = {
            "metrics": metrics,
            "config": {
                "products": products,
                "short": args.short,
                "futures": args.futures,
                "leverage": args.leverage,
                "strategies": args.strategies or "defaults",
            },
            "num_trades": n_trades,
            "final_equity": float(eq.iloc[-1]) if len(eq) > 1 else 0.0,
        }
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        print(f"\nResults saved to {args.output}")

    return results


def main():
    ap = argparse.ArgumentParser(
        description="Unified backtester with short + futures support"
    )
    ap.add_argument("--products", default="BTC-USD,ETH-USD",
                    help="Comma-separated product IDs")
    ap.add_argument("--strategies", default="",
                    help="Comma-separated strategy names (see --list-strategies)")
    ap.add_argument("--granularity", default="ONE_HOUR",
                    choices=["ONE_MINUTE", "FIVE_MINUTE", "FIFTEEN_MINUTE",
                             "THIRTY_MINUTE", "ONE_HOUR", "FOUR_HOUR", "ONE_DAY"])
    ap.add_argument("--lookback-days", type=int, default=240)
    ap.add_argument("--start", default=None, help="Start date (YYYY-MM-DD)")
    ap.add_argument("--end", default=None, help="End date (YYYY-MM-DD)")
    ap.add_argument("--cash", type=float, default=10_000.0)
    ap.add_argument("--risk", type=float, default=0.01)
    ap.add_argument("--max-positions", type=int, default=12)
    ap.add_argument("--min-notional", type=float, default=25.0)
    ap.add_argument("--leverage", type=float, default=3.0)
    ap.add_argument("--fee-bps", type=float, default=8.0)
    ap.add_argument("--warmup", type=int, default=200)
    ap.add_argument("--short", action="store_true", help="Enable short selling")
    ap.add_argument("--futures", action="store_true", help="Enable futures/perps")
    ap.add_argument("--no-niche", action="store_true", help="Skip niche strategies")
    ap.add_argument("--list-strategies", action="store_true",
                    help="List available strategies and exit")
    ap.add_argument("--dry", action="store_true",
                    help="Dry run — print config, no backtest")
    ap.add_argument("--output", default="", help="Save results to JSON file")

    args = ap.parse_args()

    if args.list_strategies:
        list_available_strategies()
        return

    if args.dry:
        print("DRY RUN: Configuration")
        print(f"  Products: {args.products}")
        print(f"  Short: {args.short}, Futures: {args.futures}")
        print(f"  Leverage: {args.leverage}x")
        print(f"  Cash: ${args.cash:,.2f}")
        print(f"  Strategies: {args.strategies or 'defaults'}")
        print(f"  Granularity: {args.granularity}")
        return

    run_backtest_v2(args)


if __name__ == "__main__":
    main()
