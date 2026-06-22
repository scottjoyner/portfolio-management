#!/usr/bin/env python3
"""Live Coinbase paper trading runner.

This is the production-facing entrypoint for the paper portfolio loop.
It uses the multi-strategy portfolio engine, persistent state, rolling
market history, and Coinbase CLI-backed market data.
"""

from __future__ import annotations

import argparse
import asyncio

from multi_strategy_paper_trading import MultiStrategyPaperTrading


def main() -> None:
    parser = argparse.ArgumentParser(description="Live Coinbase paper trading")
    parser.add_argument("--hours", type=float, default=24.0, help="Run duration in hours")
    parser.add_argument("--interval", type=int, default=60, help="Polling interval in seconds")
    parser.add_argument("--state-db", default="paper_trading_state.db", help="SQLite state database")
    parser.add_argument("--pairs", type=int, default=0, help="Max Coinbase pairs to track (0 = all)")
    parser.add_argument("--lookback-days", type=int, default=10, help="Historical lookback for seeding")
    parser.add_argument("--granularity", default="ONE_HOUR", help="Candle granularity for history seeding")
    args = parser.parse_args()

    engine = MultiStrategyPaperTrading(
        initial_capital=10000.0,
        state_db=args.state_db,
        history_lookback_days=args.lookback_days,
        max_pairs=args.pairs,
        granularity=args.granularity,
    )

    asyncio.run(engine.run_benchmark(duration_hours=args.hours, poll_interval=args.interval))


if __name__ == "__main__":
    main()
