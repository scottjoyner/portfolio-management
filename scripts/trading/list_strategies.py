#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from trading_system.catalog.strategy_registry import list_all_phase1_strategies
from trading_system.core.runtime.events import EventRecorder
from trading_system.core.runtime.models import TradingEvent


def load_strategy_catalog(category: str | None = None) -> List[Dict[str, Any]]:
    strategies = list_all_phase1_strategies()
    if category:
        strategies = [strategy for strategy in strategies if strategy.get("category") == category]
    return strategies


def print_table(strategies: List[Dict[str, Any]]) -> None:
    print("Strategy Catalog")
    print("================")
    print(f"Count: {len(strategies)}")
    print()
    for strategy in strategies:
        print(f"{strategy.get('name', ''):<24} {strategy.get('category', ''):<18} {strategy.get('status', '')}")
        print(f"  {strategy.get('description', '')}")


def main() -> int:
    parser = argparse.ArgumentParser(description="List registered trading strategies.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON")
    parser.add_argument("--category", help="Filter by category, e.g. trend_following")
    args = parser.parse_args()

    strategies = load_strategy_catalog(args.category)
    EventRecorder().record(
        TradingEvent(
            source="script.list_strategies",
            event_type="strategy_catalog_listed",
            payload={"category": args.category, "count": len(strategies)},
        )
    )
    payload = {"count": len(strategies), "strategies": strategies}
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_table(strategies)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
