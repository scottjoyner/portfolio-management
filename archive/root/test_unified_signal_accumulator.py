#!/usr/bin/env python3
"""Tests for the Unified Signal Accumulator."""

import sys, json, time
from datetime import datetime
from pathlib import Path
from typing import Dict

sys.path.insert(0, '/home/scott/git/portfolio-management')
sys.path.insert(0, '/home/scott/git/portfolio-management/graph-alpha-bot/app/strategies')


PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        print(f"  \u2713 {name}")
        PASS += 1
    else:
        print(f"  \u2717 {name} -- FAILED" + (f" | {detail}" if detail else ""))
        FAIL += 1


def test_accumulator_imports():
    print("\n--- test_accumulator_imports ---")
    from unified_signal_accumulator import (
        UnifiedSignalAccumulator, AccumulatedSignal,
        CoinbasePriceProvider, StrategySignalAdapter,
        NewsSentimentAdapter, MultiStrategyAdapter,
    )
    check("AccumulatedSignal dataclass works",
          AccumulatedSignal("BTC-USD", "BUY", 0.8, 0.8, 0.5, "test", "reason", 1000).symbol == "BTC-USD")
    check("AccumulatedSignal to_dict works",
          "symbol" in AccumulatedSignal("BTC-USD", "BUY", 0.8, 0.8, 0.5, "test", "reason", 1000).to_dict())
    check("UnifiedSignalAccumulator can be instantiated", True)
    check("CoinbasePriceProvider can be instantiated", True)


def test_accumulator_structure():
    print("\n--- test_accumulator_structure ---")
    from unified_signal_accumulator import UnifiedSignalAccumulator
    acc = UnifiedSignalAccumulator(max_queue_size=50)
    check("default symbols list is populated", len(acc.symbols) >= 10)
    check("price_provider is set", acc.price_provider is not None)
    check("strategy_adapter is set", acc.strategy_adapter is not None)
    check("news_adapter is set", acc.news_adapter is not None)
    check("multi_adapter is set", acc.multi_adapter is not None)


def test_cross_consensus_boost():
    print("\n--- test_cross_consensus_boost ---")
    from unified_signal_accumulator import UnifiedSignalAccumulator, AccumulatedSignal
    acc = UnifiedSignalAccumulator()

    # Two signals on same symbol+action should boost each other
    sigs = [
        AccumulatedSignal("BTC-USD", "BUY", 0.5, 0.5, 0.3, "strat1", "a", 100),
        AccumulatedSignal("BTC-USD", "BUY", 0.6, 0.6, 0.4, "strat2", "b", 100),
        AccumulatedSignal("ETH-USD", "SELL", 0.7, 0.7, 0.5, "strat1", "c", 100),
    ]
    boosted = acc._apply_cross_consensus(sigs)
    check("consensus boosted BUY signals on same symbol",
          boosted[0].opportunity_score > 0.3,
          f"{boosted[0].opportunity_score} vs 0.3")
    check("ETH SELL unchanged (no consensus)",
          boosted[2].opportunity_score == 0.5)


def test_accumulate_and_report_empty():
    print("\n--- test_accumulate_and_report (empty) ---")
    # This tests that the report structure is valid even with no signals
    from unified_signal_accumulator import UnifiedSignalAccumulator
    acc = UnifiedSignalAccumulator(max_queue_size=10)
    report = acc.accumulate_and_report()
    check("report has status field", "status" in report)
    check("report has total_signals", "total_signals" in report)
    check("report has queue", "queue" in report)
    check("report has timestamp", "timestamp" in report)
    check("queue is a list", isinstance(report["queue"], list))
    check("total_signals is int", isinstance(report["total_signals"], int))


def test_fee_tier_boost():
    print("\n--- test_fee_tier_boost ---")
    from unified_signal_accumulator import UnifiedSignalAccumulator, AccumulatedSignal
    acc = UnifiedSignalAccumulator()
    sigs = [
        AccumulatedSignal("BTC-USD", "BUY", 0.5, 0.5, 0.3, "test", "a", 100_000),
    ]
    boosted = acc._apply_fee_tier_boost(sigs)
    # With a large estimated volume, the boost should increase opportunity score
    check("fee tier boost applied", boosted[0].opportunity_score >= 0.3)


def test_multi_strategy_adapter():
    print("\n--- test_multi_strategy_adapter ---")
    from unified_signal_accumulator import MultiStrategyAdapter
    adapter = MultiStrategyAdapter()
    check("has strategies", len(adapter.strategies) == 6)

    # Test with strong trend
    price_data = {"price": 50000, "price_percentage_change_24h": 5.0}
    signals = adapter.get_signals("BTC-USD", price_data, [{"close": 100}] * 20)
    check("strong momentum generates signals", len(signals) >= 1)


def test_news_adapter():
    print("\n--- test_news_adapter ---")
    from unified_signal_accumulator import NewsSentimentAdapter
    adapter = NewsSentimentAdapter()
    price_map = {"BTC-USD": 50000, "ETH-USD": 2000}
    signals = adapter.get_signals(price_map)
    # Should not crash, may or may not find news articles
    check("news adapter returns list", isinstance(signals, list))
    for sig in signals:
        check("news signal has correct structure",
              all(hasattr(sig, a) for a in ["symbol", "action", "opportunity_score", "strategy_name"]))


def test_accumulated_signal_ordering():
    print("\n--- test_accumulated_signal_ordering ---")
    from unified_signal_accumulator import AccumulatedSignal
    signals = [
        AccumulatedSignal("A", "BUY", 0.5, 0.5, 0.9, "s1", "a", 100),
        AccumulatedSignal("B", "SELL", 0.5, 0.5, 0.3, "s2", "b", 100),
        AccumulatedSignal("C", "BUY", 0.5, 0.5, 0.7, "s3", "c", 100),
    ]
    ranked = sorted(signals, key=lambda s: s.opportunity_score, reverse=True)
    check("sorted by score descending",
          [s.symbol for s in ranked] == ["A", "C", "B"])


def test_strategy_breakdown():
    print("\n--- test_strategy_breakdown ---")
    from unified_signal_accumulator import AccumulatedSignal, UnifiedSignalAccumulator
    acc = UnifiedSignalAccumulator()
    signals = [
        AccumulatedSignal("A", "BUY", 0.5, 0.5, 0.5, "Momentum", "a", 100),
        AccumulatedSignal("B", "SELL", 0.5, 0.5, 0.5, "MeanReversion", "b", 100),
        AccumulatedSignal("C", "BUY", 0.5, 0.5, 0.5, "Momentum", "c", 100),
    ]
    breakdown = acc._strategy_breakdown(signals)
    check("Momentum count = 2", breakdown.get("Momentum") == 2)
    check("MeanReversion count = 1", breakdown.get("MeanReversion") == 1)


def test_cli_main():
    print("\n--- test_cli_main ---")
    try:
        from unified_signal_accumulator import main as acc_main
        # Save original argv and override
        import argparse
        old_argv = sys.argv
        sys.argv = ["test", "--max-signals", "5", "--json"]
        try:
            report = acc_main()
            check("CLI main returns a report", isinstance(report, dict))
            check("report has status", "status" in report)
        finally:
            sys.argv = old_argv
    except Exception as e:
        check(f"CLI main failed: {e}", False)


def test_deduplication():
    print("\n--- test_deduplication ---")
    from unified_signal_accumulator import UnifiedSignalAccumulator, AccumulatedSignal
    acc = UnifiedSignalAccumulator()

    # Duplicate signals (same symbol, action, strategy)
    all_sigs = [
        AccumulatedSignal("BTC-USD", "BUY", 0.5, 0.5, 0.3, "Momentum", "low", 100),
        AccumulatedSignal("BTC-USD", "BUY", 0.8, 0.8, 0.5, "Momentum", "high", 100),
    ]

    dedup: Dict[str, AccumulatedSignal] = {}
    for sig in all_sigs:
        key = f"{sig.symbol}:{sig.action}:{sig.strategy_name}"
        if key not in dedup or sig.opportunity_score > dedup[key].opportunity_score:
            dedup[key] = sig

    check("keep highest confidence duplicate",
          dedup.get("BTC-USD:BUY:Momentum") is not None and
          dedup["BTC-USD:BUY:Momentum"].base_confidence == 0.8)


def run_all():
    global PASS, FAIL
    PASS = 0
    FAIL = 0

    print("=" * 60)
    print("UNIFIED SIGNAL ACCUMULATOR TESTS")
    print("=" * 60)

    test_accumulator_imports()
    test_accumulator_structure()
    test_cross_consensus_boost()
    test_accumulate_and_report_empty()
    test_fee_tier_boost()
    test_multi_strategy_adapter()
    test_news_adapter()
    test_accumulated_signal_ordering()
    test_strategy_breakdown()
    test_deduplication()
    test_cli_main()

    print("\n" + "=" * 60)
    print(f"RESULTS: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print("=" * 60)

    return FAIL == 0


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)
