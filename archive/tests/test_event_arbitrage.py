#!/usr/bin/env python3
"""Regression tests for cross-platform event arbitrage scanning."""

from __future__ import annotations

import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from event_markets.arbitrage import EventArbitrageScanner
from event_markets.unified_client import PredictionMarket


class EventArbitrageScannerTest(unittest.TestCase):
    def test_detects_cross_platform_arb(self):
        scanner = EventArbitrageScanner(min_edge=0.01, fee_buffer=0.01, min_volume=100)
        markets = [
            PredictionMarket(
                platform="kalshi",
                market_id="K1",
                question="Will Bitcoin be above $100k by Dec 31?",
                outcomes=["YES", "NO"],
                outcome_prices={"YES": 0.42, "NO": 0.58},
                volume=50000,
                end_date="2026-12-31T00:00:00Z",
                is_open=True,
                yes_bid=0.41,
                yes_ask=0.43,
                spread=0.02,
                liquidity_score=0.9,
                category="crypto",
            ),
            PredictionMarket(
                platform="polymarket",
                market_id="P1",
                question="Will BTC trade above 100k by December 31?",
                outcomes=["YES", "NO"],
                outcome_prices={"YES": 0.61, "NO": 0.39},
                volume=60000,
                end_date="2026-12-31T00:00:00Z",
                is_open=True,
                yes_bid=0.60,
                yes_ask=0.62,
                spread=0.02,
                liquidity_score=0.9,
                category="crypto",
            ),
        ]

        arbs = scanner.scan_markets(markets)
        self.assertTrue(arbs)
        arb = arbs[0]
        self.assertEqual(arb.platform_buy, "kalshi")
        self.assertEqual(arb.platform_hedge, "polymarket")
        self.assertGreater(arb.edge, 0)
        self.assertIn("locked edge", arb.reason)


if __name__ == "__main__":
    unittest.main()
